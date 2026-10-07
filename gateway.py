#!/usr/bin/env python3
"""One address in front of every instance container.

The gateway runs no instance of its own. It reads the instances from
config/peers.toml, remembers the one the browser picked in a cookie, and
forwards every request and websocket to that instance's container, so a page,
an action and a terminal all run where the instance lives. The gateway adds
its instance picker to every HTML page it forwards, so the picker works in
front of an instance that runs older code.

    python gateway.py --port 7130
"""
import argparse
import asyncio
import json
import re
from pathlib import Path
from urllib.parse import quote, unquote, unquote_plus, urlsplit

import httpx
import uvicorn
import websockets
from fastapi import FastAPI, Request, WebSocket
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response, StreamingResponse
from starlette.background import BackgroundTask
from starlette.websockets import WebSocketDisconnect

from services import work_peers

COOKIE = "frshty_instance"
PICKER = Path(__file__).resolve().parent / "static" / "frshty-gateway-picker.js"
PICKER_TAG = b'<script src="/api/gateway/picker.js"></script>'
PIN = Path(__file__).resolve().parent / "static" / "frshty-gateway-pin.js"
PIN_TAG = b'<script src="/api/gateway/pin.js"></script>'
AT_PREFIX = "/api/gateway/at/"
IN_PREFIX = "/api/gateway/in/"
SANDBOX = re.compile(r"(^|;)\s*sandbox(\s|;|$)", re.IGNORECASE)
HOP_HEADERS = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te",
               "trailers", "transfer-encoding", "upgrade", "host", "content-length"}

app = FastAPI()
client = httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=10.0), follow_redirects=False)


def instances() -> list[dict]:
    return work_peers.peers()


def selected(cookie: str | None) -> dict | None:
    items = instances()
    for item in items:
        if item["key"] == cookie:
            return item
    return items[0] if items else None


def split_pin(query: str) -> tuple[str, str]:
    """A tab pins its instance with ?frshty_instance=<key>. The instance never
    sees that parameter, so it is cut from the query that goes upstream."""
    pin, kept = "", []
    for part in query.split("&") if query else []:
        name, _, value = part.partition("=")
        if unquote_plus(name) == COOKIE:
            pin = unquote_plus(value)
        else:
            kept.append(part)
    return pin, "&".join(kept)


def with_pin(url: str, pin: str) -> str:
    if not pin:
        return url
    head, hash_, fragment = url.partition("#")
    return head + ("&" if "?" in head else "?") + f"{COOKIE}={quote(pin, safe='')}" + hash_ + fragment


def referer_pin(headers) -> str:
    """Every request a pinned page makes names that page as its referer, so an
    iframe, an image or a navigation the page starts stays on its instance."""
    return split_pin(urlsplit(headers.get("referer", "")).query)[0]


def routed(cookie: str | None, pin: str) -> dict | None:
    """A pinned tab goes to its own instance and leaves the cookie alone, so a
    peer page opened from the board does not move the board to that peer."""
    if pin:
        return next((i for i in instances() if i["key"] == pin), None)
    return selected(cookie)


@app.get("/api/gateway/instances")
def api_instances(request: Request):
    current = routed(request.cookies.get(COOKIE), request.query_params.get(COOKIE, ""))
    return {"current": current["key"] if current else "",
            "instances": [{"key": i["key"], "label": i["label"]} for i in instances()]}


@app.get("/api/gateway/picker.js")
def api_picker():
    return FileResponse(PICKER, media_type="application/javascript",
                        headers={"Cache-Control": "no-cache"})


@app.get("/api/gateway/pin.js")
def api_pin():
    return FileResponse(PIN, media_type="application/javascript",
                        headers={"Cache-Control": "no-cache"})


def with_picker(html: bytes, pin: str = "") -> bytes:
    at = html.rfind(b"</body>")
    html = html[:at] + PICKER_TAG + html[at:] if at >= 0 else html + PICKER_TAG
    if not pin:
        return html
    head = re.search(rb"<head(\s[^>]*)?>", html, re.IGNORECASE)
    return html[:head.end()] + PIN_TAG + html[head.end():] if head else PIN_TAG + html


def with_gateway_peers(body: bytes) -> bytes:
    """The board links a peer's pages and files at the peer's base_url. That
    address is container to container, so the browser gets a gateway address
    for every peer the gateway forwards to."""
    data = json.loads(body)
    keys = {i["key"] for i in instances()}
    for peer in data.get("peers") or []:
        if isinstance(peer, dict) and peer.get("key") in keys:
            peer["base_url"] = AT_PREFIX + quote(peer["key"], safe="")
    return json.dumps(data).encode()


def with_gateway_events(body: bytes) -> bytes:
    """The global feed links each event at its instance's base_url, which the
    browser cannot reach, so every instance the gateway forwards to gets a
    gateway address."""
    data = json.loads(body)
    keys = {i["key"] for i in instances()}
    for event in data.get("events") or []:
        if isinstance(event, dict) and event.get("instance_key") in keys:
            event["base_url"] = AT_PREFIX + quote(event["instance_key"], safe="")
    return json.dumps(data).encode()


@app.get("/api/gateway/select")
def api_select(key: str, next: str = "/"):
    if not any(i["key"] == key for i in instances()):
        return JSONResponse({"error": f"unknown instance '{key}'"}, status_code=404)
    target = next if next.startswith("/") and not next.startswith("//") else "/"
    response = RedirectResponse(target, status_code=303, headers={"Referrer-Policy": "no-referrer"})
    response.set_cookie(COOKIE, key, max_age=365 * 86400, samesite="lax")
    return response


def raw_path(scope) -> str:
    """The path as the browser sent it, still percent-encoded, so an encoded
    ? or # stays part of the path upstream."""
    raw = scope.get("raw_path")
    return raw.decode("latin-1") if raw else quote(scope["path"])


@app.get("/api/gateway/at/{key}")
@app.get("/api/gateway/at/{key}/{path:path}")
def api_at(key: str, request: Request, path: str = ""):
    """Open a path on one instance in this tab only."""
    if not any(i["key"] == key for i in instances()):
        return JSONResponse({"error": f"unknown instance '{key}'"}, status_code=404)
    rest = "/".join(raw_path(request.scope).split("/")[5:])
    _, query = split_pin(request.scope.get("query_string", b"").decode("latin-1"))
    return RedirectResponse(with_pin("/" + rest.lstrip("/") + (f"?{query}" if query else ""), key),
                            status_code=303)


def _local_location(location: str, base_url: str) -> str:
    """A redirect the instance sends to its own address stays on the gateway."""
    if location == base_url:
        return "/"
    rest = location[len(base_url):] if location.startswith(base_url + "/") else ""
    return rest if rest and not rest.startswith("//") else location


def _pinned_location(location: str, pin: str) -> str:
    """A redirect inside a pinned tab keeps the tab on its instance."""
    return with_pin(location, pin) if location.startswith("/") and not location.startswith("//") else location


def split_in(raw: str) -> tuple[str, str] | None:
    """A sandboxed page sends no cookie and no query in its referer, so the
    gateway serves it under /api/gateway/in/<key>/<path>. Its relative links
    then name the instance in their own path."""
    if not raw.startswith(IN_PREFIX):
        return None
    key, _, rest = raw[len(IN_PREFIX):].partition("/")
    return unquote(key), "/" + rest


def is_sandboxed_page(headers) -> bool:
    return (headers.get("content-type", "").startswith("text/html")
            and bool(SANDBOX.search(headers.get("content-security-policy", ""))))


def _in_location(location: str, key: str) -> str:
    """A redirect inside a sandboxed page keeps the page on its instance."""
    if location.startswith("/") and not location.startswith("//"):
        return IN_PREFIX + quote(key, safe="") + location
    return location


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def forward(path: str, request: Request):
    pin, query = split_pin(request.scope.get("query_string", b"").decode("latin-1"))
    upstream_path = raw_path(request.scope)
    inside = split_in(upstream_path)
    if inside:
        pin, upstream_path = inside
    elif not pin:
        pin = referer_pin(request.headers)
        if pin and request.method in ("GET", "HEAD"):
            return RedirectResponse(with_pin("/" + raw_path(request.scope).lstrip("/") + (f"?{query}" if query else ""), pin),
                                    status_code=303, headers={"Cache-Control": "no-store"})
    target = routed(request.cookies.get(COOKIE), pin)
    if target is None and pin:
        return JSONResponse({"error": f"unknown instance '{pin}'"}, status_code=404)
    if target is None:
        return JSONResponse({"error": "config/peers.toml names no instance"}, status_code=503)
    base = target["base_url"]
    headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS]
    headers += [("host", urlsplit(base).netloc), ("x-forwarded-host", request.headers.get("host", ""))]
    upstream = client.build_request(request.method, base + upstream_path + (f"?{query}" if query else ""),
                                    headers=headers, content=await request.body())
    try:
        resp = await client.send(upstream, stream=True)
    except httpx.HTTPError as e:
        return JSONResponse({"error": f"instance '{target['key']}' is unreachable: {type(e).__name__}: {e}"},
                            status_code=502)
    if not inside and request.method in ("GET", "HEAD") and is_sandboxed_page(resp.headers):
        await resp.aclose()
        return RedirectResponse(_in_location(upstream_path + (f"?{query}" if query else ""), target["key"]),
                                status_code=303, headers={"Cache-Control": "no-store"})
    relocate = _in_location if inside else _pinned_location
    out = [(k.encode("latin-1"), (relocate(_local_location(v, base), pin)
                                  if k.lower() == "location" else v).encode("latin-1"))
           for k, v in resp.headers.multi_items()
           if k.lower() not in HOP_HEADERS and k.lower() != "content-encoding"]
    if request.method == "HEAD":
        await resp.aclose()
        response = Response(status_code=resp.status_code)
    elif resp.headers.get("content-type", "").startswith("text/html") and not inside:
        body = await resp.aread()
        await resp.aclose()
        response = Response(with_picker(body, pin), status_code=resp.status_code)
    elif (path == "api/work/peers" and request.method == "GET" and resp.status_code == 200
          and resp.headers.get("content-type", "").startswith("application/json")):
        body = await resp.aread()
        await resp.aclose()
        response = Response(with_gateway_peers(body), status_code=resp.status_code)
    elif (path == "api/global/events" and request.method == "GET" and resp.status_code == 200
          and resp.headers.get("content-type", "").startswith("application/json")):
        body = await resp.aread()
        await resp.aclose()
        response = Response(with_gateway_events(body), status_code=resp.status_code)
    else:
        response = StreamingResponse(resp.aiter_bytes(), status_code=resp.status_code,
                                     background=BackgroundTask(resp.aclose))
    response.raw_headers.extend(out)
    return response


@app.websocket("/{path:path}")
async def forward_ws(websocket: WebSocket, path: str):
    pin, query = split_pin(websocket.scope.get("query_string", b"").decode("latin-1"))
    target = routed(websocket.cookies.get(COOKIE), pin)
    if target is None:
        await websocket.close(code=1011)
        return
    base = target["base_url"].replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    url = base + raw_path(websocket.scope) + (f"?{query}" if query else "")
    headers = {k: websocket.headers[k] for k in ("origin", "cookie") if k in websocket.headers}
    try:
        upstream = await websockets.connect(url, max_size=None, additional_headers=headers)
    except (OSError, websockets.WebSocketException):
        await websocket.close(code=1011)
        return
    await websocket.accept()
    try:
        async with upstream:
            async def down():
                async for message in upstream:
                    if isinstance(message, bytes):
                        await websocket.send_bytes(message)
                    else:
                        await websocket.send_text(message)

            async def up():
                while True:
                    message = await websocket.receive()
                    if message["type"] == "websocket.disconnect":
                        return
                    if message.get("bytes") is not None:
                        await upstream.send(message["bytes"])
                    elif message.get("text") is not None:
                        await upstream.send(message["text"])

            tasks = [asyncio.create_task(down()), asyncio.create_task(up())]
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()
    except (OSError, websockets.WebSocketException, WebSocketDisconnect):
        pass
    finally:
        try:
            await websocket.close()
        except RuntimeError:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(prog="gateway.py")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=7130)
    args = parser.parse_args()
    uvicorn.run(app, host=args.host, port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
