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
from pathlib import Path
from urllib.parse import quote, urlsplit

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


@app.get("/api/gateway/instances")
def api_instances(request: Request):
    current = selected(request.cookies.get(COOKIE))
    return {"current": current["key"] if current else "",
            "instances": [{"key": i["key"], "label": i["label"]} for i in instances()]}


@app.get("/api/gateway/picker.js")
def api_picker():
    return FileResponse(PICKER, media_type="application/javascript",
                        headers={"Cache-Control": "no-cache"})


def with_picker(html: bytes) -> bytes:
    at = html.rfind(b"</body>")
    return html[:at] + PICKER_TAG + html[at:] if at >= 0 else html + PICKER_TAG


@app.get("/api/gateway/select")
def api_select(key: str, next: str = "/"):
    if not any(i["key"] == key for i in instances()):
        return JSONResponse({"error": f"unknown instance '{key}'"}, status_code=404)
    target = next if next.startswith("/") and not next.startswith("//") else "/"
    response = RedirectResponse(target, status_code=303)
    response.set_cookie(COOKIE, key, max_age=365 * 86400, samesite="lax")
    return response


def raw_path(scope) -> str:
    """The path as the browser sent it, still percent-encoded, so an encoded
    ? or # stays part of the path upstream."""
    raw = scope.get("raw_path")
    return raw.decode("latin-1") if raw else quote(scope["path"])


def _local_location(location: str, base_url: str) -> str:
    """A redirect the instance sends to its own address stays on the gateway."""
    if location == base_url:
        return "/"
    rest = location[len(base_url):] if location.startswith(base_url + "/") else ""
    return rest if rest and not rest.startswith("//") else location


@app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS", "HEAD"])
async def forward(path: str, request: Request):
    target = selected(request.cookies.get(COOKIE))
    if target is None:
        return JSONResponse({"error": "config/peers.toml names no instance"}, status_code=503)
    base = target["base_url"]
    headers = [(k, v) for k, v in request.headers.items() if k.lower() not in HOP_HEADERS]
    headers += [("host", urlsplit(base).netloc), ("x-forwarded-host", request.headers.get("host", ""))]
    query = request.scope.get("query_string", b"").decode("latin-1")
    upstream = client.build_request(request.method, base + raw_path(request.scope) + (f"?{query}" if query else ""),
                                    headers=headers, content=await request.body())
    try:
        resp = await client.send(upstream, stream=True)
    except httpx.HTTPError as e:
        return JSONResponse({"error": f"instance '{target['key']}' is unreachable: {type(e).__name__}: {e}"},
                            status_code=502)
    out = [(k.encode("latin-1"), (_local_location(v, base) if k.lower() == "location" else v).encode("latin-1"))
           for k, v in resp.headers.multi_items()
           if k.lower() not in HOP_HEADERS and k.lower() != "content-encoding"]
    if request.method == "HEAD":
        await resp.aclose()
        response = Response(status_code=resp.status_code)
    elif resp.headers.get("content-type", "").startswith("text/html"):
        body = await resp.aread()
        await resp.aclose()
        response = Response(with_picker(body), status_code=resp.status_code)
    else:
        response = StreamingResponse(resp.aiter_bytes(), status_code=resp.status_code,
                                     background=BackgroundTask(resp.aclose))
    response.raw_headers.extend(out)
    return response


@app.websocket("/{path:path}")
async def forward_ws(websocket: WebSocket, path: str):
    target = selected(websocket.cookies.get(COOKIE))
    if target is None:
        await websocket.close(code=1011)
        return
    base = target["base_url"].replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    query = websocket.scope.get("query_string", b"").decode("latin-1")
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
