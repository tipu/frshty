import asyncio
import os
import re
import ssl
import threading
import tomllib
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import websockets
from starlette.concurrency import run_in_threadpool
from starlette.websockets import WebSocket, WebSocketDisconnect

import core.log as log

PEERS_PATH = Path(__file__).parent.parent / "config" / "peers.toml"
ALLOWED_PREFIX = "api/work/"
FILE_PREFIX = "api/work/artifact_file/"
TERMINAL_KEY = re.compile(r"artifact-\d+")
FILE_HEADERS = ("content-type", "content-length", "content-range", "accept-ranges",
                "content-disposition", "content-encoding", "content-security-policy",
                "last-modified", "etag")
DEFAULT_TIMEOUT = 15.0
CONNECT_TIMEOUT = 5.0

_cache_lock = threading.Lock()
_cache: tuple[float, list[dict]] | None = None


def peers() -> list[dict]:
    """The remote frshty hosts whose task board this host federates.

    The list lives in config/peers.toml. Git ignores that file, so every host
    names its own peers and a fresh checkout never peers with itself. A host
    with no peers file behaves exactly as it did before federation."""
    global _cache
    try:
        mtime = PEERS_PATH.stat().st_mtime
    except OSError:
        with _cache_lock:
            _cache = None
        return []
    with _cache_lock:
        if _cache is not None and _cache[0] == mtime:
            return list(_cache[1])
    try:
        with open(PEERS_PATH, "rb") as f:
            raw = tomllib.load(f)
    except Exception as e:
        log.emit("work_peers_load_error",
                 f"failed to parse {PEERS_PATH}: {type(e).__name__}: {e}",
                 meta={"path": str(PEERS_PATH)})
        return []
    entries: list[dict] = []
    seen: set[str] = set()
    for entry in raw.get("peers") or []:
        if not isinstance(entry, dict):
            continue
        key = str(entry.get("key") or "").strip()
        base_url = str(entry.get("base_url") or "").strip().rstrip("/")
        if not key or not base_url or key in seen or key == os.environ.get("FRSHTY_PEER_SELF"):
            continue
        seen.add(key)
        entries.append({"key": key, "base_url": base_url,
                        "label": str(entry.get("label") or key)})
    with _cache_lock:
        _cache = (mtime, entries)
    return list(entries)


def find(key: str) -> dict | None:
    for peer in peers():
        if peer["key"] == key:
            return peer
    return None


def resolve(key: str, path: str) -> tuple[str, str]:
    """Turn a peer key and a board path into an absolute peer URL.

    Only the work API is reachable. A peer is a host the operator configured,
    but an open proxy would still make this instance a relay for every route
    the peer serves."""
    peer = find(key)
    if peer is None:
        return "", f"unknown peer '{key}'"
    clean = path.strip("/")
    if ".." in clean.split("/") or not clean.startswith(ALLOWED_PREFIX):
        return "", f"peer path not allowed: /{clean}"
    return f"{peer['base_url']}/{clean}", ""


async def request(key: str, method: str, path: str, params: dict | None = None,
                  body: dict | None = None, timeout: float = DEFAULT_TIMEOUT) -> tuple[int, dict]:
    """Proxy one board call to a peer and return its status and JSON body."""
    url, error = await run_in_threadpool(resolve, key, path)
    if error:
        return (404 if error.startswith("unknown peer") else 403), {"error": error}
    phases = httpx.Timeout(timeout, connect=min(CONNECT_TIMEOUT, timeout))
    try:
        async with asyncio.timeout(timeout):
            async with httpx.AsyncClient(timeout=phases, verify=False, follow_redirects=True) as client:
                if method.upper() == "GET":
                    resp = await client.get(url, params=params or {})
                else:
                    resp = await client.post(url, params=params or {}, json=body or {})
    except Exception as e:
        reason = (f"no answer within {timeout:g}s" if isinstance(e, TimeoutError)
                  else f"{type(e).__name__}: {e}")
        message = f"peer '{key}' is unreachable: {reason}"
        await run_in_threadpool(log.emit, "work_peer_unreachable",
                                f"{method.upper()} {url} failed: {reason}",
                                meta={"peer": key, "path": path, "url": url})
        return 502, {"error": message}
    try:
        payload = resp.json()
    except ValueError:
        message = (f"peer '{key}' answered {resp.status_code} with "
                   f"{resp.headers.get('content-type', 'no content type')}, not JSON")
        await run_in_threadpool(log.emit, "work_peer_bad_response", f"{method.upper()} {url}: {message}",
                                meta={"peer": key, "path": path, "url": url,
                                      "status": resp.status_code})
        return 502, {"error": message}
    if not isinstance(payload, dict):
        payload = {"data": payload}
    return resp.status_code, payload


async def open_file(key: str, path: str, range_header: str = "",
                    timeout: float = DEFAULT_TIMEOUT) -> tuple[int, dict, AsyncIterator[bytes] | None, str]:
    """Open one artifact file on a peer and return its status, headers, body and error.

    The browser cannot reach a peer's base_url, so the board serves the file
    from its own origin. The body streams, so a large video never sits in
    memory. A redirect stays inside the proxy: an HTML artifact answers with a
    redirect to its folder, and the page's relative asset URLs then resolve
    under the same proxy path."""
    clean = path.strip("/")
    if not clean.startswith(FILE_PREFIX):
        return 403, {}, None, f"peer path not allowed: /{clean}"
    url, error = await run_in_threadpool(resolve, key, path)
    if error:
        return (404 if error.startswith("unknown peer") else 403), {}, None, error
    if path.endswith("/"):
        url += "/"
    phases = httpx.Timeout(timeout, connect=min(CONNECT_TIMEOUT, timeout))
    client = httpx.AsyncClient(timeout=phases, verify=False, follow_redirects=False)
    try:
        async with asyncio.timeout(timeout):
            resp = await client.send(
                client.build_request("GET", url, headers={"range": range_header} if range_header else {}),
                stream=True)
    except Exception as e:
        await client.aclose()
        reason = (f"no answer within {timeout:g}s" if isinstance(e, TimeoutError)
                  else f"{type(e).__name__}: {e}")
        await run_in_threadpool(log.emit, "work_peer_unreachable",
                                f"GET {url} failed: {reason}",
                                meta={"peer": key, "path": path, "url": url})
        return 502, {}, None, f"peer '{key}' is unreachable: {reason}"
    headers = {name: resp.headers[name] for name in FILE_HEADERS if name in resp.headers}
    location = resp.headers.get("location", "")
    if location:
        target = httpx.URL(url).join(location)
        if not target.path.lstrip("/").startswith(FILE_PREFIX):
            await resp.aclose()
            await client.aclose()
            return 502, {}, None, f"peer '{key}' redirected outside the artifact files: {location}"
        headers["location"] = f"/api/work/peers/{key}{target.raw_path.decode()}"

    async def body() -> AsyncIterator[bytes]:
        try:
            async for chunk in resp.aiter_raw():
                yield chunk
        finally:
            await resp.aclose()
            await client.aclose()

    return resp.status_code, headers, body(), ""


def terminal_url(key: str, term_key: str) -> tuple[str, str]:
    """Turn a peer key and an artifact terminal key into the peer's websocket URL.

    Only an artifact terminal is reachable. A ticket or task terminal on the
    peer runs an agent with write access, and the viewer page never needs it."""
    if not TERMINAL_KEY.fullmatch(term_key):
        return "", f"peer terminal not allowed: {term_key}"
    peer = find(key)
    if peer is None:
        return "", f"unknown peer '{key}'"
    base = peer["base_url"].replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    return f"{base}/ws/terminal/{term_key}", ""


async def relay_terminal(websocket: WebSocket, key: str, term_key: str,
                         timeout: float = DEFAULT_TIMEOUT) -> None:
    """Relay one browser terminal websocket to the same terminal on a peer.

    The browser cannot reach a peer's base_url, so the board opens the
    websocket to the peer and copies frames both ways. A refused or unreachable
    peer closes the browser socket with 1008, which the viewer shows as an
    unavailable terminal instead of a reconnect loop."""
    url, error = await run_in_threadpool(terminal_url, key, term_key)
    if error:
        await websocket.accept()
        await websocket.close(code=1008, reason=error)
        return
    context = None
    if url.startswith("wss://"):
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    try:
        upstream = await websockets.connect(url, max_size=None, ssl=context,
                                            open_timeout=min(CONNECT_TIMEOUT, timeout))
    except (OSError, TimeoutError, websockets.WebSocketException) as e:
        reason = f"{type(e).__name__}: {e}"
        await run_in_threadpool(log.emit, "work_peer_unreachable",
                                f"WS {url} failed: {reason}",
                                meta={"peer": key, "path": f"ws/terminal/{term_key}", "url": url})
        await websocket.accept()
        await websocket.close(code=1008, reason=f"peer '{key}' is unreachable")
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
        code = upstream.close_code if upstream.close_code in (1000, 1008, 1011) else 1000
        try:
            await websocket.close(code=code)
        except RuntimeError:
            pass
