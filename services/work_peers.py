import threading
import tomllib
from pathlib import Path

import httpx

import core.log as log

PEERS_PATH = Path(__file__).parent.parent / "config" / "peers.toml"
ALLOWED_PREFIX = "api/work/"
DEFAULT_TIMEOUT = 15.0

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
        if not key or not base_url or key in seen:
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


def request(key: str, method: str, path: str, params: dict | None = None,
            body: dict | None = None, timeout: float = DEFAULT_TIMEOUT) -> tuple[int, dict]:
    """Proxy one board call to a peer and return its status and JSON body."""
    url, error = resolve(key, path)
    if error:
        return (404 if error.startswith("unknown peer") else 403), {"error": error}
    try:
        with httpx.Client(timeout=timeout, verify=False, follow_redirects=True) as client:
            if method.upper() == "GET":
                resp = client.get(url, params=params or {})
            else:
                resp = client.post(url, params=params or {}, json=body or {})
    except Exception as e:
        message = f"peer '{key}' is unreachable: {type(e).__name__}: {e}"
        log.emit("work_peer_unreachable",
                 f"{method.upper()} {url} failed: {type(e).__name__}: {e}",
                 meta={"peer": key, "path": path, "url": url})
        return 502, {"error": message}
    try:
        payload = resp.json()
    except ValueError:
        message = (f"peer '{key}' answered {resp.status_code} with "
                   f"{resp.headers.get('content-type', 'no content type')}, not JSON")
        log.emit("work_peer_bad_response", f"{method.upper()} {url}: {message}",
                 meta={"peer": key, "path": path, "url": url,
                       "status": resp.status_code})
        return 502, {"error": message}
    if not isinstance(payload, dict):
        payload = {"data": payload}
    return resp.status_code, payload
