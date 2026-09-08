"""Refuse a state-changing request that a page on another site sent.

The board has no login and no token. A browser attaches its own network
position to every request a page makes, so any page the operator has open can
POST to /api/tickets/{key}/merge or open /ws/terminal and run a shell command
inside a worktree. The Origin header is the one part of such a request the
foreign page cannot set: the browser writes it on every state-changing request
and on every WebSocket handshake, and script cannot change it.

Three rules decide a request:

A request that carries no Origin header is served. Browsers always send the
header on the requests this module guards, so a request without it did not come
from a page: it came from the CLI, from a peer board, or from a test.

An Origin of "null" is refused. That is the opaque origin a sandboxed artifact
page carries, and an artifact is untrusted content.

Any other Origin is decided in two steps.

First the hostname the browser addressed, which is the Host header, must be one
the board's own configuration declares. Comparing Origin against Host alone is
not enough: an attacker who points a hostname he owns at the board's address
gets a page whose Origin and Host agree, which is DNS rebinding. Such a page is
same-origin with the board as far as the browser is concerned, so it can also
set any request header it likes, including X-Forwarded-Host. Only the hostname
list stops it, and the attacker cannot change the Host header, because the
browser writes it from the address of the page itself.

Then the Origin must equal that Host header, or equal the X-Forwarded-Host a
proxy in front of the board wrote. The second case is how the board answers on a
public hostname that the proxy rewrites Host for.
"""

from urllib.parse import urlsplit

from starlette.datastructures import Headers
from starlette.responses import JSONResponse

import core.log as log
from web.state import _configs_by_host, active_config, primary_config

GUARDED_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
LOOPBACK_HOSTNAMES = frozenset({"localhost", "127.0.0.1", "::1"})
MAX_REPORTED_SENDERS = 64
_DEFAULT_PORTS = {"http": "80", "https": "443", "ws": "80", "wss": "443"}
_reported_senders: set[str] = set()


def report_once(origin: str | None, host: str | None) -> bool:
    """True the first time this sender is refused on this hostname.

    A foreign page can send a blocked request in a loop. log_events is trimmed
    only when the operator dismisses the feed, so one event per request would
    let that page grow the table and bury every real event. One event per
    sender, and at most MAX_REPORTED_SENDERS of them, still tells the operator
    that a site is probing this board.
    """
    key = f"{(origin or '').strip().lower()}|{(host or '').strip().lower()}"
    if key in _reported_senders or len(_reported_senders) >= MAX_REPORTED_SENDERS:
        return False
    _reported_senders.add(key)
    return True


def origin_is_opaque(origin: str | None) -> bool:
    return (origin or "").strip().lower() == "null"


def _hostname_of(url: str) -> str:
    """The hostname of a URL or of a bare host[:port], "" when there is none."""
    text = (url or "").strip()
    if not text:
        return ""
    try:
        parts = urlsplit(text if "//" in text else "//" + text)
        return (parts.hostname or "").lower()
    except ValueError:
        return ""


def board_hostnames() -> set[str]:
    """Every hostname this board is configured to answer on.

    --multi registers one hostname per instance from its job.host. A single
    instance registers none, so its own job.host, which core.config keeps as
    _base_url, is the source. Loopback is always this board: the operator
    reaches it at 127.0.0.1 with no config entry at all.
    """
    names = set(LOOPBACK_HOSTNAMES)
    names.update(host for host in _configs_by_host if host)
    for config in (active_config(), primary_config()):
        name = _hostname_of(config.get("_base_url", "") if config else "")
        if name:
            names.add(name)
    return names


def _endpoint(value: str, default_port: str) -> tuple[str, str]:
    """(hostname, port) of an Origin, a Host header or an X-Forwarded-Host."""
    text = (value or "").strip()
    if not text:
        return ("", "")
    try:
        parts = urlsplit(text if "//" in text else "//" + text)
        hostname = (parts.hostname or "").lower()
        port = str(parts.port) if parts.port else default_port
    except ValueError:
        return ("", "")
    return (hostname, port) if hostname else ("", "")


def origin_allowed(origin: str | None, host: str | None,
                   forwarded_host: str | None = None) -> bool:
    """True when this request may change state. See the module docstring."""
    if not origin:
        return True
    if origin_is_opaque(origin):
        return False
    text = origin.strip()
    parts = urlsplit(text)
    if not parts.scheme or not parts.netloc or "@" in parts.netloc:
        return False
    default_port = _DEFAULT_PORTS.get(parts.scheme.lower(), "")
    sender = _endpoint(text, default_port)
    if not sender[0]:
        return False
    target = _endpoint(host or "", default_port)
    if target[0] not in board_hostnames():
        return False
    if sender == target:
        return True
    return bool(forwarded_host) and sender == _endpoint(forwarded_host, default_port)


class OriginGuard:
    """ASGI middleware that answers a cross-site request instead of running it.

    Raw ASGI, not BaseHTTPMiddleware: BaseHTTPMiddleware hands a WebSocket
    handshake straight to the application, and the terminal socket is the most
    valuable target on the board.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        kind = scope.get("type")
        if kind not in ("http", "websocket"):
            await self.app(scope, receive, send)
            return
        method = (scope.get("method") or "").upper()
        if kind == "http" and method not in GUARDED_METHODS:
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        origin = headers.get("origin")
        host = headers.get("host")
        if origin_allowed(origin, host, headers.get("x-forwarded-host")):
            await self.app(scope, receive, send)
            return
        path = scope.get("path", "")
        verb = method or "WEBSOCKET"
        summary = f"refused {verb} {path} from origin {origin!r}"
        if report_once(origin, host):
            log.emit("origin_blocked", summary,
                     meta={"path": path, "method": verb,
                           "origin": origin or "", "host": host or ""})
        else:
            print(f"origin_blocked: {summary}", flush=True)
        if kind == "websocket":
            await send({"type": "websocket.close", "code": 1008,
                        "reason": "cross-site origin"})
            return
        response = JSONResponse(
            {"error": "cross-site request blocked: the Origin header does not "
                      "name this board"},
            status_code=403)
        await response(scope, receive, send)


def install(app) -> None:
    """Add the guard to an app.

    Call this before the HTTP middlewares that resolve the instance, so the
    guard ends up the innermost of them: Starlette runs the middleware it was
    given last first. A blocked HTTP request has then already been resolved to
    its instance, so the origin_blocked event lands in that instance's log and
    not in the primary's.
    """
    app.add_middleware(OriginGuard)
