import time
from uuid import uuid4

from web.state import multi_apply_host, multi_reset


_USAGE_SKIP_EXACT = frozenset({
    "/api/tickets/list",
    "/api/events",
    "/api/config",
    "/api/status",
    "/api/scheduled",
})
_USAGE_SKIP_PREFIXES = ("/static/", "/favicon")


def _is_usage_worthy(path: str) -> bool:
    if path in _USAGE_SKIP_EXACT:
        return False
    return not path.startswith(_USAGE_SKIP_PREFIXES)


def install(app):
    @app.middleware("http")
    async def resolve_instance_by_host(request, call_next):
        """In --multi mode, pick the active config by matching the request Host header.

        Unknown hosts fall through to whatever config is currently the contextvar
        default (typically the primary). Single-instance mode is a no-op.
        """
        tokens = multi_apply_host(request.headers.get("host"))
        try:
            response = await call_next(request)
        finally:
            multi_reset(tokens)
        return response

    @app.middleware("http")
    async def profile_requests(request, call_next):
        rid = uuid4().hex[:6]
        path = request.url.path
        method = request.method
        host = request.headers.get("host", "?").split(":")[0]
        t0 = time.time()
        print(f"[REQ {rid}] {time.strftime('%H:%M:%S')} {method} {path} enter", flush=True)
        try:
            response = await call_next(request)
        except Exception as e:
            elapsed = time.time() - t0
            print(f"[REQ {rid}] {time.strftime('%H:%M:%S')} {method} {path} ERROR after {elapsed:.2f}s: {e!r}", flush=True)
            raise
        elapsed = time.time() - t0
        print(f"[REQ {rid}] {time.strftime('%H:%M:%S')} {method} {path} done {elapsed:.2f}s status={response.status_code}", flush=True)
        if _is_usage_worthy(path):
            ms = int(elapsed * 1000)
            print(f"[USAGE] {host} {method} {path} status={response.status_code} ms={ms}", flush=True)
        response.headers["X-Response-Time"] = f"{elapsed:.3f}s"
        return response
