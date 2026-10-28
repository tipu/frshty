import time
from uuid import uuid4

from starlette.responses import JSONResponse

from services import usage
from web.state import _disabled_hosts, multi_apply_host, multi_reset


def install(app):
    @app.middleware("http")
    async def revalidate_static(request, call_next):
        """Make the browser check /static before reusing it.

        StaticFiles sends ETag and Last-Modified but no Cache-Control, so a
        browser is free to reuse a cached copy without asking. A shipped fix to
        frshty-nav.js then stays invisible until a hard reload. no-cache still
        allows caching; it only requires revalidation, so the usual answer is a
        304 and nothing is re-downloaded.
        """
        response = await call_next(request)
        if request.url.path.startswith("/static/"):
            response.headers["Cache-Control"] = "no-cache, must-revalidate"
        return response

    @app.middleware("http")
    async def resolve_instance_by_host(request, call_next):
        """In --multi mode, pick the active config by matching the request Host header.

        Unknown hosts fall through to whatever config is currently the contextvar
        default (typically the primary). Single-instance mode is a no-op.
        """
        host = (request.headers.get("host") or "").split(":")[0].lower()
        if host in _disabled_hosts:
            return JSONResponse(
                {"error": f"instance for {host} is disabled: its commit identity "
                          f"could not be verified. See the frshty log for "
                          f"git_identity_unverified."},
                status_code=503,
            )
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
        if usage.is_tracked(path):
            ms = int(elapsed * 1000)
            print(f"[USAGE] {host} {method} {path} status={response.status_code} ms={ms}", flush=True)
            if response.status_code not in (404, 405):
                route = request.scope.get("route")
                usage.record("route", f"{method} {getattr(route, 'path', path)}", instance=host)
        response.headers["X-Response-Time"] = f"{elapsed:.3f}s"
        return response
