import time
from uuid import uuid4

import core.log as log
import core.state as state
from web.state import _configs_by_host, _cv_config


def install(app):
    @app.middleware("http")
    async def resolve_instance_by_host(request, call_next):
        """In --multi mode, pick the active config by matching the request Host header.

        Unknown hosts fall through to whatever config is currently the contextvar
        default (typically the primary). Single-instance mode is a no-op.
        """
        config_token = None
        state_token = None
        log_tokens = None
        if _configs_by_host:
            host = (request.headers.get("host") or "").split(":")[0].lower()
            target = _configs_by_host.get(host)
            if target is not None:
                config_token = _cv_config.set(target)
                state_token = state.use(target["_state_dir"])
                log_tokens = log.use(target["_state_dir"], target["job"]["key"])
        try:
            response = await call_next(request)
        finally:
            if log_tokens is not None:
                log.reset(log_tokens)
            if state_token is not None:
                state.reset(state_token)
            if config_token is not None:
                _cv_config.reset(config_token)
        return response

    @app.middleware("http")
    async def profile_requests(request, call_next):
        rid = uuid4().hex[:6]
        path = request.url.path
        method = request.method
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
        response.headers["X-Response-Time"] = f"{elapsed:.3f}s"
        return response
