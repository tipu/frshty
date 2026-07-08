import os
from contextvars import ContextVar
from pathlib import Path


_cv_config: ContextVar[dict] = ContextVar("frshty_config", default={})
_primary_config: dict = {}
_configs_by_host: dict[str, dict] = {}


class _ConfigView(dict):
    """Dict-subclass proxy that resolves to the active config for the current request.

    In --multi mode, the hostname middleware sets the contextvar per request. In
    single-instance mode the module-level _primary_config is the fallback when
    the contextvar's default ({}) is still in effect (e.g. uvicorn reload
    subprocesses don't always inherit contextvar sets from module-import time).
    dict is the base class so FastAPI handlers typed as `dict` accept the view.
    """
    def _d(self) -> dict:
        v = _cv_config.get()
        return v if v else _primary_config
    def __getitem__(self, k): return self._d()[k]
    def __setitem__(self, k, v): self._d()[k] = v
    def __delitem__(self, k): del self._d()[k]
    def __contains__(self, k): return k in self._d()
    def __iter__(self): return iter(self._d())
    def __len__(self): return len(self._d())
    def __bool__(self): return bool(self._d())
    def __repr__(self): return repr(self._d())
    def get(self, k, default=None): return self._d().get(k, default)
    def items(self): return self._d().items()  # type: ignore[override]
    def keys(self): return self._d().keys()  # type: ignore[override]
    def values(self): return self._d().values()  # type: ignore[override]
    def setdefault(self, k, d=None): return self._d().setdefault(k, d)
    def update(self, *a, **kw): return self._d().update(*a, **kw)
    def pop(self, k, *a): return self._d().pop(k, *a)
    def copy(self): return self._d().copy()


_config: _ConfigView = _ConfigView()


def set_primary_config(c: dict) -> None:
    global _primary_config
    _primary_config = c
    _cv_config.set(c)


def primary_config() -> dict:
    return _primary_config


def active_config() -> dict:
    """The real config dict for the current request context. Capture this
    BEFORE spawning a thread: contextvars don't propagate to new threads, so
    _config inside a thread silently falls back to the primary instance."""
    v = _cv_config.get()
    return v if v else _primary_config


def multi_apply_host(host: str | None):
    """Set per-instance contextvars from a Host header. Returns reset tokens or
    None when --multi is inactive or the host is unknown. Caller MUST pass the
    return value to multi_reset() in a finally block. Used by HTTP middleware
    AND by WebSocket handlers (which bypass HTTP middleware)."""
    if not _configs_by_host:
        return None
    key = (host or "").split(":")[0].lower()
    target = _configs_by_host.get(key)
    if target is None:
        return None
    import core.log as log
    import core.state as state
    config_token = _cv_config.set(target)
    state_token = state.use(target["_state_dir"])
    log_tokens = log.use(target["_state_dir"], target["job"]["key"])
    return (config_token, state_token, log_tokens)


def multi_reset(tokens) -> None:
    if tokens is None:
        return
    import core.log as log
    import core.state as state
    config_token, state_token, log_tokens = tokens
    log.reset(log_tokens)
    state.reset(state_token)
    _cv_config.reset(config_token)


def events_enabled() -> bool:
    try:
        import core.runtime as _rt
        return _rt.instances() is not None
    except Exception:
        return False


def ensure_path() -> None:
    extra = [
        os.path.expanduser("~/.local/bin"),
        os.path.expanduser("~/.local/node/bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
    ]
    current = os.environ.get("PATH", "/usr/bin:/bin")
    os.environ["PATH"] = ":".join(extra) + ":" + current
