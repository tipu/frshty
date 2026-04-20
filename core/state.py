import fcntl
import json
import os
import tempfile
from contextvars import ContextVar
from pathlib import Path

_default_state_dir: Path | None = None
_state_dir_cv: ContextVar[Path | None] = ContextVar("state_dir", default=None)


def init(state_dir: Path):
    global _default_state_dir
    _default_state_dir = state_dir
    _default_state_dir.mkdir(parents=True, exist_ok=True)


def use(state_dir: Path):
    """Per-request override of the active state dir (multi mode)."""
    state_dir.mkdir(parents=True, exist_ok=True)
    return _state_dir_cv.set(state_dir)


def reset(token) -> None:
    _state_dir_cv.reset(token)


def _active_dir() -> Path:
    cv = _state_dir_cv.get()
    if cv is not None:
        return cv
    if _default_state_dir is None:
        raise RuntimeError("core.state not initialized; call state.init(dir) first")
    return _default_state_dir


def _lock_path(module: str) -> Path:
    return _active_dir() / f"{module}.lock"


def load(module: str) -> dict:
    path = _active_dir() / f"{module}.json"
    lock = _lock_path(module)
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_SH)
        if path.exists():
            return json.loads(path.read_text())
    return {}


def save(module: str, data: dict):
    active = _active_dir()
    path = active / f"{module}.json"
    lock = _lock_path(module)
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=str(active), suffix=".tmp", delete=False) as f:
                tmp = f.name
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp, str(path))
        except Exception:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
            raise
