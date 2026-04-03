import fcntl
import json
import os
import tempfile
from pathlib import Path

_state_dir: Path | None = None


def init(state_dir: Path):
    global _state_dir
    _state_dir = state_dir
    _state_dir.mkdir(parents=True, exist_ok=True)


def _lock_path(module: str) -> Path:
    return _state_dir / f"{module}.lock"


def load(module: str) -> dict:
    path = _state_dir / f"{module}.json"
    lock = _lock_path(module)
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_SH)
        if path.exists():
            return json.loads(path.read_text())
    return {}


def save(module: str, data: dict):
    path = _state_dir / f"{module}.json"
    lock = _lock_path(module)
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=str(_state_dir), suffix=".tmp", delete=False) as f:
                tmp = f.name
                json.dump(data, f, indent=2, default=str)
            os.replace(tmp, str(path))
        except Exception:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
            raise
