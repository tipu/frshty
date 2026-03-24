import json
from pathlib import Path

_state_dir: Path | None = None


def init(state_dir: Path):
    global _state_dir
    _state_dir = state_dir
    _state_dir.mkdir(parents=True, exist_ok=True)


def load(module: str) -> dict:
    path = _state_dir / f"{module}.json"
    if path.exists():
        return json.loads(path.read_text())
    return {}


def save(module: str, data: dict):
    path = _state_dir / f"{module}.json"
    path.write_text(json.dumps(data, indent=2, default=str))
