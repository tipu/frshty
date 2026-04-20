"""SQLite-backed state store for frshty features.

Preserves the legacy API:
    state.init(state_dir_path)
    state.load("tickets") -> dict
    state.save("tickets", {...})

Behind the scenes, every (instance_key, module) pair is stored as a JSON blob
row in the `kv` table of ~/.frshty/frshty.db. Contextvar overlay (state.use /
state.reset) switches the active instance_key per request for --multi mode.

Back-compat: init() accepts a Path (the legacy state_dir) and uses its
directory name as the instance_key.
"""
import json
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path

import core.db as db

_default_instance_key: str | None = None
_instance_key_cv: ContextVar[str | None] = ContextVar("frshty_instance_key", default=None)
_DB_INITIALIZED = False


def _ensure_db():
    global _DB_INITIALIZED
    if _DB_INITIALIZED:
        return
    if getattr(db, "_DB_PATH", None) is None:
        migrations = Path(__file__).resolve().parent.parent / "migrations"
        db.init(Path.home() / ".frshty" / "frshty.db", migrations)
    _DB_INITIALIZED = True


def init(state_dir) -> None:
    """state_dir can be a Path (legacy) or the instance_key string."""
    global _default_instance_key
    if isinstance(state_dir, Path):
        _default_instance_key = state_dir.name
        state_dir.mkdir(parents=True, exist_ok=True)
    else:
        _default_instance_key = str(state_dir)
    _ensure_db()


def use(state_dir_or_key):
    """Per-request override for --multi mode. Accepts Path or instance_key string."""
    if isinstance(state_dir_or_key, Path):
        key = state_dir_or_key.name
        state_dir_or_key.mkdir(parents=True, exist_ok=True)
    else:
        key = str(state_dir_or_key)
    _ensure_db()
    return _instance_key_cv.set(key)


def reset(token) -> None:
    _instance_key_cv.reset(token)


def _active_key() -> str:
    k = _instance_key_cv.get()
    if k is not None:
        return k
    if _default_instance_key is None:
        raise RuntimeError("core.state not initialized; call state.init(state_dir) first")
    return _default_instance_key


def load(module: str) -> dict:
    _ensure_db()
    row = db.query_one(
        "SELECT data FROM kv WHERE instance_key=? AND key=?",
        (_active_key(), module),
    )
    if not row or not row.get("data"):
        return {}
    try:
        val = json.loads(row["data"])
    except json.JSONDecodeError:
        return {}
    return val if isinstance(val, dict) else {}


def save(module: str, data: dict) -> None:
    _ensure_db()
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(data, default=str)
    db.execute(
        "INSERT INTO kv(instance_key, key, data, updated_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(instance_key, key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
        (_active_key(), module, payload, now),
    )


def active_instance_key() -> str:
    return _active_key()
