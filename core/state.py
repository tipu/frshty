"""SQLite-backed state store for frshty features.

Preserves the legacy API:
    state.init(state_dir_path)
    state.load("tickets") -> dict
    state.save("tickets", {...})

Behind the scenes, every (instance_key, module) pair is stored as a JSON blob
row in the `kv` table of ~/.frshty/frshty.db. EXCEPT "tickets" which lives in
its own per-row `tickets` table — see load_ticket/save_ticket/update_ticket
below. Concurrent writers must use update_ticket (or save_ticket per ticket)
to avoid lost updates; the legacy state.save("tickets", whole_dict) is kept
as a compatibility shim for cold call sites only.

Contextvar overlay (state.use / state.reset) switches the active instance_key
per request for --multi mode.

Back-compat: init() accepts a Path (the legacy state_dir) and uses its
directory name as the instance_key.
"""
import json
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

import core.db as db

_default_instance_key: str | None = None
_instance_key_cv: ContextVar[str | None] = ContextVar("frshty_instance_key", default=None)
_DB_INITIALIZED = False
_TICKETS_MIGRATED: set[str] = set()


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
    if module == "tickets":
        return list_tickets()
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
    if module == "tickets":
        _save_tickets_dict(data)
        return
    now = datetime.now(timezone.utc).isoformat()
    payload = json.dumps(data, default=str)
    db.execute(
        "INSERT INTO kv(instance_key, key, data, updated_at) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(instance_key, key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
        (_active_key(), module, payload, now),
    )


def active_instance_key() -> str:
    return _active_key()


# --- per-ticket row API ---------------------------------------------------

def _migrate_kv_to_rows(instance: str) -> None:
    """One-time copy of kv['tickets'] → tickets rows for an instance."""
    if instance in _TICKETS_MIGRATED:
        return
    has_rows = db.query_one(
        "SELECT 1 AS x FROM tickets WHERE instance_key=? LIMIT 1", (instance,)
    )
    if has_rows:
        _TICKETS_MIGRATED.add(instance)
        return
    kv_row = db.query_one(
        "SELECT data FROM kv WHERE instance_key=? AND key='tickets'", (instance,)
    )
    if kv_row and kv_row.get("data"):
        try:
            existing = json.loads(kv_row["data"])
        except json.JSONDecodeError:
            existing = {}
        if isinstance(existing, dict) and existing:
            now = datetime.now(timezone.utc).isoformat()
            with db.tx() as c:
                for k, v in existing.items():
                    if not isinstance(v, dict):
                        continue
                    c.execute(
                        "INSERT OR IGNORE INTO tickets"
                        "(instance_key, ticket_key, status, slug, branch, url, external_status, auto_pr, data, updated_at)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (instance, k, v.get("status", "new"), v.get("slug"), v.get("branch"),
                         v.get("url"), v.get("external_status"),
                         (1 if v.get("auto_pr") else 0) if v.get("auto_pr") is not None else None,
                         json.dumps(v, default=str), now),
                    )
    _TICKETS_MIGRATED.add(instance)


_STALE_TICKET_KEYS = (
    "triage_count", "last_triage_at",
    "last_scrollback_hash", "last_scrollback_change_at",
    "stuck_logged", "restart_count", "last_restart_at",
)


def _strip_stale(data: dict) -> dict:
    """Opportunistic removal of pre-refactor ticket fields. Converges old blobs
    to clean shape on next write; no migration script needed."""
    for k in _STALE_TICKET_KEYS:
        data.pop(k, None)
    return data


def _row_to_ticket(row: dict) -> dict:
    raw = row.get("data") or "{}"
    try:
        v = json.loads(raw)
    except json.JSONDecodeError:
        v = {}
    if not isinstance(v, dict):
        v = {}
    return v


def load_ticket(key: str) -> dict | None:
    _ensure_db()
    instance = _active_key()
    _migrate_kv_to_rows(instance)
    row = db.query_one(
        "SELECT data FROM tickets WHERE instance_key=? AND ticket_key=?",
        (instance, key),
    )
    return _row_to_ticket(row) if row else None


def list_tickets() -> dict:
    _ensure_db()
    instance = _active_key()
    _migrate_kv_to_rows(instance)
    rows = db.query_all(
        "SELECT ticket_key, data FROM tickets WHERE instance_key=?",
        (instance,),
    )
    return {r["ticket_key"]: _row_to_ticket(r) for r in rows}


def save_ticket(key: str, data: dict) -> None:
    _ensure_db()
    instance = _active_key()
    _migrate_kv_to_rows(instance)
    _strip_stale(data)
    now = datetime.now(timezone.utc).isoformat()
    auto_pr = data.get("auto_pr")
    db.execute(
        "INSERT INTO tickets"
        "(instance_key, ticket_key, status, slug, branch, url, external_status, auto_pr, data, updated_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        " ON CONFLICT(instance_key, ticket_key) DO UPDATE SET"
        "  status=excluded.status, slug=excluded.slug, branch=excluded.branch, url=excluded.url,"
        "  external_status=excluded.external_status, auto_pr=excluded.auto_pr,"
        "  data=excluded.data, updated_at=excluded.updated_at",
        (instance, key, data.get("status", "new"), data.get("slug"), data.get("branch"),
         data.get("url"), data.get("external_status"),
         (1 if auto_pr else 0) if auto_pr is not None else None,
         json.dumps(data, default=str), now),
    )


def delete_ticket(key: str) -> None:
    _ensure_db()
    instance = _active_key()
    db.execute(
        "DELETE FROM tickets WHERE instance_key=? AND ticket_key=?",
        (instance, key),
    )


def update_ticket(key: str, mutate: Callable[[dict], dict | None]) -> dict | None:
    """Transactional read-modify-write on a single ticket. Atomic against
    other update_ticket / save_ticket calls. Pass a mutator that takes the
    current dict (or {} if missing) and returns the new dict, or None to
    delete. Returns the saved dict, or None if deleted/no-op."""
    _ensure_db()
    instance = _active_key()
    _migrate_kv_to_rows(instance)
    now = datetime.now(timezone.utc).isoformat()
    with db.tx() as c:
        row = c.execute(
            "SELECT data FROM tickets WHERE instance_key=? AND ticket_key=?",
            (instance, key),
        ).fetchone()
        current = _row_to_ticket(dict(row)) if row else {}
        new = mutate(current)
        if new is None:
            c.execute(
                "DELETE FROM tickets WHERE instance_key=? AND ticket_key=?",
                (instance, key),
            )
            return None
        _strip_stale(new)
        auto_pr = new.get("auto_pr")
        c.execute(
            "INSERT INTO tickets"
            "(instance_key, ticket_key, status, slug, branch, url, external_status, auto_pr, data, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(instance_key, ticket_key) DO UPDATE SET"
            "  status=excluded.status, slug=excluded.slug, branch=excluded.branch, url=excluded.url,"
            "  external_status=excluded.external_status, auto_pr=excluded.auto_pr,"
            "  data=excluded.data, updated_at=excluded.updated_at",
            (instance, key, new.get("status", "new"), new.get("slug"), new.get("branch"),
             new.get("url"), new.get("external_status"),
             (1 if auto_pr else 0) if auto_pr is not None else None,
             json.dumps(new, default=str), now),
        )
        return new


def _save_tickets_dict(data: dict) -> None:
    """Compatibility shim for legacy state.save('tickets', whole_dict).
    Diffs against current rows: upserts everything in data, deletes rows not
    present. NOT safe under concurrent writers — prefer save_ticket /
    update_ticket. Kept so cold call sites keep working without churn."""
    _ensure_db()
    instance = _active_key()
    _migrate_kv_to_rows(instance)
    now = datetime.now(timezone.utc).isoformat()
    with db.tx() as c:
        existing = {r["ticket_key"] for r in c.execute(
            "SELECT ticket_key FROM tickets WHERE instance_key=?", (instance,)
        ).fetchall()}
        new_keys = set(data.keys())
        for k in existing - new_keys:
            c.execute(
                "DELETE FROM tickets WHERE instance_key=? AND ticket_key=?",
                (instance, k),
            )
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            _strip_stale(v)
            auto_pr = v.get("auto_pr")
            c.execute(
                "INSERT INTO tickets"
                "(instance_key, ticket_key, status, slug, branch, url, external_status, auto_pr, data, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                " ON CONFLICT(instance_key, ticket_key) DO UPDATE SET"
                "  status=excluded.status, slug=excluded.slug, branch=excluded.branch, url=excluded.url,"
                "  external_status=excluded.external_status, auto_pr=excluded.auto_pr,"
                "  data=excluded.data, updated_at=excluded.updated_at",
                (instance, k, v.get("status", "new"), v.get("slug"), v.get("branch"),
                 v.get("url"), v.get("external_status"),
                 (1 if auto_pr else 0) if auto_pr is not None else None,
                 json.dumps(v, default=str), now),
            )
