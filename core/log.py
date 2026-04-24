import json
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import core.db as _db
from core.state import _instance_key_cv as _instance_key_cv

_default_job_key: str = ""

_job_key_cv: ContextVar[str | None] = ContextVar("frshty_log_job_key", default=None)


def init(state_dir: Path, job_key: str):
    global _default_job_key
    _default_job_key = job_key
    logs_dir = state_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)


def use(state_dir: Path, job_key: str):
    logs_dir = state_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    k_token = _job_key_cv.set(job_key)
    return (k_token,)


def reset(tokens):
    (k,) = tokens
    _job_key_cv.reset(k)


def _active_job_key() -> str:
    k = _job_key_cv.get()
    return k if k is not None else _default_job_key


def _active_instance_key() -> str:
    k = _instance_key_cv.get()
    if k is not None:
        return k
    import core.state as _state
    try:
        return _state.active_instance_key()
    except RuntimeError:
        return ""


def emit(event: str, summary: str, links: dict | None = None, meta: dict | None = None):
    clean_links = {k: v for k, v in (links or {}).items() if v}
    job = _active_job_key()
    instance_key = _active_instance_key()
    ts = datetime.now(timezone.utc).isoformat()
    record_id = uuid4().hex[:12]
    record = {
        "ts": ts,
        "job": job,
        "event": event,
        "id": record_id,
        "summary": summary,
        "links": clean_links,
        "meta": meta or {},
    }
    _db.execute(
        "INSERT OR IGNORE INTO log_events(id, instance_key, job, event, summary, links, meta, ts) VALUES(?,?,?,?,?,?,?,?)",
        (record_id, instance_key, job, event, summary, json.dumps(clean_links), json.dumps(meta or {}), ts)
    )
    print(f"[{job}] {event}: {summary}", flush=True)
    return record


MAX_LOG_LINES = 50000


def get_events(limit: int = 100, after: str | None = None, unread_only: bool = False) -> list[dict]:
    instance_key = _active_instance_key()
    job = _active_job_key()
    if not instance_key or not job:
        return []

    query = """
        SELECT e.id, e.job, e.event, e.summary, e.ts,
               COALESCE(json_extract(e.links, '$'), '{}') as links,
               COALESCE(json_extract(e.meta, '$'), '{}') as meta,
               (r.event_id IS NOT NULL) as read
        FROM log_events e
        LEFT JOIN log_read_state r ON r.instance_key=e.instance_key AND r.event_id=e.id
        WHERE e.instance_key=? AND e.job=?
    """
    params = [instance_key, job]

    if after:
        query += " AND e.ts > ?"
        params.append(after)

    query += " ORDER BY e.ts DESC LIMIT ?"
    params.append(MAX_LOG_LINES)

    rows = _db.query_all(query, tuple(params))
    events = []
    for row in rows:
        ev = dict(row)
        ev["links"] = json.loads(ev.get("links") or "{}")
        ev["meta"] = json.loads(ev.get("meta") or "{}")
        ev["read"] = bool(ev["read"])
        if unread_only and ev["read"]:
            continue
        events.append(ev)

    return events[:limit]


def dismiss(event_id: str):
    _dismiss_ids({event_id})


def dismiss_ids(event_ids) -> int:
    """Dismiss a batch in a single atomic write. Returns how many were added."""
    ids = set(event_ids) if not isinstance(event_ids, set) else event_ids
    return _dismiss_ids(ids)


def _dismiss_ids(new_ids: set) -> int:
    instance_key = _active_instance_key()
    if not instance_key:
        return 0
    existing = {r["event_id"] for r in _db.query_all(
        "SELECT event_id FROM log_read_state WHERE instance_key=?", (instance_key,)
    )}
    to_add = new_ids - existing
    for eid in to_add:
        _db.execute("INSERT OR IGNORE INTO log_read_state(instance_key, event_id) VALUES(?,?)", (instance_key, eid))
    return len(to_add)


def dismiss_all():
    instance_key = _active_instance_key()
    job = _active_job_key()
    if not instance_key or not job:
        return
    with _db.tx() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO log_read_state(instance_key, event_id) "
            "SELECT instance_key, id FROM log_events WHERE instance_key=? AND job=?",
            (instance_key, job)
        )
        conn.execute(
            "DELETE FROM log_events WHERE instance_key=? AND job=? AND id NOT IN ("
            "  SELECT id FROM log_events WHERE instance_key=? AND job=? ORDER BY ts DESC LIMIT ?"
            ")",
            (instance_key, job, instance_key, job, MAX_LOG_LINES)
        )
