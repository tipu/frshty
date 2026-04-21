import json
from datetime import datetime, timezone

import core.db as _db


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def emit_event(source: str, kind: str, payload: dict, instance_key: str | None = None) -> int:
    with _db.tx() as c:
        cur = c.execute(
            "INSERT INTO events(instance_key, source, kind, payload, created_at) VALUES (?, ?, ?, ?, ?)",
            (instance_key, source, kind, _db.dump_json(payload or {}), _now()),
        )
        return cur.lastrowid or 0


def enqueue_job(instance_key: str, task: str, payload: dict | None = None,
                ticket_key: str | None = None, triggering_event_id: int | None = None) -> int:
    with _db.tx() as c:
        cur = c.execute(
            "INSERT INTO jobs(instance_key, ticket_key, task, payload, enqueued_at, triggering_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (instance_key, ticket_key, task, _db.dump_json(payload or {}), _now(), triggering_event_id),
        )
        return cur.lastrowid or 0


def claim_next() -> dict | None:
    """Atomically claim the next queued job that doesn't conflict on ticket_key."""
    with _db.tx() as c:
        row = c.execute(
            """
            SELECT j.id, j.instance_key, j.ticket_key, j.task, j.payload, j.triggering_event_id
            FROM jobs j
            WHERE j.status = 'queued'
              AND NOT EXISTS (
                SELECT 1 FROM jobs r
                WHERE r.ticket_key IS NOT NULL
                  AND r.ticket_key = j.ticket_key
                  AND r.instance_key = j.instance_key
                  AND r.status = 'running'
              )
            ORDER BY j.id
            LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        updated = c.execute(
            "UPDATE jobs SET status='running', started_at=? WHERE id=? AND status='queued'",
            (_now(), row["id"]),
        ).rowcount
        if updated == 0:
            return None
        return {
            "id": row["id"],
            "instance_key": row["instance_key"],
            "ticket_key": row["ticket_key"],
            "task": row["task"],
            "payload": json.loads(row["payload"] or "{}"),
            "triggering_event_id": row["triggering_event_id"],
        }


def mark_done(job_id: int, status: str, response: dict) -> None:
    with _db.tx() as c:
        c.execute(
            "UPDATE jobs SET status=?, finished_at=?, response=? WHERE id=?",
            (status, _now(), _db.dump_json(response), job_id),
        )


def sweep_stale(max_age_seconds: int = 3600) -> int:
    """Reset stuck jobs back to queued so another worker can pick them up.

    Uses julianday() on both sides so the ISO-8601 `T`-separated started_at
    and the `datetime()`-returned space-separated threshold compare as real
    timestamps instead of lexicographic strings.
    """
    with _db.tx() as c:
        cur = c.execute(
            "UPDATE jobs SET status='queued' WHERE status='running'"
            " AND julianday(started_at) < julianday(?, '-' || ? || ' seconds')",
            (_now(), max_age_seconds),
        )
        return cur.rowcount


def undispatched_events(limit: int = 100) -> list[dict]:
    return _db.query_all(
        "SELECT id, instance_key, source, kind, payload FROM events"
        " WHERE dispatched_at IS NULL ORDER BY id LIMIT ?",
        (limit,),
    )


def mark_dispatched(event_id: int, reason: str = "") -> None:
    with _db.tx() as c:
        c.execute(
            "UPDATE events SET dispatched_at=?, dispatch_reason=? WHERE id=?",
            (_now(), reason, event_id),
        )


def set_event_instance(event_id: int, instance_key: str) -> None:
    _db.execute("UPDATE events SET instance_key=? WHERE id=?", (instance_key, event_id))


def jobs_for_ticket(instance_key: str, ticket_key: str, limit: int = 100) -> list[dict]:
    return _db.query_all(
        "SELECT id, task, status, enqueued_at, started_at, finished_at, response, payload"
        " FROM jobs WHERE instance_key=? AND ticket_key=? ORDER BY id DESC LIMIT ?",
        (instance_key, ticket_key, limit),
    )
