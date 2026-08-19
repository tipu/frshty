import threading
from datetime import datetime, timedelta, timezone

import core.db as db

STALE_AFTER_MINUTES = 30
DONE_WINDOW_DAYS = 7
GROUPS = ("needs_you", "agent_working", "waiting_external", "failed_stale", "done")

launch_lock = threading.Lock()

_EVENT_TRANSITIONS = {
    "SessionStart": ("running", "agent_working", None),
    "UserPromptSubmit": ("running", "agent_working", None),
    "Stop": ("stopped", "needs_you", "Claude finished its turn"),
    "Notification": (None, "needs_you", "Notification"),
    "SessionEnd": ("finished", "needs_you", "Session ended"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_item(objective: str, scope: str = "ad-hoc", scope_ref: str = "",
                instance_key: str | None = None) -> int:
    now = _now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_items(objective, scope, scope_ref, instance_key, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (objective, scope, scope_ref, instance_key, now, now),
        )
        return cur.lastrowid


def add_run(item_id: int, session_id: str, tmux_key: str, cwd: str,
            provider: str = "claude") -> int:
    now = _now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_runs(work_item_id, provider, session_id, tmux_key, cwd, started_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (item_id, provider, session_id, tmux_key, cwd, now),
        )
        run_id = cur.lastrowid
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'run_created', '{}', ?)",
            (item_id, run_id, now),
        )
        return run_id


def mark_launch_failed(run_id: int, error: str) -> None:
    now = _now()
    with db.tx() as c:
        row = c.execute("SELECT work_item_id FROM work_runs WHERE id = ?", (run_id,)).fetchone()
        if not row:
            return
        item_id = row["work_item_id"]
        c.execute("UPDATE work_runs SET status = 'launch_failed', finished_at = ? WHERE id = ?",
                  (now, run_id))
        c.execute(
            "UPDATE work_items SET state = 'failed_stale', stop_reason = ?, updated_at = ? WHERE id = ?",
            (f"launch failed: {error}", now, item_id),
        )
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'launch_failed', ?, ?)",
            (item_id, run_id, db.dump_json({"error": error}), now),
        )


def record_event(session_id: str, kind: str, payload: dict) -> bool:
    transition = _EVENT_TRANSITIONS.get(kind)
    if transition is None:
        return False
    run_status, item_state, default_reason = transition
    now = _now()
    with db.tx() as c:
        run = c.execute(
            "SELECT id, work_item_id FROM work_runs WHERE session_id = ?", (session_id,)
        ).fetchone()
        if not run:
            return False
        item = c.execute(
            "SELECT state FROM work_items WHERE id = ?", (run["work_item_id"],)
        ).fetchone()
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (run["work_item_id"], run["id"], kind, db.dump_json(payload), now),
        )
        run_sets = ["transcript_path = COALESCE(NULLIF(?, ''), transcript_path)",
                    "transcript_cursor = MAX(transcript_cursor, ?)"]
        run_params: list = [payload.get("transcript_path") or "", int(payload.get("transcript_cursor") or 0)]
        if run_status:
            run_sets.append("status = ?")
            run_params.append(run_status)
        if kind == "SessionEnd":
            run_sets.append("finished_at = ?")
            run_params.append(now)
        run_params.append(run["id"])
        c.execute(f"UPDATE work_runs SET {', '.join(run_sets)} WHERE id = ?", tuple(run_params))
        if item_state == "needs_you" and item["state"] == "done":
            return True
        if item_state:
            reason = ""
            if item_state == "needs_you":
                reason = payload.get("message") or payload.get("reason") or default_reason
                excerpt = (payload.get("last_assistant_message") or "").strip()
                if excerpt:
                    reason = f"{reason}: {excerpt[:300]}"
            c.execute(
                "UPDATE work_items SET state = ?, stop_reason = ?, updated_at = ? WHERE id = ?",
                (item_state, reason, now, run["work_item_id"]),
            )
    return True


def apply_action(item_id: int, action: str, until: str | None = None) -> dict:
    now = _now()
    with db.tx() as c:
        item = c.execute("SELECT id, state FROM work_items WHERE id = ?", (item_id,)).fetchone()
        if not item:
            return {"error": "unknown work item"}
        if action == "done":
            c.execute("UPDATE work_items SET state = 'done', stop_reason = '', updated_at = ? WHERE id = ?",
                      (now, item_id))
        elif action == "snooze":
            if not until:
                return {"error": "snooze requires until"}
            c.execute(
                "UPDATE work_items SET state = 'waiting_external', snoozed_until = ?, updated_at = ? WHERE id = ?",
                (until, now, item_id),
            )
        elif action == "reopen":
            c.execute(
                "UPDATE work_items SET state = 'needs_you', snoozed_until = NULL, updated_at = ? WHERE id = ?",
                (now, item_id),
            )
        else:
            return {"error": f"unknown action: {action}"}
        c.execute(
            "INSERT INTO work_events(work_item_id, kind, payload, created_at) VALUES (?, ?, ?, ?)",
            (item_id, f"operator_{action}", db.dump_json({"until": until}), now),
        )
    return {"id": item_id, "action": action}


def grouped_items(now: datetime | None = None) -> dict[str, list[dict]]:
    now = now or datetime.now(timezone.utc)
    stale_cutoff = (now - timedelta(minutes=STALE_AFTER_MINUTES)).isoformat()
    done_cutoff = (now - timedelta(days=DONE_WINDOW_DAYS)).isoformat()
    now_iso = now.isoformat()
    rows = db.query_all(
        "SELECT i.*, "
        "(SELECT session_id FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_session_id, "
        "(SELECT tmux_key FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_tmux_key "
        "FROM work_items i ORDER BY i.priority DESC, i.updated_at DESC"
    )
    groups: dict[str, list[dict]] = {g: [] for g in GROUPS}
    for row in rows:
        state = row["state"]
        if state == "done":
            if row["updated_at"] >= done_cutoff:
                groups["done"].append(row)
            continue
        if state == "waiting_external" and row["snoozed_until"] and row["snoozed_until"] <= now_iso:
            row["state"] = "needs_you"
            groups["needs_you"].append(row)
            continue
        if state == "agent_working" and row["updated_at"] < stale_cutoff:
            groups["failed_stale"].append(row)
            continue
        groups[state].append(row)
    return groups
