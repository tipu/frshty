import json
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone

import core.db as db

STALE_AFTER_MINUTES = 30
DONE_WINDOW_DAYS = 7
GROUPS = ("needs_you", "agent_working", "waiting_external", "failed_stale", "done")

launch_lock = threading.Lock()

TMUX_SOCKET = os.path.expanduser("~/.frshty-tmux")
DONE_MARKER = "WORK_DONE"
ARTIFACT_MARKER = "ARTIFACT:"
CONTINUE_PROMPT = (
    "Continue toward the objective. If you are blocked on a decision only the "
    "operator can make, ask the question and stop. Never send outward "
    "communications (Slack messages, GitHub or Bitbucket comments, emails, "
    "posts to external services) unless the operator explicitly asked for that "
    "in this conversation; draft the content and ask instead. If the objective "
    f"is fully met, end your message with the single line {DONE_MARKER}."
)

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
        elif action in ("autocontinue_on", "autocontinue_off"):
            c.execute("UPDATE work_items SET autocontinue = ?, updated_at = ? WHERE id = ?",
                      (1 if action == "autocontinue_on" else 0, now, item_id))
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


def tmux_send(tmux_key: str, text: str) -> bool:
    session = f"term-{tmux_key}"
    tmux = "tmux"
    for candidate in ("/usr/bin/tmux", "/opt/homebrew/bin/tmux", os.path.expanduser("~/.local/bin/tmux")):
        if os.path.exists(candidate):
            tmux = candidate
            break
    alive = subprocess.run([tmux, "-S", TMUX_SOCKET, "has-session", "-t", session],
                           capture_output=True)
    if alive.returncode != 0:
        return False
    sent = subprocess.run([tmux, "-S", TMUX_SOCKET, "send-keys", "-t", session, text],
                          capture_output=True)
    if sent.returncode != 0:
        return False
    time.sleep(0.4)
    enter = subprocess.run([tmux, "-S", TMUX_SOCKET, "send-keys", "-t", session, "Enter"],
                          capture_output=True)
    return enter.returncode == 0


def last_assistant_text(transcript_path: str) -> str:
    if not transcript_path or not os.path.isfile(transcript_path):
        return ""
    try:
        with open(transcript_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 131072))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return ""
    text = ""
    for line in tail.splitlines():
        if '"type":"assistant"' not in line and '"type": "assistant"' not in line:
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        content = (d.get("message") or {}).get("content") or []
        parts = [b.get("text", "") for b in content
                 if isinstance(b, dict) and b.get("type") == "text"]
        if parts:
            text = "\n".join(parts)
    return text.strip()


def _assistant_texts(transcript_path: str) -> list[str]:
    if not transcript_path or not os.path.isfile(transcript_path):
        return []
    try:
        with open(transcript_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 262144))
            raw = f.read().decode("utf-8", errors="replace")
    except OSError:
        return []
    texts = []
    for line in raw.splitlines():
        if '"type":"assistant"' not in line and '"type": "assistant"' not in line:
            continue
        try:
            d = json.loads(line)
        except ValueError:
            continue
        content = (d.get("message") or {}).get("content") or []
        for b in content:
            if isinstance(b, dict) and b.get("type") == "text" and b.get("text"):
                texts.append(b["text"])
    return texts


def record_artifacts(session_id: str, transcript_path: str) -> int:
    found = []
    lines = [ln for t in _assistant_texts(transcript_path) for ln in t.splitlines()]
    for line in lines:
        idx = line.find(ARTIFACT_MARKER)
        if idx < 0:
            continue
        rest = line[idx + len(ARTIFACT_MARKER):].strip().replace("\u2014", " - ").replace("—", " - ")
        for sep in (" — ", " - ", " -- "):
            if sep in rest:
                path, note = rest.split(sep, 1)
                break
        else:
            path, note = rest, ""
        path = path.strip()
        if path.startswith("/") and len(path) < 500:
            found.append((path, note.strip()[:200]))
    if not found:
        return 0
    now = _now()
    added = 0
    with db.tx() as c:
        run = c.execute(
            "SELECT id, work_item_id FROM work_runs WHERE session_id = ?", (session_id,)
        ).fetchone()
        if not run:
            return 0
        for path, note in found:
            cur = c.execute(
                "INSERT OR IGNORE INTO work_artifacts(work_item_id, work_run_id, path, note, created_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (run["work_item_id"], run["id"], path, note, now),
            )
            added += cur.rowcount
    return added


def find_artifacts(query: str = "", limit: int = 20) -> list[dict]:
    like = f"%{query}%"
    return db.query_all(
        "SELECT a.id, a.path, a.note, a.created_at, a.work_item_id, i.objective "
        "FROM work_artifacts a LEFT JOIN work_items i ON i.id = a.work_item_id "
        "WHERE a.path LIKE ? OR a.note LIKE ? OR i.objective LIKE ? "
        "ORDER BY a.id DESC LIMIT ?",
        (like, like, like, limit),
    )


def _looks_like_question(text: str) -> bool:
    return "?" in text[-300:]


def maybe_autocontinue(session_id: str, transcript_path: str) -> str:
    tail = last_assistant_text(transcript_path)
    now = _now()
    with db.tx() as c:
        run = c.execute(
            "SELECT id, work_item_id, tmux_key FROM work_runs WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if not run:
            return "unknown_session"
        item = c.execute(
            "SELECT state, autocontinue, continues_used, continue_cap "
            "FROM work_items WHERE id = ?", (run["work_item_id"],),
        ).fetchone()
        if not item or item["state"] != "needs_you":
            return "not_applicable"
        excerpt = tail[:300]
        if DONE_MARKER in tail:
            c.execute(
                "UPDATE work_items SET state = 'done', stop_reason = '', "
                "current_checkpoint = ?, updated_at = ? WHERE id = ?",
                (excerpt, now, run["work_item_id"]),
            )
            c.execute("UPDATE work_runs SET status = 'finished', finished_at = ? WHERE id = ?",
                      (now, run["id"]))
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'self_reported_done', ?, ?)",
                (run["work_item_id"], run["id"], db.dump_json({"tail": excerpt}), now),
            )
            return "done"
        if excerpt:
            c.execute("UPDATE work_items SET stop_reason = ? WHERE id = ?",
                      (excerpt, run["work_item_id"]))
        if _looks_like_question(tail):
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'question_detected', '{}', ?)",
                (run["work_item_id"], run["id"], now),
            )
            return "question"
        if not item["autocontinue"] or item["continues_used"] >= item["continue_cap"]:
            return "capped" if item["autocontinue"] else "disabled"
        if not tmux_send(run["tmux_key"], CONTINUE_PROMPT):
            c.execute("UPDATE work_items SET stop_reason = 'tmux session gone', "
                      "state = 'failed_stale', updated_at = ? WHERE id = ?",
                      (now, run["work_item_id"]))
            return "session_gone"
        c.execute(
            "UPDATE work_items SET state = 'agent_working', continues_used = continues_used + 1, "
            "updated_at = ? WHERE id = ?", (now, run["work_item_id"]),
        )
        c.execute("UPDATE work_runs SET status = 'running' WHERE id = ?", (run["id"],))
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'auto_continued', ?, ?)",
            (run["work_item_id"], run["id"], db.dump_json({"n": item["continues_used"] + 1}), now),
        )
    return "continued"


def reply(item_id: int, text: str) -> dict:
    now = _now()
    with db.tx() as c:
        run = c.execute(
            "SELECT id, tmux_key FROM work_runs WHERE work_item_id = ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        if not run:
            return {"error": "no run for this item"}
    if not tmux_send(run["tmux_key"], text):
        return {"error": "tmux session gone"}
    with db.tx() as c:
        c.execute(
            "UPDATE work_items SET state = 'agent_working', stop_reason = '', updated_at = ? "
            "WHERE id = ?", (now, item_id),
        )
        c.execute("UPDATE work_runs SET status = 'running' WHERE id = ?", (run["id"],))
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'operator_reply', ?, ?)",
            (item_id, run["id"], db.dump_json({"text": text[:500]}), now),
        )
    return {"id": item_id, "action": "reply"}


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
