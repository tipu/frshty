import base64
import binascii
import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone

import core.codex_session as codex_session
import core.db as db

STALE_AFTER_MINUTES = 30
STUCK_AFTER_MINUTES = 90
BG_WAIT_RECHECK_HOURS = 2
PROPOSED_STATE = "proposed"
DECLINED_REASON = "Proposal declined"
GROUPS = ("proposed", "needs_ack", "needs_you", "agent_working", "waiting_external",
          "failed_stale", "done")
FINISHED_STATES = ("needs_ack", "done")
FINISHED_STATES_SQL = "('needs_ack', 'done')"
_ACK_EVENT_KINDS_SQL = "('operator_done', 'operator_ack')"

launch_lock = threading.Lock()
_send_locks: dict[str, threading.Lock] = {}
_send_locks_guard = threading.Lock()

TMUX_SOCKET = os.path.expanduser("~/.frshty-tmux")
AGENTS = ("claude", "codex")
DONE_MARKER = "WORK_DONE"
ARTIFACT_MARKER = "ARTIFACT:"
_IMAGE_ID_RE = re.compile(r"^(\d+)-(\d+)$")
_IMAGE_MEDIA_RE = re.compile(r"^image/[a-z0-9.+-]+$")
_MAX_IMAGE_BASE64 = 64 * 1024 * 1024
CONTINUE_PROMPT = (
    "Continue toward the objective. When you hit a decision point, decide "
    "yourself by default: pick the most correct, cleanest, simplest option and "
    "keep going. Ask the operator only when you truly cannot decide — the "
    "choice is irreversible or destructive, or it depends on operator intent "
    "you cannot infer. Ask with the AskUserQuestion tool, then end your turn "
    "immediately; the work board shows the question to the operator, and when "
    "their answer arrives as your next message, resume work from it. "
    "Use that same tool for anything only the operator can supply — a "
    "secret, a one-time code, an approval — not only for a decision. A "
    "request written in prose does not reach the operator. "
    "Never send outward "
    "communications (Slack messages, GitHub or Bitbucket comments, emails, "
    "posts to external services) unless the operator explicitly asked for that "
    "in this conversation; draft the content and ask instead. If the objective "
    f"is fully met, end your message with the single line {DONE_MARKER}."
)

_OPERATOR_ASK_RE = re.compile(
    r"(?:^|[.!?\n]\s+|\u2014\s*)(?:please\s+)?"
    r"(?:send|paste|provide|reply with|confirm|approve)\b"
    r"|\b(?:send|paste|give|provide|share|tell|hand)\s+(?:me|us|it over|them)\b"
    r"|\bwaiting (?:on|for) (?:you|your)\b"
    r"|\blet me know\b"
    r"|\bfrom you\b",
    re.IGNORECASE,
)

_IDLE_STOP_KINDS_SQL = "'Stop', 'Notification', 'SessionEnd'"
_DECIDED_KINDS_SQL = ("'auto_continued', 'question_detected', 'operator_reply', "
                      "'self_reported_done', 'stuck_tool'")

_EVENT_TRANSITIONS = {
    "SessionStart": ("running", "agent_working", None),
    "UserPromptSubmit": ("running", "agent_working", None),
    "Stop": ("stopped", "needs_you", "The agent finished its turn"),
    "Notification": (None, "needs_you", "Notification"),
    "SessionEnd": ("finished", "needs_you", "Session ended"),
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


_BG_LINE_MARKERS = ("running in background with ID:", "Monitor started (task",
                    "agentId:", "Task ID:", "<task-notification>", '"TaskStop"')
_WAKEUP_PROMPT_MARKER = "<task-notification>"
_BG_START_RES = (
    re.compile(r"running in background with ID: ([A-Za-z0-9_-]+)"),
    re.compile(r"Monitor started \(task ([A-Za-z0-9_-]+)"),
    re.compile(r"agentId: ([A-Za-z0-9_-]+)"),
    re.compile(r"Workflow launched in background\. Task ID: ([A-Za-z0-9_-]+)"),
)
_BG_NOTIF_ID_RE = re.compile(r"<task-id>([A-Za-z0-9_-]+)</task-id>")
_BG_NOTIF_TERMINAL_RE = re.compile(r"<status>(?:completed|failed|killed|stopped)</status>")


def _bg_scan_result_text(text: str, started: set[str], ended: set[str]) -> None:
    for pattern in _BG_START_RES:
        for m in pattern.finditer(text):
            started.add(m.group(1))
    if "<task-notification>" in text and _BG_NOTIF_TERMINAL_RE.search(text):
        m = _BG_NOTIF_ID_RE.search(text)
        if m:
            ended.add(m.group(1))


def pending_background_tasks(transcript_path: str) -> set[str]:
    if not transcript_path or not os.path.isfile(transcript_path):
        return set()
    started: set[str] = set()
    ended: set[str] = set()
    try:
        with open(transcript_path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if not any(marker in line for marker in _BG_LINE_MARKERS):
                    continue
                try:
                    d = json.loads(line)
                except ValueError:
                    continue
                if d.get("isSidechain"):
                    continue
                if isinstance(d.get("content"), str):
                    _bg_scan_result_text(d["content"], started, ended)
                attachment_prompt = (d.get("attachment") or {}).get("prompt")
                if isinstance(attachment_prompt, str):
                    _bg_scan_result_text(attachment_prompt, started, ended)
                content = (d.get("message") or {}).get("content")
                if d.get("type") == "user":
                    if isinstance(content, str):
                        _bg_scan_result_text(content, started, ended)
                    elif isinstance(content, list):
                        for b in content:
                            if not isinstance(b, dict):
                                continue
                            if b.get("type") == "text":
                                _bg_scan_result_text(b.get("text") or "", started, ended)
                            elif b.get("type") == "tool_result":
                                inner = b.get("content")
                                if isinstance(inner, str):
                                    _bg_scan_result_text(inner, started, ended)
                                elif isinstance(inner, list):
                                    for x in inner:
                                        if isinstance(x, dict) and x.get("type") == "text":
                                            _bg_scan_result_text(x.get("text") or "", started, ended)
                elif d.get("type") == "assistant" and isinstance(content, list):
                    for b in content:
                        if (isinstance(b, dict) and b.get("type") == "tool_use"
                                and b.get("name") == "TaskStop"):
                            task_id = (b.get("input") or {}).get("task_id")
                            if task_id:
                                ended.add(str(task_id))
    except OSError:
        return set()
    return started - ended


def is_idle_stop(kind: str, payload: dict) -> bool:
    if kind == "Stop":
        return True
    return kind == "Notification" and "waiting for your input" in (payload.get("message") or "")


def create_item(objective: str, scope: str = "ad-hoc", scope_ref: str = "",
                instance_key: str | None = None, contexts: str = "",
                source_item_id: int | None = None, tags: str = "") -> int:
    now = _now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_items(objective, scope, scope_ref, instance_key, contexts, "
            "source_item_id, tags, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (objective, scope, scope_ref, instance_key, contexts, source_item_id, tags, now, now),
        )
        return cur.lastrowid


def create_proposal(objective: str, note: str = "", instance_key: str | None = None,
                    contexts: str = "", tags: str = "", cwd: str = "",
                    brief: str = "", conn=None, now: str | None = None) -> int:
    """Put a task on the board that no agent has started.

    frshty writes this when it decides by itself that something needs doing.
    The operator approves it before an agent reads it, so the row carries the
    launch arguments an approval will need: the working directory and the
    brief that gives the agent the evidence frshty acted on.

    `conn` lets a caller that has to record the proposal somewhere else write
    both rows in one transaction. db.tx opens its own connection and takes an
    immediate lock, so a caller cannot nest it; passing the open connection is
    what makes the two writes commit or roll back together.

    `now` lets a caller stamp the row with the moment its own scan reads, so a
    count over created_at answers the same question that caller's other counts
    answer. It defaults to the wall clock."""
    if conn is not None:
        return _insert_proposal(conn, objective, note, instance_key, contexts,
                                tags, cwd, brief, now)
    with db.tx() as c:
        return _insert_proposal(c, objective, note, instance_key, contexts,
                                tags, cwd, brief, now)


def _insert_proposal(c, objective: str, note: str, instance_key: str | None,
                     contexts: str, tags: str, cwd: str, brief: str,
                     now: str | None = None) -> int:
    now = now or _now()
    cur = c.execute(
        "INSERT INTO work_items(objective, scope, instance_key, contexts, tags, "
        "state, current_checkpoint, launch_cwd, launch_brief, created_at, updated_at) "
        "VALUES (?, 'proposal', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (objective, instance_key, contexts, tags, PROPOSED_STATE, note, cwd,
         brief, now, now),
    )
    item_id = cur.lastrowid
    c.execute(
        "INSERT INTO work_events(work_item_id, kind, payload, created_at) "
        "VALUES (?, 'proposal_created', ?, ?)",
        (item_id, db.dump_json({"note": note}), now),
    )
    return item_id


def claim_proposal(item_id: int) -> bool:
    """Take a proposal off the board so exactly one approval can launch it.

    Two clicks on Approve race, and the loser must not start a second agent
    on the same objective. The state flip is the claim: it succeeds once."""
    now = _now()
    with db.tx() as c:
        claimed = c.execute(
            "UPDATE work_items SET state = 'agent_working', updated_at = ? "
            "WHERE id = ? AND state = ?", (now, item_id, PROPOSED_STATE))
        if claimed.rowcount != 1:
            return False
        c.execute(
            "INSERT INTO work_events(work_item_id, kind, payload, created_at) "
            "VALUES (?, 'proposal_approved', '{}', ?)", (item_id, now))
        return True


def has_run(item_id: int) -> bool:
    """Whether any agent session was ever created for this work item."""
    row = db.query_one("SELECT 1 AS present FROM work_runs WHERE work_item_id = ? LIMIT 1",
                       (item_id,))
    return bool(row)


def release_proposal(item_id: int) -> None:
    """Put a claimed proposal back when its launch never started an agent."""
    now = _now()
    with db.tx() as c:
        c.execute(
            "UPDATE work_items SET state = ?, updated_at = ? "
            "WHERE id = ? AND state = 'agent_working'", (PROPOSED_STATE, now, item_id))


def add_run(item_id: int, session_id: str, tmux_key: str, cwd: str,
            provider: str = "claude", env_recorded: bool = False,
            env_key: str = "", env_config_dir: str = "") -> int:
    """Record one agent session of a work item.

    `env_recorded` says the caller resolved the agent environment and wrote it
    into env_key and env_config_dir. An empty directory then means the run uses
    no configuration directory, which is a different statement from a run that
    never recorded one."""
    now = _now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_runs(work_item_id, provider, session_id, tmux_key, cwd, "
            "env_recorded, env_key, env_config_dir, started_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (item_id, provider, session_id, tmux_key, cwd, 1 if env_recorded else 0,
             env_key, env_config_dir, now),
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


def is_wakeup_prompt(prompt: str) -> bool:
    """True when a submitted prompt is a background wake-up, not an answer.

    A finished background task, a Monitor event and a workflow result all
    reach the session as a user prompt, so each one fires UserPromptSubmit
    exactly like a reply typed by the operator. None of them answers
    anything, so a question waiting on the board must survive them."""
    return _WAKEUP_PROMPT_MARKER in (prompt or "")


def record_event(session_id: str, kind: str, payload: dict) -> bool:
    transition = _EVENT_TRANSITIONS.get(kind)
    if transition is None:
        return False
    run_status, item_state, default_reason = transition
    bg_pending = False
    if item_state == "needs_you" and is_idle_stop(kind, payload):
        transcript_path = payload.get("transcript_path") or ""
        tail = last_assistant_text(transcript_path)
        final_lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
        finished = bool(final_lines) and final_lines[-1] == DONE_MARKER
        if not finished and not _blocked_on_operator(tail):
            bg_pending = bool(pending_background_tasks(transcript_path))
    now = _now()
    with db.tx() as c:
        run = c.execute(
            "SELECT id, work_item_id FROM work_runs WHERE session_id = ?", (session_id,)
        ).fetchone()
        if not run:
            return False
        item = c.execute(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (run["work_item_id"],)
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
        if item["state"] in FINISHED_STATES:
            return True
        if item_state:
            reason = ""
            snooze = None
            if item_state == "needs_you":
                reason = payload.get("message") or payload.get("reason") or default_reason
                excerpt = (payload.get("last_assistant_message") or "").strip()
                if excerpt:
                    reason = f"{reason}: {excerpt[:300]}"
                if bg_pending and not item["pending_question"]:
                    item_state = "waiting_external"
                    snooze = (datetime.now(timezone.utc)
                              + timedelta(hours=BG_WAIT_RECHECK_HOURS)).isoformat()
                    reason = f"Waiting on a background task: {excerpt[:300]}" if excerpt \
                        else "Waiting on a background task"
            if kind == "UserPromptSubmit":
                if item["pending_question"] and is_wakeup_prompt(payload.get("prompt") or ""):
                    c.execute("UPDATE work_items SET updated_at = ? WHERE id = ?",
                              (now, run["work_item_id"]))
                else:
                    c.execute(
                        "UPDATE work_items SET state = ?, stop_reason = ?, pending_question = '', "
                        "snoozed_until = NULL, updated_at = ? WHERE id = ?",
                        (item_state, reason, now, run["work_item_id"]),
                    )
            else:
                c.execute(
                    "UPDATE work_items SET state = ?, stop_reason = ?, snoozed_until = ?, "
                    "updated_at = ? WHERE id = ?",
                    (item_state, reason, snooze, now, run["work_item_id"]),
                )
    return True


def record_agent_session(session_id: str, agent_session_id: str) -> bool:
    """Store the agent's own conversation id for a run. Codex mints its
    thread id itself, and a resume needs that id, not the work session id."""
    agent_session_id = (agent_session_id or "").strip()
    if not agent_session_id:
        return False
    with db.tx() as c:
        cur = c.execute(
            "UPDATE work_runs SET agent_session_id = ? WHERE session_id = ? "
            "AND agent_session_id != ?", (agent_session_id, session_id, agent_session_id))
        return bool(cur.rowcount)


def record_question(session_id: str, tool_input: dict) -> bool:
    questions = (tool_input or {}).get("questions") or []
    questions = [q for q in questions if isinstance(q, dict) and (q.get("question") or "").strip()]
    if not questions:
        return False
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
        if not item or item["state"] in FINISHED_STATES:
            return False
        first = (questions[0].get("question") or "").strip()
        c.execute(
            "UPDATE work_items SET state = 'needs_you', stop_reason = ?, "
            "pending_question = ?, updated_at = ? WHERE id = ?",
            (first[:300], db.dump_json({"questions": questions}), now, run["work_item_id"]),
        )
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'question_asked', ?, ?)",
            (run["work_item_id"], run["id"], db.dump_json({"questions": questions}), now),
        )
    return True


def record_gate(session_id: str, kind: str, verdict: str, payload: dict) -> bool:
    now = _now()
    with db.tx() as c:
        run = c.execute(
            "SELECT id, work_item_id FROM work_runs WHERE session_id = ?", (session_id,)
        ).fetchone()
        if not run:
            return False
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (run["work_item_id"], run["id"], kind,
             db.dump_json({**payload, "verdict": verdict}), now),
        )
    return True


def apply_action(item_id: int, action: str, until: str | None = None) -> dict:
    now = _now()
    with db.tx() as c:
        item = c.execute("SELECT id, state FROM work_items WHERE id = ?", (item_id,)).fetchone()
        if not item:
            return {"error": "unknown work item"}
        if item["state"] == PROPOSED_STATE and action != "decline":
            # A proposal has no run. Every other action would leave it in a
            # state that assumes one: snooze parks it in waiting_external and
            # the board then shows it as needs_you with nothing to reply to,
            # and none of them can be undone back to proposed.
            return {"error": "a proposal can only be approved or declined"}
        if action == "done":
            c.execute("UPDATE work_items SET state = 'done', stop_reason = '', "
                      "pending_question = '', updated_at = ? WHERE id = ?",
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
                "UPDATE work_items SET state = 'needs_you', snoozed_until = NULL, "
                "archived_at = NULL, updated_at = ? WHERE id = ?",
                (now, item_id),
            )
        elif action == "ack":
            if item["state"] != "needs_ack":
                return {"error": "only an agent-reported task can be acknowledged"}
            c.execute("UPDATE work_items SET state = 'done', stop_reason = '', "
                      "pending_question = '', updated_at = ? WHERE id = ?",
                      (now, item_id))
        elif action == "decline":
            if item["state"] != PROPOSED_STATE:
                return {"error": "only a proposed task can be declined"}
            c.execute(
                "UPDATE work_items SET state = 'done', archived_at = ?, "
                "stop_reason = ?, updated_at = ? WHERE id = ?",
                (now, DECLINED_REASON, now, item_id),
            )
        elif action == "archive":
            if item["state"] not in FINISHED_STATES:
                return {"error": "only a completed task can be archived"}
            if item["state"] == "needs_ack":
                c.execute("UPDATE work_items SET state = 'done', stop_reason = '', "
                          "pending_question = '', updated_at = ? WHERE id = ?",
                          (now, item_id))
                c.execute(
                    "INSERT INTO work_events(work_item_id, kind, payload, created_at) "
                    "VALUES (?, 'operator_ack', '{}', ?)", (item_id, now))
            c.execute("UPDATE work_items SET archived_at = ? WHERE id = ?", (now, item_id))
        elif action == "unarchive":
            c.execute("UPDATE work_items SET archived_at = NULL WHERE id = ?", (item_id,))
        else:
            return {"error": f"unknown action: {action}"}
        c.execute(
            "INSERT INTO work_events(work_item_id, kind, payload, created_at) VALUES (?, ?, ?, ?)",
            (item_id, f"operator_{action}", db.dump_json({"until": until}), now),
        )
    return {"id": item_id, "action": action}


def _tmux_bin() -> str:
    for candidate in ("/usr/bin/tmux", "/opt/homebrew/bin/tmux", os.path.expanduser("~/.local/bin/tmux")):
        if os.path.exists(candidate):
            return candidate
    return "tmux"


def _pane_lock(session: str) -> threading.Lock:
    with _send_locks_guard:
        return _send_locks.setdefault(session, threading.Lock())


def agent_running(tmux_key: str, agent: str = "claude") -> bool:
    session = f"term-{tmux_key}"
    tmux = _tmux_bin()
    panes = subprocess.run([tmux, "-S", TMUX_SOCKET, "list-panes", "-t", session, "-F", "#{pane_pid}"],
                           capture_output=True, text=True)
    if panes.returncode != 0 or not panes.stdout.strip():
        return False
    pane_pid = panes.stdout.strip().splitlines()[0]
    check = subprocess.run(["pgrep", "-P", pane_pid, "-f", agent if agent in AGENTS else "claude"],
                           capture_output=True, text=True)
    return bool(check.stdout.strip())


def pane_activity(tmux_key: str) -> str:
    """Last-output time of the pane as an ISO timestamp, or "" when the tmux
    session is gone. A codex rollout is written only between tool calls, so
    the pane is the freshness signal while one long command runs. The format
    is `window_activity`: tmux advances that one on pane output, while
    `session_activity` stays frozen at the time the session was created and
    reports every live pane as idle."""
    result = subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "display-message", "-p", "-t", f"term-{tmux_key}",
         "#{window_activity}"], capture_output=True, text=True)
    stamp = result.stdout.strip()
    if result.returncode != 0 or not stamp.isdigit():
        return ""
    return datetime.fromtimestamp(int(stamp), timezone.utc).isoformat()


def tmux_send(tmux_key: str, text: str) -> bool:
    session = f"term-{tmux_key}"
    tmux = _tmux_bin()
    with _pane_lock(session):
        alive = subprocess.run([tmux, "-S", TMUX_SOCKET, "has-session", "-t", session],
                               capture_output=True)
        if alive.returncode != 0:
            return False
        if not close_btw_panel(session):
            return False
        sent = subprocess.run([tmux, "-S", TMUX_SOCKET, "send-keys", "-t", session, "-l", "--", text],
                              capture_output=True)
        if sent.returncode != 0:
            return False
        time.sleep(0.4)
        enter = subprocess.run([tmux, "-S", TMUX_SOCKET, "send-keys", "-t", session, "Enter"],
                              capture_output=True)
        return enter.returncode == 0


BTW_ANSWER_TIMEOUT = 120
_BTW_CLOSE_HINT = "Esc to close"
_BTW_DONE_HINT = "c to copy"
_BTW_SCROLL_LIMIT = 200
_BTW_SCROLL_SETTLES = 3
_BTW_SCROLL_SETTLE_SECONDS = 0.06


def _tmux_run(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run([_tmux_bin(), "-S", TMUX_SOCKET, *args],
                          capture_output=True, text=True)


def _capture_pane(session: str) -> list[str]:
    out = _tmux_run("capture-pane", "-t", session, "-p")
    return out.stdout.splitlines() if out.returncode == 0 else []


def btw_overlay(lines: list[str]) -> dict | None:
    """The /btw panel drawn over the pane, or None when no panel is open.

    The panel lists the questions asked so far, then the selected answer, then
    a footer of key hints. The footer offers the copy hint only once the answer
    is complete, so it doubles as the done signal."""
    footer = None
    for i in range(len(lines) - 1, -1, -1):
        if _BTW_CLOSE_HINT in lines[i]:
            footer = i
            break
    if footer is None:
        return None
    start, asked = footer, ""
    for i in range(footer - 1, -1, -1):
        stripped = lines[i].lstrip()
        if stripped.startswith("/btw "):
            start, asked = i + 1, stripped[len("/btw "):].strip()
            break
    body = [ln.rstrip() for ln in lines[start:footer]]
    while body and not body[0]:
        body.pop(0)
    while body and not body[-1]:
        body.pop()
    return {"done": _BTW_DONE_HINT in lines[footer], "asked": asked, "body": body}


def close_btw_panel(session: str) -> bool:
    """Close an open /btw panel and report whether the pane takes keys again.

    The panel swallows every keystroke while it is open, so a reply or a
    continue prompt sent into the pane would be lost with nothing to show for
    it. A pane with no panel is already open for keys."""
    if not btw_overlay(_capture_pane(session)):
        return True
    _tmux_run("send-keys", "-t", session, "Escape")
    time.sleep(0.4)
    return btw_overlay(_capture_pane(session)) is None


def _merge_window(body: list[str], window: list[str]) -> bool:
    """Append the part of a scrolled panel view that is not in body yet."""
    if not window:
        return False
    for k in range(min(len(body), len(window)), 0, -1):
        if body[-k:] == window[:k]:
            rest = window[k:]
            body.extend(rest)
            return bool(rest)
    body.extend(window)
    return True


def _btw_answer_text(body: list[str]) -> str:
    rows = list(body)
    while rows and not rows[0].strip():
        rows.pop(0)
    while rows and not rows[-1].strip():
        rows.pop()
    if not rows:
        return ""
    pad = min(len(r) - len(r.lstrip()) for r in rows if r.strip())
    return "\n".join(r[pad:] if r.strip() else "" for r in rows)


def ask_btw(tmux_key: str, question: str, timeout: float = BTW_ANSWER_TIMEOUT) -> dict:
    """Ask a /btw side question in the pane and read the answer back.

    /btw answers from a fork of the conversation, so the main turn keeps
    running and the question never enters it. The exchange is not written to
    the session transcript, so the answer is only readable from the pane
    panel. The panel swallows keystrokes while it is open, so a stale panel is
    closed first and ours is closed at the end. Answers taller than the pane
    are stitched from successive views, one Down key at a time."""
    session = f"term-{tmux_key}"
    question = " ".join(question.split())
    with _pane_lock(session):
        if _tmux_run("has-session", "-t", session).returncode != 0:
            return {"error": "tmux session gone"}
        if not close_btw_panel(session):
            return {"error": "a /btw panel is stuck open in the terminal"}
        if _tmux_run("send-keys", "-t", session, "-l", "--",
                     f"/btw {question}").returncode != 0:
            return {"error": "tmux send failed"}
        time.sleep(0.4)
        if _tmux_run("send-keys", "-t", session, "Enter").returncode != 0:
            return {"error": "tmux send failed"}
        panel = None
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            time.sleep(0.5)
            found = btw_overlay(_capture_pane(session))
            if found and found["done"]:
                panel = found
                break
        if panel is None:
            close_btw_panel(session)
            return {"error": f"no /btw answer within {int(timeout)}s"}
        head = panel["asked"].rstrip("\u2026").rstrip()
        if not head or not question.startswith(head):
            close_btw_panel(session)
            return {"error": "the terminal shows an answer to a different /btw question"}
        body = panel["body"]
        for _ in range(_BTW_SCROLL_LIMIT):
            _tmux_run("send-keys", "-t", session, "Down")
            grew = False
            for _ in range(_BTW_SCROLL_SETTLES):
                time.sleep(_BTW_SCROLL_SETTLE_SECONDS)
                scrolled = btw_overlay(_capture_pane(session))
                if not scrolled:
                    break
                if _merge_window(body, scrolled["body"]):
                    grew = True
                    break
            if not grew:
                break
        close_btw_panel(session)
    answer = _btw_answer_text(body)
    return {"answer": answer} if answer else {"error": "empty /btw answer"}


def last_assistant_text(transcript_path: str) -> str:
    if not transcript_path or not os.path.isfile(transcript_path):
        return ""
    if codex_session.is_rollout(transcript_path):
        return codex_session.last_assistant_text(transcript_path)
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
    if codex_session.is_rollout(transcript_path):
        return codex_session.assistant_texts(transcript_path)
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


def record_artifacts(session_id: str, transcript_path: str,
                     texts: list[str] | None = None) -> int:
    found = []
    source = _assistant_texts(transcript_path) if texts is None else texts
    lines = [ln for t in source for ln in t.splitlines()]
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


def _salient_arg(name: str, inp: dict) -> str:
    if not isinstance(inp, dict):
        return ""
    for key in ("command", "file_path", "path", "description", "prompt", "url", "query"):
        v = inp.get(key)
        if v:
            return str(v).replace("\n", " ")[:120]
    return ""


def _tail_json_records(path: str, max_bytes: int):
    """Yield complete JSONL tail records and their byte offsets.

    The first line is allowed to exceed ``max_bytes`` because an inline image
    can make one otherwise-relevant user record several megabytes long.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            start = max(0, size - max_bytes)
            cursor = start
            while cursor > 0:
                chunk_start = max(0, cursor - 65536)
                f.seek(chunk_start)
                chunk = f.read(cursor - chunk_start)
                newline = chunk.rfind(b"\n")
                if newline >= 0:
                    start = chunk_start + newline + 1
                    break
                cursor = chunk_start
            else:
                start = 0
            f.seek(start)
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                try:
                    yield json.loads(line.decode("utf-8", errors="replace")), offset
                except ValueError:
                    continue
    except OSError:
        return


def _claude_image_data(block: dict) -> tuple[str, str] | None:
    if not isinstance(block, dict) or block.get("type") != "image":
        return None
    source = block.get("source") or {}
    if isinstance(source, dict) and source.get("type") == "base64":
        media_type = str(source.get("media_type") or "").lower()
        data = source.get("data")
    else:
        file_data = block.get("file") or {}
        media_type = str(file_data.get("media_type") or "").lower()
        data = file_data.get("base64")
    if not _IMAGE_MEDIA_RE.fullmatch(media_type) or not isinstance(data, str) or not data:
        return None
    return data, media_type


def _claude_image_refs(content: list, offset: int) -> list[dict]:
    refs = []
    for block_index, block in enumerate(content):
        image = _claude_image_data(block)
        if image is not None:
            refs.append({"id": f"{offset}-{block_index}", "media_type": image[1]})
    return refs


def transcript_timeline(transcript_path: str, max_bytes: int = 4194304) -> list[dict]:
    if not transcript_path or not os.path.isfile(transcript_path):
        return []
    if codex_session.is_rollout(transcript_path):
        return codex_session.timeline(transcript_path, max_bytes)
    timeline: list[dict] = []
    for d, offset in _tail_json_records(transcript_path, max_bytes):
        t = d.get("type")
        msg = d.get("message") or {}
        if t == "user":
            if d.get("toolUseResult") is not None or d.get("isSidechain"):
                continue
            content = msg.get("content")
            if isinstance(content, str):
                text = content
                images = []
            elif isinstance(content, list):
                if any(isinstance(b, dict) and b.get("type") == "tool_result" for b in content):
                    continue
                text = " ".join(b.get("text", "") for b in content
                                if isinstance(b, dict) and b.get("type") == "text")
                images = _claude_image_refs(content, offset)
            else:
                continue
            if text.strip() or images:
                entry = {"kind": "prompt", "text": text.strip()[:500],
                         "at": d.get("timestamp", "")}
                if images:
                    entry["images"] = images
                timeline.append(entry)
        elif t == "assistant" and not d.get("isSidechain"):
            for b in msg.get("content") or []:
                if not isinstance(b, dict):
                    continue
                if b.get("type") == "tool_use":
                    timeline.append({"kind": "tool", "name": b.get("name", "?"),
                                     "arg": _salient_arg(b.get("name", ""), b.get("input")),
                                     "at": d.get("timestamp", "")})
                elif b.get("type") == "text" and b.get("text", "").strip():
                    timeline.append({"kind": "text", "text": b["text"].strip()[:2000],
                                     "at": d.get("timestamp", "")})
    return timeline


def transcript_image(transcript_path: str, image_id: str) -> tuple[bytes, str] | None:
    """Read one image referenced by :func:`transcript_timeline`."""
    if codex_session.is_rollout(transcript_path):
        return codex_session.embedded_image(transcript_path, image_id)
    match = _IMAGE_ID_RE.fullmatch(image_id or "")
    if not match or not transcript_path or not os.path.isfile(transcript_path):
        return None
    offset, block_index = (int(part) for part in match.groups())
    try:
        with open(transcript_path, "rb") as f:
            if offset < 0 or offset >= os.fstat(f.fileno()).st_size:
                return None
            f.seek(offset)
            record = json.loads(f.readline().decode("utf-8", errors="replace"))
    except (OSError, ValueError):
        return None
    if record.get("type") != "user" or record.get("toolUseResult") is not None \
            or record.get("isSidechain"):
        return None
    content = (record.get("message") or {}).get("content") or []
    if not isinstance(content, list) or block_index >= len(content):
        return None
    image = _claude_image_data(content[block_index])
    if image is None or len(image[0]) > _MAX_IMAGE_BASE64:
        return None
    try:
        return base64.b64decode(image[0], validate=True), image[1]
    except (ValueError, binascii.Error):
        return None


def resolve_transcript_path(run: dict) -> str:
    """The transcript file of a run, discovered when no hook has named it yet.

    A claude run gets its transcript path from its first hook event and a
    codex run gets it from its first notify call. A session still inside its
    first turn therefore has no path, so the item detail page shows an empty
    timeline for a run that is plainly working. An auxiliary Codex
    notification can also name a thread that has no rollout. Codex records
    the working directory and the start time in its real rollout, so a
    missing recorded thread falls back to those launch facts and the run
    keeps the corrected path and thread id once they are known."""
    path = (run.get("transcript_path") or "").strip()
    if path or run.get("provider") != "codex":
        return path
    agent_session_id = (run.get("agent_session_id") or "").strip()
    path = codex_session.rollout_path(agent_session_id) if agent_session_id else ""
    if not path:
        path = codex_session.find_rollout(run.get("cwd") or "",
                                          run.get("started_at") or "")
    if not path:
        return ""
    thread_id = codex_session.rollout_thread_id(path) or agent_session_id
    with db.tx() as c:
        claimed = c.execute(
            "UPDATE work_runs SET transcript_path = ?, agent_session_id = ? "
            "WHERE id = ? AND COALESCE(transcript_path, '') = '' "
            "AND NOT EXISTS (SELECT 1 FROM work_runs o WHERE o.transcript_path = ? "
            "AND o.id != work_runs.id)",
            (path, thread_id, run["id"], path),
        ).rowcount
    if not claimed:
        current = db.query_one("SELECT transcript_path FROM work_runs WHERE id = ?",
                               (run["id"],)) or {}
        run["transcript_path"] = (current.get("transcript_path") or "").strip()
        return run["transcript_path"]
    run["transcript_path"] = path
    run["agent_session_id"] = thread_id
    return path


def item_detail(item_id: int) -> dict:
    item = db.query_one("SELECT * FROM work_items WHERE id = ?", (item_id,))
    if not item:
        return {"error": "unknown work item"}
    if item["state"] == "waiting_external" and item["snoozed_until"] \
            and item["snoozed_until"] <= _now():
        item["state"] = "needs_you"
    runs = db.query_all("SELECT * FROM work_runs WHERE work_item_id = ? ORDER BY id", (item_id,))
    events = db.query_all(
        "SELECT id, work_run_id, kind, payload, created_at FROM work_events "
        "WHERE work_item_id = ? ORDER BY id", (item_id,))
    artifacts = db.query_all(
        "SELECT id, path, note, created_at FROM work_artifacts WHERE work_item_id = ? ORDER BY id",
        (item_id,))
    kinds = {e["kind"] for e in events}
    if item["state"] in FINISHED_STATES:
        item["done_source"] = "operator" if kinds & {"operator_done", "operator_ack"} else (
            "agent" if "self_reported_done" in kinds else "unknown")
    timeline = transcript_timeline(resolve_transcript_path(runs[-1])) if runs else []
    source_item = None
    if item["source_item_id"]:
        source_item = db.query_one(
            "SELECT id, objective, state FROM work_items WHERE id = ?",
            (item["source_item_id"],))
    followup_children = db.query_all(
        "SELECT id, objective, state FROM work_items WHERE source_item_id = ? ORDER BY id",
        (item_id,))
    return {"item": item, "runs": runs, "events": events, "artifacts": artifacts,
            "timeline": timeline, "source_item": source_item,
            "followup_children": followup_children}


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


def _asks_operator(text: str) -> bool:
    """True when the agent's last message asks the operator to hand something over.

    A request for a secret, a one-time code or an approval is written as an
    order, not as a question: "Send another code when ready" carries no
    question mark. Without this the item was read as an unattended background
    wait, was snoozed for hours, and the agent sat at the prompt while the
    operator saw nothing to answer."""
    return bool(_OPERATOR_ASK_RE.search(text[-300:]))


def _blocked_on_operator(text: str) -> bool:
    return _looks_like_question(text) or _asks_operator(text)


def maybe_autocontinue(session_id: str, transcript_path: str, tail: str | None = None) -> str:
    """Decide what a finished agent turn means for the item: done, a question
    for the operator, or another turn.

    `tail` overrides the transcript read for an agent that hands its last
    message over directly: the codex notify program is given the message, so
    it does not have to wait for the rollout file to catch up."""
    tail = last_assistant_text(transcript_path) if tail is None else tail.strip()
    now = _now()
    with db.tx() as c:
        run = c.execute(
            "SELECT id, work_item_id, tmux_key FROM work_runs WHERE session_id = ?",
            (session_id,),
        ).fetchone()
        if not run:
            return "unknown_session"
        item = c.execute(
            "SELECT state, autocontinue, continues_used, continue_cap, pending_question "
            "FROM work_items WHERE id = ?", (run["work_item_id"],),
        ).fetchone()
        if not item or item["state"] != "needs_you":
            return "not_applicable"
        excerpt = tail[:300]
        if item["pending_question"]:
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'question_detected', '{}', ?)",
                (run["work_item_id"], run["id"], now),
            )
            return "question"
        final_lines = [ln.strip() for ln in tail.splitlines() if ln.strip()]
        if final_lines and final_lines[-1] == DONE_MARKER:
            c.execute(
                "UPDATE work_items SET state = 'needs_ack', stop_reason = '', "
                "pending_question = '', current_checkpoint = ?, updated_at = ? WHERE id = ?",
                (excerpt, now, run["work_item_id"]),
            )
            c.execute("UPDATE work_runs SET status = 'finished', finished_at = ? WHERE id = ?",
                      (now, run["id"]))
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'self_reported_done', ?, ?)",
                (run["work_item_id"], run["id"], db.dump_json({"tail": excerpt}), now),
            )
            return "needs_ack"
        if excerpt:
            c.execute("UPDATE work_items SET stop_reason = ? WHERE id = ?",
                      (excerpt, run["work_item_id"]))
        if _blocked_on_operator(tail):
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'question_detected', '{}', ?)",
                (run["work_item_id"], run["id"], now),
            )
            return "question"
        if not item["autocontinue"]:
            return "disabled"
        if item["continues_used"] >= item["continue_cap"]:
            return "capped"
    sent = tmux_send(run["tmux_key"], CONTINUE_PROMPT)
    now = _now()
    with db.tx() as c:
        current = c.execute("SELECT state FROM work_items WHERE id = ?",
                            (run["work_item_id"],)).fetchone()
        if not current or current["state"] != "needs_you":
            return "not_applicable"
        if not sent:
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
        item = c.execute("SELECT state FROM work_items WHERE id = ?", (item_id,)).fetchone()
        if not item:
            return {"error": "unknown work item"}
        if item["state"] in FINISHED_STATES:
            return {"error": "item is finished; reopen it before replying"}
        run = c.execute(
            "SELECT id, tmux_key, provider FROM work_runs WHERE work_item_id = ? "
            "ORDER BY id DESC LIMIT 1", (item_id,),
        ).fetchone()
        if not run:
            return {"error": "no run for this item"}
    if not agent_running(run["tmux_key"], run["provider"]):
        return {"error": f"no live {run['provider'].capitalize()} in the session; open the terminal"}
    if not tmux_send(run["tmux_key"], text):
        return {"error": "tmux session gone"}
    with db.tx() as c:
        c.execute(
            "UPDATE work_items SET state = 'agent_working', stop_reason = '', "
            "pending_question = '', snoozed_until = NULL, updated_at = ? WHERE id = ?",
            (now, item_id),
        )
        c.execute("UPDATE work_runs SET status = 'running' WHERE id = ?", (run["id"],))
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'operator_reply', ?, ?)",
            (item_id, run["id"], db.dump_json({"text": text[:500]}), now),
        )
    return {"id": item_id, "action": "reply"}


def side_question(item_id: int, text: str) -> dict:
    """Ask the item's live Claude a side question without touching its run."""
    question = " ".join((text or "").split())
    if not question:
        return {"error": "empty question"}
    item = db.query_one("SELECT id FROM work_items WHERE id = ?", (item_id,))
    if not item:
        return {"error": "unknown work item"}
    run = db.query_one(
        "SELECT id, tmux_key, provider FROM work_runs WHERE work_item_id = ? "
        "ORDER BY id DESC LIMIT 1", (item_id,))
    if not run:
        return {"error": "no run for this item"}
    if run["provider"] != "claude":
        return {"error": f"side questions need a claude session; this run is {run['provider']}"}
    if not agent_running(run["tmux_key"], run["provider"]):
        return {"error": "no live Claude in the session; open the terminal"}
    out = ask_btw(run["tmux_key"], question)
    if "error" in out:
        return out
    now = _now()
    with db.tx() as c:
        c.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'btw', ?, ?)",
            (item_id, run["id"],
             db.dump_json({"question": question, "answer": out["answer"][:8000]}), now),
        )
    return {"id": item_id, "question": question, "answer": out["answer"]}


def pending_tool_calls(transcript_path: str) -> bool:
    """True when the transcript tail ends inside an unanswered tool call.

    A tool_use id with no matching tool_result means Claude is blocked in
    that call, not idle. The sweep must not synthesize a Stop for it. A
    codex rollout records a command only once it completes, so it can never
    show a call in flight and always answers False.
    """
    if not transcript_path or not os.path.isfile(transcript_path):
        return False
    if codex_session.is_rollout(transcript_path):
        return False
    try:
        with open(transcript_path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            f.seek(max(0, size - 262144))
            tail = f.read().decode("utf-8", errors="replace")
    except OSError:
        return False
    pending: set[str] = set()
    for line in tail.splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        content = (d.get("message") or {}).get("content")
        if not isinstance(content, list):
            continue
        if d.get("type") == "assistant":
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_use" and b.get("id"):
                    pending.add(str(b["id"]))
        elif d.get("type") == "user":
            for b in content:
                if isinstance(b, dict) and b.get("type") == "tool_result":
                    pending.discard(str(b.get("tool_use_id") or ""))
    return bool(pending)


def retry_missed_autocontinues(cutoff: str) -> list[dict]:
    """Run the autocontinue decision for a needs_you item that never got one.

    The idle-stop hook is what decides whether to auto-continue. When that
    hook is dropped, the item flips to needs_you with no decision recorded,
    so it holds an unused continue budget and nothing moves it. This finds
    those items and runs the decision late. An item whose decision already
    ran carries an event newer than its last idle stop, so it is skipped.
    """
    rows = db.query_all(
        "SELECT i.id AS item_id, r.id, r.session_id, r.tmux_key, r.transcript_path, "
        "r.provider, r.cwd, r.started_at, r.agent_session_id "
        "FROM work_items i JOIN work_runs r ON r.id = "
        "(SELECT r2.id FROM work_runs r2 WHERE r2.work_item_id = i.id ORDER BY r2.id DESC LIMIT 1) "
        "WHERE i.state = 'needs_you' AND i.autocontinue = 1 "
        "AND i.continues_used < i.continue_cap "
        "AND COALESCE(i.pending_question, '') = '' "
        "AND i.updated_at < ? "
        "AND (SELECT COALESCE(MAX(e.id), 0) FROM work_events e WHERE e.work_item_id = i.id "
        f"AND e.kind IN ({_IDLE_STOP_KINDS_SQL})) > "
        "(SELECT COALESCE(MAX(e.id), 0) FROM work_events e WHERE e.work_item_id = i.id "
        f"AND e.kind IN ({_DECIDED_KINDS_SQL}))",
        (cutoff,),
    )
    actions: list[dict] = []
    for row in rows:
        if not agent_running(row["tmux_key"], row["provider"]):
            continue
        outcome = maybe_autocontinue(row["session_id"], resolve_transcript_path(row))
        actions.append({"id": row["item_id"], "action": f"autocontinue_retry:{outcome}"})
    return actions


def sweep_stale_items(now: datetime | None = None) -> list[dict]:
    """Reconcile agent_working items whose hook events have gone quiet.

    A single agent turn longer than STALE_AFTER_MINUTES fires no hooks, so
    updated_at goes stale while the transcript still grows and the board
    shows a live session as failed_stale. The sweep refreshes updated_at
    from the transcript mtime for a session that is still writing, holds a
    session blocked in a tool call until STUCK_AFTER_MINUTES and then hands
    it to the operator, synthesizes the missed Stop for a live-but-idle
    session so the question/done/autocontinue path runs, and marks the item
    failed_stale when the agent process is gone. A codex rollout is written
    only between tool calls, so pane activity counts as freshness too. A
    second pass runs the autocontinue decision for a needs_you item whose
    idle-stop hook was dropped before it made one. A third pass fails an
    agent_working item that has no run at all: the launch path writes the item
    before the run, so a process that dies in that window leaves a row the
    join below can never reach.
    """
    now_dt = now or datetime.now(timezone.utc)
    cutoff = (now_dt - timedelta(minutes=STALE_AFTER_MINUTES)).isoformat()
    stuck_cutoff = (now_dt - timedelta(minutes=STUCK_AFTER_MINUTES)).isoformat()
    rows = db.query_all(
        "SELECT i.id AS item_id, r.id AS run_id, r.session_id, r.tmux_key, "
        "r.transcript_path, r.provider, r.cwd, r.started_at, r.agent_session_id "
        "FROM work_items i JOIN work_runs r ON r.id = "
        "(SELECT r2.id FROM work_runs r2 WHERE r2.work_item_id = i.id ORDER BY r2.id DESC LIMIT 1) "
        "WHERE i.state = 'agent_working' AND i.updated_at < ?",
        (cutoff,),
    )
    actions: list[dict] = []
    for row in rows:
        row["id"] = row["run_id"]
        transcript_path = resolve_transcript_path(row)
        mtime_iso = ""
        transcript_size = 0
        try:
            stat = os.stat(transcript_path)
            mtime_iso = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
            transcript_size = stat.st_size
        except OSError:
            pass
        if row["provider"] != "claude":
            mtime_iso = max(mtime_iso, pane_activity(row["tmux_key"]))
        if mtime_iso > cutoff:
            with db.tx() as c:
                c.execute(
                    "UPDATE work_items SET updated_at = ? WHERE id = ? "
                    "AND state = 'agent_working' AND updated_at < ?",
                    (mtime_iso, row["item_id"], mtime_iso),
                )
            actions.append({"id": row["item_id"], "action": "refreshed"})
            continue
        if not agent_running(row["tmux_key"], row["provider"]):
            flipped = 0
            with db.tx() as c:
                flipped = c.execute(
                    "UPDATE work_items SET state = 'failed_stale', "
                    "stop_reason = 'Agent process gone without a Stop event', "
                    "updated_at = ? WHERE id = ? AND state = 'agent_working'",
                    (_now(), row["item_id"]),
                ).rowcount
                if flipped:
                    c.execute("UPDATE work_runs SET status = 'stopped' WHERE id = ?",
                              (row["run_id"],))
                    c.execute(
                        "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                        "VALUES (?, ?, 'stale_failed', '{}', ?)",
                        (row["item_id"], row["run_id"], _now()),
                    )
            if flipped:
                actions.append({"id": row["item_id"], "action": "failed"})
            continue
        if pending_tool_calls(transcript_path):
            if mtime_iso and mtime_iso > stuck_cutoff:
                with db.tx() as c:
                    c.execute(
                        "UPDATE work_items SET updated_at = ? WHERE id = ? "
                        "AND state = 'agent_working'", (_now(), row["item_id"]),
                    )
                actions.append({"id": row["item_id"], "action": "busy_tool"})
                continue
            reason = (f"A tool call has not returned for {STUCK_AFTER_MINUTES} minutes. "
                      "Open the terminal to see what it is waiting on.")
            flipped = 0
            with db.tx() as c:
                flipped = c.execute(
                    "UPDATE work_items SET state = 'needs_you', stop_reason = ?, "
                    "updated_at = ? WHERE id = ? AND state = 'agent_working'",
                    (reason, _now(), row["item_id"]),
                ).rowcount
                if flipped:
                    c.execute(
                        "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                        "VALUES (?, ?, 'stuck_tool', '{}', ?)",
                        (row["item_id"], row["run_id"], _now()),
                    )
            if flipped:
                actions.append({"id": row["item_id"], "action": "stuck_tool"})
            continue
        payload = {
            "transcript_path": transcript_path,
            "transcript_cursor": transcript_size,
            "reason": f"No hook events for {STALE_AFTER_MINUTES} minutes; Stop synthesized by the stale sweep",
            "last_assistant_message": last_assistant_text(transcript_path)[:300],
        }
        record_event(row["session_id"], "Stop", payload)
        record_artifacts(row["session_id"], transcript_path)
        outcome = maybe_autocontinue(row["session_id"], transcript_path)
        actions.append({"id": row["item_id"], "action": f"stop_synthesized:{outcome}"})
    actions.extend(fail_runless_items(cutoff))
    actions.extend(revive_resumed_runs())
    actions.extend(retry_missed_autocontinues(cutoff))
    return actions


def fail_runless_items(cutoff: str) -> list[dict]:
    """Fail an agent_working item that never got a run.

    launch() and launch_proposed() both write the work item, then the run. A
    kill between the two, or an artifact failure that raises there, leaves an
    item claiming an agent is working with nothing behind it. Every other
    sweep pass joins work_runs, so none of them can see it, and the operator
    is left with a task that never moves. Only rows older than the staleness
    cutoff are touched, so a launch still in progress is not caught."""
    rows = db.query_all(
        "SELECT id FROM work_items WHERE state = 'agent_working' AND updated_at < ?"
        " AND NOT EXISTS(SELECT 1 FROM work_runs r WHERE r.work_item_id = work_items.id)",
        (cutoff,))
    actions = []
    for row in rows:
        now = _now()
        with db.tx() as c:
            flipped = c.execute(
                "UPDATE work_items SET state = 'failed_stale', "
                "stop_reason = 'The launch never started a session', "
                "updated_at = ? WHERE id = ? AND state = 'agent_working'",
                (now, row["id"])).rowcount
            if flipped:
                c.execute(
                    "INSERT INTO work_events(work_item_id, kind, payload, created_at) "
                    "VALUES (?, 'stale_failed', '{}', ?)", (row["id"], now))
        if flipped:
            actions.append({"id": row["id"], "action": "failed_without_run"})
    return actions


def revive_resumed_runs() -> list[dict]:
    """Return a failed_stale item to agent_working when its agent came back.

    The sweep marks an item failed_stale when the agent process is gone. The
    operator usually restarts the agent in the same pane — `codex resume`
    picks the same rollout back up — and from then on the board shows a dead
    item while the terminal works. A live agent in the pane plus a transcript
    written after the failure is proof this run resumed, so the item goes
    back to agent_working and the stale sweep owns it again."""
    rows = db.query_all(
        "SELECT i.id AS item_id, i.updated_at, r.id AS run_id, r.session_id, r.tmux_key, "
        "r.transcript_path, r.provider, r.cwd, r.started_at, r.agent_session_id "
        "FROM work_items i JOIN work_runs r ON r.id = "
        "(SELECT r2.id FROM work_runs r2 WHERE r2.work_item_id = i.id ORDER BY r2.id DESC LIMIT 1) "
        "WHERE i.state = 'failed_stale' AND r.status = 'stopped'"
    )
    actions: list[dict] = []
    for row in rows:
        row["id"] = row["run_id"]
        try:
            stat = os.stat(resolve_transcript_path(row))
        except OSError:
            continue
        mtime_iso = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
        if mtime_iso <= row["updated_at"]:
            continue
        if not agent_running(row["tmux_key"], row["provider"]):
            continue
        revived = 0
        with db.tx() as c:
            revived = c.execute(
                "UPDATE work_items SET state = 'agent_working', stop_reason = '', "
                "updated_at = ? WHERE id = ? AND state = 'failed_stale'",
                (mtime_iso, row["item_id"]),
            ).rowcount
            if revived:
                c.execute("UPDATE work_runs SET status = 'running' WHERE id = ?",
                          (row["run_id"],))
                c.execute(
                    "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                    "VALUES (?, ?, 'run_resumed', '{}', ?)",
                    (row["item_id"], row["run_id"], _now()),
                )
        if revived:
            actions.append({"id": row["item_id"], "action": "revived"})
    return actions


def grouped_items(now: datetime | None = None, q: str = "",
                  tags: str = "", archived: bool = False) -> dict[str, list[dict]]:
    """Group the work items for one board view.

    A task frshty opened by itself lands in proposed. No agent has read it
    yet: the operator approves it to start one, or declines it, which files
    it in the archive without ever running.

    A task the agent reported done lands in needs_ack, not in done. The
    operator acknowledges it to complete it, or archives it, which
    acknowledges and files it in one step. An unacknowledged task is never
    archived, so it never reaches the archive view.

    A completed task stays in the done group until the operator archives it.
    Archiving is the only way it leaves, so the board and the archive split
    the completed tasks between them and none of them becomes unreachable.
    The archive view holds archived tasks only, so a running task never
    appears in it.

    A search reads the whole task history, so a search on the board also
    returns the archived tasks. Without them a completed task becomes
    unfindable the moment it is archived."""
    now = now or datetime.now(timezone.utc)
    stale_cutoff = (now - timedelta(minutes=STALE_AFTER_MINUTES)).isoformat()
    now_iso = now.isoformat()
    q = (q or "").strip().lower()
    wanted_tags = {t.strip().lower() for t in (tags or "").split(",") if t.strip()}
    rows = db.query_all(
        "SELECT i.*, "
        "(SELECT session_id FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_session_id, "
        "(SELECT tmux_key FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_tmux_key, "
        "(SELECT provider FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_provider, "
        "(SELECT created_at FROM work_events e WHERE e.work_item_id = i.id "
        "AND e.kind IN ('operator_done', 'operator_ack', 'self_reported_done') "
        "ORDER BY e.id DESC LIMIT 1) AS completed_at, "
        f"EXISTS(SELECT 1 FROM work_events e WHERE e.work_item_id = i.id "
        f"AND e.kind IN {_ACK_EVENT_KINDS_SQL}) AS operator_confirmed "
        "FROM work_items i ORDER BY i.priority DESC, i.created_at DESC"
    )

    def keep_done(row: dict) -> bool:
        if archived:
            return True
        return bool(q) or not row["archived_at"]

    groups: dict[str, list[dict]] = {g: [] for g in GROUPS}
    for row in rows:
        if q and q not in (row["objective"] or "").lower():
            continue
        if wanted_tags and wanted_tags.isdisjoint((row["tags"] or "").split(",")):
            continue
        if archived and not row["archived_at"]:
            continue
        state = row["state"]
        if state == "needs_ack":
            groups["needs_ack"].append(row)
            continue
        if state == "done":
            if keep_done(row):
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
    # Tags and debriefs can update a completed item later, so use the actual
    # completion event rather than the board-wide priority/creation ordering.
    # Legacy done rows without an event fall back to their last update.
    groups["done"].sort(
        key=lambda row: (row["completed_at"] or row["updated_at"], row["id"]),
        reverse=True,
    )
    return groups


THREAD_TITLE_CHARS = 60
NON_PROJECT_CONTEXTS = ("slack_int",)


def _thread_components() -> list[list[dict]]:
    """Group work items into threads by their follow-up chain.

    A thread is a connected component of the source_item_id graph. No separate
    table is needed: a follow-up already records which item it continues, and a
    chain of continued work is exactly the effort a thread names."""
    rows = db.query_all(
        "SELECT id, objective, state, source_item_id, contexts, updated_at, created_at "
        "FROM work_items ORDER BY id")
    parent = {row["id"]: row["id"] for row in rows}

    def find(item_id: int) -> int:
        while parent[item_id] != item_id:
            parent[item_id] = parent[parent[item_id]]
            item_id = parent[item_id]
        return item_id

    for row in rows:
        source = row["source_item_id"]
        if source in parent:
            a, b = find(row["id"]), find(source)
            if a != b:
                parent[max(a, b)] = min(a, b)

    components: dict[int, list[dict]] = {}
    for row in rows:
        components.setdefault(find(row["id"]), []).append(row)
    return [members for members in components.values() if len(members) > 1]


def _thread_title(objective: str) -> str:
    text = " ".join((objective or "").split())
    if len(text) <= THREAD_TITLE_CHARS:
        return text
    cut = text[:THREAD_TITLE_CHARS].rsplit(" ", 1)[0] or text[:THREAD_TITLE_CHARS]
    return cut + "…"


def thread_projects(members: list[dict]) -> list[str]:
    """Name the projects a thread touches.

    A launch records the picked projects in the item contexts, so the contexts
    of the member tasks are the project context the list has to show. slack_int
    is a capability the launcher adds to the same field, not a project."""
    projects: list[str] = []
    for member in members:
        for label in (member["contexts"] or "").split(","):
            label = label.strip()
            if label and label not in NON_PROJECT_CONTEXTS and label not in projects:
                projects.append(label)
    return projects


def archive_completed() -> int:
    """Archive every completed task the board still shows, and count them.

    The board keeps a completed task until it is archived, so the first use of
    the archive faces a long list. One call clears that list, and Unarchive
    still brings any single task back. A task waiting to be acknowledged is
    not completed yet, so this call leaves it on the board."""
    now = _now()
    with db.tx() as c:
        return c.execute(
            "UPDATE work_items SET archived_at = ? WHERE state = 'done' AND archived_at IS NULL",
            (now,)).rowcount


def archive_thread(root_id: int) -> dict:
    """Archive every completed task in one thread, and count them.

    A finished thread leaves one completed task per member on the board, and
    the board keeps each one until it is archived. One call clears the whole
    thread, and Unarchive still brings any single task back. A task waiting to
    be acknowledged is not completed yet, so this call leaves it on the board.
    archive_completed follows the same rule."""
    members = next((m for m in _thread_components() if m[0]["id"] == root_id), None)
    if members is None:
        return {"error": f"unknown thread: {root_id}"}
    ids = [member["id"] for member in members]
    marks = ",".join("?" * len(ids))
    now = _now()
    with db.tx() as c:
        archived = c.execute(
            f"UPDATE work_items SET archived_at = ? WHERE id IN ({marks}) "
            "AND state = 'done' AND archived_at IS NULL", (now, *ids)).rowcount
    return {"root_id": root_id, "archived": archived}


def attention_count(now: datetime | None = None) -> int:
    """Count the tasks that wait on the operator: proposed, needs_ack, needs_you.

    grouped_items promotes a waiting_external item back to needs_you once its
    snooze expires, so the rail badge has to apply the same rule or it will
    disagree with the board it links to. A task the agent reported done also
    waits on the operator, so the badge counts it too, and so does a proposal
    frshty opened by itself: nothing happens on it until the operator approves
    it."""
    now = now or datetime.now(timezone.utc)
    row = db.query_one(
        "SELECT COUNT(*) AS n FROM work_items WHERE "
        "state IN ('proposed', 'needs_ack', 'needs_you') "
        "OR (state = 'waiting_external' AND snoozed_until IS NOT NULL AND snoozed_until <= ?)",
        (now.isoformat(),))
    return row["n"] if row else 0


def thread_map() -> dict[int, dict]:
    """Map every threaded item id to the thread it belongs to."""
    mapping: dict[int, dict] = {}
    for members in _thread_components():
        root = members[0]
        thread = {"root_id": root["id"], "title": _thread_title(root["objective"])}
        for member in members:
            mapping[member["id"]] = thread
    return mapping


def threads(now: datetime | None = None) -> list[dict]:
    now = now or datetime.now(timezone.utc)
    stale_cutoff = (now - timedelta(minutes=STALE_AFTER_MINUTES)).isoformat()
    artifact_counts = {
        row["work_item_id"]: row["n"] for row in db.query_all(
            "SELECT work_item_id, COUNT(*) AS n FROM work_artifacts GROUP BY work_item_id")}
    providers_by_item: dict[int, list[str]] = {}
    for row in db.query_all(
            "SELECT DISTINCT work_item_id, provider FROM work_runs ORDER BY provider"):
        providers_by_item.setdefault(row["work_item_id"], []).append(row["provider"])

    out = []
    for members in _thread_components():
        root = members[0]
        tasks = []
        providers: list[str] = []
        for member in members:
            state = member["state"]
            if state == "agent_working" and member["updated_at"] < stale_cutoff:
                state = "failed_stale"
            tasks.append({"id": member["id"], "objective": member["objective"],
                          "state": state, "updated_at": member["updated_at"]})
            for provider in providers_by_item.get(member["id"], []):
                if provider not in providers:
                    providers.append(provider)
        counts: dict[str, int] = {}
        for task in tasks:
            counts[task["state"]] = counts.get(task["state"], 0) + 1
        out.append({
            "root_id": root["id"],
            "title": _thread_title(root["objective"]),
            "objective": root["objective"],
            "tasks": tasks,
            "task_count": len(tasks),
            "counts": counts,
            "artifact_count": sum(artifact_counts.get(t["id"], 0) for t in tasks),
            "projects": thread_projects(members),
            "providers": providers,
            "updated_at": max(t["updated_at"] for t in tasks),
        })
    out.sort(key=lambda thread: (thread["updated_at"], thread["root_id"]), reverse=True)
    return out


def thread_detail(root_id: int, now: datetime | None = None) -> dict:
    """The roll-up one thread page needs: its member tasks oldest first, the
    artifacts they produced, and the newest finished task a new member can
    follow. A task waiting to be acknowledged is finished work, so a new
    member can follow it before the operator acknowledges it.

    A thread has no row of its own, so its title, objective and summary all
    come from its members. The summary is the newest member summary rather
    than a stored field, because that is the latest synthesized state."""
    now = now or datetime.now(timezone.utc)
    stale_cutoff = (now - timedelta(minutes=STALE_AFTER_MINUTES)).isoformat()
    now_iso = now.isoformat()
    members = next((m for m in _thread_components() if m[0]["id"] == root_id), None)
    if members is None:
        return {"error": f"unknown thread: {root_id}"}
    ids = [member["id"] for member in members]
    marks = ",".join("?" * len(ids))
    rows = db.query_all(
        "SELECT i.*, "
        "(SELECT tmux_key FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_tmux_key, "
        "(SELECT provider FROM work_runs r WHERE r.work_item_id = i.id ORDER BY r.id DESC LIMIT 1) AS last_provider, "
        f"EXISTS(SELECT 1 FROM work_events e WHERE e.work_item_id = i.id "
        f"AND e.kind IN {_ACK_EVENT_KINDS_SQL}) AS operator_confirmed "
        f"FROM work_items i WHERE i.id IN ({marks}) ORDER BY i.id", tuple(ids))
    artifacts = db.query_all(
        "SELECT id, work_item_id, path, note, created_at FROM work_artifacts "
        f"WHERE work_item_id IN ({marks}) ORDER BY work_item_id, id", tuple(ids))
    artifact_counts: dict[int, int] = {}
    for artifact in artifacts:
        artifact_counts[artifact["work_item_id"]] = artifact_counts.get(
            artifact["work_item_id"], 0) + 1

    tasks = []
    providers: list[str] = []
    for row in rows:
        state = row["state"]
        if state == "waiting_external" and row["snoozed_until"] and row["snoozed_until"] <= now_iso:
            state = "needs_you"
        elif state == "agent_working" and row["updated_at"] < stale_cutoff:
            state = "failed_stale"
        row["state"] = state
        row["artifact_count"] = artifact_counts.get(row["id"], 0)
        tasks.append(row)
        if row["last_provider"] and row["last_provider"] not in providers:
            providers.append(row["last_provider"])

    counts: dict[str, int] = {}
    for task in tasks:
        counts[task["state"]] = counts.get(task["state"], 0) + 1
    done_ids = [task["id"] for task in tasks if task["state"] == "done"]
    finished_ids = [task["id"] for task in tasks if task["state"] in FINISHED_STATES]
    summarised = [task for task in tasks if (task["summary"] or "").strip()]
    return {
        "root_id": root_id,
        "title": _thread_title(members[0]["objective"]),
        "objective": members[0]["objective"],
        "status": "complete" if len(done_ids) == len(tasks) else "active",
        "tasks": tasks,
        "task_count": len(tasks),
        "counts": counts,
        "done_count": len(done_ids),
        "needs_you": counts.get("needs_you", 0),
        "artifacts": artifacts,
        "artifact_count": len(artifacts),
        "providers": providers,
        "created_at": min(task["created_at"] for task in tasks),
        "updated_at": max(task["updated_at"] for task in tasks),
        "summary": (summarised[-1]["summary"] or "").strip() if summarised else "",
        "summary_from": summarised[-1]["id"] if summarised else None,
        "continue_from": finished_ids[-1] if finished_ids else None,
    }
