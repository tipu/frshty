"""Diagnose one stuck ticket by launching a work-board task.

The operator presses Doctor on a ticket detail page and types what looks
wrong. This module takes a snapshot of the ticket and of the instance that
owns it, then launches a normal /tasks work item whose brief carries that
snapshot plus the map of the frshty pipeline. The task is tagged with the
instance key so it lands under the right project on the board.

Every doctor request for one ticket belongs to one thread. A request made
while an earlier run is still working goes to that run as a reply. A request
made after it finished starts a new task that follows the earlier one, so the
new run opens with the previous run's outcome and artifacts.
"""
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import core.config as cfg
import core.db as db
import core.log as log
import core.queue as q
import core.scheduler as scheduler
import core.state as state
from services import work_launch, work_store

FRSHTY_ROOT = str(Path(__file__).resolve().parent.parent)
FRSHTY_TAG = "frshty"
MAX_DESCRIPTION = 2000
MAX_STATE_CHARS = 12000
JOB_LIMIT = 25
EVENT_LIMIT = 40
TRANSITION_LIMIT = 25
DOC_LIMIT = 40
HISTORY_LIMIT = 20


def _instance_key(config: dict) -> str:
    return state.active_instance_key() or (config.get("job") or {}).get("key", "")


def _trim(text: str, limit: int) -> str:
    text = text or ""
    if len(text) <= limit:
        return text
    return text[:limit] + f"\n[truncated at {limit} characters]"


def _state_block(ts: dict) -> str:
    return _trim(json.dumps(ts, indent=2, default=str, sort_keys=True), MAX_STATE_CHARS)


def _job_lines(instance_key: str, key: str) -> list[str]:
    rows = q.jobs_for_ticket(instance_key, key, JOB_LIMIT)
    lines = []
    for r in rows:
        response = _trim((r.get("response") or "").strip().replace("\n", " "), 300)
        lines.append(
            f"- job {r['id']} {r['task']} status={r['status']}"
            f" enqueued={r.get('enqueued_at')} started={r.get('started_at')}"
            f" finished={r.get('finished_at')}"
            + (f" response={response}" if response else ""))
    return lines


def _event_lines(key: str) -> list[str]:
    lines = []
    for e in log.get_events_for_ticket(key, limit=EVENT_LIMIT):
        summary = _trim((e.get("summary") or "").replace("\n", " "), 300)
        lines.append(f"- {e.get('ts')} {e.get('event')}: {summary}")
    return lines


def _transition_lines(instance_key: str, key: str) -> list[str]:
    rows = db.query_all(
        "SELECT prior_status, new_status, rejected, rejection_reason, actor, reason, ts"
        " FROM ticket_transitions WHERE instance_key=? AND ticket_key=?"
        " ORDER BY ts DESC, id DESC LIMIT ?",
        (instance_key, key, TRANSITION_LIMIT),
    )
    lines = []
    for r in rows:
        verdict = "REJECTED" if r["rejected"] else "applied"
        why = r["rejection_reason"] if r["rejected"] else (r["reason"] or "")
        lines.append(
            f"- {r['ts']} {r['prior_status']} -> {r['new_status']} {verdict}"
            f" actor={r['actor']}" + (f" {why}" if why else ""))
    return lines


def _worktree_lines(config: dict, slug: str) -> list[str]:
    if not slug:
        return []
    lines = []
    for repo in cfg.get_repos(config):
        path = cfg.ticket_worktree_path(config, slug, repo["name"])
        lines.append(f"- {repo['name']}: {path}"
                     f" ({'present' if path.is_dir() else 'MISSING'})")
    return lines


def _doc_lines(docs_dir: Path) -> list[str]:
    if not docs_dir.is_dir():
        return []
    lines = []
    for f in sorted(docs_dir.iterdir())[:DOC_LIMIT]:
        try:
            size = f.stat().st_size
        except OSError:
            continue
        lines.append(f"- {f.name} ({size} bytes)")
    return lines


def _scheduled_lines(instance_key: str, key: str) -> list[str]:
    return [f"- {r.get('run_at')} {r.get('key')} {json.dumps(r, default=str)[:300]}"
            for r in scheduler.list_for_ticket(instance_key, key)]


def _section(title: str, lines: list[str], empty: str) -> str:
    body = "\n".join(lines) if lines else empty
    return f"### {title}\n\n{body}\n"


def snapshot(config: dict, key: str, ts: dict) -> str:
    """The evidence block handed to the doctor task.

    Everything here is read at launch time so the task starts from the state
    the operator was looking at. The task is told to re-read the live state
    too, because a pipeline that is still running moves under it."""
    instance_key = _instance_key(config)
    ws = config["workspace"]
    slug = ts.get("slug", "")
    ticket_dir = Path(ws["root"]) / ws["tickets_dir"] / slug
    db_path = db.path()
    base_url = config.get("_base_url", "")
    header = [
        f"- ticket: {key}",
        f"- instance: {instance_key}",
        f"- status: {ts.get('status')}",
        f"- external status: {ts.get('external_status')}",
        f"- work type: {ts.get('work_type')}",
        f"- slug: {slug}",
        f"- ticket directory: {ticket_dir}",
        f"- workspace root: {ws['root']}",
        f"- instance config: {config.get('_config_path')}",
        f"- frshty database: {db_path}",
        f"- frshty source: {FRSHTY_ROOT}",
        f"- ticket page: {base_url}/tickets/{key}",
    ]
    return (
        "\n\n## Ticket snapshot\n\n"
        + "\n".join(header) + "\n\n"
        + f"### Stored ticket state\n\n```json\n{_state_block(ts)}\n```\n"
        + "\n" + _section("Recent jobs (newest first)", _job_lines(instance_key, key),
                          "no jobs recorded for this ticket")
        + "\n" + _section("Recent events (newest first)", _event_lines(key),
                          "no events recorded for this ticket")
        + "\n" + _section("Status transitions (newest first)",
                          _transition_lines(instance_key, key),
                          "no transitions recorded for this ticket")
        + "\n" + _section("Scheduled rows", _scheduled_lines(instance_key, key),
                          "nothing scheduled for this ticket")
        + "\n" + _section("Worktrees", _worktree_lines(config, slug),
                          "no repos configured for this instance")
        + "\n" + _section("Ticket docs", _doc_lines(ticket_dir / "docs"),
                          "no docs directory for this ticket")
    )


PIPELINE_MAP = """
### How the pipeline moves a ticket

- `features/ticket_states.py` holds `_STATUS_HANDLERS`: one handler per ticket
  status. The handler for the current status decides the next job.
- `features/tickets.py` `check()` runs every handler on a poll;
  `advance_ticket()` runs only the handler for this ticket's status and returns
  early when another job for the ticket is already running.
- `core/tasks/tickets.py` holds the task bodies, their preconditions,
  `on_entry_status` and `on_success_status`.
- `core/tasks/preconditions.py` holds the gates. A task whose precondition
  fails is marked `skipped`, and the ticket does not move.
- `core/tasks/routes.py` maps an event kind to the jobs it enqueues.
- `core/queue.py`, `core/worker.py` and `core/event_bus.py` run the queue.
- Tables: `tickets`, `jobs`, `events`, `log_events`, `ticket_transitions`,
  `scheduler`, `claude_invocations`.

### How to work

Read the snapshot first, then confirm every claim against the live system:
query the database, read the job log, read the handler source. State no cause
you cannot show evidence for. There is no psql and no `sqlite3` binary on this
host. The database is SQLite, so query it with Python, for example
`python3 -c "import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute('select ...').fetchall())" <db path>`.

Find the exact reason the ticket is not moving. Name the file and line that
makes the decision. Say whether the cause is ticket state, a failing job, a
precondition that never clears, a scheduler row, or a bug in the frshty code.

Then propose the smallest remedy. Do not change ticket state, do not enqueue or
retry jobs, and do not edit or push frshty code until the operator approves the
remedy with AskUserQuestion. Reading anything is unrestricted.

Write the diagnosis as a markdown report artifact and print its ARTIFACT line.
"""


def brief(config: dict, key: str, ts: dict) -> str:
    return snapshot(config, key, ts) + "\n" + PIPELINE_MAP


def _newest_run(instance_key: str, key: str) -> dict | None:
    """The most recent doctor work item recorded for this ticket.

    Returns None when no run was recorded or when its work item was deleted."""
    return db.query_one(
        "SELECT r.work_item_id AS item_id, w.state AS state"
        " FROM ticket_doctor_runs r JOIN work_items w ON w.id = r.work_item_id"
        " WHERE r.instance_key=? AND r.ticket_key=?"
        " ORDER BY r.id DESC LIMIT 1",
        (instance_key, key))


def _live_item(run: dict | None) -> int | None:
    """The work item of a doctor run that is still working, if any.

    A second Doctor press while the first run is still going used to create a
    second work item that could not see the first one's findings. DEV-678 got
    two runs twenty seconds apart and both asked the operator the same
    question. The live run is the thread the second report belongs in."""
    if not run or run["state"] in work_store.FINISHED_STATES:
        return None
    return int(run["item_id"])


def launch(config: dict, key: str, description: str) -> dict:
    """Launch the doctor task for one ticket. Returns the work_launch result,
    or {"error": ...} when the ticket is unknown or the work layer is down."""
    description = (description or "").strip()
    if not description:
        return {"error": "describe what is wrong before running the doctor"}
    ts = state.load_ticket(key)
    if not ts:
        return {"error": f"unknown ticket: {key}"}
    instance_key = _instance_key(config)
    previous = _newest_run(instance_key, key)
    live = _live_item(previous)
    if live is not None:
        sent = work_store.reply(live, _trim(description, MAX_DESCRIPTION))
        if "error" not in sent:
            record(instance_key, key, live, description)
            return {"item_id": live, "state": "agent_working", "action": "reply"}
    contexts = [c for c in (instance_key, FRSHTY_TAG) if c]
    cwd = FRSHTY_ROOT if os.path.isdir(FRSHTY_ROOT) else ""
    objective = (
        f"Doctor ticket {key} ({instance_key}, status {ts.get('status')}). "
        f"The operator reports: {_trim(description, MAX_DESCRIPTION)}")
    result = work_launch.launch(objective, cwd=cwd, contexts=contexts,
                                brief=brief(config, key, ts),
                                source_item_id=int(previous["item_id"]) if previous else None)
    if "error" not in result and result.get("item_id"):
        record(instance_key, key, int(result["item_id"]), description)
    return result


def record(instance_key: str, key: str, item_id: int, description: str) -> None:
    """Remember one doctor request so the ticket page can list it later.

    The work item alone does not say which ticket it came from, and the
    operator wants the history on the ticket page, not on the board."""
    db.execute(
        "INSERT INTO ticket_doctor_runs"
        "(instance_key, ticket_key, work_item_id, description, created_at)"
        " VALUES (?, ?, ?, ?, ?)",
        (instance_key, key, item_id, _trim(description, MAX_DESCRIPTION),
         datetime.now(timezone.utc).isoformat()))


def history(config: dict, key: str, limit: int = HISTORY_LIMIT) -> list[dict]:
    """Every doctor request made for this ticket, newest first.

    Each row carries the live state of its work item, so the page can say
    which request is still running and what the finished ones concluded. A
    row whose work item was deleted still lists, with an empty state."""
    instance_key = _instance_key(config)
    rows = db.query_all(
        "SELECT r.id, r.work_item_id, r.description, r.created_at,"
        " w.state, w.summary, w.current_checkpoint, w.pending_question, w.updated_at"
        " FROM ticket_doctor_runs r"
        " LEFT JOIN work_items w ON w.id = r.work_item_id"
        " WHERE r.instance_key=? AND r.ticket_key=?"
        " ORDER BY r.id DESC LIMIT ?",
        (instance_key, key, limit))
    runs = []
    for r in rows:
        item_state = r["state"] or ""
        runs.append({
            "id": r["id"],
            "item_id": r["work_item_id"],
            "description": r["description"],
            "created_at": r["created_at"],
            "state": item_state,
            "running": bool(item_state) and item_state not in work_store.FINISHED_STATES,
            "question": r["pending_question"] or "",
            "outcome": (r["summary"] or r["current_checkpoint"] or "").strip(),
            "updated_at": r["updated_at"] or r["created_at"],
            "url": f"/tasks/{r['work_item_id']}",
        })
    return runs
