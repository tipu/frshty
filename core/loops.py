"""Find a job, a retry or a proposal that repeats with no state change.

A job loops when its last LOOP_THRESHOLD runs of one task on one ticket
ended the same way. A failure counts when its normalized reason repeats. A
success counts only when every run belongs to one chain of jobs, the chain
one root job started, and each run left the ticket state the same. A cron
tick that runs the same no-op job again starts a new chain, so a ticket that
waits for days is not a loop. advance_ticket -> sync_pr_base ->
advance_ticket on one ticket, with the ticket unchanged, is one.

A retry loops when one failure event repeats LOOP_THRESHOLD times for one
ticket with the same normalized text. A push that fails inside a job that
reports ok is seen only this way.

A proposal loops when the operator declined the same objective
PROPOSAL_THRESHOLD times before and it is filed again.

Each check answers once per loop: at the run that makes the streak exactly
the threshold long. A longer streak answers nothing, so the feed gets one
loop_detected event per loop, not one per run."""

import hashlib
import json
import re

import core.db as db

LOOP_THRESHOLD = 20
PROPOSAL_THRESHOLD = 2
RETRY_EVENT_SUFFIXES = ("_failed", "_error")
LOOP_EVENT = "loop_detected"
LOOP_CHECK_ERROR = "loop_check_error"

_FINISHED = ("ok", "failed", "partial", "skipped")
_FAILED = ("failed", "partial")

_HEX_RE = re.compile(r"\b(?=[0-9a-f]*\d)[0-9a-f]{7,}\b", re.IGNORECASE)
_DIGITS_RE = re.compile(r"\d+")
_DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?"
                      r"(?:Z|[+-]\d{2}:?\d{2})?)?")


def normalize_reason(reason: str | None) -> str:
    """Strip the parts of a failure reason that change between runs of the
    same failure: commit shas, ids, counts and timestamps."""
    text = _HEX_RE.sub("<hex>", reason or "")
    text = _DIGITS_RE.sub("N", text)
    return " ".join(text.split())[:300]


def normalize_objective(objective: str | None) -> str:
    """A proposal objective without its dates. Ticket keys and task numbers
    stay, so "Unblock WB-209" and "Unblock WB-283" are two proposals."""
    return " ".join(_DATE_RE.sub("<date>", objective or "").split()).lower()


def _strip_stamps(value):
    if isinstance(value, dict):
        return {k: _strip_stamps(v) for k, v in value.items() if not str(k).endswith("_at")}
    if isinstance(value, list):
        return [_strip_stamps(v) for v in value]
    return value


def ticket_digest(data: dict | None) -> str:
    """A hash of the ticket state without its *_at timestamps. Two runs that
    leave the same digest changed nothing a later stage reads."""
    text = json.dumps(_strip_stamps(data or {}), sort_keys=True, default=str)
    return hashlib.sha1(text.encode()).hexdigest()


def ticket_digest_in(c, instance_key: str, ticket_key: str) -> str:
    """The digest of the ticket row as the open transaction c sees it."""
    row = c.execute("SELECT data FROM tickets WHERE instance_key=? AND ticket_key=?",
                    (instance_key, ticket_key)).fetchone()
    try:
        data = json.loads(row["data"]) if row else None
    except (TypeError, ValueError):
        data = None
    return ticket_digest(data if isinstance(data, dict) else None)


def _rows(sql: str, params: tuple, conn=None) -> list:
    if conn is not None:
        return conn.execute(sql, params).fetchall()
    return db.query_all(sql, params)


def _streak_reached(signatures: list) -> bool:
    """Whether the newest LOOP_THRESHOLD signatures are one loop that this
    newest one just made long enough. signatures holds up to
    LOOP_THRESHOLD + 1 entries, newest first; None never matches."""
    if len(signatures) < LOOP_THRESHOLD:
        return False
    first = signatures[0]
    if first is None or any(s != first for s in signatures[1:LOOP_THRESHOLD]):
        return False
    return len(signatures) == LOOP_THRESHOLD or signatures[LOOP_THRESHOLD] != first


def _reason(response: str | None) -> str:
    try:
        data = json.loads(response or "{}")
    except (TypeError, ValueError):
        return ""
    return str(data.get("reason") or "") if isinstance(data, dict) else ""


def _job_signature(row) -> tuple | None:
    status = row["status"]
    if status in _FAILED:
        return ("failed", normalize_reason(_reason(row["response"]) or status))
    if not row["ticket_digest"]:
        return None
    return (status, normalize_reason(_reason(row["response"])),
            row["ticket_digest"], row["root"])


def job_loop(job: dict) -> dict | None:
    """Say whether a finished job now loops. job holds id, instance_key, task
    and ticket_key. The worker writes the ticket digest in the transaction
    that writes the ticket and finishes the job, so the digest is the state
    the job left, and every older run read here already carries its own. Returns the loop as a dict, or None."""
    rows = db.query_all(
        "SELECT id, status, response, ticket_digest, COALESCE(root_job_id, id) AS root"
        " FROM jobs WHERE instance_key=? AND task=? AND ticket_key IS ? AND id<=?"
        " AND status IN (?, ?, ?, ?) ORDER BY id DESC LIMIT ?",
        (job["instance_key"], job["task"], job["ticket_key"], job["id"],
         *_FINISHED, LOOP_THRESHOLD + 1))
    if not rows or rows[0]["id"] != job["id"]:
        return None
    if not _streak_reached([_job_signature(r) for r in rows]):
        return None
    newest = rows[0]
    return {"task": job["task"], "ticket_key": job["ticket_key"],
            "count": LOOP_THRESHOLD, "status": newest["status"],
            "reason": _reason(newest["response"]),
            "first_job_id": rows[LOOP_THRESHOLD - 1]["id"], "last_job_id": newest["id"]}


def is_retry_event(event: str) -> bool:
    return event not in (LOOP_EVENT, LOOP_CHECK_ERROR) and event.endswith(RETRY_EVENT_SUFFIXES)


def retry_loop(instance_key: str, event: str, ticket, conn=None) -> bool:
    """Whether the failure event just written is the LOOP_THRESHOLD-th in a
    row with the same normalized text for this instance and ticket."""
    rows = _rows(
        "SELECT summary FROM log_events WHERE instance_key=? AND event=?"
        " AND json_extract(meta, '$.ticket') IS ? ORDER BY ts DESC LIMIT ?",
        (instance_key, event, ticket, LOOP_THRESHOLD + 1), conn)
    return _streak_reached([normalize_reason(r["summary"]) for r in rows])


def declined_repeats(c, objective: str, instance_key: str | None,
                     declined_state: str, declined_reason: str,
                     before_id: int) -> list[int]:
    """The ids of the declined proposals with this objective when there are
    exactly PROPOSAL_THRESHOLD of them, else an empty list. c is the open
    transaction that files the new proposal before_id."""
    wanted = normalize_objective(objective)
    if not wanted:
        return []
    rows = c.execute(
        "SELECT id, objective FROM work_items WHERE scope='proposal' AND state=?"
        " AND stop_reason=? AND instance_key IS ? AND id<? ORDER BY id",
        (declined_state, declined_reason, instance_key, before_id)).fetchall()
    ids = [int(r["id"]) for r in rows if normalize_objective(r["objective"]) == wanted]
    return ids if len(ids) == PROPOSAL_THRESHOLD else []
