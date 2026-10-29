"""A durable record of an action that was tried and blocked.

A cron tick decides from outside state alone whether work is owed. Without a
record of the last try, a blocked action looks the same as an action that was
never tried, and the next tick runs it again. DEV-624 ran sync_pr_base 74,762
times in 6 days this way. One PR with commits by another author had its
push refused 235 times, two more each time main moved.

One row names a resource (a ticket key, or a PR as "repo#id"), an action and
the blocker that stopped it. The wake column holds the input that has to
change before one more try is worth it: the PR branch head for a base sync,
the ticket digest for a job the loop detector stopped. A new commit on main
is not a wake input, because it does not change a foreign author, a dirty
worktree or a missing token.

A write here never fails its caller. A database error is emitted as an event
and the caller goes on. A gate that cannot read the record lets the job run,
and says so on the event feed.
"""
from datetime import datetime, timezone

import core.db as db
import core.log as log
import core.loops as loops
import core.state as state

SYNC_BASE = "sync_base"
JOB_PREFIX = "job:"
UNBLOCKED_TASKS = ("advance_ticket",)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve(instance_key: str) -> str:
    if instance_key:
        return instance_key
    try:
        return state.active_instance_key()
    except RuntimeError:
        return ""


def _failed(what: str, resource: str, action: str, error: Exception) -> None:
    log.emit("blocked_attempts_failed",
             f"Could not {what} the blocked record {action} {resource}: "
             f"{type(error).__name__}: {error}",
             meta={"resource": resource, "action": action, "what": what,
                   "error": f"{type(error).__name__}: {error}"})


def blocker_of(result: str, error: str = "") -> str:
    """The key of one blocker: the outcome and its error without the parts
    that change between tries of the same blocker."""
    text = loops.normalize_reason(error)
    return f"{result}: {text}" if text else result


def record(resource: str, action: str, blocker: str, error: str = "", wake: str = "",
           *, instance_key: str = "") -> int:
    """Count one blocked try. Returns the tries on record for this blocker,
    or 0 when the row could not be written."""
    key = _resolve(instance_key)
    if not key or not resource:
        return 0
    now = _now()
    try:
        with db.tx() as c:
            c.execute(
                "INSERT INTO blocked_attempts"
                " (instance_key, resource, action, blocker, error, wake, attempts, first_at, last_at)"
                " VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)"
                " ON CONFLICT(instance_key, resource, action, blocker) DO UPDATE SET"
                "  error=excluded.error, wake=excluded.wake,"
                "  attempts=blocked_attempts.attempts+1, last_at=excluded.last_at",
                (key, resource, action, blocker, error[:500], wake, now, now))
            row = c.execute(
                "SELECT attempts FROM blocked_attempts"
                " WHERE instance_key=? AND resource=? AND action=? AND blocker=?",
                (key, resource, action, blocker)).fetchone()
        return int(row["attempts"]) if row else 0
    except Exception as e:
        _failed("write", resource, action, e)
        return 0


def find(resource: str, action: str, *, instance_key: str = "") -> list[dict]:
    key = _resolve(instance_key)
    if not key or not resource:
        return []
    return db.query_all(
        "SELECT * FROM blocked_attempts WHERE instance_key=? AND resource=? AND action=?"
        " ORDER BY last_at DESC", (key, resource, action))


def clear(resource: str, action: str, *, instance_key: str = "") -> None:
    """Forget every blocker of this action: it succeeded, or its blocker
    went away."""
    key = _resolve(instance_key)
    if not key or not resource:
        return
    try:
        db.execute("DELETE FROM blocked_attempts WHERE instance_key=? AND resource=? AND action=?",
                   (key, resource, action))
    except Exception as e:
        _failed("clear", resource, action, e)


def _ticket_digest(instance_key: str, ticket_key: str) -> str:
    with db.cursor() as c:
        return loops.ticket_digest_in(c, instance_key, ticket_key)


def job_blocker(status: str, reason: str | None) -> str:
    return blocker_of("failed" if status in ("failed", "partial") else status, reason or "")


def block_job(instance_key: str, ticket_key: str | None, task: str, status: str,
              reason: str | None) -> bool:
    """Stop the next run of a task the loop detector caught. The block holds
    until the ticket state changes. Returns whether a block was written."""
    if not ticket_key or task in UNBLOCKED_TASKS:
        return False
    try:
        wake = _ticket_digest(instance_key, ticket_key)
    except Exception as e:
        _failed("read the ticket for", ticket_key, JOB_PREFIX + task, e)
        return False
    return record(ticket_key, JOB_PREFIX + task, job_blocker(status, reason), reason or "",
                  wake, instance_key=instance_key) > 0


def job_blocked(instance_key: str, ticket_key: str | None, task: str) -> dict | None:
    """The block that stops this job, or None. A block whose ticket state
    has changed since it was written no longer stops the job: the change
    buys one more try."""
    if not ticket_key or task in UNBLOCKED_TASKS:
        return None
    try:
        rows = find(ticket_key, JOB_PREFIX + task, instance_key=instance_key)
        if not rows:
            return None
        digest = _ticket_digest(instance_key, ticket_key)
    except Exception as e:
        _failed("read", ticket_key, JOB_PREFIX + task, e)
        return None
    for row in rows:
        if row["wake"] == digest:
            return row
    return None


def after_job(instance_key: str, ticket_key: str | None, task: str, status: str,
              reason: str | None) -> bool:
    """Settle the block of a job that ran after its wake. The same failure
    again blocks the task again at once, so a woken loop costs one run, not
    another LOOP_THRESHOLD runs. Any other outcome clears the block. Returns
    whether the task is blocked again."""
    if not ticket_key or task in UNBLOCKED_TASKS:
        return False
    try:
        rows = find(ticket_key, JOB_PREFIX + task, instance_key=instance_key)
    except Exception as e:
        _failed("read", ticket_key, JOB_PREFIX + task, e)
        return False
    if not rows:
        return False
    blocker = job_blocker(status, reason)
    if status in ("failed", "partial") and any(r["blocker"] == blocker for r in rows):
        return block_job(instance_key, ticket_key, task, status, reason)
    clear(ticket_key, JOB_PREFIX + task, instance_key=instance_key)
    return False
