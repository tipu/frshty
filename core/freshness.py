"""A ledger of what a ticket has claimed about itself, and what staled it.

A ticket carries four claims. A proof was run against a branch diff. CI went
green against a set of PR heads. Every review comment up to a watermark was
triaged. The smoke pool replayed. Each claim is established once and then
decays on its own schedule, and today each one is written by a single caller
for a single purpose. No query can ask whether a ticket is still what it
claimed to be, because the answer is spread across proof_fingerprint,
ci_passed, last_comment_ids and the base_sync record, and nothing reads them
together.

This module records the claims and the events that stale them. It decides
nothing. No gate reads it. The ticket detail page renders it and nothing
else does, so a row here can never hold a merge, enqueue a task or run a
model. That is deliberate: the ledger has to run for a while before its
answer is worth acting on.

A write here can never fail its caller. A ledger that breaks the pipeline it
observes is worse than no ledger, so every database error is caught and
emitted as an event rather than raised. Silence is not an option either: the
event feed names the ticket, the claim and the error.
"""
import hashlib
import json
from datetime import datetime, timezone

import core.db as db
import core.log as log
import core.state as state

CLAIMS = ("proof", "ci", "comments", "smoke")


def enabled(config: dict | None) -> bool:
    """Whether this instance writes the ledger. Default on.

    Shadow mode is the whole of the feature today, so the lever exists to
    turn the writing off, not to turn a gate on."""
    return bool((config or {}).get("freshness", {}).get("enabled", True))


def digest(parts) -> str:
    """A short stable name for a set of things a claim stood against.

    Used where the set is too large to read on a page — the smoke pool, for
    one. A claim whose subject is already short records it verbatim
    instead."""
    joined = "\n".join(sorted(str(p) for p in parts))
    if not joined:
        return ""
    return hashlib.sha1(joined.encode()).hexdigest()[:16]


def watermark(ts: dict) -> str:
    """The comment watermark a ticket has triaged up to, as a stable string."""
    return json.dumps(
        {"review": ts.get("last_comment_ids") or {},
         "issue": ts.get("last_issue_comment_ids") or {}},
        sort_keys=True, default=str)


def pr_heads(prs: list[dict], pr_info_map: dict | None = None) -> str:
    """The PR heads a CI verdict was measured against.

    A verdict is green for a head, not for a ticket. Recording the heads is
    what makes a later 'is this still true' answerable without asking the
    forge again."""
    info = pr_info_map or {}
    out = []
    for pr in prs or []:
        repo, pr_id = pr.get("repo", ""), pr.get("id", "")
        sha = (info.get((repo, pr_id)) or {}).get("head_sha") or pr.get("head_sha") or ""
        out.append(f"{repo}/{pr_id}@{sha}")
    return " ".join(sorted(out))


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve(instance_key: str) -> str:
    if instance_key:
        return instance_key
    try:
        return state.active_instance_key()
    except RuntimeError:
        return ""


def _unaddressed(action: str, instance_key: str, ticket_key: str, claim: str) -> None:
    log.emit("freshness_ledger_skipped",
             f"Could not {action} the {claim} claim: "
             f"instance_key={instance_key!r} ticket_key={ticket_key!r}",
             meta={"ticket": ticket_key, "claim": claim, "action": action,
                   "instance_key": instance_key})


def _failed(action: str, ticket_key: str, claim: str, error: Exception) -> None:
    log.emit("freshness_ledger_failed",
             f"Could not {action} the {claim} claim for {ticket_key}: "
             f"{type(error).__name__}: {error}",
             meta={"ticket": ticket_key, "claim": claim, "action": action,
                   "error": f"{type(error).__name__}: {error}"})


def record(ticket_key: str, claim: str, against: str,
           *, instance_key: str = "") -> None:
    """Establish a claim, against the thing that makes it true.

    A claim that already stands against the same subject is left alone, so
    established_at names when the claim was first made rather than when it
    was last polled. A claim that was invalidated is established again, even
    against the same subject: the invalidation says something happened in
    between, and the new row says it was checked after."""
    key = _resolve(instance_key)
    if not key or not ticket_key:
        _unaddressed("record", key, ticket_key, claim)
        return
    try:
        row = db.query_one(
            "SELECT established_against, invalidated_at FROM verification_claim"
            " WHERE instance_key=? AND ticket_key=? AND claim=?",
            (key, ticket_key, claim))
        if (row and row["invalidated_at"] is None
                and row["established_against"] == against):
            return
        db.execute(
            "INSERT INTO verification_claim"
            " (instance_key, ticket_key, claim, established_at,"
            "  established_against, invalidated_at, invalidated_by)"
            " VALUES (?, ?, ?, ?, ?, NULL, NULL)"
            " ON CONFLICT(instance_key, ticket_key, claim) DO UPDATE SET"
            "  established_at=excluded.established_at,"
            "  established_against=excluded.established_against,"
            "  invalidated_at=NULL, invalidated_by=NULL",
            (key, ticket_key, claim, _now(), against))
    except Exception as e:
        _failed("record", ticket_key, claim, e)


def invalidate(ticket_key: str, claim: str, by: str,
               *, instance_key: str = "") -> None:
    """Record that an event staled a claim.

    A claim that is already stale keeps the first event that staled it. That
    event is the one that explains the gap, and a second one would overwrite
    the answer with a later symptom."""
    key = _resolve(instance_key)
    if not key or not ticket_key:
        _unaddressed("invalidate", key, ticket_key, claim)
        return
    try:
        db.execute(
            "UPDATE verification_claim SET invalidated_at=?, invalidated_by=?"
            " WHERE instance_key=? AND ticket_key=? AND claim=?"
            "   AND invalidated_at IS NULL",
            (_now(), by, key, ticket_key, claim))
    except Exception as e:
        _failed("invalidate", ticket_key, claim, e)


def claims(instance_key: str, ticket_key: str) -> list[dict]:
    """Every claim recorded for a ticket, for the detail page to render.

    Returns an empty list rather than raising. This feeds a page, and a
    ledger that cannot be read must not take the page down with it."""
    key = _resolve(instance_key)
    if not key or not ticket_key:
        return []
    try:
        return db.query_all(
            "SELECT claim, established_at, established_against,"
            "       invalidated_at, invalidated_by"
            " FROM verification_claim"
            " WHERE instance_key=? AND ticket_key=? ORDER BY claim",
            (key, ticket_key))
    except Exception as e:
        _failed("read", ticket_key, "*", e)
        return []
