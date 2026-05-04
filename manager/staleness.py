"""Aggregation queries for the manager's daily digest.

Important: tickets.updated_at is NOT a reliable staleness signal because
scan_tickets writes external_status on every poll, which bumps updated_at.
Use json_extract(data, '$.discovered_at') and status semantics instead.
"""
import json

import core.db as db


_LIMIT = 50


def stale_own_prs(instance_key: str, threshold_hours: int = 24) -> list[dict]:
    rows = db.query_all(
        "SELECT repo, pr_id, data FROM own_prs"
        " WHERE instance_key=?"
        " ORDER BY updated_at DESC LIMIT ?",
        (instance_key, _LIMIT * 4),
    )
    out: list[dict] = []
    cutoff = f"-{threshold_hours} hours"
    cutoff_row = db.query_one(
        "SELECT datetime('now', ?) AS cutoff",
        (cutoff,),
    )
    cutoff_iso = cutoff_row["cutoff"] if cutoff_row else None
    for r in rows:
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        created_on = d.get("created_on") or ""
        if cutoff_iso and created_on and created_on[:19] < cutoff_iso[:19]:
            out.append({
                "repo": r["repo"],
                "pr_id": r["pr_id"],
                "title": d.get("title", ""),
                "url": d.get("url", ""),
                "created_on": created_on,
                "review_state": d.get("review_state", ""),
            })
        if len(out) >= _LIMIT:
            break
    return out


def stale_tickets(instance_key: str, threshold_hours: int = 72) -> list[dict]:
    rows = db.query_all(
        "SELECT ticket_key, status, slug, data FROM tickets"
        " WHERE instance_key=?"
        " AND status NOT IN ('done', 'in_review', 'pr_ready', 'merged', 'validation')"
        " AND COALESCE(obsolete_at, '') = ''"
        " AND datetime(json_extract(data, '$.discovered_at')) < datetime('now', ?)"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, f"-{threshold_hours} hours", _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        out.append({
            "ticket_key": r["ticket_key"],
            "status": r["status"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
            "url": d.get("url", ""),
        })
    return out


def stuck_in_review(instance_key: str) -> list[dict]:
    rows = db.query_all(
        "SELECT ticket_key, slug, data FROM tickets"
        " WHERE instance_key=? AND status='in_review'"
        " AND (json_extract(data, '$.ci_passed') IS NOT 1"
        "      OR json_extract(data, '$.ci_passed') IS NULL)"
        " ORDER BY updated_at DESC LIMIT ?",
        (instance_key, _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        out.append({
            "ticket_key": r["ticket_key"],
            "summary": (d.get("summary") or "")[:140],
            "ci_passed": d.get("ci_passed"),
            "ci_fix_attempts": d.get("ci_fix_attempts", 0),
            "url": d.get("url", ""),
        })
    return out


def regressions_recent(instance_key: str, days: int = 7) -> list[dict]:
    rows = db.query_all(
        "SELECT ticket_key, criterion_id, run_at, triggered_by_ticket_key"
        " FROM validation_run"
        " WHERE instance_key=? AND result='regression'"
        " AND run_at > datetime('now', ?)"
        " ORDER BY run_at DESC LIMIT ?",
        (instance_key, f"-{days} days", _LIMIT),
    )
    return [dict(r) for r in rows]


def pending_approvals_stuck(instance_key: str, threshold_hours: int = 12) -> list[dict]:
    rows = db.query_all(
        "SELECT ticket_key, source, slug, data FROM tickets"
        " WHERE instance_key=? AND status='pending_approval'"
        " AND datetime(json_extract(data, '$.discovered_at')) < datetime('now', ?)"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, f"-{threshold_hours} hours", _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        out.append({
            "ticket_key": r["ticket_key"],
            "source": r["source"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
        })
    return out


def aggregate_all(instance_key: str) -> dict:
    return {
        "stale_own_prs":           stale_own_prs(instance_key),
        "stale_tickets":           stale_tickets(instance_key),
        "stuck_in_review":         stuck_in_review(instance_key),
        "regressions_recent":      regressions_recent(instance_key),
        "pending_approvals_stuck": pending_approvals_stuck(instance_key),
    }
