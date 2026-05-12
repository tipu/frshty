"""Aggregation queries for the manager's daily digest.

Important: tickets.updated_at is NOT a reliable staleness signal because
scan_tickets writes external_status on every poll, which bumps updated_at
(features/tickets.py:462). Use json_extract(data, '$.discovered_at') and
status semantics instead.
"""
import json
from pathlib import Path

import core.db as db
import core.state as state


_LIMIT = 50


def _load_ticket_data(row: dict) -> dict:
    try:
        return json.loads(row["data"]) if row["data"] else {}
    except (json.JSONDecodeError, ValueError):
        return {}


def stale_own_prs(instance_key: str, threshold_hours: int = 24) -> list[dict]:
    """Own PRs older than threshold with no review/approval signal.
    Reads from kv blob (state.save('own_prs', ...) — features/own_prs.py:42)."""
    pr_state = state.load("own_prs")
    if not pr_state:
        return []
    cutoff_row = db.query_one(
        "SELECT datetime('now', ?) AS cutoff",
        (f"-{threshold_hours} hours",),
    )
    cutoff_iso = cutoff_row["cutoff"] if cutoff_row else None
    out: list[dict] = []
    for pr_key, seen in pr_state.items():
        if not isinstance(seen, dict):
            continue
        created_on = seen.get("created_on") or ""
        if not created_on or not cutoff_iso:
            continue
        if created_on[:19] >= cutoff_iso[:19]:
            continue
        repo, _, pr_id = pr_key.partition("/")
        out.append({
            "repo": repo,
            "pr_id": pr_id,
            "title": seen.get("title", ""),
            "url": seen.get("url", ""),
            "created_on": created_on,
            "review_state": seen.get("review_state", ""),
            "approvers": seen.get("approvers", []),
            "ci_state": seen.get("ci_state", ""),
        })
    out.sort(key=lambda r: r["created_on"])
    return out[:_LIMIT]


def merge_ready_ticket_prs(instance_key: str, config: dict | None = None,
                            live: bool = False) -> list[dict]:
    """Tickets in_review with passing CI AND at least one approver. Reads
    approver state cached in ts['prs'][i]['approvers'] by features.tickets._check_in_review.
    Pass live=True to bypass cache and re-fetch from the platform (used by the
    manual Refresh button on /today).
    Serves: 'get my approved PRs merged'."""
    rows = db.query_all(
        "SELECT ticket_key, slug, data FROM tickets"
        " WHERE instance_key=? AND status='in_review'"
        " AND COALESCE(obsolete_at, '') = ''"
        " AND json_extract(data, '$.ci_passed')=1"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, _LIMIT),
    )
    if not rows:
        return []
    platform = None
    if live and config:
        try:
            from features.platforms import make_platform
            platform = make_platform(config)
        except Exception:
            platform = None
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        prs = d.get("prs") or []
        enriched: list[dict] = []
        any_approved = False
        for p in prs:
            approvers = list(p.get("approvers") or [])
            if platform:
                try:
                    info = platform.get_pr_info(p.get("repo", ""), p.get("id")) or {}
                    approvers = info.get("approvers") or []
                except Exception:
                    pass
            if approvers:
                any_approved = True
            enriched.append({
                "repo": p.get("repo"),
                "id": p.get("id"),
                "url": p.get("url"),
                "author": p.get("author"),
                "approvers": approvers,
            })
        if not prs or not any_approved:
            continue
        out.append({
            "ticket_key": r["ticket_key"],
            "summary": (d.get("summary") or "")[:140],
            "prs": enriched,
            "approval_status": "approved",
        })
    return out


def in_review_no_ci(instance_key: str) -> list[dict]:
    """Tickets in_review where CI hasn't passed yet (or failed). Diagnostic;
    not necessarily 'stuck' — could be a fresh PR. Renamed from 'stuck_in_review'."""
    rows = db.query_all(
        "SELECT ticket_key, slug, data FROM tickets"
        " WHERE instance_key=? AND status='in_review'"
        " AND COALESCE(obsolete_at, '') = ''"
        " AND (json_extract(data, '$.ci_passed') IS NOT 1"
        "      OR json_extract(data, '$.ci_passed') IS NULL)"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        out.append({
            "ticket_key": r["ticket_key"],
            "summary": (d.get("summary") or "")[:140],
            "ci_fix_attempts": d.get("ci_fix_attempts", 0),
            "discovered_at": d.get("discovered_at", ""),
            "url": d.get("url", ""),
        })
    return out


def ready_to_submit_prs(instance_key: str) -> list[dict]:
    """Tickets in pr_ready state with no open PRs — work that needs PR submitted.
    Serves: 'PR my dev-complete tickets'."""
    rows = db.query_all(
        "SELECT ticket_key, slug, data FROM tickets"
        " WHERE instance_key=? AND status='pr_ready'"
        " AND COALESCE(obsolete_at, '') = ''"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        if (d.get("prs") or []):
            continue
        out.append({
            "ticket_key": r["ticket_key"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
            "url": d.get("url", ""),
        })
    return out


def pickup_new_tickets(instance_key: str) -> list[dict]:
    """Tickets in new or pending_approval — daily nudge regardless of age.
    Serves: 'pick up new tickets'."""
    rows = db.query_all(
        "SELECT ticket_key, status, source, approval_status, data FROM tickets"
        " WHERE instance_key=?"
        " AND status IN ('new', 'pending_approval')"
        " AND COALESCE(obsolete_at, '') = ''"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        out.append({
            "ticket_key": r["ticket_key"],
            "status": r["status"],
            "source": r["source"],
            "approval_status": r["approval_status"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
            "url": d.get("url", ""),
        })
    return out


def pr_failed_tickets(instance_key: str) -> list[dict]:
    """Tickets in pr_failed state — CI or conflict resolution exhausted retries.
    No age threshold; the state itself signals immediate need for attention."""
    rows = db.query_all(
        "SELECT ticket_key, slug, data FROM tickets"
        " WHERE instance_key=? AND status='pr_failed'"
        " AND COALESCE(obsolete_at, '') = ''"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        prs = [
            {"repo": p.get("repo"), "id": p.get("id"), "url": p.get("url")}
            for p in (d.get("prs") or [])
        ]
        out.append({
            "ticket_key": r["ticket_key"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
            "ci_fix_attempts": d.get("ci_fix_attempts", 0),
            "url": d.get("url", ""),
            "prs": prs,
        })
    return out


def stale_unattended_tickets(instance_key: str, threshold_hours: int = 72) -> list[dict]:
    """Tickets older than threshold in mid-pipeline states — work that hasn't
    advanced. Excludes pr_ready and in_review (those have dedicated buckets).
    Renamed from 'stale_tickets' to disambiguate."""
    rows = db.query_all(
        "SELECT ticket_key, status, slug, data FROM tickets"
        " WHERE instance_key=?"
        " AND status IN ('planning', 'reviewing')"
        " AND COALESCE(obsolete_at, '') = ''"
        " AND datetime(json_extract(data, '$.discovered_at')) < datetime('now', ?)"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, f"-{threshold_hours} hours", _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        out.append({
            "ticket_key": r["ticket_key"],
            "status": r["status"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
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
    """Pending-approval tickets sitting longer than threshold."""
    rows = db.query_all(
        "SELECT ticket_key, source, slug, data FROM tickets"
        " WHERE instance_key=? AND status='pending_approval'"
        " AND COALESCE(obsolete_at, '') = ''"
        " AND datetime(json_extract(data, '$.discovered_at')) < datetime('now', ?)"
        " ORDER BY json_extract(data, '$.discovered_at') ASC LIMIT ?",
        (instance_key, f"-{threshold_hours} hours", _LIMIT),
    )
    out: list[dict] = []
    for r in rows:
        d = _load_ticket_data(r)
        out.append({
            "ticket_key": r["ticket_key"],
            "source": r["source"],
            "summary": (d.get("summary") or "")[:140],
            "discovered_at": d.get("discovered_at", ""),
        })
    return out


def pr_comments_needing_reply(instance_key: str, config: dict) -> list[dict]:
    """Tickets in_review with at least one pr_comments.json entry status='needs_reply'.
    File-walk because pr_comments is on disk (features/tickets.py:829-832)."""
    rows = db.query_all(
        "SELECT ticket_key, slug, data FROM tickets"
        " WHERE instance_key=? AND status='in_review'"
        " AND slug IS NOT NULL"
        " AND COALESCE(obsolete_at, '') = ''",
        (instance_key,),
    )
    ws = config.get("workspace") or {}
    root = ws.get("root")
    tickets_dir = ws.get("tickets_dir", "tickets")
    if not root:
        return []
    out: list[dict] = []
    for r in rows:
        slug = r["slug"]
        if not slug:
            continue
        path = Path(root) / tickets_dir / slug / "pr_comments.json"
        if not path.is_file():
            continue
        try:
            entries = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError, ValueError):
            continue
        if not isinstance(entries, list):
            continue
        needs_reply = [e for e in entries if isinstance(e, dict) and e.get("status") == "needs_reply"]
        if not needs_reply:
            continue
        d = _load_ticket_data(r)
        out.append({
            "ticket_key": r["ticket_key"],
            "summary": (d.get("summary") or "")[:140],
            "comments": [{
                "id": e.get("id"),
                "pr_repo": e.get("pr_repo"),
                "pr_id": e.get("pr_id"),
                "path": e.get("path"),
                "line": e.get("line"),
                "body": (e.get("body") or "")[:200],
                "suggested_reply": (e.get("suggested_reply") or "")[:200],
            } for e in needs_reply[:5]],
            "count": len(needs_reply),
        })
        if len(out) >= _LIMIT:
            break
    return out


def peer_pr_reviews(instance_key: str, config: dict | None = None,
                    live: bool = False) -> list[dict]:
    """Open PRs where the operator is a requested reviewer (not author) and
    has not yet approved. Reads the state.load('peer_reviews') cache populated
    by features.peer_reviews.refresh (scheduled as poll_peer_reviews).
    Pass live=True to bypass cache and re-fetch (manual Refresh button).
    Serves: 'review other PRs assigned to me'."""
    from features import peer_reviews
    if live and config:
        try:
            return peer_reviews.refresh(config)[:_LIMIT]
        except Exception:
            pass
    return peer_reviews.cached()[:_LIMIT]


def billcom_invoice_due(instance_key: str, config: dict,
                        live: bool = False) -> list[dict]:
    """Surface 'last calendar month not yet invoiced' as a manual-action item.
    Read-only — never sends or auto-creates. Defaults to the cached
    state.load('billing_invoices') blob (populated by features.billing on
    other code paths); pass live=True to force a bill.com round-trip (manual
    Refresh button)."""
    if not config.get("features", {}).get("billing"):
        return []
    b = config.get("billing") or {}
    if not b.get("billcom_customer_id"):
        return []
    if b.get("billing_freq") != "monthly":
        return []
    from datetime import date
    today = date.today()
    if today.month == 1:
        last_year, last_month = today.year - 1, 12
    else:
        last_year, last_month = today.year, today.month - 1
    last_month_str = f"{last_year:04d}-{last_month:02d}"

    invoices: list[dict] = []
    snapshot = state.load("billing_invoices_remote") or {}
    cached_invoices = list(snapshot.get("items") or [])
    if live or not cached_invoices:
        try:
            import asyncio
            from features import billing
            invoices = asyncio.run(billing.list_invoices(config)) or []
        except Exception:
            invoices = cached_invoices or [
                inv for inv in (state.load("billing_invoices") or {}).values()
                if isinstance(inv, dict)
            ]
    else:
        invoices = cached_invoices

    for inv in invoices:
        start = (inv.get("start") or "")[:7]
        end = (inv.get("end") or "")[:7]
        if start == last_month_str or end == last_month_str:
            return []
    base_url = config.get("_base_url", "")
    return [{
        "month": last_month_str,
        "create_url": f"{base_url}/billing" if base_url else "",
        "billcom_url": "https://app.bill.com/neo/inbox",
        "note": "draft locally if not started; verify SENT manually on bill.com (no auto-send)",
    }]


def timesheet_underfilled(instance_key: str, config: dict, target_hours: float = 8.0) -> list[dict]:
    """Last business day with < target_hours logged. Single-row return.
    Serves: 'last business day has 8h logged'."""
    if not config.get("features", {}).get("timesheet"):
        return []
    try:
        from features import timesheet
    except Exception:
        return []
    from datetime import date, timedelta
    today = date.today()
    day = today - timedelta(days=1)
    while day.weekday() >= 5:
        day = day - timedelta(days=1)
    try:
        sheet = timesheet.build_timesheet(config, day, day)
    except Exception:
        return []
    if not sheet:
        return []
    days = sheet.get("days") or []
    if not days:
        return []
    d0 = days[0]
    hours = d0.get("hours", 0) or 0
    if hours >= target_hours:
        return []
    return [{
        "date": d0.get("date") or day.isoformat(),
        "hours": hours,
        "target": target_hours,
        "shortfall": round(target_hours - hours, 2),
    }]


def aggregate_all(instance_key: str, config: dict | None = None,
                  thresholds: dict | None = None, live: bool = False) -> dict:
    t = thresholds or {}
    cfg = config or {}
    return {
        "merge_ready":            merge_ready_ticket_prs(instance_key, cfg, live=live),
        "ready_to_submit":        ready_to_submit_prs(instance_key),
        "pr_comments_needs_reply": pr_comments_needing_reply(instance_key, cfg),
        "peer_pr_reviews":        peer_pr_reviews(instance_key, cfg, live=live),
        "pickup_new":             pickup_new_tickets(instance_key),
        "in_review_no_ci":        in_review_no_ci(instance_key),
        "pr_failed_tickets":      pr_failed_tickets(instance_key),
        "stale_own_prs":          stale_own_prs(instance_key, t.get("stale_pr_hours", 24)),
        "stale_unattended":       stale_unattended_tickets(instance_key, t.get("stale_ticket_hours", 72)),
        "pending_approvals_stuck": pending_approvals_stuck(instance_key, t.get("pending_approval_hours", 12)),
        "regressions_recent":     regressions_recent(instance_key, t.get("regression_days", 7)),
        "timesheet_underfilled":  timesheet_underfilled(instance_key, cfg),
        "billcom_invoice_due":    billcom_invoice_due(instance_key, cfg, live=live),
    }
