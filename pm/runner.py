"""PM agent runner. Persists findings to pm_review."""
import json
from datetime import datetime, timezone

import core.db as db
import core.log as log
from core.claude_runner import extract_json, run_haiku
from pm.prompts import PRE_APPROVAL


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _candidate_summary(instance_key: str, exclude_key: str, limit: int = 50) -> str:
    """Active tickets + recently-merged tickets, summarized as one line each."""
    rows = db.query_all(
        "SELECT ticket_key, status, slug, data FROM tickets"
        " WHERE instance_key=? AND ticket_key != ? AND status != 'done'"
        " ORDER BY updated_at DESC LIMIT ?",
        (instance_key, exclude_key, limit),
    )
    out: list[str] = []
    for r in rows:
        try:
            d = json.loads(r["data"])
        except (json.JSONDecodeError, ValueError):
            d = {}
        summary = (d.get("summary") or d.get("description") or "")[:140]
        out.append(f"  - {r['ticket_key']} [{r['status']}] {r['slug'] or ''}: {summary}")
    return "\n".join(out) if out else "  (no candidates)"


def _ticket_summary(ticket_data: dict) -> str:
    title = ticket_data.get("summary") or ""
    desc = (ticket_data.get("description") or "")[:1000]
    src = ticket_data.get("source", "?")
    crit_blob = ticket_data.get("acceptance_criteria_json") or {}
    crit_text = ""
    if isinstance(crit_blob, dict):
        crit_text = json.dumps(crit_blob.get("criteria", []), indent=2)[:1500]
    return (
        f"TICKET source={src}\n"
        f"summary: {title}\n"
        f"description:\n{desc}\n"
        f"acceptance_criteria:\n{crit_text}\n"
    )


def run_pre_approval(instance_key: str, ticket_key: str, ticket_data: dict) -> dict | None:
    """Run pre-approval PM review for a ticket. Persists to pm_review.
    Returns the saved review dict, or None on failure."""
    if not ticket_key or not ticket_data:
        return None
    candidates = _candidate_summary(instance_key, ticket_key)
    payload = (
        f"{_ticket_summary(ticket_data)}\n"
        f"CANDIDATE_SET (active + in-flight tickets in same instance):\n"
        f"{candidates}\n"
    )
    raw = run_haiku(PRE_APPROVAL + payload)
    if not raw:
        log.emit("pm_review_failed",
                 f"haiku returned empty for pre-approval review of {ticket_key}",
                 meta={"ticket": ticket_key})
        return None
    parsed = extract_json(raw)
    if not parsed or not isinstance(parsed.get("findings"), list):
        log.emit("pm_review_parse_failed",
                 f"could not parse pm output for {ticket_key}",
                 meta={"ticket": ticket_key, "raw": raw[:500]})
        return None
    verdict = parsed.get("verdict", "clean")
    findings = parsed.get("findings", [])
    db.execute(
        "INSERT INTO pm_review"
        "(instance_key, ticket_key, prd_section_id, checkpoint_type, verdict, findings, created_at)"
        " VALUES (?, ?, NULL, 'pre_approval', ?, ?, ?)",
        (instance_key, ticket_key, verdict, json.dumps(findings), _now()),
    )
    log.emit(
        "pm_review_complete",
        f"[{instance_key}] {ticket_key} pre-approval: {verdict} ({len(findings)} findings)",
        meta={"ticket": ticket_key, "verdict": verdict, "findings_count": len(findings)},
    )
    return {
        "instance_key": instance_key,
        "ticket_key": ticket_key,
        "checkpoint_type": "pre_approval",
        "verdict": verdict,
        "findings": findings,
    }


def latest_findings(instance_key: str, ticket_key: str, limit: int = 20) -> list[dict]:
    rows = db.query_all(
        "SELECT id, checkpoint_type, verdict, findings, created_at FROM pm_review"
        " WHERE instance_key=? AND ticket_key=?"
        " ORDER BY created_at DESC LIMIT ?",
        (instance_key, ticket_key, limit),
    )
    out: list[dict] = []
    for r in rows:
        try:
            findings = json.loads(r["findings"]) if r["findings"] else []
        except (json.JSONDecodeError, ValueError):
            findings = []
        out.append({
            "id": r["id"],
            "checkpoint_type": r["checkpoint_type"],
            "verdict": r["verdict"],
            "findings": findings,
            "created_at": r["created_at"],
        })
    return out


def findings_count(instance_key: str, ticket_key: str) -> int:
    """Total findings across most-recent pre_approval review."""
    rows = db.query_all(
        "SELECT findings FROM pm_review"
        " WHERE instance_key=? AND ticket_key=? AND checkpoint_type='pre_approval'"
        " ORDER BY created_at DESC LIMIT 1",
        (instance_key, ticket_key),
    )
    if not rows:
        return 0
    try:
        f = json.loads(rows[0]["findings"]) if rows[0]["findings"] else []
        return len(f) if isinstance(f, list) else 0
    except (json.JSONDecodeError, ValueError):
        return 0
