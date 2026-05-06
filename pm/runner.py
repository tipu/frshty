"""PM agent runner. Persists findings to pm_review."""
import json
from datetime import datetime, timezone

import core.db as db
import core.log as log
import core.queue as q
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
    if findings:
        q.emit_event(
            source="pm",
            kind="pm_findings_written",
            payload={
                "ticket_key": ticket_key,
                "verdict": verdict,
                "findings_count": len(findings),
            },
            instance_key=instance_key,
        )
    return {
        "instance_key": instance_key,
        "ticket_key": ticket_key,
        "checkpoint_type": "pre_approval",
        "verdict": verdict,
        "findings": findings,
    }


def _shipped_tickets_for_section(instance_key: str, section_id: int) -> list[dict]:
    rows = db.query_all(
        "SELECT pst.ticket_key, t.status, t.data FROM prd_section_ticket pst"
        " LEFT JOIN tickets t ON t.instance_key=pst.instance_key AND t.ticket_key=pst.ticket_key"
        " WHERE pst.prd_section_id=? AND pst.instance_key=?",
        (section_id, instance_key),
    )
    out: list[dict] = []
    for r in rows:
        if r["status"] not in ("merged", "validation", "done"):
            continue
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        out.append({
            "key": r["ticket_key"],
            "status": r["status"],
            "summary": d.get("summary") or "",
            "description": (d.get("description") or "")[:500],
        })
    return out


def run_prd_update(instance_key: str, section_id: int,
                   old_section: dict, new_section: dict) -> dict | None:
    from pm.prd_prompts import PRD_UPDATE
    shipped = _shipped_tickets_for_section(instance_key, section_id)
    payload = (
        f"OLD SECTION (## {old_section['header']}):\n{old_section.get('content', '')}\n\n"
        f"NEW SECTION (## {new_section['header']}):\n{new_section.get('content', '')}\n\n"
        f"SHIPPED TICKETS FROM THIS SECTION:\n"
        + ("\n".join(f"  - {t['key']} [{t['status']}]: {t['summary']}" for t in shipped)
           or "  (none)")
    )
    raw = run_haiku(PRD_UPDATE + payload)
    if not raw:
        log.emit("pm_prd_update_failed", f"haiku empty for section {section_id}",
                 meta={"section_id": section_id})
        return None
    parsed = extract_json(raw)
    if not parsed or not isinstance(parsed.get("findings"), list):
        log.emit("pm_prd_update_parse_failed", f"parse failed for section {section_id}",
                 meta={"section_id": section_id, "raw": raw[:500]})
        return None
    db.execute(
        "INSERT INTO pm_review"
        "(instance_key, ticket_key, prd_section_id, checkpoint_type, verdict, findings, created_at)"
        " VALUES (?, NULL, ?, 'prd_update', ?, ?, ?)",
        (instance_key, section_id, parsed.get("verdict", "clean"),
         json.dumps(parsed.get("findings", [])), _now()),
    )
    log.emit("pm_prd_update_complete",
             f"[{instance_key}] section {section_id}: {parsed.get('verdict')}",
             meta={"section_id": section_id})
    return parsed


def run_post_shipping(instance_key: str) -> dict | None:
    from pm.prd_prompts import POST_SHIPPING_COHESION
    prd_row = db.query_one(
        "SELECT id FROM prd WHERE instance_key=?",
        (instance_key,),
    )
    if not prd_row:
        return None
    sections = db.query_all(
        "SELECT id, header, content FROM prd_section WHERE prd_id=? AND deleted_at IS NULL",
        (prd_row["id"],),
    )
    if not sections:
        return None
    parts: list[str] = ["PRD:"]
    for s in sections:
        parts.append(f"\n## {s['header']}\n{s['content']}")
    parts.append("\n\nSHIPPED TICKETS:")
    rows = db.query_all(
        "SELECT t.ticket_key, t.status, t.data FROM tickets t"
        " WHERE t.instance_key=? AND t.source='prd' AND t.status IN ('merged','validation','done')"
        " AND COALESCE(t.obsolete_at, '') = ''",
        (instance_key,),
    )
    for r in rows:
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        parts.append(f"\n- {r['ticket_key']} [{r['status']}]: {d.get('summary','')}")
    payload = "\n".join(parts)[:30000]
    raw = run_haiku(POST_SHIPPING_COHESION + payload, timeout=300)
    if not raw:
        return None
    parsed = extract_json(raw)
    if not parsed or not isinstance(parsed.get("findings"), list):
        return None
    db.execute(
        "INSERT INTO pm_review"
        "(instance_key, ticket_key, prd_section_id, checkpoint_type, verdict, findings, created_at)"
        " VALUES (?, NULL, NULL, 'post_shipping', ?, ?, ?)",
        (instance_key, parsed.get("verdict", "clean"),
         json.dumps(parsed.get("findings", [])), _now()),
    )
    log.emit("pm_post_shipping_complete",
             f"[{instance_key}] post-shipping cohesion: {parsed.get('verdict')}",
             meta={"findings_count": len(parsed.get('findings', []))})
    return parsed


def section_findings(instance_key: str, section_id: int, limit: int = 5) -> list[dict]:
    rows = db.query_all(
        "SELECT id, checkpoint_type, verdict, findings, created_at FROM pm_review"
        " WHERE instance_key=? AND prd_section_id=?"
        " ORDER BY created_at DESC LIMIT ?",
        (instance_key, section_id, limit),
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


def post_shipping_findings(instance_key: str, limit: int = 5) -> list[dict]:
    rows = db.query_all(
        "SELECT id, checkpoint_type, verdict, findings, created_at FROM pm_review"
        " WHERE instance_key=? AND checkpoint_type='post_shipping'"
        " ORDER BY created_at DESC LIMIT ?",
        (instance_key, limit),
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


def latest_pre_approval_review(instance_key: str, ticket_key: str) -> dict | None:
    rows = db.query_all(
        "SELECT id, verdict, findings, created_at FROM pm_review"
        " WHERE instance_key=? AND ticket_key=? AND checkpoint_type='pre_approval'"
        " ORDER BY created_at DESC LIMIT 1",
        (instance_key, ticket_key),
    )
    if not rows:
        return None
    r = rows[0]
    try:
        findings = json.loads(r["findings"]) if r["findings"] else []
    except (json.JSONDecodeError, ValueError):
        findings = []
    return {
        "id": r["id"],
        "verdict": r["verdict"],
        "findings": findings,
        "created_at": r["created_at"],
    }
