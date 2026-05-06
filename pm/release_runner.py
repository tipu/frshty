"""Whole-release inspection runner. Mirrors pm.runner.run_post_shipping."""
from __future__ import annotations

import core.log as log
from core.claude_runner import extract_json, run_haiku
from features import releases as _releases
from pm.release_prompt import RELEASE_INSPECT


def run_release_inspection(instance_key: str, release_id: int,
                           config: dict, force: bool = False) -> dict | None:
    """Run a whole-release inspection. Idempotent against unchanged inputs.

    Returns the inserted release_review row, or None if skipped/failed.
    """
    rel = _releases.get_release_by_id(instance_key, release_id)
    if not rel:
        log.emit("release_inspect_skipped", "release missing",
                 meta={"instance": instance_key, "release_id": release_id})
        return None
    tickets = _releases.list_release_tickets(instance_key, release_id)
    if not tickets:
        log.emit("release_inspect_skipped", "no tickets",
                 meta={"release_key": rel["release_key"]})
        return None
    if not _releases.all_terminal(instance_key, release_id):
        log.emit("release_inspect_skipped", "not all tickets terminal",
                 meta={"release_key": rel["release_key"]})
        return None
    md_text, md_hash = _releases.read_release_md(config, rel["release_key"])
    new_hash = _releases.compute_ticket_set_hash(tickets, md_hash)
    if not force and rel.get("ticket_set_hash") == new_hash:
        log.emit("release_inspect_skipped", "idempotent: ticket_set_hash unchanged",
                 meta={"release_key": rel["release_key"], "hash": new_hash})
        return None
    payload = _releases.build_inspection_payload(config, rel, tickets, md_text)
    raw = run_haiku(RELEASE_INSPECT + payload[:30000], timeout=300)
    if not raw:
        log.emit("release_inspect_failed",
                 f"haiku returned empty for release {rel['release_key']}",
                 meta={"release_key": rel["release_key"]})
        return None
    parsed = extract_json(raw)
    if not parsed or parsed.get("verdict") not in ("pass", "fail"):
        log.emit("release_inspect_parse_failed",
                 f"could not parse haiku output for release {rel['release_key']}",
                 meta={"release_key": rel["release_key"], "raw_head": (raw or "")[:500]})
        return None
    findings = parsed.get("findings") or []
    if not isinstance(findings, list):
        findings = []
    verdict = parsed["verdict"]
    review = _releases.insert_release_review(
        release_id=release_id,
        instance_key=instance_key,
        verdict=verdict,
        findings=findings,
        ticket_set_hash=new_hash,
        release_md_hash=md_hash,
    )
    _releases.mark_release_inspected(instance_key, release_id, ticket_set_hash=new_hash)
    log.emit("release_review_complete",
             f"[{instance_key}] release {rel['release_key']} → {verdict} ({len(findings)} findings)",
             meta={"release_key": rel["release_key"], "verdict": verdict,
                   "findings_count": len(findings)})
    return review
