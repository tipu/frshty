"""High-level PRD scan orchestration: read, parse, diff, act, persist."""
import json
from datetime import datetime, timezone

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
from prd import diff as prd_diff
from prd import generator
from prd import parser
from prd import reader


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_prd_row(instance_key: str, file_path: str) -> dict:
    row = db.query_one(
        "SELECT id, file_path, last_parsed_at, last_file_hash FROM prd"
        " WHERE instance_key=?",
        (instance_key,),
    )
    if row:
        return dict(row)
    db.execute(
        "INSERT INTO prd (instance_key, file_path) VALUES (?, ?)",
        (instance_key, file_path),
    )
    row = db.query_one(
        "SELECT id, file_path, last_parsed_at, last_file_hash FROM prd"
        " WHERE instance_key=?",
        (instance_key,),
    )
    return dict(row)


def _save_section(prd_id: int, section: dict) -> int:
    db.execute(
        "INSERT INTO prd_section (prd_id, stable_key, header, content, content_hash)"
        " VALUES (?, ?, ?, ?, ?)"
        " ON CONFLICT(prd_id, stable_key) DO UPDATE SET"
        "  header=excluded.header, content=excluded.content,"
        "  content_hash=excluded.content_hash, deleted_at=NULL",
        (prd_id, section["stable_key"], section["header"], section["content"],
         section["content_hash"]),
    )
    row = db.query_one(
        "SELECT id FROM prd_section WHERE prd_id=? AND stable_key=?",
        (prd_id, section["stable_key"]),
    )
    return row["id"] if row else 0


def _link_ticket(section_id: int, instance_key: str, ticket_key: str) -> None:
    db.execute(
        "INSERT OR IGNORE INTO prd_section_ticket"
        " (prd_section_id, instance_key, ticket_key) VALUES (?, ?, ?)",
        (section_id, instance_key, ticket_key),
    )


def _generate_ticket_key(instance_key: str, section_stable_key: str, idx: int) -> str:
    """Synthetic key for generated tickets. Format: PRD-<section>-<idx>"""
    base = section_stable_key.upper().replace("-", "_")[:40]
    return f"PRD-{base}-{idx}"


def _create_generated_ticket(instance_key: str, section_id: int,
                             section: dict, gen_ticket: dict, idx: int,
                             config: dict) -> str:
    key = _generate_ticket_key(instance_key, section["stable_key"], idx)
    # respect approval gate; PRD source defaults to pending_approval
    apr_cfg = config.get("ticket_approval") or {}
    sources = apr_cfg.get("sources")
    approval_required = apr_cfg.get("required") and (sources is None or "prd" in sources)
    initial_status = "pending_approval" if approval_required else "new"
    ts = {
        "status": initial_status,
        "source": "prd",
        "approval_status": "pending" if initial_status == "pending_approval" else None,
        "summary": gen_ticket.get("title") or gen_ticket.get("summary") or "",
        "description": gen_ticket.get("description") or gen_ticket.get("summary") or "",
        "acceptance_criteria_json": gen_ticket.get("acceptance_criteria"),
        "acceptance_criteria_source_text": (
            gen_ticket.get("acceptance_criteria", {}) or {}
        ).get("source_text") or "",
        "discovered_at": _now(),
        "prd_section_id": section_id,
    }
    state.save_ticket(key, ts)
    _link_ticket(section_id, instance_key, key)
    if (config.get("pm_agent") or {}).get("enabled", True):
        q.enqueue_job(instance_key, "pm_pre_approval", ticket_key=key)
    if not approval_required:
        q.enqueue_job(instance_key, "start_planning", ticket_key=key)
    return key


def _obsolete_section_tickets(instance_key: str, section_id: int) -> dict:
    """Mark tickets attached to a deleted section.
    Unmerged → done + obsolete_at; merged/done → obsolete_at flag only."""
    rows = db.query_all(
        "SELECT ticket_key FROM prd_section_ticket"
        " WHERE prd_section_id=? AND instance_key=?",
        (section_id, instance_key),
    )
    obsoleted_unmerged: list[str] = []
    flagged_merged: list[str] = []
    for r in rows:
        key = r["ticket_key"]
        ts = state.load_ticket(key)
        if not ts:
            continue
        cur = ts.get("status")
        ts["obsolete_at"] = _now()
        if cur in ("merged", "validation", "done"):
            state.save_ticket(key, ts)
            flagged_merged.append(key)
        else:
            try:
                state.transition_ticket(key, "done", obsolete_at=ts["obsolete_at"],
                                        approval_status="rejected",
                                        done_at=_now())
                obsoleted_unmerged.append(key)
            except state.TicketStateError:
                ts["obsolete_at"] = _now()
                state.save_ticket(key, ts)
                flagged_merged.append(key)
            try:
                from features import validation as v
                v.remove_from_pool(instance_key, key)
            except Exception:
                pass
    return {"obsoleted_unmerged": obsoleted_unmerged, "flagged_merged": flagged_merged}


def scan(config: dict, instance_key: str) -> dict:
    """Read configured PRD, diff against persisted state, act on changes.
    Returns a summary of work done."""
    prd_cfg = config.get("prd") or {}
    if not prd_cfg.get("enabled"):
        return {"skipped": "prd disabled"}
    file_path = prd_cfg.get("file")
    if not file_path:
        log.emit("prd_no_file", "[prd].file not configured but [prd].enabled=true")
        return {"error": "no file configured"}
    read_result = reader.read(file_path)
    if read_result is None:
        log.emit("prd_unreadable", f"PRD file missing or unreadable: {file_path}")
        return {"error": "file unreadable"}
    text, file_hash = read_result
    prd_row = _ensure_prd_row(instance_key, file_path)
    if prd_row.get("last_file_hash") == file_hash:
        return {"skipped": "file unchanged"}

    parsed = parser.parse(text)
    persisted = prd_diff.load_persisted(prd_row["id"])
    d = prd_diff.diff(parsed, persisted)

    summary: dict = {"new": 0, "modified": 0, "deleted": 0,
                     "tickets_created": 0, "tickets_obsoleted": 0,
                     "tickets_flagged": 0}

    for n in d["new"]:
        sid = _save_section(prd_row["id"], n)
        gen_tickets = generator.generate(n)
        for idx, gt in enumerate(gen_tickets, 1):
            _create_generated_ticket(instance_key, sid, n, gt, idx, config)
            summary["tickets_created"] += 1
        summary["new"] += 1

    for old, new in d["modified"]:
        sid = _save_section(prd_row["id"], new)
        if (config.get("pm_agent") or {}).get("enabled", True):
            q.enqueue_job(instance_key, "pm_prd_update", payload={
                "section_id": sid,
                "old": {"header": old["header"], "content": old["content"]},
                "new": {"header": new["header"], "content": new["content"]},
            })
        gen_tickets = generator.generate(new)
        for idx, gt in enumerate(gen_tickets, 1):
            existing = db.query_all(
                "SELECT ticket_key FROM prd_section_ticket"
                " WHERE prd_section_id=? AND instance_key=?",
                (sid, instance_key),
            )
            existing_keys = {r["ticket_key"] for r in existing}
            attempt = idx
            while True:
                synth_key = _generate_ticket_key(instance_key, new["stable_key"], attempt)
                if synth_key not in existing_keys:
                    break
                attempt += 1
            ts = {
                "status": "pending_approval" if (config.get("ticket_approval") or {}).get("required") else "new",
                "source": "prd",
                "summary": gt.get("title") or "",
                "description": gt.get("description") or "",
                "acceptance_criteria_json": gt.get("acceptance_criteria"),
                "discovered_at": _now(),
                "prd_section_id": sid,
            }
            state.save_ticket(synth_key, ts)
            _link_ticket(sid, instance_key, synth_key)
            if (config.get("pm_agent") or {}).get("enabled", True):
                q.enqueue_job(instance_key, "pm_pre_approval", ticket_key=synth_key)
            if ts["status"] == "new":
                q.enqueue_job(instance_key, "start_planning", ticket_key=synth_key)
            summary["tickets_created"] += 1
        summary["modified"] += 1

    for old in d["deleted"]:
        result = _obsolete_section_tickets(instance_key, old["id"])
        summary["tickets_obsoleted"] += len(result["obsoleted_unmerged"])
        summary["tickets_flagged"] += len(result["flagged_merged"])
        db.execute(
            "UPDATE prd_section SET deleted_at=? WHERE id=?",
            (_now(), old["id"]),
        )
        summary["deleted"] += 1

    db.execute(
        "UPDATE prd SET last_parsed_at=?, last_file_hash=? WHERE id=?",
        (_now(), file_hash, prd_row["id"]),
    )
    log.emit("prd_scan_complete",
             f"[{instance_key}] PRD scan: {json.dumps(summary)}",
             meta=summary)
    return summary


def regenerate_section_tickets(instance_key: str, section_id: int, config: dict) -> dict:
    """Re-run the generator for a single existing section and create tickets
    with non-colliding keys. Returns counts; does not touch existing tickets."""
    row = db.query_one(
        "SELECT id, stable_key, header, content FROM prd_section WHERE id=?",
        (section_id,),
    )
    if not row:
        return {"error": "section not found"}
    section = dict(row)
    gen_tickets = generator.generate(section)
    if not gen_tickets:
        return {"created": 0, "section_id": section_id,
                "reason": "generator returned no tickets"}
    existing = db.query_all(
        "SELECT ticket_key FROM prd_section_ticket"
        " WHERE prd_section_id=? AND instance_key=?",
        (section_id, instance_key),
    )
    existing_keys = {r["ticket_key"] for r in existing}
    created = 0
    next_idx = 1
    for gt in gen_tickets:
        while _generate_ticket_key(instance_key, section["stable_key"], next_idx) in existing_keys:
            next_idx += 1
        key = _create_generated_ticket(instance_key, section_id, section, gt, next_idx, config)
        existing_keys.add(key)
        created += 1
        next_idx += 1
    log.emit("prd_section_regenerated",
             f"[{instance_key}] regenerated {created} tickets for section {section_id} ({section['stable_key']})",
             meta={"section_id": section_id, "created": created})
    return {"created": created, "section_id": section_id}


def render_for_ui(instance_key: str) -> dict:
    """Snapshot for /api/prd: PRD metadata + sections + tickets per section."""
    prd_row = db.query_one(
        "SELECT id, file_path, last_parsed_at, last_file_hash FROM prd"
        " WHERE instance_key=?",
        (instance_key,),
    )
    if not prd_row:
        return {"prd": None, "sections": []}
    sections = db.query_all(
        "SELECT id, stable_key, header, content, deleted_at FROM prd_section"
        " WHERE prd_id=? ORDER BY id",
        (prd_row["id"],),
    )
    from pm import runner as pm_runner
    out_sections: list[dict] = []
    for s in sections:
        ticket_rows = db.query_all(
            "SELECT pst.ticket_key, t.status, t.approval_status, t.obsolete_at, t.data"
            " FROM prd_section_ticket pst"
            " LEFT JOIN tickets t ON t.instance_key=pst.instance_key AND t.ticket_key=pst.ticket_key"
            " WHERE pst.prd_section_id=? AND pst.instance_key=?",
            (s["id"], instance_key),
        )
        tickets = []
        for tr in ticket_rows:
            try:
                d = json.loads(tr["data"]) if tr["data"] else {}
            except (ValueError, TypeError):
                d = {}
            tickets.append({
                "key": tr["ticket_key"],
                "status": tr["status"],
                "approval_status": tr["approval_status"],
                "obsolete_at": tr["obsolete_at"],
                "summary": d.get("summary") or "",
            })
        out_sections.append({
            "id": s["id"],
            "stable_key": s["stable_key"],
            "header": s["header"],
            "content": s["content"],
            "deleted_at": s["deleted_at"],
            "tickets": tickets,
            "pm_findings": pm_runner.section_findings(instance_key, s["id"]),
        })
    return {
        "prd": dict(prd_row),
        "sections": out_sections,
        "post_shipping_findings": pm_runner.post_shipping_findings(instance_key),
    }
