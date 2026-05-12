from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.db as db
import core.log as log
import manager.runner as runner
import manager.staleness as staleness
from web.state import _config


router = APIRouter()

_ALLOWED_LOOPS = frozenset({
    "merge_ready", "ready_to_submit", "pr_comments_needs_reply",
    "peer_pr_reviews", "pickup_new", "in_review_no_ci", "pr_failed_tickets",
    "stale_own_prs", "stale_unattended", "pending_approvals_stuck",
    "regressions_recent", "timesheet_underfilled", "billcom_invoice_due",
})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@router.get("/api/today/loops")
def api_today_loops():
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    thresholds = (_config.get("manager") or {}).get("thresholds") or {}
    try:
        loops = staleness.aggregate_all(instance_key, config=_config, thresholds=thresholds)
    except Exception as e:
        log.emit("today_loops_aggregate_failed",
                 f"[{instance_key}] aggregate_all crashed: {type(e).__name__}: {e}")
        return JSONResponse({"error": f"aggregate_all failed: {e}"}, status_code=500)

    counts = {k: len(v) for k, v in loops.items()}
    snoozed = _list_active_snoozes(instance_key)

    latest = runner.latest(instance_key)
    current_hash = runner.current_priorities_hash(_config)
    last_hash = (latest or {}).get("priorities_hash") or ""
    policy_stale = bool(current_hash and last_hash and current_hash != last_hash)

    return {
        "instance_key": instance_key,
        "generated_at": _now_iso(),
        "loops": loops,
        "counts": counts,
        "snoozed": snoozed,
        "policy_stale": policy_stale,
        "manager_latest": latest,
        "errors": [],
    }


@router.post("/api/today/snoozes")
async def api_today_snooze_create(body: dict):
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    loop_type = (body.get("loop_type") or "").strip()
    entity_id = (body.get("entity_id") or "").strip()
    if not loop_type or not entity_id:
        return JSONResponse({"error": "loop_type and entity_id required"}, status_code=400)
    if loop_type not in _ALLOWED_LOOPS:
        return JSONResponse({"error": f"unknown loop_type: {loop_type}"}, status_code=400)
    snooze_until = body.get("snooze_until")
    reason = (body.get("reason") or "").strip() or None
    _upsert_snooze(instance_key, loop_type, entity_id, snooze_until, reason)
    return {"status": "snoozed", "loop_type": loop_type,
            "entity_id": entity_id, "snooze_until": snooze_until}


@router.delete("/api/today/snoozes/{loop_type}/{entity_id:path}")
def api_today_snooze_delete(loop_type: str, entity_id: str):
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    _delete_snooze(instance_key, loop_type, entity_id)
    return {"status": "removed"}


def _upsert_snooze(instance_key, loop_type, entity_id, snooze_until, reason):
    db.execute(
        "INSERT INTO today_snoozes(instance_key, loop_type, entity_id, snooze_until, created_at, reason)"
        " VALUES (?, ?, ?, ?, ?, ?)"
        " ON CONFLICT(instance_key, loop_type, entity_id) DO UPDATE SET"
        " snooze_until=excluded.snooze_until, created_at=excluded.created_at, reason=excluded.reason",
        (instance_key, loop_type, entity_id, snooze_until, _now_iso(), reason),
    )


def _delete_snooze(instance_key, loop_type, entity_id):
    db.execute(
        "DELETE FROM today_snoozes WHERE instance_key=? AND loop_type=? AND entity_id=?",
        (instance_key, loop_type, entity_id),
    )


def _list_active_snoozes(instance_key: str) -> list[dict]:
    rows = db.query_all(
        "SELECT loop_type, entity_id, snooze_until, created_at, reason"
        " FROM today_snoozes"
        " WHERE instance_key=?"
        " AND (snooze_until IS NULL OR snooze_until > datetime('now'))",
        (instance_key,),
    )
    return [dict(r) for r in rows]
