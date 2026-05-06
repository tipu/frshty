from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.log as log
import core.state as state
import core.tz as _ctz
from web.state import _config


router = APIRouter()


_WEEKDAY = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _project_virtual_rows(config: dict, days: int = 7) -> list[dict]:
    pst = ZoneInfo("America/Los_Angeles")
    out = []
    recurring = config.get("timesheet", {}).get("recurring", []) or []
    if not recurring:
        return out
    today = _ctz.today_local()
    for offset in range(days):
        day = today + timedelta(days=offset)
        for entry in recurring:
            day_names = [d.lower() for d in entry.get("days", [])]
            weekdays = {_WEEKDAY[n] for n in day_names if n in _WEEKDAY}
            if day.weekday() not in weekdays:
                continue
            fire_pst = datetime.combine(day, time(19, 0), tzinfo=pst)
            fire_utc = fire_pst.astimezone(timezone.utc)
            if fire_utc < datetime.now(timezone.utc):
                continue
            out.append({
                "key": f"config:timesheet:{entry.get('ticket','?')}:{day.isoformat()}",
                "type": "recurring_virtual",
                "source": "config",
                "mutable": False,
                "task": "log_worklog",
                "run_at": fire_utc.isoformat(),
                "ticket": entry.get("ticket", ""),
                "time": entry.get("time", ""),
                "label": entry.get("label", ""),
            })
    return out


@router.get("/api/scheduled")
def api_scheduled():
    import core.scheduler as _sch
    instance_key = _config.get("job", {}).get("key", "")
    rows = _sch.list_all(instance_key) if instance_key else _sch.list_all()
    items = []
    for r in rows:
        kind = r.get("kind") or "oneshot"
        if kind == "recurring":
            items.append({
                "key": r["key"],
                "type": "recurring",
                "task": r.get("task"),
                "cadence": r.get("cadence"),
                "run_at": r["run_at"],
                "last_run_at": r.get("last_run_at"),
                "payload": r.get("payload", {}),
                "source": "scheduler",
                "mutable": True,
            })
        else:
            items.append({
                "key": r["key"],
                "type": "scheduled_pr" if r.get("action") == "create_pr" else "oneshot",
                "run_at": r["run_at"],
                "scheduled_at": r.get("scheduled_at"),
                "action": r.get("action"),
                "meta": r.get("meta", {}),
                "source": "scheduler",
                "mutable": True,
            })
    items.extend(_project_virtual_rows(_config))
    items.sort(key=lambda x: x.get("run_at") or "")
    tickets = state.load("tickets")
    for key, ts in tickets.items():
        if (ts.get("status") == "in_review" and not ts.get("ci_passed")
                and not ts.get("ci_fix_attempts")):
            items.append({"key": key, "type": "ci_pending", "status": ts["status"],
                          "checks_started_at": ts.get("checks_started_at"),
                          "branch": ts.get("branch", ""),
                          "prs": ts.get("prs", [])})
    return items


@router.post("/api/scheduled/{key}/reschedule")
def api_reschedule(key: str, body: dict):
    if key.startswith("config:"):
        return JSONResponse(
            {"error": "this row is derived from config (read-only); edit the TOML and restart"},
            status_code=405,
        )
    import core.db as _db
    instance_key = _config.get("job", {}).get("key", "")
    row = _db.query_one(
        "SELECT data FROM scheduler WHERE instance_key=? AND key=?",
        (instance_key, key),
    )
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    run_at = body.get("run_at", "")
    if not run_at:
        return JSONResponse({"error": "run_at required"}, status_code=400)
    try:
        dt = datetime.fromisoformat(run_at)
    except ValueError:
        return JSONResponse({"error": "invalid datetime"}, status_code=400)
    from core.scheduler import _to_utc_iso
    _db.execute(
        "UPDATE scheduler SET run_at=? WHERE instance_key=? AND key=?",
        (_to_utc_iso(dt), instance_key, key),
    )
    log.emit("schedule_updated", f"Rescheduled {key} to {run_at}",
        meta={"ticket": key, "run_at": run_at})
    return {"status": "updated", "run_at": _to_utc_iso(dt)}
