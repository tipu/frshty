from fastapi import APIRouter

import core.scheduler as scheduler
import features.autopilot as autopilot
from web.state import _config


router = APIRouter()


@router.get("/api/autopilot")
def api_autopilot():
    instance_key = _config.get("job", {}).get("key", "")
    schedule = next((r for r in scheduler.list_all(instance_key)
                     if r["key"] == "autopilot_sweep"), {})
    return {
        "enabled": autopilot.enabled(_config),
        "cadence": autopilot.cadence(_config),
        "next_run_at": schedule.get("run_at"),
        "last_run_at": schedule.get("last_run_at"),
        "runs": autopilot.runs(instance_key),
    }
