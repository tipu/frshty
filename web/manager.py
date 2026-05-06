from fastapi import APIRouter
from fastapi.responses import JSONResponse

from web.state import _config


router = APIRouter()


@router.get("/api/manager/latest")
def api_manager_latest():
    from manager import runner
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"empty": True}
    out = runner.latest(instance_key)
    return out or {"empty": True}


@router.post("/api/manager/run-now")
def api_manager_run_now():
    from manager import runner
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    out = runner.run_daily_digest(instance_key, _config)
    if out is None:
        return JSONResponse({"error": "haiku unavailable"}, status_code=503)
    return out


@router.get("/api/manager/status")
def api_manager_status():
    from manager import runner
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"enabled": False, "policy_stale": False}
    enabled = bool((_config.get("manager") or {}).get("enabled"))
    current_hash = runner.current_priorities_hash(_config)
    latest = runner.latest(instance_key)
    last_hash = (latest or {}).get("priorities_hash") or ""
    policy_stale = bool(current_hash and last_hash and current_hash != last_hash)
    return {
        "enabled": enabled,
        "policy_stale": policy_stale,
        "current_priorities_hash": current_hash,
        "last_digest_at": (latest or {}).get("generated_at"),
    }
