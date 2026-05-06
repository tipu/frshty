from fastapi import APIRouter
from fastapi.responses import JSONResponse

from web.state import _config


router = APIRouter()


@router.get("/api/prd")
def api_prd():
    from prd import orchestrator
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"prd": None, "sections": []}
    return orchestrator.render_for_ui(instance_key)


@router.post("/api/prd/reload")
def api_prd_reload():
    from prd import orchestrator
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    summary = orchestrator.scan(_config, instance_key)
    return summary


@router.post("/api/prd/sections/{section_id}/regenerate")
def api_prd_regenerate(section_id: int):
    from prd import orchestrator
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    out = orchestrator.regenerate_section_tickets(instance_key, section_id, _config)
    if out.get("error"):
        return JSONResponse(out, status_code=404)
    return out
