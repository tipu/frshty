from fastapi import APIRouter, Body
from fastapi.responses import HTMLResponse, JSONResponse

from services import standup, work_launch, work_store
from web.pages import _template
from web.state import _config


router = APIRouter()


@router.get("/standup", response_class=HTMLResponse)
def standup_page():
    resp = _template("standup.html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


def _refused(e: standup.StandupError) -> JSONResponse:
    return JSONResponse({"error": str(e)}, status_code=409)


def _payload(day: str | None = None) -> dict:
    config = _config
    view = standup.day_view(day, config)
    view["projects"] = work_launch.project_entries()
    view["personal_loaded"] = work_launch.personal_config() is not None
    view["rail_attention"] = work_store.attention_count()
    view["enabled"] = standup.enabled(config)
    return view


@router.get("/api/standup")
def api_standup(day: str | None = None):
    """Today's standup, or a past day. A day nobody drafted is not created by
    reading it: the draft is a morning beat, and a read must not start a day
    the operator never opened."""
    return _payload(day)


@router.post("/api/standup/draft")
def api_standup_draft(body: dict | None = Body(default=None)):
    config = _config
    day = (body or {}).get("day") or None
    standup.ensure_day(config, day)
    return _payload(day)


@router.post("/api/standup/redraft")
def api_standup_redraft(body: dict | None = Body(default=None)):
    config = _config
    day = (body or {}).get("day") or None
    try:
        standup.redraft(standup.ensure_day(config, day), config)
    except standup.StandupError as e:
        return _refused(e)
    return _payload(day)


@router.post("/api/standup/open")
def api_standup_open(body: dict | None = Body(default=None)):
    config = _config
    day = (body or {}).get("day") or None
    try:
        standup.open_day(standup.ensure_day(config, day))
    except standup.StandupError as e:
        return _refused(e)
    return _payload(day)


@router.post("/api/standup/close")
def api_standup_close(body: dict | None = Body(default=None)):
    config = _config
    day = (body or {}).get("day") or None
    view = standup.day_view(day, config)
    if not view["exists"]:
        return JSONResponse({"error": "no standup for that day"}, status_code=404)
    try:
        standup.close_day(view["id"], config)
    except standup.StandupError as e:
        return _refused(e)
    return _payload(day)


@router.post("/api/standup/items")
def api_standup_add(body: dict):
    config = _config
    day = body.get("day") or None
    try:
        standup.add_item(standup.ensure_day(config, day), body.get("text") or "",
                         contexts=body.get("contexts") or "")
    except standup.StandupError as e:
        return _refused(e)
    return _payload(day)


@router.post("/api/standup/items/{item_id}")
def api_standup_update(item_id: int, body: dict):
    try:
        standup.update_item(item_id, text=body.get("text"),
                            contexts=body.get("contexts"))
    except standup.StandupError as e:
        return _refused(e)
    return _payload(body.get("day") or None)


@router.post("/api/standup/items/{item_id}/state")
def api_standup_state(item_id: int, body: dict):
    try:
        standup.set_item_state(item_id, body.get("state") or "")
    except standup.StandupError as e:
        return _refused(e)
    return _payload(body.get("day") or None)


@router.post("/api/standup/items/{item_id}/snooze")
def api_standup_snooze(item_id: int, body: dict):
    try:
        standup.snooze(item_id, body.get("until") or "")
    except standup.StandupError as e:
        return _refused(e)
    return _payload(body.get("day") or None)


@router.post("/api/standup/items/{item_id}/answer")
def api_standup_answer(item_id: int, body: dict):
    config = _config
    try:
        out = standup.answer(item_id, body.get("option") or "", config)
    except standup.StandupError as e:
        return _refused(e)
    payload = _payload(body.get("day") or None)
    launch = out.get("launch") or {}
    if "error" in launch:
        payload["launch_error"] = launch["error"]
    if out.get("stale"):
        payload["answer_stale"] = ("the question changed while you answered it; "
                                   "read the one on the item now")
    return payload


@router.post("/api/standup/items/{item_id}/compose")
def api_standup_compose(item_id: int, body: dict):
    """The ad hoc box on one action item.

    It answers with the whole day plus whatever the request produced, because
    a side question returns an answer the operator has to read and a launch
    changes the item's linked tasks."""
    try:
        out = standup.compose(item_id, body.get("text") or "",
                              agent=body.get("agent") or "claude")
    except standup.StandupError as e:
        return _refused(e)
    payload = _payload(body.get("day") or None)
    if "error" in out:
        payload["compose_error"] = out["error"]
    else:
        payload["compose_result"] = out
    return payload


@router.post("/api/standup/reorder")
def api_standup_reorder(body: dict):
    config = _config
    day = body.get("day") or None
    view = standup.day_view(day, config)
    if not view["exists"]:
        return JSONResponse({"error": "no standup for that day"}, status_code=404)
    standup.reorder(view["id"], [int(i) for i in body.get("order") or []])
    return _payload(day)


@router.get("/api/standup/items/{item_id}/events")
def api_standup_events(item_id: int):
    return {"events": standup.events(item_id)}
