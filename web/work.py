import os
import uuid

from fastapi import APIRouter
from fastapi.responses import HTMLResponse, JSONResponse

import core.db as db
import core.log as log
import core.runtime as runtime
import core.terminal as terminal
from services import work_store
from web.pages import _template


router = APIRouter()

def _personal_config() -> dict | None:
    instances = runtime.instances()
    if not instances:
        return None
    entry = instances.get("personal")
    if not entry:
        return None
    return entry.config


@router.get("/work", response_class=HTMLResponse)
def work_page():
    return _template("work.html")


@router.get("/api/work/items")
def api_work_items():
    groups = work_store.grouped_items()
    return {"groups": groups, "counts": {g: len(rows) for g, rows in groups.items()},
            "personal_loaded": _personal_config() is not None}


@router.post("/api/work/items/{item_id}/action")
def api_work_action(item_id: int, body: dict):
    result = work_store.apply_action(item_id, (body.get("action") or "").strip(),
                                     until=body.get("until"))
    if "error" in result:
        return JSONResponse(result, status_code=400)
    return result


@router.post("/api/work/intake")
def api_work_intake(body: dict):
    objective = (body.get("text") or "").strip()
    if not objective:
        return JSONResponse({"error": "empty objective"}, status_code=400)
    config = _personal_config()
    if config is None:
        return JSONResponse({"error": "personal instance not loaded; work layer is read-only"},
                            status_code=503)
    cwd = (body.get("cwd") or "").strip() or str(config["workspace"]["root"])
    if not os.path.isdir(cwd):
        return JSONResponse({"error": f"cwd does not exist: {cwd}"}, status_code=400)
    item_id = work_store.create_item(objective, instance_key="personal")
    session_id = str(uuid.uuid4())
    tmux_key = f"work-{item_id}"
    run_id = work_store.add_run(item_id, session_id, tmux_key, cwd)
    context = (
        f"# Work item {item_id}\n\n## Objective\n\n{objective}\n\n"
        "Report a one-line checkpoint of where things stand before you stop."
    )
    try:
        with work_store.launch_lock:
            terminal.launch_claude(tmux_key, cwd, session_id, context, True, config=config)
        health = terminal.session_healthy(tmux_key)
        if not health.get("alive"):
            raise RuntimeError("tmux session did not start")
    except Exception as e:
        work_store.mark_launch_failed(run_id, f"{type(e).__name__}: {e}")
        log.emit("work_launch_failed", f"work item {item_id}: {type(e).__name__}: {e}")
        return JSONResponse({"error": f"launch failed: {e}", "item_id": item_id}, status_code=500)
    counts = {g: len(rows) for g, rows in work_store.grouped_items().items()}
    return {"item_id": item_id, "run_id": run_id, "session_id": session_id,
            "tmux_key": tmux_key, "state": "agent_working", "counts": counts}


@router.get("/work/{item_id}/terminal", response_class=HTMLResponse)
def work_terminal_page(item_id: int):
    return _template("work_terminal.html")


@router.get("/api/work/items/{item_id}/events")
def api_work_events(item_id: int):
    rows = db.query_all(
        "SELECT id, work_run_id, kind, payload, created_at FROM work_events "
        "WHERE work_item_id = ? ORDER BY id DESC LIMIT 100",
        (item_id,),
    )
    return {"events": rows}
