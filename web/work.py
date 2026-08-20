import os
import threading
import time
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


def _fresh(resp: HTMLResponse) -> HTMLResponse:
    resp.headers["Cache-Control"] = "no-store"
    return resp


@router.get("/work", response_class=HTMLResponse)
def work_page():
    return _fresh(_template("work.html"))


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
        "Work toward the objective. When you stop, state a one-line checkpoint. "
        "If you need a decision only the operator can make, ask the question and stop. "
        "Never send outward communications (Slack messages, GitHub or Bitbucket comments, "
        "emails, posts to external services) unless the operator explicitly asks for that "
        "in this conversation; draft the content and ask instead. "
        "When you produce a file the operator will open (report, page, video, image), "
        "print a line: ARTIFACT: /absolute/path - one-line description. "
        f"When the objective is fully met, end your final message with the single line {work_store.DONE_MARKER}."
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
    threading.Thread(target=_kickoff, args=(tmux_key, run_id), daemon=True).start()
    counts = {g: len(rows) for g, rows in work_store.grouped_items().items()}
    return {"item_id": item_id, "run_id": run_id, "session_id": session_id,
            "tmux_key": tmux_key, "state": "agent_working", "counts": counts}


@router.post("/api/work/items/{item_id}/reply")
def api_work_reply(item_id: int, body: dict):
    text = (body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "empty reply"}, status_code=400)
    result = work_store.reply(item_id, text)
    if "error" in result:
        return JSONResponse(result, status_code=409)
    return result


@router.get("/api/work/items/{item_id}/detail")
def api_work_detail(item_id: int):
    result = work_store.item_detail(item_id)
    if "error" in result:
        return JSONResponse(result, status_code=404)
    return result


@router.get("/work/{item_id}", response_class=HTMLResponse)
def work_detail_page(item_id: int):
    return _fresh(_template("work_detail.html"))


@router.get("/work/{item_id}/terminal", response_class=HTMLResponse)
def work_terminal_page(item_id: int):
    return _fresh(_template("work_terminal.html"))


def _kickoff(tmux_key: str, run_id: int):
    try:
        for _ in range(30):
            time.sleep(3)
            if terminal.session_healthy(tmux_key).get("claude_running"):
                time.sleep(4)
                if work_store.tmux_send(tmux_key, "Begin the objective from your system prompt now."):
                    return
                break
        work_store.mark_launch_failed(run_id, "kickoff never delivered: Claude did not start in the pane")
    except Exception as e:
        work_store.mark_launch_failed(run_id, f"kickoff error: {type(e).__name__}: {e}")


@router.get("/api/work/artifacts")
def api_work_artifacts(q: str = ""):
    return {"artifacts": work_store.find_artifacts(q)}


@router.get("/api/work/items/{item_id}/events")
def api_work_events(item_id: int):
    rows = db.query_all(
        "SELECT id, work_run_id, kind, payload, created_at FROM work_events "
        "WHERE work_item_id = ? ORDER BY id DESC LIMIT 100",
        (item_id,),
    )
    return {"events": rows}
