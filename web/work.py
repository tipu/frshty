import os

from fastapi import APIRouter, Body, Request
from fastapi.responses import (FileResponse, HTMLResponse, JSONResponse,
                               RedirectResponse, Response)

import core.db as db
import core.terminal as terminal
from services import (work_artifacts, work_debrief, work_launch, work_peers,
                      work_store, work_tags, work_worktree)
from web.pages import _template


router = APIRouter()



def _fresh(resp: HTMLResponse) -> HTMLResponse:
    resp.headers["Cache-Control"] = "no-store"
    return resp


@router.get("/tasks", response_class=HTMLResponse)
def tasks_page():
    return _fresh(_template("work.html"))


@router.get("/threads", response_class=HTMLResponse)
def threads_page():
    return _fresh(_template("threads.html"))


@router.get("/threads/{root_id}", response_class=HTMLResponse)
def thread_detail_page(root_id: int):
    return _fresh(_template("thread_detail.html"))


@router.get("/work")
def work_page():
    return RedirectResponse("/tasks", status_code=308)


DONE_PAGE_SIZE = 20


@router.get("/api/work/peers")
def api_work_peers():
    """The remote task boards this host can read and write.

    The board asks every source for its own items and merges them in the
    browser, so a peer that is asleep costs one failed request instead of
    stalling the local board."""
    return {"peers": work_peers.peers()}


@router.get("/api/work/peers/{key}/{path:path}")
def api_work_peer_get(key: str, path: str, request: Request):
    status, payload = work_peers.request(key, "GET", path,
                                         params=dict(request.query_params))
    return JSONResponse(payload, status_code=status)


@router.post("/api/work/peers/{key}/{path:path}")
def api_work_peer_post(key: str, path: str, request: Request,
                       body: dict | None = Body(default=None)):
    status, payload = work_peers.request(key, "POST", path,
                                         params=dict(request.query_params),
                                         body=body or {})
    return JSONResponse(payload, status_code=status)


@router.get("/api/work/threads")
def api_work_threads():
    return {"threads": work_store.threads(),
            "attention": work_store.attention_count()}


@router.get("/api/work/threads/{root_id}")
def api_work_thread(root_id: int):
    result = work_store.thread_detail(root_id)
    if "error" in result:
        return JSONResponse(result, status_code=404)
    result["rail_attention"] = work_store.attention_count()
    result["personal_loaded"] = work_launch.personal_config() is not None
    result["projects"] = work_launch.project_entries()
    result["agents"] = list(terminal.AGENTS)
    result["slack_available"] = work_launch.slack_available()
    return result


@router.post("/api/work/threads/{root_id}/tasks")
def api_work_thread_task(root_id: int, body: dict):
    """Launch a task into a thread.

    Thread membership is the follow-up chain, so a new member has to continue
    an existing one. The newest completed task is the source: its summary and
    artifacts are the compressed context the design asks a thread to pass on."""
    thread = work_store.thread_detail(root_id)
    if "error" in thread:
        return JSONResponse(thread, status_code=404)
    if thread["continue_from"] is None:
        return JSONResponse(
            {"error": "no completed task in this thread yet; a new task continues a completed one"},
            status_code=409)
    result = work_launch.launch_followup(thread["continue_from"], body.get("text") or "",
                                         cwd=body.get("cwd") or "",
                                         contexts=body.get("contexts"),
                                         slack=body.get("slack"),
                                         agent=body.get("agent") or "")
    if "error" in result:
        status = 503 if "personal instance" in result["error"] else (
            500 if "launch failed" in result["error"] else 400)
        return JSONResponse(result, status_code=status)
    return result


@router.post("/api/work/threads/{root_id}/archive")
def api_work_thread_archive(root_id: int):
    """Archive every completed task in one thread.

    A thread page lists the whole chain, so the operator who reads it there is
    the one who wants it off the board. The call archives the completed tasks
    only, so an unacknowledged task stays where the operator can read it."""
    result = work_store.archive_thread(root_id)
    if "error" in result:
        return JSONResponse(result, status_code=404)
    return result


@router.get("/api/work/items")
def api_work_items(q: str = "", tags: str = "", done_page: int = 1, archive: int = 0):
    groups = work_store.grouped_items(q=q, tags=tags, archived=bool(archive))
    threads = work_store.thread_map()
    for rows in groups.values():
        for row in rows:
            row["thread"] = threads.get(row["id"])
    counts = {g: len(rows) for g, rows in groups.items()}
    done_pages = max(1, -(-counts["done"] // DONE_PAGE_SIZE))
    done_page = min(max(1, done_page), done_pages)
    start = (done_page - 1) * DONE_PAGE_SIZE
    groups["done"] = groups["done"][start:start + DONE_PAGE_SIZE]
    return {"groups": groups, "counts": counts,
            "all_tags": work_tags.known_tags(),
            "done_page": done_page, "done_pages": done_pages,
            "personal_loaded": work_launch.personal_config() is not None,
            "projects": work_launch.project_entries(),
            "agents": list(terminal.AGENTS),
            "slack_available": work_launch.slack_available()}


@router.post("/api/work/items/archive-completed")
def api_work_archive_completed():
    return {"archived": work_store.archive_completed()}


@router.post("/api/work/items/{item_id}/action")
def api_work_action(item_id: int, body: dict):
    result = work_store.apply_action(item_id, (body.get("action") or "").strip(),
                                     until=body.get("until"))
    if "error" in result:
        return JSONResponse(result, status_code=400)
    return result


@router.post("/api/work/items/{item_id}/approve")
def api_work_approve(item_id: int, body: dict | None = Body(default=None)):
    """Approve a proposal frshty opened by itself and start the agent on it.

    A proposal is the only task on the board that has never run. Approval is
    what turns it into a normal task, so it goes through the same launch every
    other task uses."""
    result = work_launch.launch_proposed(item_id,
                                         agent=(body or {}).get("agent") or "claude")
    if "error" in result:
        error = result["error"]
        status = 503 if "personal instance" in error else (
            500 if "launch failed" in error else (
                409 if "awaiting approval" in error else 400))
        return JSONResponse(result, status_code=status)
    return result


@router.post("/api/work/items/{item_id}/worktree")
def api_work_worktree(item_id: int, body: dict):
    """Hand one task a worktree of the repository it tried to write into.

    The work hook runs as its own process and resolves no instance config, so
    it cannot work out a project's base branch or dependency commands and
    cannot create a worktree itself. It asks here instead. The repository
    comes from the file the session tried to change, which names the
    repository even for a project that holds several."""
    repo_path = (body.get("repo_path") or "").strip()
    if not repo_path:
        return JSONResponse({"error": "repo_path is required"}, status_code=400)
    # Deliberately outside work_store.launch_lock. Creating a worktree fetches
    # from the remote and installs dependencies, and holding the global launch
    # lock across either would stall every launch, resume and other write gate
    # behind it, until their own HTTP calls time out. The task asking here has
    # a live session, and gc keeps any worktree whose holder has one.
    row = work_worktree.ensure_for_repo(
        item_id, repo_path, entries=work_launch.project_entries())
    if not row:
        return JSONResponse(
            {"error": f"no worktree could be created for {repo_path}"},
            status_code=409)
    work_worktree.adopt_run(item_id, row["path"])
    return {"path": row["path"], "branch": row["branch"],
            "base_branch": row["base_branch"], "repo_path": row["repo_path"]}


@router.post("/api/work/intake")
def api_work_intake(body: dict):
    result = work_launch.launch(body.get("text") or "", cwd=body.get("cwd") or "",
                                contexts=body.get("contexts") or [],
                                slack=bool(body.get("slack")),
                                agent=body.get("agent") or "claude",
                                repo=body.get("repo") or "",
                                no_worktree=bool(body.get("no_worktree")))
    if "error" in result:
        status = 503 if "personal instance" in result["error"] else (
            500 if "launch failed" in result["error"] else 400)
        return JSONResponse(result, status_code=status)
    result["counts"] = {g: len(rows) for g, rows in work_store.grouped_items().items()}
    return result


@router.post("/api/work/items/{item_id}/reply")
def api_work_reply(item_id: int, body: dict):
    text = (body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "empty reply"}, status_code=400)
    result = work_store.reply(item_id, text)
    if "error" in result:
        return JSONResponse(result, status_code=409)
    return result


@router.post("/api/work/items/{item_id}/btw")
def api_work_btw(item_id: int, body: dict):
    result = work_store.side_question(item_id, body.get("text") or "")
    if "error" in result:
        return JSONResponse(result, status_code=409)
    return result


@router.post("/api/work/items/{item_id}/followup")
def api_work_followup(item_id: int, body: dict):
    """Launch a task that continues one finished task.

    The board offers this on a task the agent reported done, where the
    operator has read the report and wants the next run without leaving the
    board. A body that carries no projects and no agent inherits both from
    the source task, so the follow-up runs where its source ran."""
    result = work_launch.launch_followup(item_id, body.get("text") or "",
                                         cwd=body.get("cwd") or "",
                                         contexts=body.get("contexts"),
                                         slack=body.get("slack"),
                                         agent=body.get("agent") or "")
    if "error" in result:
        status = 503 if "personal instance" in result["error"] else (
            500 if "launch failed" in result["error"] else 400)
        return JSONResponse(result, status_code=status)
    return result


@router.get("/api/work/items/{item_id}/detail")
def api_work_detail(item_id: int):
    result = work_store.item_detail(item_id)
    if "error" in result:
        return JSONResponse(result, status_code=404)
    result["followups"] = work_debrief.followups_for(item_id)
    result["projects"] = work_launch.project_entries()
    result["agents"] = list(terminal.AGENTS)
    result["slack_available"] = work_launch.slack_available()
    result["system_prompt"] = work_launch.read_system_prompt(result["runs"])
    result["thread"] = work_store.thread_map().get(item_id)
    result["attention"] = work_store.attention_count()
    result["worktree"] = work_worktree.for_item(item_id)
    return result


@router.get("/api/work/items/{item_id}/transcript-image/{image_id}")
def api_work_transcript_image(item_id: int, image_id: str):
    run = db.query_one(
        "SELECT id, transcript_path, provider, cwd, started_at, agent_session_id "
        "FROM work_runs WHERE work_item_id = ? ORDER BY id DESC LIMIT 1", (item_id,))
    image = work_store.transcript_image(
        work_store.resolve_transcript_path(run), image_id) if run else None
    if image is None:
        return JSONResponse({"error": "transcript image not found"}, status_code=404)
    data, media_type = image
    return Response(content=data, media_type=media_type, headers={
        "Cache-Control": "private, max-age=3600",
        "Content-Security-Policy": "sandbox",
        "X-Content-Type-Options": "nosniff",
    })


@router.get("/tasks/{item_id}", response_class=HTMLResponse)
def task_detail_page(item_id: int):
    return _fresh(_template("work_detail.html"))


@router.get("/tasks/{item_id}/terminal", response_class=HTMLResponse)
def task_terminal_page(item_id: int):
    return _fresh(_template("work_terminal.html"))


@router.get("/work/{item_id}")
def work_detail_page(item_id: int):
    return RedirectResponse(f"/tasks/{item_id}", status_code=308)


@router.get("/work/{item_id}/terminal")
def work_terminal_page(item_id: int):
    return RedirectResponse(f"/tasks/{item_id}/terminal", status_code=308)



@router.get("/api/work/linkmap")
def api_work_linkmap():
    return work_launch.link_map()


@router.get("/api/work/artifacts")
def api_work_artifacts(q: str = ""):
    return {"artifacts": work_store.find_artifacts(q)}


@router.get("/tasks/{item_id}/summary", response_class=HTMLResponse)
def task_summary_page(item_id: int):
    return _fresh(_template("work_summary.html"))


@router.get("/work/{item_id}/summary")
def work_summary_page(item_id: int):
    return RedirectResponse(f"/tasks/{item_id}/summary", status_code=308)


@router.get("/api/work/items/{item_id}/summary")
def api_work_summary(item_id: int):
    item = db.query_one(
        "SELECT id, objective, state, summary, updated_at FROM work_items WHERE id = ?",
        (item_id,))
    if not item:
        return JSONResponse({"error": "unknown work item"}, status_code=404)
    artifacts = db.query_all(
        "SELECT id, path, note FROM work_artifacts WHERE work_item_id = ? ORDER BY id",
        (item_id,))
    return {"item": item, "artifacts": artifacts,
            "followups": work_debrief.followups_for(item_id)}


_SCRATCH_ROOT = "/tmp/"


def _artifact_roots(artifact_id: int) -> list[str]:
    roots = [str(work_artifacts.root()) + os.sep, _SCRATCH_ROOT]
    rows = db.query_all(
        "SELECT r.cwd FROM work_artifacts a "
        "JOIN work_runs r ON r.work_item_id = a.work_item_id "
        "WHERE a.id = ? AND r.cwd != ''", (artifact_id,))
    candidates = [r["cwd"] for r in rows]
    candidates += [e["root"] for e in work_launch.project_entries() if e["root"]]
    for path in candidates:
        root = os.path.realpath(path) + os.sep
        if root not in roots:
            roots.append(root)
    return roots


def _serve_artifact(artifact_id: int, real: str, shown: str):
    if not os.path.isfile(real):
        return JSONResponse({"error": f"file missing: {shown}"}, status_code=404)
    if not any(real.startswith(root) for root in _artifact_roots(artifact_id)):
        return JSONResponse(
            {"error": f"artifact path outside the run's workspace: {shown}"},
            status_code=403)
    resp = FileResponse(real)
    resp.headers["Content-Security-Policy"] = "sandbox"
    return resp


@router.get("/api/work/artifact_file/{artifact_id}")
def api_work_artifact_file(artifact_id: int):
    row = db.query_one("SELECT path FROM work_artifacts WHERE id = ?", (artifact_id,))
    if not row:
        return JSONResponse({"error": "unknown artifact"}, status_code=404)
    real = os.path.realpath(row["path"])
    if real.lower().endswith((".html", ".htm")):
        return RedirectResponse(f"/api/work/artifact_file/{artifact_id}/",
                                status_code=307)
    return _serve_artifact(artifact_id, real, row["path"])


@router.get("/api/work/artifact_file/{artifact_id}/{name:path}")
def api_work_artifact_asset(artifact_id: int, name: str):
    row = db.query_one("SELECT path FROM work_artifacts WHERE id = ?", (artifact_id,))
    if not row:
        return JSONResponse({"error": "unknown artifact"}, status_code=404)
    real = os.path.realpath(row["path"])
    if not name:
        return _serve_artifact(artifact_id, real, row["path"])
    folder = os.path.dirname(real) + os.sep
    asset = os.path.realpath(os.path.join(folder, name))
    if not asset.startswith(folder):
        return JSONResponse(
            {"error": f"asset path outside the artifact folder: {name}"},
            status_code=403)
    return _serve_artifact(artifact_id, asset, name)


@router.post("/api/work/items/{item_id}/debrief")
def api_work_debrief(item_id: int):
    result = work_debrief.run_debrief(item_id)
    if "error" in result:
        return JSONResponse(result, status_code=409)
    return result


@router.post("/api/work/followups/{followup_id}/send")
def api_followup_send(followup_id: int, body: dict):
    result = work_debrief.send_followup(followup_id, text=(body.get("text") or ""),
                                        contexts=body.get("contexts") or [],
                                        slack=bool(body.get("slack")),
                                        agent=body.get("agent") or "claude")
    if "error" in result:
        return JSONResponse(result, status_code=409)
    return result


@router.post("/api/work/followups/{followup_id}/dismiss")
def api_followup_dismiss(followup_id: int):
    result = work_debrief.dismiss_followup(followup_id)
    if "error" in result:
        return JSONResponse(result, status_code=409)
    return result


@router.get("/api/work/items/{item_id}/events")
def api_work_events(item_id: int):
    rows = db.query_all(
        "SELECT id, work_run_id, kind, payload, created_at FROM work_events "
        "WHERE work_item_id = ? ORDER BY id DESC LIMIT 100",
        (item_id,),
    )
    return {"events": rows}
