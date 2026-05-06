#!/usr/bin/env python3
import asyncio
import contextlib
import json
import multiprocessing
import os
import random
import re
import shlex
import subprocess
import sys
import threading
import time
import traceback
import urllib.parse
import urllib.request
from uuid import uuid4

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from starlette.websockets import WebSocket
from pathlib import Path

import core.config as cfg
import core.log as log
import core.state as state
import core.terminal as terminal
from core.claude_runner import run_haiku
from core.config import get_repos
from core.ticket_status import TicketStatus
from features.platforms import make_platform
import features.own_prs as own_prs
import features.reviewer as reviewer
import features.slack_monitor as slack_monitor
import features.tickets as _tickets_mod
import features.timesheet as ts
import features.billing as billing
from features.billing import OverlapError
import core.events as events
import core.scheduler as scheduler
from services import review_store
from actions.record_demo import handle as _record_demo_action
from actions.schedule_pr import handle as _schedule_pr_action

events.register_action("record_demo", _record_demo_action)
events.register_action("schedule_pr", _schedule_pr_action)

STATIC_DIR = Path(__file__).parent / "static"
CUSTOM_CONTEXT_DIR = Path(__file__).parent / "docs" / "custom-context"

from web.state import _cv_config, _config, _configs_by_host, primary_config as _primary_config, set_primary_config as _set_primary_config, events_enabled as _events_enabled, ensure_path as _ensure_path
from web.pages import router as _pages_router
from web.slack import router as _slack_router
from web.timesheet import router as _timesheet_router
from web.billing import router as _billing_router
from web.scheduling import router as _scheduling_router
from web.prd import router as _prd_router
from web.manager import router as _manager_router
from web.observability import router as _observability_router
from web.reviews import router as _reviews_router


if len(sys.argv) >= 2 and Path(sys.argv[1]).is_file():
    _primary = cfg.load_config(sys.argv[1])
    _set_primary_config(_primary)
    import core.llm as _llm
    _llm.configure(_primary)
    state.init(_primary["_state_dir"])
    log.init(_primary["_state_dir"], _primary["job"]["key"])
    # db.init will be called by start_events() in main()


@contextlib.asynccontextmanager
async def _lifespan(a):
    # Event system started in main() before uvicorn
    yield


app = FastAPI(lifespan=_lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.middleware("http")
async def resolve_instance_by_host(request, call_next):
    """In --multi mode, pick the active config by matching the request Host header.

    Unknown hosts fall through to whatever config is currently the contextvar
    default (typically the primary). Single-instance mode is a no-op.
    """
    config_token = None
    state_token = None
    log_tokens = None
    if _configs_by_host:
        host = (request.headers.get("host") or "").split(":")[0].lower()
        target = _configs_by_host.get(host)
        if target is not None:
            config_token = _cv_config.set(target)
            state_token = state.use(target["_state_dir"])
            log_tokens = log.use(target["_state_dir"], target["job"]["key"])
    try:
        response = await call_next(request)
    finally:
        if log_tokens is not None:
            log.reset(log_tokens)
        if state_token is not None:
            state.reset(state_token)
        if config_token is not None:
            _cv_config.reset(config_token)
    return response


@app.middleware("http")
async def profile_requests(request, call_next):
    rid = uuid4().hex[:6]
    path = request.url.path
    method = request.method
    t0 = time.time()
    print(f"[REQ {rid}] {time.strftime('%H:%M:%S')} {method} {path} enter", flush=True)
    try:
        response = await call_next(request)
    except Exception as e:
        elapsed = time.time() - t0
        print(f"[REQ {rid}] {time.strftime('%H:%M:%S')} {method} {path} ERROR after {elapsed:.2f}s: {e!r}", flush=True)
        raise
    elapsed = time.time() - t0
    print(f"[REQ {rid}] {time.strftime('%H:%M:%S')} {method} {path} done {elapsed:.2f}s status={response.status_code}", flush=True)
    response.headers["X-Response-Time"] = f"{elapsed:.3f}s"
    return response


app.include_router(_pages_router)
app.include_router(_slack_router)
app.include_router(_timesheet_router)
app.include_router(_billing_router)
app.include_router(_scheduling_router)
app.include_router(_prd_router)
app.include_router(_manager_router)
app.include_router(_observability_router)
app.include_router(_reviews_router)


_LOCKFILE_NOISE = frozenset({
    "Pipfile.lock", "poetry.lock", "uv.lock",
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "npm-shrinkwrap.json",
    "Cargo.lock", "Gemfile.lock", "go.sum", "composer.lock", "mix.lock",
})


def _changed_files(wt: Path, base_ref: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--name-only", f"{base_ref}...HEAD"],
        cwd=str(wt), capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        return []
    return [f for f in result.stdout.splitlines() if f.strip()]


def _is_meaningful_change(files: list[str]) -> bool:
    if not files:
        return False
    return any(f.split("/")[-1] not in _LOCKFILE_NOISE for f in files)


@app.get("/api/tickets/{ticket_key}/pr-info")
def api_ticket_pr_info(ticket_key: str):
    import features.tickets as tickets_mod

    try:
        tickets = state.load("tickets")
        ticket = tickets.get(ticket_key)
        if not ticket:
            return JSONResponse({"error": "Ticket not found"}, status_code=404)

        slug = ticket.get("slug", "")
        if not slug:
            return JSONResponse({"error": "No slug found"}, status_code=400)

        ws = _config.get("workspace", {})
        base_branch = ws.get("base_branch", "main")
        ticket_dir = ws.get("root", Path(".")) / ws.get("tickets_dir", "tickets") / slug
        docs_dir = ticket_dir / "docs"

        ticket_summary = ""
        if docs_dir.is_dir():
            summary_cache = docs_dir / ".change-summary.txt"
            manifest = docs_dir / "change-manifest.md"
            if manifest.exists():
                if summary_cache.exists() and summary_cache.stat().st_mtime >= manifest.stat().st_mtime:
                    ticket_summary = summary_cache.read_text()
                else:
                    ticket_summary = run_haiku(
                        f"Summarize this change manifest in 2-3 sentences. Be direct and technical.\n\n{manifest.read_text()[:4000]}"
                    )
                    if ticket_summary:
                        summary_cache.write_text(ticket_summary)

        default_title = f"{ticket_key}: {ticket_summary.split('.')[0] if ticket_summary else 'Work'}"
        default_description = ticket_summary if ticket_summary else f"Implementation for {ticket_key}"

        repos_out = []
        for repo in get_repos(_config):
            wt = tickets_mod.ticket_worktree_path(_config, slug, repo["name"])
            if not wt.is_dir():
                continue
            subprocess.run(["git", "fetch", "origin", base_branch],
                           cwd=str(wt), capture_output=True, timeout=60)
            files = _changed_files(wt, f"origin/{base_branch}")
            if not _is_meaningful_change(files):
                continue
            branch = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=str(wt), capture_output=True, text=True, timeout=10,
            ).stdout.strip() or ticket.get("branch", "")
            repos_out.append({
                "name": repo["name"],
                "branch": branch,
                "files_changed": len(files),
                "title": default_title,
                "description": default_description,
            })

        return {"repos": repos_out}
    except Exception as e:
        log.emit("pr_info_error", f"Error generating PR info: {e}", meta={"ticket": ticket_key})
        return JSONResponse({"error": str(e)}, status_code=500)


@app.post("/api/tickets/{ticket_key}/submit-pr")
async def api_submit_pr(ticket_key: str, request: Request):
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    return await asyncio.to_thread(_submit_pr_sync, ticket_key, data)


def _submit_pr_sync(ticket_key: str, data: dict):
    import features.tickets as tickets_mod

    repos_in = data.get("repos") or []
    if not isinstance(repos_in, list) or not repos_in:
        return JSONResponse({"error": "repos required"}, status_code=400)
    for r in repos_in:
        if not isinstance(r, dict) or not r.get("name") or not r.get("title") or not r.get("description"):
            return JSONResponse({"error": "each repo needs name, title, description"}, status_code=400)

    tickets = state.load("tickets")
    ticket = tickets.get(ticket_key)
    if not ticket:
        return JSONResponse({"error": "Ticket not found"}, status_code=404)

    if ticket.get("status") != "pr_ready":
        return JSONResponse({"error": f"Ticket is {ticket.get('status')}, not pr_ready"}, status_code=400)

    ws = _config.get("workspace", {})
    slug = ticket.get("slug", "")
    if not slug:
        return JSONResponse({"error": "No slug found"}, status_code=400)

    platform = make_platform(_config)
    base_branch = ws.get("base_branch", "main")
    prs = []

    for r in repos_in:
        repo_name = r["name"]
        wt = tickets_mod.ticket_worktree_path(_config, slug, repo_name)
        if not wt.is_dir():
            return JSONResponse({"error": f"worktree missing for {repo_name}"}, status_code=400)

        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "commit", "--no-verify", "-m", f"{ticket_key}: {ticket.get('summary', '')}"],
                       cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "fetch", "origin", base_branch], cwd=str(wt), capture_output=True, timeout=60)

        files = _changed_files(wt, f"origin/{base_branch}")
        if not _is_meaningful_change(files):
            return JSONResponse({"error": f"no meaningful changes in {repo_name}"}, status_code=400)

        actual_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=10).stdout.strip()
        push_branch = actual_branch or ticket.get("branch", "")

        pushed = platform.push_branch(wt, push_branch)
        if not pushed.get("ok"):
            return JSONResponse({"error": f"Failed to push {repo_name}: {pushed.get('error', 'unknown')}"}, status_code=400)

        result = platform.create_pr(repo_name, wt, push_branch, r["title"], r["description"], base_branch)
        if result.get("error"):
            return JSONResponse({"error": f"Failed to create PR for {repo_name}: {result['error']}"}, status_code=400)

        pr_url = result.get("url", "")
        pr_id = result.get("id")
        if pr_id:
            prs.append({"repo": repo_name, "id": pr_id, "url": pr_url})

    if not prs:
        return JSONResponse({"error": "No PRs were created"}, status_code=400)

    try:
        state.transition_ticket(ticket_key, "in_review", prs=prs)
    except state.TicketStateError as e:
        log.emit("ticket_pr_transition_failed",
                 f"PRs created for {ticket_key} but transition to in_review failed: {e}",
                 meta={"ticket": ticket_key, "prs": prs})
        return JSONResponse({"error": f"PRs created but state transition failed: {e}", "prs": prs}, status_code=500)
    log.emit("ticket_pr_created", f"PR submitted for {ticket_key}: {len(prs)} repo(s)",
             meta={"ticket": ticket_key, "repos": [p["repo"] for p in prs]})
    return {"status": "ok", "prs": prs}


def _ticket_repo_count(slug: str) -> int:
    if not slug:
        return 0
    import core.config as cfg
    n = 0
    for repo in get_repos(_config):
        if cfg.ticket_worktree_path(_config, slug, repo["name"]).is_dir():
            n += 1
    return n


@app.get("/api/tickets/list")
def api_tickets_list():
    from datetime import datetime, timezone, timedelta
    import core.db as _db
    tickets = state.list_tickets()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    expired = [k for k, v in tickets.items() if v.get("status") == "done" and v.get("done_at", "") < cutoff]
    for k in expired:
        state.delete_ticket(k)
        del tickets[k]
    instance_key = _config.get("job", {}).get("key", "")
    pm_counts: dict[str, int] = {}
    val_badges: dict[str, str] = {}
    if instance_key:
        rows = _db.query_all(
            "SELECT ticket_key, findings FROM pm_review pr"
            " WHERE instance_key=? AND checkpoint_type='pre_approval'"
            " AND created_at = ("
            "   SELECT MAX(created_at) FROM pm_review"
            "   WHERE instance_key=? AND ticket_key=pr.ticket_key AND checkpoint_type='pre_approval'"
            " )",
            (instance_key, instance_key),
        )
        for r in rows:
            try:
                import json as _json
                f = _json.loads(r["findings"]) if r["findings"] else []
                pm_counts[r["ticket_key"]] = len(f) if isinstance(f, list) else 0
            except (ValueError, TypeError):
                pm_counts[r["ticket_key"]] = 0
        from features import validation as _val
        val_badges = _val.badges_bulk(instance_key)
    out = {}
    for k, v in tickets.items():
        if v.get("status") == "done":
            continue
        v["repo_count"] = _ticket_repo_count(v.get("slug", ""))
        v["pm_findings_count"] = pm_counts.get(k, 0)
        v["validation_badge"] = val_badges.get(k, "pending")
        out[k] = v
    return out


@app.get("/api/raw/tickets")
def api_raw_tickets():
    from features.ticket_systems import make_ticket_system
    ts = make_ticket_system(_config)
    if not ts:
        return JSONResponse({"error": "no ticket system configured"}, status_code=400)
    try:
        return ts.fetch_tickets()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/raw/prs")
def api_raw_prs():
    platform = make_platform(_config)
    try:
        my_prs = platform.list_my_open_prs()
        return {"my_prs": my_prs}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/tickets/{key}/detail")
def api_ticket_detail(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)

    slug = ts.get("slug", "")
    ws = _config["workspace"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug

    docs = {}
    docs_dir = ticket_dir / "docs"
    if docs_dir.is_dir():
        for f in docs_dir.iterdir():
            if f.is_file() and f.suffix == ".md":
                docs[f.name] = f.read_text()

    events = log.get_events(limit=200)
    history = [e for e in events if key in e.get("summary", "") or key in str(e.get("meta", {}))]

    terminal_alive = False
    if ts.get("status") in ("planning", "reviewing", "in_review"):
        health = terminal.session_healthy(key)
        terminal_alive = health["alive"] and health["claude_running"]

    summary = None
    if docs_dir.is_dir():
        summary_cache = docs_dir / ".change-summary.txt"
        manifest = docs_dir / "change-manifest.md"
        if manifest.exists():
            if summary_cache.exists() and summary_cache.stat().st_mtime >= manifest.stat().st_mtime:
                summary = summary_cache.read_text()
            else:
                summary = run_haiku(
                    f"Summarize this change manifest in 2-3 sentences. Be direct and technical.\n\n{manifest.read_text()[:4000]}"
                )
                if summary:
                    summary_cache.write_text(summary)

    all_statuses = [s.value for s in TicketStatus]
    demo_video = (docs_dir / "demo.webm").exists() if docs_dir.is_dir() else False
    ts["repo_count"] = _ticket_repo_count(slug)
    return {"key": key, "state": ts, "docs": docs, "history": history, "summary": summary, "terminal_alive": terminal_alive, "all_statuses": all_statuses, "demo_video": demo_video}


@app.get("/api/tickets/{key}/demo")
def api_ticket_demo(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    slug = ts.get("slug", "")
    ws = _config["workspace"]
    demo = ws["root"] / ws["tickets_dir"] / slug / "docs" / "demo.webm"
    if not demo.exists():
        return JSONResponse({"error": "no demo"}, status_code=404)
    return FileResponse(str(demo), media_type="video/webm")


@app.websocket("/ws/terminal/{key}")
async def ws_terminal(websocket: WebSocket, key: str):
    await terminal.terminal_handler(websocket, key, _config)


@app.delete("/api/tickets/{key}/terminal")
def api_kill_terminal(key: str):
    terminal.kill_terminal(key)
    return {"status": "ok"}


@app.post("/api/tickets/{key}/terminal/reset")
def api_reset_terminal(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    slug = ts.get("slug", "")
    ws = _config["workspace"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    terminal.kill_terminal(key)
    time.sleep(1)
    terminal.ensure_session(key, str(ticket_dir))
    time.sleep(1)
    terminal.send_keys(key, "claude --dangerously-skip-permissions")
    return {"status": "ok"}


@app.get("/api/tickets/{key}/diff")
def api_ticket_diff(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return {"diff": ""}
    if ts.get("prs"):
        platform = make_platform(_config)
        pr = ts["prs"][0]
        diff = platform.get_pr_diff(pr["repo"], pr["id"])
        return {"diff": diff or ""}
    return {"diff": _local_worktree_diff(ts)}


def _local_worktree_diff(ts: dict) -> str:
    import subprocess
    slug = ts.get("slug")
    if not slug:
        return ""
    base_branch = _config["workspace"].get("base_branch", "main")
    parts = []
    for repo in get_repos(_config):
        wt = cfg.ticket_worktree_path(_config, slug, repo["name"])
        if not wt.is_dir():
            continue
        result = subprocess.run(
            ["git", "diff", f"origin/{base_branch}...HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=30)
        if result.stdout.strip():
            parts.append(result.stdout)
    return "\n".join(parts)


@app.get("/api/tickets/{key}/pr-comments")
def api_ticket_pr_comments(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return []
    slug = ts.get("slug", "")
    ws = _config["workspace"]
    path = ws["root"] / ws["tickets_dir"] / slug / "pr_comments.json"
    if not path.exists():
        return []
    return json.loads(path.read_text())


@app.post("/api/tickets/{key}/pr-comments/{comment_id}/reply")
def api_ticket_reply(key: str, comment_id: int, body: dict):
    body = body.get("body", "")
    if not body:
        return JSONResponse({"error": "body required"}, status_code=400)
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)

    slug = ts.get("slug", "")
    ws = _config["workspace"]
    path = ws["root"] / ws["tickets_dir"] / slug / "pr_comments.json"
    if not path.exists():
        return JSONResponse({"error": "no comments"}, status_code=404)

    comments = json.loads(path.read_text())
    entry = next((c for c in comments if c["id"] == comment_id), None)
    if not entry:
        return JSONResponse({"error": "comment not found"}, status_code=404)

    platform = make_platform(_config)
    result = platform.post_pr_comment(
        entry["pr_repo"], entry["pr_id"], body,
        parent_id=entry["id"],
    )

    if result.get("status") == "posted":
        entry["status"] = "replied"
        entry["suggested_reply"] = body
        path.write_text(json.dumps(comments, indent=2, default=str))
        log.emit("ticket_pr_reply_sent", f"Replied to comment on {key}",
            links={"detail": f"{_config['_base_url']}/tickets/{key}"},
            meta={"ticket": key, "comment_id": comment_id})
    return result


@app.post("/api/tickets/{key}/restart")
def api_restart_ticket(key: str):
    import core.queue as q

    def _reset(ts: dict) -> dict:
        if not ts:
            return ts
        ts.pop("ci_fix_attempts", None)
        ts.pop("pr_attempts", None)
        return ts

    ts = state.update_ticket(key, _reset)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") == TicketStatus.pr_failed.value:
        try:
            ts = state.transition_ticket(key, "pr_ready")
        except state.TicketStateError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
    status = ts.get("status", "")
    instance_key = _config.get("job", {}).get("key", "")
    if instance_key and status in ("new", "planning"):
        q.enqueue_job(instance_key, "start_planning", ticket_key=key)
    elif instance_key and status == "reviewing":
        q.enqueue_job(instance_key, "start_reviewing", ticket_key=key)
    return {"status": "restarted"}


@app.post("/api/tickets/{key}/status")
def api_set_ticket_status(key: str, body: dict):
    target = body.get("status", "")
    try:
        TicketStatus(target)
    except ValueError:
        return JSONResponse({"error": f"invalid status: {target}"}, status_code=400)
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    old_status = ts.get("status", "unknown")
    fields: dict = {
        "ci_fix_attempts": 0,
        "conflict_resolution_attempts": 0,
        "ci_passed": None,
        "checks_started_at": None,
    }
    if target == "merged" and not ts.get("merged_external_status"):
        fields["merged_external_status"] = ts.get("external_status", "")
    try:
        state.transition_ticket(key, target, **fields)
    except state.TicketStateError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    log.emit("ticket_status_override", f"Manual override {old_status} → {target} for {key}",
        links={"detail": f"{_config['_base_url']}/tickets/{key}"},
        meta={"ticket": key, "old_status": old_status, "new_status": target})
    return {"status": target, "old_status": old_status}


@app.delete("/api/tickets/{key}")
def api_discard_ticket(key: str):
    import shutil
    import core.scheduler as scheduler
    ts = state.load_ticket(key)
    if ts is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    terminal.kill_terminal(key)
    slug = ts.get("slug", "")
    if slug:
        ws = _config["workspace"]
        repos = get_repos(_config)
        for repo in repos:
            wt_path = cfg.ticket_worktree_path(_config, slug, repo["name"])
            if (wt_path / ".git").is_file():
                subprocess.run(["git", "worktree", "remove", "--force", str(wt_path)], cwd=str(repo["path"]), capture_output=True, timeout=60)
        ticket_dir = ws["root"] / ws["tickets_dir"] / slug
        if ticket_dir.is_dir():
            shutil.rmtree(ticket_dir)
        for repo in repos:
            subprocess.run(["git", "worktree", "prune"], cwd=str(repo["path"]), capture_output=True, timeout=60)
    instance_key = _config.get("job", {}).get("key", "")
    if instance_key:
        scheduler.delete(instance_key, key)
    state.delete_ticket(key)
    return {"status": "discarded"}


@app.post("/api/tickets/{key}/approve")
def api_approve_ticket(key: str):
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") != "pending_approval":
        return JSONResponse({"error": f"cannot approve from status {ts.get('status')}"}, status_code=400)
    try:
        state.transition_ticket(key, "new", approval_status="approved")
    except state.TicketStateError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    log.emit("ticket_approved", f"Approved {key}",
             links={"detail": f"{_config['_base_url']}/tickets/{key}"},
             meta={"ticket": key})
    setup_enqueued = False
    if ts.get("source") == "prd":
        instance_key = _config.get("job", {}).get("key", "")
        if instance_key:
            try:
                from features.tickets import _enqueue_stage
                _enqueue_stage(instance_key, key, "setup_prd_ticket")
                setup_enqueued = True
            except Exception as e:
                log.emit("prd_setup_enqueue_failed",
                         f"failed to enqueue setup_prd_ticket for {key}: {type(e).__name__}: {e}",
                         meta={"ticket": key})
    return {"status": "new", "approval_status": "approved", "setup_enqueued": setup_enqueued}


@app.post("/api/tickets/{key}/reject")
def api_reject_ticket(key: str):
    from datetime import datetime, timezone
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") != "pending_approval":
        return JSONResponse({"error": f"cannot reject from status {ts.get('status')}"}, status_code=400)
    now = datetime.now(timezone.utc).isoformat()
    try:
        state.transition_ticket(key, "done",
                                approval_status="rejected",
                                obsolete_at=now,
                                done_at=now)
    except state.TicketStateError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    log.emit("ticket_rejected", f"Rejected {key}",
             links={"detail": f"{_config['_base_url']}/tickets/{key}"},
             meta={"ticket": key})
    return {"status": "done", "approval_status": "rejected"}


@app.post("/api/tickets/{key}/obsolete")
def api_obsolete_ticket(key: str):
    from datetime import datetime, timezone
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    now = datetime.now(timezone.utc).isoformat()
    def _mutate(current: dict) -> dict:
        if not current:
            raise state.TicketStateError(f"ticket {key}: not found")
        merged = dict(current)
        merged["obsolete_at"] = now
        return merged
    state.update_ticket(key, _mutate)
    try:
        from features import validation as v
        instance_key = _config.get("job", {}).get("key", "")
        if instance_key:
            v.remove_from_pool(instance_key, key)
    except Exception as e:
        log.emit("obsolete_pool_remove_failed", f"{type(e).__name__}: {e}",
                 meta={"ticket": key})
    log.emit("ticket_obsoleted", f"Marked obsolete {key}",
             links={"detail": f"{_config['_base_url']}/tickets/{key}"},
             meta={"ticket": key})
    return {"obsolete_at": now}


@app.get("/api/tickets/{key}/validation")
def api_ticket_validation(key: str, limit: int = 50):
    from features import validation as v
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"badge": "pending", "runs": []}
    runs = v.latest_runs(instance_key, key, limit)
    badge = v.badge_for(instance_key, key)
    return {"badge": badge, "runs": runs}


@app.get("/api/tickets/{key}/pm-findings")
def api_ticket_pm_findings(key: str, limit: int = 20):
    from pm import runner
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"reviews": []}
    reviews = runner.latest_findings(instance_key, key, limit)
    return {"reviews": reviews}


@app.post("/api/tickets/{key}/retry-job")
def api_retry_job(key: str, body: dict):
    if not _events_enabled():
        return JSONResponse({"error": "events not enabled"}, status_code=400)
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    job_id = body.get("job_id")
    if job_id is None:
        jobs = q.jobs_for_ticket(instance_key, key, limit=50)
        failed = [j for j in jobs if j["status"] in ("failed", "skipped")]
        if not failed:
            return JSONResponse({"error": "no failed job to retry"}, status_code=404)
        job = failed[0]
    else:
        import core.db as db
        row = db.query_one(
            "SELECT id, task, payload FROM jobs WHERE id=? AND instance_key=? AND ticket_key=?",
            (int(job_id), instance_key, key),
        )
        if not row:
            return JSONResponse({"error": "job not found"}, status_code=404)
        job = {"task": row["task"], "payload": row["payload"]}
    import json as _json
    payload = job["payload"] if isinstance(job["payload"], dict) else _json.loads(job["payload"] or "{}")
    q.emit_event(source="ui", kind="ui_retry",
                 payload={"task": job["task"], "payload": payload, "ticket_key": key},
                 instance_key=instance_key)
    return {"status": "enqueued", "task": job["task"]}


@app.post("/api/tickets/{key}/notes")
def api_ticket_notes(key: str, body: dict):
    note = (body.get("note") or "").strip()
    if not note:
        return JSONResponse({"error": "note required"}, status_code=400)
    if not _events_enabled():
        return JSONResponse({"error": "events not enabled"}, status_code=400)
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    q.emit_event(source="ui", kind="ui_notes",
                 payload={"note": note, "ticket_key": key},
                 instance_key=instance_key)
    return {"status": "enqueued"}


@app.get("/api/tickets/{key}/context")
def api_ticket_context_get(key: str):
    f = CUSTOM_CONTEXT_DIR / f"{key}.md"
    return {"context": f.read_text() if f.exists() else ""}


@app.put("/api/tickets/{key}/context")
def api_ticket_context_put(key: str, body: dict):
    CUSTOM_CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
    (CUSTOM_CONTEXT_DIR / f"{key}.md").write_text(body.get("context", ""))
    return {"status": "saved"}


@app.delete("/api/tickets/{key}/context")
def api_ticket_context_delete(key: str):
    f = CUSTOM_CONTEXT_DIR / f"{key}.md"
    if f.exists():
        f.unlink()
    return {"status": "deleted"}


@app.post("/api/tickets/{key}/set-state")
def api_set_state_event(key: str, body: dict):
    target = (body.get("target") or "").strip()
    if not target:
        return JSONResponse({"error": "target required"}, status_code=400)
    try:
        TicketStatus(target)
    except ValueError:
        return JSONResponse({"error": f"invalid target: {target}"}, status_code=400)
    if not _events_enabled():
        return JSONResponse({"error": "events not enabled"}, status_code=400)
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    q.emit_event(source="ui", kind="ui_set_state",
                 payload={"target": target, "ticket_key": key},
                 instance_key=instance_key)
    return {"status": "enqueued", "target": target}


@app.patch("/api/tickets/{key}/auto-pr")
def api_set_auto_pr(key: str, body: dict):
    ts_row = state.load_ticket(key)
    if not ts_row:
        return JSONResponse({"error": "not found"}, status_code=404)
    status_val = ts_row.get("status", "")
    gate = {"in_review", "merged", "done"}
    if status_val in gate:
        return JSONResponse({"error": f"auto_pr locked; status={status_val}"}, status_code=400)
    ts_row["auto_pr"] = bool(body.get("auto_pr"))
    state.save_ticket(key, ts_row)
    return {"status": "ok", "auto_pr": ts_row["auto_pr"]}


@app.post("/api/tickets/{key}/start-dev")
def api_start_dev(key: str):
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") != "new":
        return JSONResponse({"error": f"ticket is {ts.get('status')}, not new"}, status_code=400)
    assigned = _tickets_mod._fetch_tickets(_config)
    ticket = next((t for t in assigned if t["key"] == key), None)
    if not ticket:
        return JSONResponse({"error": "ticket not found in ticket system"}, status_code=404)
    ts = _tickets_mod._setup_ticket(_config, ticket, _config["_base_url"])
    state.save_ticket(key, ts)
    return {"status": "started", "new_status": ts.get("status")}








def main():
    import argparse
    parser = argparse.ArgumentParser(prog="frshty.py")
    parser.add_argument("config", nargs="?", help="single-instance config.toml path")
    parser.add_argument("--multi", nargs="+", metavar="CONFIG",
                        help="boot all listed configs in one process with shared event system")
    parser.add_argument("--port", type=int, default=None,
                        help="override listen port (multi mode default: first config's port)")
    parser.add_argument("--host", default=None, help="override bind host")
    args = parser.parse_args()

    if not args.config and not args.multi:
        parser.error("pass a config path or --multi <config1> <config2> ...")

    _ensure_path()

    configs = [cfg.load_config(p) for p in (args.multi or [args.config])]
    import core.llm as _llm
    for c in configs:
        _llm.configure(c)
    primary = configs[0]
    _set_primary_config(primary)
    # Fully event-driven: all periodic work via cron_tick -> Dispatcher -> WorkerPool
    try:
        import core.runtime as _rt
        _rt.start_events(configs, cron_interval=240)
    except Exception as e:
        log.emit("events_boot_failed", f"{type(e).__name__}: {e}")
        raise


    if args.multi:
        for c in configs:
            host = c.get("job", {}).get("host", "")
            if host.startswith("http://"):
                host = host[len("http://"):]
            elif host.startswith("https://"):
                host = host[len("https://"):]
            host = host.split(":")[0].split("/")[0].lower()
            if host:
                if host in _configs_by_host:
                    raise ValueError(f"hostname {host} claimed by two configs")
                _configs_by_host[host] = c

    state.init(primary["_state_dir"])
    log.init(primary["_state_dir"], primary["job"]["key"])

    port = args.port or _config["job"]["port"]

    host = args.host or _config["job"].get("bind", "127.0.0.1")
    # Always disable reload: event system (started before uvicorn) handles cron internally
    reload = False
    src = Path(__file__).parent
    reload_dirs = [str(src / d) for d in ("core", "features", "templates") if (src / d).exists()] if reload else None
    log_level = _config["job"].get("log_level", "info")
    if args.multi:
        # Pass app object directly so _configs_by_host populated in __main__ is
        # visible to middleware (vs. uvicorn re-importing frshty as a fresh module).
        uvicorn.run(app, host=host, port=port, log_level=log_level,
                    ws_ping_interval=60, ws_ping_timeout=60)
    else:
        uvicorn.run("frshty:app", host=host, port=port, log_level=log_level,
                    reload=reload, reload_dirs=reload_dirs,
                    ws_ping_interval=60, ws_ping_timeout=60)


if __name__ == "__main__":
    main()
