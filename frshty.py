#!/usr/bin/env python3
import contextlib
import json
import multiprocessing
import os
import random
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
from actions.record_demo import handle as _record_demo_action
from actions.schedule_pr import handle as _schedule_pr_action

events.register_action("record_demo", _record_demo_action)
events.register_action("schedule_pr", _schedule_pr_action)

TEMPLATES_DIR = Path(__file__).parent / "templates"
STATIC_DIR = Path(__file__).parent / "static"

from contextvars import ContextVar as _ContextVar
_cv_config: _ContextVar[dict] = _ContextVar("frshty_config", default={})
_primary_config: dict = {}  # module-level fallback: survives uvicorn reload subprocess boundaries


class _ConfigView(dict):
    """Dict-subclass proxy that resolves to the active config for the current request.

    In --multi mode, the hostname middleware sets the contextvar per request. In
    single-instance mode the module-level _primary_config is the fallback when
    the contextvar's default ({}) is still in effect (e.g. uvicorn reload
    subprocesses don't always inherit contextvar sets from module-import time).
    dict is the base class so FastAPI handlers typed as `dict` accept the view.
    """
    def _d(self) -> dict:
        v = _cv_config.get()
        return v if v else _primary_config
    def __getitem__(self, k): return self._d()[k]
    def __setitem__(self, k, v): self._d()[k] = v
    def __delitem__(self, k): del self._d()[k]
    def __contains__(self, k): return k in self._d()
    def __iter__(self): return iter(self._d())
    def __len__(self): return len(self._d())
    def __bool__(self): return bool(self._d())
    def __repr__(self): return repr(self._d())
    def get(self, k, default=None): return self._d().get(k, default)
    def items(self): return self._d().items()  # type: ignore[override]
    def keys(self): return self._d().keys()  # type: ignore[override]
    def values(self): return self._d().values()  # type: ignore[override]
    def setdefault(self, k, d=None): return self._d().setdefault(k, d)
    def update(self, *a, **kw): return self._d().update(*a, **kw)
    def pop(self, k, *a): return self._d().pop(k, *a)
    def copy(self): return self._d().copy()


_config: _ConfigView = _ConfigView()
_configs_by_host: dict[str, dict] = {}
_worker_proc = None


def _set_primary_config(c: dict) -> None:
    global _primary_config
    _primary_config = c
    _cv_config.set(c)


if len(sys.argv) >= 2 and Path(sys.argv[1]).exists():
    _primary = cfg.load_config(sys.argv[1])
    _set_primary_config(_primary)
    state.init(_primary["_state_dir"])
    log.init(_primary["_state_dir"], _primary["job"]["key"])


@contextlib.asynccontextmanager
async def _lifespan(a):
    global _worker_proc
    if len(sys.argv) >= 2:
        _worker_proc = multiprocessing.Process(target=_run_worker, args=(sys.argv[1],), daemon=True)
        _worker_proc.start()
    yield
    if _worker_proc and _worker_proc.is_alive():
        _worker_proc.kill()
        _worker_proc.join(timeout=5)


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


def _template(name: str) -> HTMLResponse:
    return HTMLResponse((TEMPLATES_DIR / name).read_text())


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return _template("index.html")


@app.get("/global", response_class=HTMLResponse)
def global_feed_page():
    return _template("global.html")


@app.get("/reviews", response_class=HTMLResponse)
def reviews_list():
    return _template("reviews.html")


@app.get("/reviews/{repo}/{pr_id}", response_class=HTMLResponse)
def review_detail(repo: str, pr_id: int):
    return _template("review_detail.html")


@app.get("/reviews/{repo}/{pr_id}/discuss", response_class=HTMLResponse)
def review_discuss(repo: str, pr_id: int):
    return _template("review_discuss.html")


@app.get("/tickets", response_class=HTMLResponse)
def tickets_page():
    return _template("tickets.html")


@app.get("/tickets/{key}", response_class=HTMLResponse)
def ticket_detail(key: str):
    return _template("ticket_detail.html")


@app.get("/slack", response_class=HTMLResponse)
def slack_page():
    return _template("slack.html")


@app.get("/scheduled", response_class=HTMLResponse)
def scheduled_page():
    return _template("scheduled.html")


@app.get("/config", response_class=HTMLResponse)
def config_page():
    return _template("config.html")


@app.get("/timesheet", response_class=HTMLResponse)
def timesheet_page():
    return _template("timesheet.html")


@app.get("/billing", response_class=HTMLResponse)
def billing_page():
    return _template("billing.html")


@app.get("/api/events")
def api_events(limit: int = 100, after: str = "", unread: bool = False, since_hours: int = 0):
    if since_hours > 0 and not after:
        from datetime import datetime, timezone, timedelta
        after = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
    return log.get_events(limit=limit, after=after or None, unread_only=unread)


@app.post("/api/events/{event_id}/dismiss")
def api_dismiss_event(event_id: str):
    log.dismiss(event_id)
    return {"status": "ok"}


@app.post("/api/events/dismiss-batch")
def api_dismiss_batch(body: dict):
    ids = body.get("ids") or []
    if not isinstance(ids, list):
        return JSONResponse({"error": "ids must be a list"}, status_code=400)
    added = log.dismiss_ids([str(i) for i in ids])
    return {"status": "ok", "added": added, "total": len(ids)}


@app.post("/api/events/dismiss-all")
def api_dismiss_all():
    log.dismiss_all()
    return {"status": "ok"}


_global_remote_cache: dict[str, tuple[float, dict]] = {}
_GLOBAL_REMOTE_TTL = 3.0


def _fetch_local_global_events(limit: int, unread_only: bool, after: str) -> list[dict]:
    out = []
    for config in _configs_by_host.values():
        state_dir = config["_state_dir"]
        key = config["job"]["key"]
        log_tokens = log.use(state_dir, key)
        try:
            for ev in log.get_events(limit=limit, unread_only=unread_only, after=after or None):
                ev["instance_key"] = key
                ev["global_id"] = f"{key}:{ev['id']}"
                ev["base_url"] = config.get("_base_url") or config["job"].get("host", "")
                out.append(ev)
        finally:
            log.reset(log_tokens)
    return out


def _fetch_remote_global_events(limit: int, unread_only: bool, since_hours: int) -> tuple[list[dict], dict[str, str]]:
    import asyncio
    from core.discovery import discover_instances, call_instance

    local_keys = {c["job"]["key"] for c in _configs_by_host.values()}
    if not local_keys and _primary_config:
        local_keys = {_primary_config.get("job", {}).get("key", "")}
    remote = [i for i in discover_instances() if i["key"] not in local_keys]
    if not remote:
        return [], {}

    cache_key = f"{limit}:{unread_only}:{since_hours}"
    cached = _global_remote_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _GLOBAL_REMOTE_TTL:
        return cached[1]["events"], cached[1]["errors"]

    since_param = f"&since_hours={since_hours}" if since_hours > 0 else ""
    path = f"/api/events?limit={limit}&unread={'true' if unread_only else 'false'}{since_param}"
    errors: dict[str, str] = {}
    events: list[dict] = []

    async def _one(inst):
        result = await call_instance(inst["base_url"], "GET", path, timeout=3.0)
        return inst, result

    async def _all():
        return await asyncio.gather(*[_one(i) for i in remote], return_exceptions=True)

    results = asyncio.run(_all())
    for item in results:
        if isinstance(item, Exception):
            continue
        inst, payload = item
        if isinstance(payload, dict) and payload.get("error"):
            errors[inst["key"]] = str(payload["error"])[:200]
            continue
        rows = payload if isinstance(payload, list) else (payload.get("events") if isinstance(payload, dict) else None)
        if not isinstance(rows, list):
            errors[inst["key"]] = "unexpected response shape"
            continue
        for ev in rows:
            if not isinstance(ev, dict):
                continue
            ev["instance_key"] = inst["key"]
            ev["global_id"] = f"{inst['key']}:{ev.get('id', '')}"
            ev["base_url"] = inst["base_url"]
            events.append(ev)

    _global_remote_cache[cache_key] = (time.time(), {"events": events, "errors": errors})
    return events, errors


@app.get("/api/global/events")
def api_global_events(limit: int = 5000, unread: bool = False, since_hours: int = 8):
    after = ""
    if since_hours > 0:
        from datetime import datetime, timezone, timedelta
        after = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
    local_events = _fetch_local_global_events(limit=limit, unread_only=unread, after=after)
    remote_events, errors = _fetch_remote_global_events(limit=limit, unread_only=unread, since_hours=since_hours)
    merged = local_events + remote_events
    merged.sort(key=lambda e: e.get("ts") or "", reverse=True)
    return {"events": merged[:limit], "errors": errors}


SYSTEM_EVENTS = {"cycle_start", "cycle_end", "cycle_sleep"}


@app.get("/api/status")
def api_status():
    events = log.get_events(limit=500, unread_only=True)
    filtered = [ev for ev in events if ev["event"] not in SYSTEM_EVENTS]
    counts = {}
    for ev in filtered:
        t = ev["event"].split("_")[0]
        counts[t] = counts.get(t, 0) + 1

    slack_alive = False
    raw_path = _config.get("slack", {}).get("raw_path", "")
    if raw_path:
        try:
            mtime = os.path.getmtime(raw_path)
            slack_alive = (time.time() - mtime) < 120
        except OSError:
            pass

    return {
        "job": _config.get("job", {}),
        "features": _config.get("features", {}),
        "unread_total": len(filtered),
        "counts": counts,
        "slack_alive": slack_alive,
    }


@app.get("/api/config")
def api_config():
    return {
        "job": _config.get("job", {}),
        "features": _config.get("features", {}),
        "workspace": {
            "root": str(_config.get("workspace", {}).get("root", "")),
        },
        "run_commands": _config.get("workspace", {}).get("run_commands", []),
    }


@app.get("/api/config/raw")
def api_config_raw():
    config_path = _config.get("_config_path")
    if not config_path or not config_path.exists():
        return JSONResponse({"error": "config not found"}, status_code=404)
    return {"content": config_path.read_text(), "path": str(config_path)}


@app.post("/api/config/raw")
def api_config_raw_save(body: dict):
    config_path = _config.get("_config_path")
    if not config_path:
        return JSONResponse({"error": "config not found"}, status_code=404)
    config_path.write_text(body.get("content", ""))
    return {"ok": True}


@app.post("/api/poll")
def api_poll():
    multiprocessing.Process(target=_run_poll, args=(str(_config["_config_path"]),), daemon=True).start()
    return {"status": "started"}


@app.post("/api/reviews/submit")
def api_submit_review(body: dict):
    url = body.get("url", "").strip()
    if not url:
        return JSONResponse({"error": "url required"}, status_code=400)
    import re
    m = re.match(r"https?://github\.com/([^/]+/[^/]+)/pull/(\d+)", url)
    if not m:
        return JSONResponse({"error": "invalid github PR url"}, status_code=400)
    full_repo = m.group(1)
    repo_short = full_repo.split("/")[-1]
    pr_id = int(m.group(2))
    pending_dir = _config["_state_dir"] / "reviews" / repo_short / f"pending-{pr_id}"
    pending_dir.mkdir(parents=True, exist_ok=True)
    (pending_dir / "queued_comments.json").write_text(json.dumps([{"pr_id": pr_id, "repo": repo_short, "pr_url": url, "status": "pending"}]))
    (pending_dir / "review.json").write_text(json.dumps({"verdict": "", "status": "reviewing"}))
    multiprocessing.Process(
        target=_run_review, args=(str(_config["_config_path"]), full_repo, pr_id, url), daemon=True
    ).start()
    return {"status": "started", "repo": full_repo, "pr_id": pr_id}


@app.put("/api/settings")
def api_settings(body: dict):
    for feature, enabled in body.get("features", {}).items():
        cfg.save_feature_toggle(_config, feature, enabled)
    return {"status": "ok", "features": _config.get("features", {})}


def _populate_repo_cache(platform, repo: str):
    import re
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    if not reviews_dir.exists():
        return
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if not comments:
            continue
        pr_url = comments[0].get("pr_url", "")
        m = re.match(r"https?://github\.com/([^/]+/[^/]+)/pull/", pr_url)
        if m:
            platform._repo_cache[repo] = m.group(1)
            return


@app.get("/api/reviews")
def api_reviews_list():
    reviews_dir = _config["_state_dir"] / "reviews"
    if not reviews_dir.exists():
        return []
    candidates = []
    for repo_dir in sorted(reviews_dir.iterdir()):
        if not repo_dir.is_dir():
            continue
        for branch_dir in repo_dir.iterdir():
            queued = branch_dir / "queued_comments.json"
            review = branch_dir / "review.json"
            if not queued.exists():
                continue
            comments = json.loads(queued.read_text())
            review_data = json.loads(review.read_text()) if review.exists() else {}
            candidates.append({
                "repo": repo_dir.name,
                "branch": branch_dir.name,
                "verdict": review_data.get("verdict", ""),
                "reviewed_at": review_data.get("date", ""),
                "total_comments": len(comments),
                "pending": sum(1 for c in comments if c.get("status") == "pending"),
                "pr_url": comments[0]["pr_url"] if comments else "",
                "pr_id": comments[0]["pr_id"] if comments else 0,
            })
    platform = make_platform(_config)
    job_platform = _config.get("job", {}).get("platform", "")
    if job_platform == "bitbucket":
        my_id = _config.get("bitbucket", {}).get("user_account_id", "")
    else:
        my_id = _config.get("github", {}).get("user", "") or "@me"
    from concurrent.futures import ThreadPoolExecutor
    import shutil

    def check_open(r):
        if not r["pr_id"]:
            return None
        info = platform.get_pr_info(r["repo"], r["pr_id"])
        state_val = info.get("state", "UNKNOWN")
        if state_val in ("MERGED", "DECLINED", "CLOSED", "SUPERSEDED", "DELETED"):
            branch_dir = reviews_dir / r["repo"] / r["branch"]
            if branch_dir.is_dir():
                shutil.rmtree(branch_dir, ignore_errors=True)
                log.emit("review_removed", f"Removed review for {r['repo']}#{r['pr_id']} ({state_val.lower()})",
                    meta={"repo": r["repo"], "pr_id": r["pr_id"], "state": state_val})
            return None
        if state_val != "OPEN":
            return None
        r["updated_on"] = info["updated_on"]
        r["author"] = info.get("author", "")
        approvers = info.get("approvers") or []
        r["approved_by_me"] = bool(my_id) and my_id in approvers
        return r
    with ThreadPoolExecutor(max_workers=10) as pool:
        results = [r for r in pool.map(check_open, candidates) if r]
    return results


@app.get("/api/reviews/{repo}/{pr_id}/comments")
def api_review_comments(repo: str, pr_id: int):
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    if not reviews_dir.exists():
        return []
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if comments and comments[0].get("pr_id") == pr_id:
            return comments
    return []


@app.get("/api/reviews/{repo}/{pr_id}/info")
def api_review_info(repo: str, pr_id: int):
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    if not reviews_dir.exists():
        return {}
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if comments and comments[0].get("pr_id") == pr_id:
            review_json = branch_dir / "review.json"
            review_data = json.loads(review_json.read_text()) if review_json.exists() else {}
            result = {
                "summary": review_data.get("summary", ""),
                "verdict": review_data.get("verdict", ""),
                "branch": review_data.get("source_branch", ""),
                "author": review_data.get("author", ""),
                "date": review_data.get("date", ""),
                "pr_url": comments[0].get("pr_url", ""),
                "pr_title": comments[0].get("pr_title", ""),
            }
            platform = make_platform(_config)
            try:
                pr_info = platform.get_pr_info(repo, pr_id)
                result["pr_description"] = pr_info.get("description", "")
                result["pr_title"] = pr_info.get("title", result["pr_title"])
            except Exception:
                pass
            return result
    return {}


@app.post("/api/reviews/{repo}/{pr_id}/discuss")
def api_start_discuss(repo: str, pr_id: int, body: dict):
    idx = body.get("idx", "general")
    file_path = body.get("file", "")
    line = body.get("line", "")
    severity = body.get("severity", "")
    comment_body = body.get("body", "")

    session_id = f"discuss-{repo}-{pr_id}-{idx}"

    context = f"You are helping review a pull request in {repo} (PR #{pr_id}).\n\n"
    if idx != "general" and file_path:
        context += f"The reviewer flagged an issue:\nFile: {file_path}\n"
        if line:
            context += f"Line: {line}\n"
        if severity:
            context += f"Severity: {severity}\n"
        context += f"\nReview comment:\n{comment_body}\n\nRead the file and help discuss this issue."
    else:
        context += "Help the reviewer understand and discuss this PR."

    review_dir = _config["_state_dir"] / "reviews" / repo
    cwd = None
    matched_dir = None
    if review_dir.exists():
        for branch_dir in review_dir.iterdir():
            queued = branch_dir / "queued_comments.json"
            if not queued.exists():
                continue
            comments = json.loads(queued.read_text())
            if comments and comments[0].get("pr_id") == pr_id:
                matched_dir = branch_dir
                wt = branch_dir / "worktree"
                if (wt / ".git").exists():
                    cwd = str(wt)
                break
    if not cwd and matched_dir:
        review_json = matched_dir / "review.json"
        branch = None
        if review_json.exists():
            branch = json.loads(review_json.read_text()).get("source_branch")
        if branch:
            repos = get_repos(_config)
            matching = [r for r in repos if r["name"] == repo]
            if matching:
                repo_path = matching[0]["path"]
                wt = matched_dir / "worktree"
                wt.parent.mkdir(parents=True, exist_ok=True)
                subprocess.run(["git", "fetch", "origin", branch], cwd=str(repo_path), capture_output=True, timeout=60)
                subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=60)
                subprocess.run(["git", "worktree", "add", str(wt), branch], cwd=str(repo_path), capture_output=True, timeout=60)
                if (wt / ".git").exists():
                    cwd = str(wt)
    if not cwd:
        platform = make_platform(_config)
        _populate_repo_cache(platform, repo)
        worktree = review_dir / f"pr-{pr_id}" / "worktree"
        platform.ensure_pr_worktree(repo, pr_id, worktree)
        if (worktree / ".git").exists():
            cwd = str(worktree)
    if not cwd:
        cwd = str(review_dir)

    terminal.kill_terminal(session_id)
    terminal.ensure_session(session_id, cwd)
    terminal.send_keys(session_id, f"claude --dangerously-skip-permissions --append-system-prompt {shlex.quote(context)}")

    return {"session_id": session_id}


@app.websocket("/ws/discuss/{session_id}")
async def ws_discuss(websocket: WebSocket, session_id: str):
    await terminal.terminal_handler(websocket, session_id, _config)


@app.get("/api/reviews/{repo}/{pr_id}/diff")
def api_review_diff(repo: str, pr_id: int):
    platform = make_platform(_config)
    _populate_repo_cache(platform, repo)
    diff = platform.get_pr_diff(repo, pr_id)
    return {"diff": diff or ""}


@app.get("/api/reviews/{repo}/{pr_id}/bb-comments")
def api_bb_comments(repo: str, pr_id: int):
    platform = make_platform(_config)
    comments = platform.get_pr_comments(repo, pr_id)
    names = list({c["author_name"] for c in comments if c.get("author_name")})
    initials = []
    for name in names:
        parts = name.split()
        initials.append("".join(p[0].upper() for p in parts if p))
    return {"count": len(comments), "initials": initials}


@app.post("/api/reviews/{repo}/{pr_id}/comments/{idx}/submit")
def api_submit_comment(repo: str, pr_id: int, idx: int):
    platform = make_platform(_config)
    _populate_repo_cache(platform, repo)
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if not comments or comments[0].get("pr_id") != pr_id:
            continue
        if idx >= len(comments):
            return JSONResponse({"error": "invalid index"}, status_code=400)
        comment = comments[idx]
        remote_id = comment.get("remote_id")
        if remote_id:
            result = platform.edit_pr_comment(repo, pr_id, remote_id, comment["body"])
            if result.get("status") == "updated":
                comments[idx]["status"] = "submitted"
                queued.write_text(json.dumps(comments, indent=2))
                log.emit("review_comment_edited", f"Edited comment on {repo} PR #{pr_id}",
                    links={"pr": comment.get("pr_url", "")},
                    meta={"repo": repo, "pr_id": pr_id})
        else:
            result = platform.post_pr_comment(repo, pr_id, comment["body"], comment.get("path"), comment.get("line"))
            if result.get("status") == "posted":
                comments[idx]["status"] = "submitted"
                comments[idx]["remote_id"] = result.get("id")
                queued.write_text(json.dumps(comments, indent=2))
                log.emit("review_comment_submitted", f"Submitted comment on {repo} PR #{pr_id}",
                    links={"pr": comment.get("pr_url", "")},
                    meta={"repo": repo, "pr_id": pr_id})
        return result
    return JSONResponse({"error": "not found"}, status_code=404)


@app.post("/api/reviews/{repo}/{pr_id}/comments/new")
def api_new_comment(repo: str, pr_id: int, body: dict):
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if not comments or comments[0].get("pr_id") != pr_id:
            continue
        new_comment = {
            "pr_id": pr_id,
            "repo": repo,
            "pr_url": comments[0].get("pr_url", ""),
            "path": body.get("path"),
            "line": body.get("line"),
            "severity": body.get("severity", "suggestion"),
            "persona": "manual",
            "body": body.get("body", ""),
            "status": "draft",
        }
        comments.append(new_comment)
        queued.write_text(json.dumps(comments, indent=2))
        return {"status": "ok", "idx": len(comments) - 1}
    return JSONResponse({"error": "not found"}, status_code=404)


@app.delete("/api/reviews/{repo}/{pr_id}/comments/{idx}")
def api_delete_comment(repo: str, pr_id: int, idx: int):
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if not comments or comments[0].get("pr_id") != pr_id:
            continue
        if idx >= len(comments):
            return JSONResponse({"error": "invalid index"}, status_code=400)
        comments.pop(idx)
        queued.write_text(json.dumps(comments, indent=2))
        return {"status": "ok"}
    return JSONResponse({"error": "not found"}, status_code=404)


@app.put("/api/reviews/{repo}/{pr_id}/comments/{idx}")
def api_update_comment(repo: str, pr_id: int, idx: int, body: dict):
    reviews_dir = _config["_state_dir"] / "reviews" / repo
    for branch_dir in reviews_dir.iterdir():
        queued = branch_dir / "queued_comments.json"
        if not queued.exists():
            continue
        comments = json.loads(queued.read_text())
        if not comments or comments[0].get("pr_id") != pr_id:
            continue
        if idx >= len(comments):
            return JSONResponse({"error": "invalid index"}, status_code=400)
        if "body" in body:
            comments[idx]["body"] = body["body"]
        if "line" in body:
            comments[idx]["line"] = body["line"]
        queued.write_text(json.dumps(comments, indent=2))
        return {"status": "ok"}
    return JSONResponse({"error": "not found"}, status_code=404)


@app.get("/api/tickets/list")
def api_tickets_list():
    from datetime import datetime, timezone, timedelta
    tickets = state.list_tickets()
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    expired = [k for k, v in tickets.items() if v.get("status") == "done" and v.get("done_at", "") < cutoff]
    for k in expired:
        state.delete_ticket(k)
        del tickets[k]
    return {k: v for k, v in tickets.items() if v.get("status") != "done"}


@app.get("/api/raw/tickets")
def api_raw_tickets():
    from features.ticket_systems import make_ticket_system
    ts = make_ticket_system(_config)
    if not ts:
        return {"error": "no ticket system configured"}
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


_WEEKDAY = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}


def _project_virtual_rows(config: dict, days: int = 7) -> list[dict]:
    """Read-only projection of config-driven recurring entries for the next `days`.

    Today this covers [timesheet].recurring. Rows carry source='config' and
    mutable=False so any mutation route must refuse them.
    """
    from datetime import date, datetime, time, timedelta, timezone
    from zoneinfo import ZoneInfo
    pst = ZoneInfo("America/Los_Angeles")
    out = []
    recurring = config.get("timesheet", {}).get("recurring", []) or []
    if not recurring:
        return out
    import core.tz as _ctz
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


@app.get("/api/scheduled")
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
        if ts.get("status") == "pr_created" and not ts.get("ci_passed"):
            items.append({"key": key, "type": "ci_pending", "status": ts["status"],
                          "checks_started_at": ts.get("checks_started_at"),
                          "branch": ts.get("branch", ""),
                          "prs": ts.get("prs", [])})
    return items


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
        subprocess.run(["git", "fetch", "origin", base_branch],
            cwd=str(wt), capture_output=True, timeout=60)
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
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    ts.pop("ci_fix_attempts", None)
    ts.pop("pr_attempts", None)
    if ts.get("status") == TicketStatus.pr_failed.value:
        ts["status"] = "pr_ready"
    state.save_ticket(key, ts)
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
    ts["status"] = target
    ts["ci_fix_attempts"] = 0
    ts["conflict_resolution_attempts"] = 0
    ts.pop("ci_passed", None)
    ts.pop("checks_started_at", None)
    if target == "merged" and "merged_external_status" not in ts:
        ts["merged_external_status"] = ts.get("external_status", "")
    state.save_ticket(key, ts)
    log.emit("ticket_status_override", f"Manual override {old_status} → {target} for {key}",
        links={"detail": f"{_config['_base_url']}/tickets/{key}"},
        meta={"ticket": key, "old_status": old_status, "new_status": target})
    return {"status": target, "old_status": old_status}


@app.delete("/api/tickets/{key}")
def api_discard_ticket(key: str):
    import shutil
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
    state.delete_ticket(key)
    return {"status": "discarded"}


def _events_enabled() -> bool:
    try:
        import core.runtime as _rt
        return _rt.instances() is not None
    except Exception:
        return False


@app.get("/api/tickets/{key}/jobs")
def api_ticket_jobs(key: str, limit: int = 100):
    if not _events_enabled():
        return []
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    return q.jobs_for_ticket(instance_key, key, limit)


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
    gate = {"pr_created", "in_review", "merged", "done"}
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


@app.post("/api/scheduled/{key}/reschedule")
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
    from datetime import datetime
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


@app.get("/api/slack/data")
def api_slack_data():
    return state.load("slack")


@app.post("/api/slack/send/{reply_id}")
def api_slack_send(reply_id: str, body: dict):
    text = body.get("text", "")
    if not text:
        return JSONResponse({"error": "text required"}, status_code=400)

    sl = state.load("slack")
    replies = sl.get("replies", {})
    ctx = replies.get(reply_id)
    if not ctx:
        return JSONResponse({"error": "reply_id not found"}, status_code=404)

    workspace = ctx["workspace"]
    channel = ctx["channel"]
    thread_ts = ctx.get("thread_ts", "")

    tokens_path = _config.get("slack", {}).get("raw_path", "")
    if tokens_path:
        tokens_file = str(Path(tokens_path).parent.parent / "tokens.json")
        try:
            tokens = json.loads(Path(tokens_file).read_text())
        except (FileNotFoundError, json.JSONDecodeError):
            return JSONResponse({"error": "tokens.json not found"}, status_code=500)
        creds = tokens.get(workspace)
        if not creds:
            return JSONResponse({"error": f"no token for workspace {workspace}"}, status_code=400)

        post_data = {"token": creds["token"], "channel": channel, "text": text}
        if thread_ts:
            post_data["thread_ts"] = thread_ts

        req = urllib.request.Request(
            f"https://{workspace}.slack.com/api/chat.postMessage",
            urllib.parse.urlencode(post_data).encode(),
            headers={"Cookie": creds["cookie"].replace(", ", "; ")},
        )
        resp = urllib.request.urlopen(req)
        result = json.loads(resp.read())
        if result.get("ok"):
            log.emit("slack_reply_sent", f"Replied in {channel}: {text[:80]}",
                links={"detail": f"{_config['_base_url']}/slack"},
                meta={"channel": channel, "text": text, "reply_id": reply_id})
            return {"status": "sent"}
        return JSONResponse({"error": result.get("error", "unknown")}, status_code=400)

    return JSONResponse({"error": "slack not configured"}, status_code=400)


@app.get("/api/timesheet")
def api_timesheet(start: str = "", end: str = "", force: bool = False):
    return ts.build_timesheet(_config, start, end, force)


@app.post("/api/timesheet/log")
def api_timesheet_log(body: dict):
    ticket = body.get("ticket", "")
    date_str = body.get("date", "")
    time_str = body.get("time", "")
    if not ticket or not date_str or not time_str:
        return JSONResponse({"error": "ticket, date, and time required"}, status_code=400)
    result = ts.log_work(_config, ticket, date_str, time_str)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@app.put("/api/timesheet/worklog")
def api_timesheet_worklog(body: dict):
    ticket = body.get("ticket", "")
    worklog_id = body.get("worklog_id", "")
    time_str = body.get("time", "")
    if not ticket or not worklog_id or not time_str:
        return JSONResponse({"error": "ticket, worklog_id, and time required"}, status_code=400)
    result = ts.update_worklog(_config, ticket, worklog_id, time_str)
    if result.get("error"):
        return JSONResponse(result, status_code=400)
    return result


@app.get("/api/billing/client")
def api_billing_client():
    return billing.get_client(_config)


@app.get("/api/billing/schedule-status")
async def api_billing_schedule_status():
    return await billing.get_schedule_status(_config)


@app.get("/api/billing/entries")
def api_billing_entries(month: str = ""):
    return billing.list_entries(_config, month)


@app.post("/api/billing/entries")
async def api_billing_upsert_entry(request: Request):
    body = await request.json()
    return billing.upsert_entries(_config, body)


@app.delete("/api/billing/entries/{day}")
def api_billing_delete_entry(day: str):
    return billing.delete_entry(_config, day)


@app.get("/api/billing/invoices")
async def api_billing_invoices():
    return await billing.list_invoices(_config)


@app.post("/api/billing/invoices")
async def api_billing_create_invoice(body: dict):
    try:
        return await billing.create_invoice(_config, body, source="manual")
    except OverlapError as e:
        return JSONResponse({"error": str(e), "conflict": e.conflict}, status_code=409)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    except httpx.HTTPError as e:
        log.emit("invoice_create_failed", f"bill.com create failed: {e}", meta={"err": str(e)[:200]})
        return JSONResponse({"error": f"billcom failed: {e}"}, status_code=502)


@app.get("/api/billing/next-invoice-number")
async def api_billing_next_number():
    return await billing.next_invoice_number(_config)


@app.get("/api/billing/preview")
def api_billing_preview(start: str, end: str):
    return {"descriptions": billing.preview_descriptions(_config, start, end)}


def _reload_config(config: dict):
    try:
        fresh = cfg.load_config(str(config["_config_path"]))
        config.update(fresh)
    except Exception:
        pass


def run_cycle(config: dict):
    _reload_config(config)
    log.emit("cycle_start", "Cycle started")
    if _events_enabled():
        try:
            import core.queue as _q
            _q.emit_event(source="cron", kind="cron_tick", payload={},
                           instance_key=config.get("job", {}).get("key", ""))
            log.emit("cycle_end", "Cycle emitted cron_tick (worker pool owns execution)")
            return
        except Exception as e:
            log.emit("cycle_error", f"cron_tick emit failed, falling back to direct: {e}")

    try:
        own_prs.check(config)
    except Exception as e:
        log.emit("cycle_error", f"own_prs failed: {e}")

    if config.get("features", {}).get("review_prs"):
        try:
            reviewer.check(config)
        except Exception as e:
            log.emit("cycle_error", f"reviewer failed: {e}")

    if config.get("features", {}).get("tickets"):
        try:
            _tickets_mod.check(config)
        except Exception as e:
            log.emit("cycle_error", f"tickets failed: {e}\n{traceback.format_exc()}")

    if config.get("features", {}).get("timesheet"):
        try:
            ts.check(config)
        except Exception as e:
            log.emit("cycle_error", f"timesheet failed: {e}")

    if config.get("features", {}).get("billing"):
        try:
            billing.check(config)
        except Exception as e:
            log.emit("cycle_error", f"billing failed: {e}")

    try:
        scheduler.check_due(config)
    except Exception as e:
        log.emit("cycle_error", f"scheduler failed: {e}")

    log.emit("cycle_end", "Cycle complete")


def main_loop(config: dict):
    while True:
        try:
            run_cycle(config)
        except Exception:
            log.emit("cycle_error", traceback.format_exc())
        sleep_time = random.randint(180, 420)
        log.emit("cycle_sleep", f"Sleeping {sleep_time}s")
        time.sleep(sleep_time)


def slack_loop(config: dict):
    while True:
        if config.get("features", {}).get("slack"):
            try:
                slack_monitor.check(config)
            except Exception as e:
                log.emit("cycle_error", f"slack_monitor failed: {e}")
        time.sleep(random.randint(50, 90))


def _run_review(config_path: str, full_repo: str, pr_id: int, url: str):
    _ensure_path()
    review_config = cfg.load_config(config_path)
    state.init(review_config["_state_dir"])
    log.init(review_config["_state_dir"], review_config["job"]["key"])
    platform = make_platform(review_config)
    repo_short = full_repo.split("/")[-1]
    platform._repo_cache[repo_short] = full_repo
    branch = platform.get_pr_branch(repo_short, pr_id) or f"pr-{pr_id}"
    pr = {
        "id": pr_id,
        "repo": repo_short,
        "full_repo": full_repo,
        "url": url,
        "branch": branch,
        "base": "",
        "title": "",
        "author": "",
        "created_on": "",
        "updated_on": "",
    }
    base_url = review_config["_base_url"]
    log.emit("review_started", f"Manual review: {full_repo} PR #{pr_id}",
        links={"pr": url, "detail": f"{base_url}/reviews/{pr['repo']}/{pr_id}"},
        meta={"repo": pr["repo"], "pr_id": pr_id})
    pending_dir = review_config["_state_dir"] / "reviews" / pr["repo"] / f"pending-{pr_id}"
    result = reviewer.review_pr(review_config, platform, pr)
    import shutil
    shutil.rmtree(pending_dir, ignore_errors=True)
    if result:
        review_state = state.load("reviews")
        review_state[f"{pr['repo']}/{pr_id}"] = {"reviewed": True, "branch": pr["branch"], "last_updated": pr.get("updated_on")}
        state.save("reviews", review_state)
        issues = result.get("issues", [])
        log.emit("review_complete", f"Review done: {result.get('verdict', 'unknown')}, {len(issues)} issues",
            links={"pr": url, "detail": f"{base_url}/reviews/{pr['repo']}/{pr_id}"},
            meta={"repo": pr["repo"], "pr_id": pr_id, "verdict": result.get("verdict"), "issue_count": len(issues)})
    else:
        log.emit("cycle_error", f"Manual review failed for {full_repo} PR #{pr_id}",
            meta={"repo": pr["repo"], "pr_id": pr_id})


def _run_poll(config_path: str):
    _ensure_path()
    poll_config = cfg.load_config(config_path)
    state.init(poll_config["_state_dir"])
    log.init(poll_config["_state_dir"], poll_config["job"]["key"])
    run_cycle(poll_config)


def _run_worker(config_path: str):
    _ensure_path()
    worker_config = cfg.load_config(config_path)
    state.init(worker_config["_state_dir"])
    log.init(worker_config["_state_dir"], worker_config["job"]["key"])
    loop_thread = threading.Thread(target=main_loop, args=(worker_config,), daemon=True)
    loop_thread.start()
    slack_loop(worker_config)


def _ensure_path():
    extra = [
        os.path.expanduser("~/.local/bin"),
        os.path.expanduser("~/.local/node/bin"),
        "/opt/homebrew/bin",
        "/usr/local/bin",
    ]
    current = os.environ.get("PATH", "/usr/bin:/bin")
    os.environ["PATH"] = ":".join(extra) + ":" + current


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
    primary = configs[0]
    _set_primary_config(primary)

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

    if args.multi or os.environ.get("FRSHTY_EVENTS") == "1":
        try:
            import core.runtime as _rt
            # In single-instance FRSHTY_EVENTS=1 mode, main_loop is the cron ticker
            # (it emits cron_tick from run_cycle), so disable the runtime ticker to
            # avoid double-firing. In --multi mode, no main_loop runs, so keep the
            # runtime ticker at its default interval.
            cron_interval = 0 if (not args.multi and os.environ.get("FRSHTY_EVENTS") == "1") else 240
            _rt.start_events(configs, cron_interval=cron_interval)
        except Exception as e:
            log.emit("events_boot_failed", f"{type(e).__name__}: {e}")

    port = args.port or _config["job"]["port"]

    host = args.host or _config["job"].get("bind", "127.0.0.1")
    reload = False if args.multi else _config["job"].get("reload", True)
    src = Path(__file__).parent
    reload_dirs = [str(src / d) for d in ("core", "features", "templates") if (src / d).exists()] if reload else None
    log_level = _config["job"].get("log_level", "info")
    if args.multi:
        # Pass app object directly so _configs_by_host populated in __main__ is
        # visible to middleware (vs. uvicorn re-importing frshty as a fresh module).
        uvicorn.run(app, host=host, port=port, log_level=log_level)
    else:
        uvicorn.run("frshty:app", host=host, port=port, log_level=log_level, reload=reload, reload_dirs=reload_dirs)


if __name__ == "__main__":
    main()
