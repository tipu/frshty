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

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.websockets import WebSocket
from pathlib import Path

import core.config as cfg
import core.log as log
import core.state as state
import core.terminal as terminal
from core.claude_runner import run_haiku
from core.config import get_repos
from features.platforms import make_platform
import features.own_prs as own_prs
import features.reviewer as reviewer
import features.slack_monitor as slack_monitor
import features.tickets as _tickets_mod
import features.timesheet as ts

TEMPLATES_DIR = Path(__file__).parent / "templates"

_config: dict = {}
_worker_proc = None

if len(sys.argv) >= 2 and Path(sys.argv[1]).exists():
    _config = cfg.load_config(sys.argv[1])
    state.init(_config["_state_dir"])
    log.init(_config["_state_dir"], _config["job"]["key"])


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


@app.get("/timesheet", response_class=HTMLResponse)
def timesheet_page():
    return _template("timesheet.html")


@app.get("/api/events")
def api_events(limit: int = 100, after: str = "", unread: bool = False):
    return log.get_events(limit=limit, after=after or None, unread_only=unread)


@app.post("/api/events/{event_id}/dismiss")
def api_dismiss_event(event_id: str):
    log.dismiss(event_id)
    return {"status": "ok"}


@app.post("/api/events/dismiss-all")
def api_dismiss_all():
    log.dismiss_all()
    return {"status": "ok"}


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
    from concurrent.futures import ThreadPoolExecutor
    def check_open(r):
        if not r["pr_id"]:
            return None
        info = platform.get_pr_info(r["repo"], r["pr_id"])
        if info["state"] != "OPEN":
            return None
        r["updated_on"] = info["updated_on"]
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
    tickets = state.load("tickets")
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    expired = [k for k, v in tickets.items() if v.get("status") == "done" and v.get("done_at", "") < cutoff]
    if expired:
        for k in expired:
            del tickets[k]
        state.save("tickets", tickets)
    return {k: v for k, v in tickets.items() if v.get("status") != "done"}


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

    return {"key": key, "state": ts, "docs": docs, "history": history, "summary": summary, "terminal_alive": terminal_alive}


@app.websocket("/ws/terminal/{key}")
async def ws_terminal(websocket: WebSocket, key: str):
    await terminal.terminal_handler(websocket, key, _config)


@app.delete("/api/tickets/{key}/terminal")
def api_kill_terminal(key: str):
    terminal.kill_terminal(key)
    return {"status": "ok"}


@app.get("/api/tickets/{key}/diff")
def api_ticket_diff(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts or not ts.get("prs"):
        return {"diff": ""}
    platform = make_platform(_config)
    pr = ts["prs"][0]
    diff = platform.get_pr_diff(pr["repo"], pr["id"])
    return {"diff": diff or ""}


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
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    ts["restart_count"] = 0
    _tickets_mod.restart_session(_config, key, ts, _config["_base_url"])
    state.save("tickets", tickets)
    return {"status": "restarted"}


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


def _reload_config(config: dict):
    try:
        fresh = cfg.load_config(str(config["_config_path"]))
        config.update(fresh)
    except Exception:
        pass


def run_cycle(config: dict):
    _reload_config(config)
    log.emit("cycle_start", "Cycle started")
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


def health_loop(config: dict):
    while True:
        if config.get("features", {}).get("tickets"):
            try:
                _tickets_mod.check_health(config)
            except Exception as e:
                log.emit("cycle_error", f"health_check failed: {e}")
        time.sleep(random.randint(60, 90))


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
    health_thread = threading.Thread(target=health_loop, args=(worker_config,), daemon=True)
    health_thread.start()
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
    if len(sys.argv) < 2:
        print("Usage: frshty.py <config.toml>")
        sys.exit(1)

    _ensure_path()

    global _config
    _config = cfg.load_config(sys.argv[1])

    state.init(_config["_state_dir"])
    log.init(_config["_state_dir"], _config["job"]["key"])

    port = _config["job"]["port"]

    host = _config["job"].get("bind", "127.0.0.1")
    src = Path(__file__).parent
    reload_dirs = [str(src / d) for d in ("core", "features", "templates") if (src / d).exists()]
    uvicorn.run("frshty:app", host=host, port=port, log_level="info", reload=True, reload_dirs=reload_dirs)


if __name__ == "__main__":
    main()
