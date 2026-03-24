#!/usr/bin/env python3
import random
import sys
import threading
import time
import traceback

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from starlette.websockets import WebSocket
from pathlib import Path

import core.config as cfg
import core.log as log
import core.state as state
import core.terminal as terminal

TEMPLATES_DIR = Path(__file__).parent / "templates"

app = FastAPI()
_config: dict = {}


def _template(name: str) -> HTMLResponse:
    return HTMLResponse((TEMPLATES_DIR / name).read_text())


@app.get("/", response_class=HTMLResponse)
async def dashboard():
    return _template("index.html")


@app.get("/reviews", response_class=HTMLResponse)
async def reviews_list():
    return _template("reviews.html")


@app.get("/reviews/{repo}/{pr_id}", response_class=HTMLResponse)
async def review_detail(repo: str, pr_id: int):
    return _template("review_detail.html")


@app.get("/reviews/{repo}/{pr_id}/discuss", response_class=HTMLResponse)
async def review_discuss(repo: str, pr_id: int):
    return _template("review_discuss.html")


@app.get("/tickets", response_class=HTMLResponse)
async def tickets_page():
    return _template("tickets.html")


@app.get("/tickets/{key}", response_class=HTMLResponse)
async def ticket_detail(key: str):
    return _template("ticket_detail.html")


@app.get("/slack", response_class=HTMLResponse)
async def slack_page():
    return _template("slack.html")


@app.get("/timesheet", response_class=HTMLResponse)
async def timesheet_page():
    return _template("timesheet.html")


@app.get("/api/events")
async def api_events(limit: int = 100, after: str = "", unread: bool = False):
    return log.get_events(limit=limit, after=after or None, unread_only=unread)


@app.post("/api/events/{event_id}/dismiss")
async def api_dismiss_event(event_id: str):
    log.dismiss(event_id)
    return {"status": "ok"}


@app.post("/api/events/dismiss-all")
async def api_dismiss_all():
    log.dismiss_all()
    return {"status": "ok"}


SYSTEM_EVENTS = {"cycle_start", "cycle_end", "cycle_sleep"}


@app.get("/api/status")
async def api_status():
    events = log.get_events(limit=500, unread_only=True)
    filtered = [ev for ev in events if ev["event"] not in SYSTEM_EVENTS]
    counts = {}
    for ev in filtered:
        t = ev["event"].split("_")[0]
        counts[t] = counts.get(t, 0) + 1

    slack_alive = False
    raw_path = _config.get("slack", {}).get("raw_path", "")
    if raw_path:
        import os, time
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
async def api_config():
    return {
        "job": _config.get("job", {}),
        "features": _config.get("features", {}),
        "workspace": {
            "root": str(_config.get("workspace", {}).get("root", "")),
        },
        "run_commands": _config.get("workspace", {}).get("run_commands", []),
    }


@app.post("/api/poll")
async def api_poll():
    import threading
    threading.Thread(target=run_cycle, args=(_config,), daemon=True).start()
    return {"status": "started"}


@app.put("/api/settings")
async def api_settings(request: Request):
    body = await request.json()
    for feature, enabled in body.get("features", {}).items():
        cfg.save_feature_toggle(_config, feature, enabled)
    return {"status": "ok", "features": _config.get("features", {})}


@app.get("/api/reviews")
async def api_reviews_list():
    import json
    reviews_dir = _config["_state_dir"] / "reviews"
    if not reviews_dir.exists():
        return []
    results = []
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
            results.append({
                "repo": repo_dir.name,
                "branch": branch_dir.name,
                "verdict": review_data.get("verdict", ""),
                "total_comments": len(comments),
                "pending": sum(1 for c in comments if c.get("status") == "pending"),
                "pr_url": comments[0]["pr_url"] if comments else "",
                "pr_id": comments[0]["pr_id"] if comments else 0,
            })
    return results


@app.get("/api/reviews/{repo}/{pr_id}/comments")
async def api_review_comments(repo: str, pr_id: int):
    import json
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


@app.post("/api/reviews/{repo}/{pr_id}/discuss")
async def api_start_discuss(repo: str, pr_id: int, request: Request):
    body = await request.json()
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

    from platforms import make_platform
    platform = make_platform(_config)
    pr_data = None
    try:
        prs = platform.list_review_prs()
        pr_data = next((p for p in prs if p["id"] == pr_id and p["repo"] == repo), None)
    except Exception:
        pass

    cwd = str(_config["workspace"]["root"])
    if pr_data:
        from config import get_repos
        repos = get_repos(_config)
        repo_path = next((r["path"] for r in repos if r["name"] == repo), None)
        if repo_path:
            cwd = str(repo_path)

    terminal.kill_terminal(session_id)
    import time
    time.sleep(1)
    terminal.ensure_session(session_id, cwd)
    time.sleep(2)
    import shlex
    terminal.send_keys(session_id, f"claude --dangerously-skip-permissions --append-system-prompt {shlex.quote(context)}")

    return {"session_id": session_id}


@app.websocket("/ws/discuss/{session_id}")
async def ws_discuss(websocket: WebSocket, session_id: str):
    await terminal.terminal_handler(websocket, session_id, _config)


@app.get("/api/reviews/{repo}/{pr_id}/diff")
async def api_review_diff(repo: str, pr_id: int):
    from platforms import make_platform
    platform = make_platform(_config)
    diff = platform.get_pr_diff(repo, pr_id)
    return {"diff": diff or ""}


@app.post("/api/reviews/{repo}/{pr_id}/comments/{idx}/submit")
async def api_submit_comment(repo: str, pr_id: int, idx: int):
    import json
    from platforms import make_platform
    platform = make_platform(_config)
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
async def api_new_comment(repo: str, pr_id: int, request: Request):
    import json
    body = await request.json()
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
async def api_delete_comment(repo: str, pr_id: int, idx: int):
    import json
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
async def api_update_comment(repo: str, pr_id: int, idx: int, request: Request):
    import json
    body = await request.json()
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
async def api_tickets_list():
    return state.load("tickets")


@app.get("/api/tickets/{key}/detail")
async def api_ticket_detail(key: str):
    import json as _json
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
        from terminal import _tmux_session_exists, _tmux_session_name
        terminal_alive = _tmux_session_exists(_tmux_session_name(key))

    summary = None
    if docs_dir.is_dir():
        summary_cache = docs_dir / ".change-summary.txt"
        manifest = docs_dir / "change-manifest.md"
        if manifest.exists():
            if summary_cache.exists() and summary_cache.stat().st_mtime >= manifest.stat().st_mtime:
                summary = summary_cache.read_text()
            else:
                from claude_runner import run_haiku
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
async def api_kill_terminal(key: str):
    terminal.kill_terminal(key)
    return {"status": "ok"}


@app.get("/api/tickets/{key}/diff")
async def api_ticket_diff(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts or not ts.get("prs"):
        return {"diff": ""}
    from platforms import make_platform
    platform = make_platform(_config)
    pr = ts["prs"][0]
    diff = platform.get_pr_diff(pr["repo"], pr["id"])
    return {"diff": diff or ""}


@app.get("/api/tickets/{key}/pr-comments")
async def api_ticket_pr_comments(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return []
    slug = ts.get("slug", "")
    ws = _config["workspace"]
    path = ws["root"] / ws["tickets_dir"] / slug / "pr_comments.json"
    if not path.exists():
        return []
    import json as _json
    return _json.loads(path.read_text())


@app.post("/api/tickets/{key}/pr-comments/{comment_id}/reply")
async def api_ticket_reply(key: str, comment_id: int, request: Request):
    body = (await request.json()).get("body", "")
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

    import json as _json
    comments = _json.loads(path.read_text())
    entry = next((c for c in comments if c["id"] == comment_id), None)
    if not entry:
        return JSONResponse({"error": "comment not found"}, status_code=404)

    from platforms import make_platform
    platform = make_platform(_config)
    result = platform.post_pr_comment(
        entry["pr_repo"], entry["pr_id"], body,
        parent_id=entry["id"],
    )

    if result.get("status") == "posted":
        entry["status"] = "replied"
        entry["suggested_reply"] = body
        path.write_text(_json.dumps(comments, indent=2, default=str))
        log.emit("ticket_pr_reply_sent", f"Replied to comment on {key}",
            links={"detail": f"{_config['_base_url']}/tickets/{key}"},
            meta={"ticket": key, "comment_id": comment_id})
    return result


@app.post("/api/tickets/{key}/restart")
async def api_restart_ticket(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    slug = ts.get("slug", "")
    ws = _config["workspace"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    if not ticket_dir.is_dir():
        return JSONResponse({"error": "ticket dir not found"}, status_code=404)

    terminal.kill_terminal(key)
    import time
    time.sleep(1)
    terminal.ensure_session(key, str(ticket_dir))
    time.sleep(2)
    terminal.send_keys(key, "claude --dangerously-skip-permissions")
    time.sleep(3)
    if ts["status"] == "planning":
        terminal.send_keys(key, "/confer-technical-plan docs/")
    elif ts["status"] == "reviewing":
        terminal.send_keys(key, "Run /tri-review and save the full output to docs/tri-review.md")
    return {"status": "restarted"}


@app.get("/api/slack/data")
async def api_slack_data():
    return state.load("slack")


@app.post("/api/slack/send/{reply_id}")
async def api_slack_send(reply_id: str, request: Request):
    body = await request.json()
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

    import json as _json
    import urllib.request
    import urllib.parse
    tokens_path = _config.get("slack", {}).get("raw_path", "")
    if tokens_path:
        tokens_file = str(Path(tokens_path).parent.parent / "tokens.json")
        try:
            tokens = _json.loads(Path(tokens_file).read_text())
        except (FileNotFoundError, _json.JSONDecodeError):
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
        result = _json.loads(resp.read())
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
        import features.own_prs as own_prs
        own_prs.check(config)
    except Exception as e:
        log.emit("cycle_error", f"own_prs failed: {e}")

    if config.get("features", {}).get("review_prs"):
        try:
            import features.reviewer as reviewer
            reviewer.check(config)
        except Exception as e:
            log.emit("cycle_error", f"reviewer failed: {e}")

    if config.get("features", {}).get("tickets"):
        try:
            import features.tickets as _tickets_mod
            _tickets_mod.check(config)
        except Exception as e:
            log.emit("cycle_error", f"tickets failed: {e}")

    if config.get("features", {}).get("timesheet"):
        try:
            import features.timesheet as timesheet
            timesheet.check(config)
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
                import features.slack_monitor as slack_monitor
                slack_monitor.check(config)
            except Exception as e:
                log.emit("cycle_error", f"slack_monitor failed: {e}")
        time.sleep(random.randint(50, 90))


def main():
    if len(sys.argv) < 2:
        print("Usage: frshty.py <config.toml>")
        sys.exit(1)

    global _config
    _config = cfg.load_config(sys.argv[1])

    state.init(_config["_state_dir"])
    log.init(_config["_state_dir"], _config["job"]["key"])

    port = _config["job"]["port"]

    loop_thread = threading.Thread(target=main_loop, args=(_config,), daemon=True)
    loop_thread.start()

    slack_thread = threading.Thread(target=slack_loop, args=(_config,), daemon=True)
    slack_thread.start()

    uvicorn.run(app, host="127.0.0.1", port=port, log_level="warning")


if __name__ == "__main__":
    main()
