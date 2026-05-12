import asyncio
import json
import subprocess
import time
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.websockets import WebSocket

import core.config as cfg
import core.log as log
import core.state as state
import core.terminal as terminal
import features.releases as releases
import features.tickets as _tickets_mod
from core.claude_runner import run_haiku
from core.config import get_repos
from core.ticket_status import TicketStatus
from features.platforms import make_platform
from web.state import _config, events_enabled


router = APIRouter()

CUSTOM_CONTEXT_DIR = Path(__file__).parent.parent / "docs" / "custom-context"

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


def _ticket_repo_count(slug: str) -> int:
    if not slug:
        return 0
    n = 0
    for repo in get_repos(_config):
        if cfg.ticket_worktree_path(_config, slug, repo["name"]).is_dir():
            n += 1
    return n


def _bucket_invocation_status(status: str) -> str:
    if status == "success":
        return "ok"
    if status in ("error", "timeout"):
        return "failed"
    if status == "blocked":
        return "blocked"
    return "other"


def _empty_llm_breakdown() -> dict:
    return {"ok": 0, "failed": 0, "blocked": 0, "total": 0}


def _llm_breakdowns_by_slug(instance_key: str, slugs: set[str]) -> dict[str, dict]:
    if not instance_key or not slugs:
        return {}
    import core.db as _db
    rows = _db.query_all(
        "SELECT cwd, status, COUNT(*) AS n FROM claude_invocations"
        " WHERE instance_key=? AND cwd IS NOT NULL AND cwd != ''"
        " GROUP BY cwd, status",
        (instance_key,),
    )
    out: dict[str, dict] = {s: _empty_llm_breakdown() for s in slugs}
    for r in rows:
        cwd = r["cwd"] or ""
        for slug in slugs:
            if not slug:
                continue
            needle_mid = f"/{slug}/"
            needle_end = f"/{slug}"
            if needle_mid in cwd or cwd.endswith(needle_end):
                bucket = _bucket_invocation_status(r["status"])
                bd = out[slug]
                bd["total"] += r["n"]
                if bucket in bd:
                    bd[bucket] += r["n"]
                break
    return out


def _llm_breakdown_for_slug(instance_key: str, slug: str) -> dict:
    if not instance_key or not slug:
        return _empty_llm_breakdown()
    import core.db as _db
    rows = _db.query_all(
        "SELECT status, COUNT(*) AS n FROM claude_invocations"
        " WHERE instance_key=? AND (cwd LIKE ? OR cwd LIKE ?)"
        " GROUP BY status",
        (instance_key, f"%/{slug}/%", f"%/{slug}"),
    )
    bd = _empty_llm_breakdown()
    for r in rows:
        bucket = _bucket_invocation_status(r["status"])
        bd["total"] += r["n"]
        if bucket in bd:
            bd[bucket] += r["n"]
    return bd


def _local_worktree_diff(ts: dict) -> str:
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


@router.get("/api/tickets/{ticket_key}/pr-info")
def api_ticket_pr_info(ticket_key: str):
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
            wt = _tickets_mod.ticket_worktree_path(_config, slug, repo["name"])
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


@router.post("/api/tickets/{ticket_key}/submit-pr")
async def api_submit_pr(ticket_key: str, request: Request):
    try:
        data = await request.json()
    except json.JSONDecodeError:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)
    return await asyncio.to_thread(_submit_pr_sync, ticket_key, data)


def _submit_pr_sync(ticket_key: str, data: dict):
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
        wt = _tickets_mod.ticket_worktree_path(_config, slug, repo_name)
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


@router.get("/api/tickets/list")
def api_tickets_list():
    from datetime import datetime, timedelta, timezone
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
                f = json.loads(r["findings"]) if r["findings"] else []
                pm_counts[r["ticket_key"]] = len(f) if isinstance(f, list) else 0
            except (ValueError, TypeError):
                pm_counts[r["ticket_key"]] = 0
        from features import validation as _val
        val_badges = _val.badges_bulk(instance_key)
    out = {}
    slugs: set[str] = set()
    for k, v in tickets.items():
        if v.get("status") == "done":
            continue
        slug = v.get("slug", "")
        v["repo_count"] = _ticket_repo_count(slug)
        v["pm_findings_count"] = pm_counts.get(k, 0)
        v["validation_badge"] = val_badges.get(k, "pending")
        if slug:
            slugs.add(slug)
        out[k] = v
    llm_by_slug = _llm_breakdowns_by_slug(instance_key, slugs)
    for v in out.values():
        v["llm_invocations"] = llm_by_slug.get(v.get("slug", ""), _empty_llm_breakdown())
    return out


@router.get("/api/raw/tickets")
def api_raw_tickets():
    from features.ticket_systems import make_ticket_system
    ts = make_ticket_system(_config)
    if not ts:
        return JSONResponse({"error": "no ticket system configured"}, status_code=400)
    try:
        return ts.fetch_tickets()
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/raw/prs")
def api_raw_prs():
    platform = make_platform(_config)
    try:
        my_prs = platform.list_my_open_prs()
        return {"my_prs": my_prs}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@router.get("/api/tickets/{key}/detail")
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
    release_block = None
    if (_config.get("features") or {}).get("releases"):
        release_block = _release_block_for_ticket(key, ts)
    instance_key = _config.get("job", {}).get("key", "")
    llm_invocations = _llm_breakdown_for_slug(instance_key, slug)
    return {"key": key, "state": ts, "docs": docs, "history": history, "summary": summary, "terminal_alive": terminal_alive, "all_statuses": all_statuses, "demo_video": demo_video, "release": release_block, "llm_invocations": llm_invocations}


def _release_block_for_ticket(ticket_key: str, ticket_state: dict) -> dict | None:
    instance_key = _config.get("job", {}).get("key", "")
    rk = ticket_state.get("release_key") or releases.ticket_release_key(instance_key, ticket_key)
    if not rk:
        return None
    rel = releases.get_release_by_key(instance_key, rk)
    if not rel:
        return None
    counts = {"ticket_count": 0, "terminal_count": 0}
    for s in releases.list_summaries(instance_key):
        if s["id"] == rel["id"]:
            counts["ticket_count"] = s["ticket_count"]
            counts["terminal_count"] = s["terminal_count"]
            break
    latest = releases.latest_review(rel["id"])
    return {
        "id": rel["id"],
        "release_key": rel["release_key"],
        "title": rel.get("title"),
        "status": rel["status"],
        "ticket_count": counts["ticket_count"],
        "terminal_count": counts["terminal_count"],
        "latest_review": latest and {
            "verdict": latest["verdict"],
            "findings": latest["findings"],
            "created_at": latest["created_at"],
        },
    }


def _releases_enabled() -> bool:
    return bool((_config.get("features") or {}).get("releases"))


@router.post("/api/tickets/{key}/release")
def api_set_ticket_release(key: str, body: dict):
    if not _releases_enabled():
        return JSONResponse({"error": "releases feature disabled"}, status_code=404)
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "ticket not found"}, status_code=404)
    raw = body.get("release_key")
    release_key: str | None
    if raw is None or raw == "":
        release_key = None
    elif isinstance(raw, str):
        release_key = raw.strip() or None
    else:
        return JSONResponse({"error": "release_key must be string or null"}, status_code=400)
    if release_key is not None and not releases.is_valid_release_key(release_key):
        return JSONResponse({"error": f"invalid release_key: {release_key!r}"}, status_code=400)
    instance_key = _config.get("job", {}).get("key", "")
    title = body.get("title")
    if release_key is not None and isinstance(title, str) and title.strip():
        releases.upsert_release(instance_key, release_key, title=title.strip())
    try:
        updated = releases.assign_ticket(instance_key, key, release_key)
    except ValueError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    if updated is None:
        return JSONResponse({"error": "ticket not found"}, status_code=404)
    log.emit("ticket_release_updated",
             f"{key} → {release_key if release_key else '(unassigned)'}",
             meta={"ticket": key, "release_key": release_key})
    return {"status": "ok", "ticket_key": key,
            "release": _release_block_for_ticket(key, updated)}


@router.get("/api/releases")
def api_list_releases():
    if not _releases_enabled():
        return JSONResponse({"error": "releases feature disabled"}, status_code=404)
    instance_key = _config.get("job", {}).get("key", "")
    return {"releases": releases.list_summaries(instance_key)}


@router.get("/api/releases/{release_key}")
def api_release_detail(release_key: str):
    if not _releases_enabled():
        return JSONResponse({"error": "releases feature disabled"}, status_code=404)
    instance_key = _config.get("job", {}).get("key", "")
    rel = releases.get_release_by_key(instance_key, release_key)
    if not rel:
        return JSONResponse({"error": "release not found"}, status_code=404)
    tickets_in_release = releases.list_release_tickets(instance_key, rel["id"])
    latest = releases.latest_review(rel["id"])
    return {
        "id": rel["id"],
        "release_key": rel["release_key"],
        "title": rel.get("title"),
        "status": rel["status"],
        "ticket_set_hash": rel.get("ticket_set_hash"),
        "last_inspected_at": rel.get("last_inspected_at"),
        "tickets": tickets_in_release,
        "latest_review": latest,
    }


@router.post("/api/releases/{release_key}/inspect")
def api_release_inspect(release_key: str, body: dict | None = None):
    if not _releases_enabled():
        return JSONResponse({"error": "releases feature disabled"}, status_code=404)
    if not events_enabled():
        return JSONResponse({"error": "events not enabled"}, status_code=400)
    instance_key = _config.get("job", {}).get("key", "")
    rel = releases.get_release_by_key(instance_key, release_key)
    if not rel:
        return JSONResponse({"error": "release not found"}, status_code=404)
    force = bool((body or {}).get("force"))
    import core.queue as q
    q.enqueue_job(instance_key, "release_inspect",
                  payload={"release_id": rel["id"], "force": force})
    log.emit("release_inspect_enqueued",
             f"manual inspect requested for {release_key} (force={force})",
             meta={"release_key": release_key, "force": force})
    return {"status": "enqueued", "release_key": release_key, "force": force}


@router.get("/api/tickets/{key}/demo")
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


@router.websocket("/ws/terminal/{key}")
async def ws_terminal(websocket: WebSocket, key: str):
    from web.state import multi_apply_host, multi_reset
    tokens = multi_apply_host(websocket.headers.get("host"))
    try:
        await terminal.terminal_handler(websocket, key, _config)
    finally:
        multi_reset(tokens)


@router.delete("/api/tickets/{key}/terminal")
def api_kill_terminal(key: str):
    terminal.kill_terminal(key)
    return {"status": "ok"}


@router.post("/api/tickets/{key}/terminal/reset")
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


@router.get("/api/tickets/{key}/diff")
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


@router.get("/api/tickets/{key}/pr-comments")
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


@router.post("/api/tickets/{key}/pr-comments/{comment_id}/reply")
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


@router.post("/api/tickets/{key}/merge")
def api_merge_ticket(key: str):
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") != "in_review":
        return JSONResponse({"error": f"ticket is {ts.get('status')}, not in_review"}, status_code=400)
    if not ts.get("prs"):
        return JSONResponse({"error": "no PRs to merge"}, status_code=400)
    base_url = _config.get("_base_url", "")
    ticket_payload = {"key": key, "summary": ts.get("summary", "")}
    updated = _tickets_mod._merge(_config, ticket_payload, ts, base_url)
    state.save_ticket(key, updated)
    return {"status": "ok", "new_status": updated.get("status", ts.get("status"))}


@router.post("/api/tickets/{key}/restart")
def api_restart_ticket(key: str):
    import core.queue as q

    def _reset(ts: dict) -> dict:
        if not ts:
            return ts
        ts.pop("ci_fix_attempts", None)
        ts.pop("pr_attempts", None)
        ts.pop("conflict_resolution_attempts", None)
        ts.pop("last_conflict_error", None)
        return ts

    ts = state.update_ticket(key, _reset)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") == TicketStatus.pr_failed.value:
        target = "in_review" if ts.get("prs") else "pr_ready"
        try:
            ts = state.transition_ticket(key, target)
        except state.TicketStateError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
    status = ts.get("status", "")
    instance_key = _config.get("job", {}).get("key", "")
    if instance_key and status in ("new", "planning"):
        q.enqueue_job(instance_key, "start_planning", ticket_key=key)
    elif instance_key and status == "reviewing":
        q.enqueue_job(instance_key, "start_reviewing", ticket_key=key)
    return {"status": "restarted"}


@router.post("/api/tickets/{key}/status")
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


@router.delete("/api/tickets/{key}")
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


@router.post("/api/tickets/{key}/approve")
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


@router.post("/api/tickets/{key}/reject")
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


@router.post("/api/tickets/{key}/obsolete")
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


@router.get("/api/tickets/{key}/validation")
def api_ticket_validation(key: str, limit: int = 50):
    from features import validation as v
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"badge": "pending", "runs": []}
    runs = v.latest_runs(instance_key, key, limit)
    badge = v.badge_for(instance_key, key)
    return {"badge": badge, "runs": runs}


@router.get("/api/tickets/{key}/pm-findings")
def api_ticket_pm_findings(key: str, limit: int = 20):
    from pm import runner
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"reviews": []}
    reviews = runner.latest_findings(instance_key, key, limit)
    return {"reviews": reviews}


@router.post("/api/tickets/{key}/retry-job")
def api_retry_job(key: str, body: dict):
    if not events_enabled():
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
    payload = job["payload"] if isinstance(job["payload"], dict) else json.loads(job["payload"] or "{}")
    q.emit_event(source="ui", kind="ui_retry",
                 payload={"task": job["task"], "payload": payload, "ticket_key": key},
                 instance_key=instance_key)
    return {"status": "enqueued", "task": job["task"]}


@router.post("/api/tickets/{key}/notes")
def api_ticket_notes(key: str, body: dict):
    note = (body.get("note") or "").strip()
    if not note:
        return JSONResponse({"error": "note required"}, status_code=400)
    if not events_enabled():
        return JSONResponse({"error": "events not enabled"}, status_code=400)
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    q.emit_event(source="ui", kind="ui_notes",
                 payload={"note": note, "ticket_key": key},
                 instance_key=instance_key)
    return {"status": "enqueued"}


@router.get("/api/tickets/{key}/context")
def api_ticket_context_get(key: str):
    f = CUSTOM_CONTEXT_DIR / f"{key}.md"
    return {"context": f.read_text() if f.exists() else ""}


@router.put("/api/tickets/{key}/context")
def api_ticket_context_put(key: str, body: dict):
    CUSTOM_CONTEXT_DIR.mkdir(parents=True, exist_ok=True)
    (CUSTOM_CONTEXT_DIR / f"{key}.md").write_text(body.get("context", ""))
    return {"status": "saved"}


@router.delete("/api/tickets/{key}/context")
def api_ticket_context_delete(key: str):
    f = CUSTOM_CONTEXT_DIR / f"{key}.md"
    if f.exists():
        f.unlink()
    return {"status": "deleted"}


@router.post("/api/tickets/{key}/set-state")
def api_set_state_event(key: str, body: dict):
    target = (body.get("target") or "").strip()
    if not target:
        return JSONResponse({"error": "target required"}, status_code=400)
    try:
        TicketStatus(target)
    except ValueError:
        return JSONResponse({"error": f"invalid target: {target}"}, status_code=400)
    if not events_enabled():
        return JSONResponse({"error": "events not enabled"}, status_code=400)
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    q.emit_event(source="ui", kind="ui_set_state",
                 payload={"target": target, "ticket_key": key},
                 instance_key=instance_key)
    return {"status": "enqueued", "target": target}


@router.patch("/api/tickets/{key}/auto-pr")
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


@router.post("/api/tickets/{key}/start-dev")
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
