import asyncio
import json
import subprocess
import threading
from datetime import datetime, timezone
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import FileResponse, JSONResponse
from starlette.websockets import WebSocket

import core.config as cfg
import core.db as db
import core.git_util as git_util
import core.log as log
import core.queue as q
import core.scheduler as scheduler
import core.state as state
import core.terminal as terminal
import features.presentation as presentation
import features.releases as releases
import features.tickets as _tickets_mod
from core.claude_runner import run_haiku
from core.config import get_repos
from core.ticket_status import TicketStatus
from features.platforms import make_platform
from services import work_launch
from web.state import _config, events_enabled


router = APIRouter()

_LOCKFILE_NOISE = frozenset({
    "Pipfile.lock", "poetry.lock", "uv.lock",
    "package-lock.json", "yarn.lock", "pnpm-lock.yaml", "npm-shrinkwrap.json",
    "Cargo.lock", "Gemfile.lock", "go.sum", "composer.lock", "mix.lock",
})


def _changed_files(wt: Path, base_ref: str) -> list[str] | None:
    """Files changed against the base, or None when the diff could not be run.

    Returning [] on failure made a broken diff indistinguishable from a branch
    with no changes, and every caller reads an empty list as "nothing here".
    That is how the Submit PR modal came to state "branch has no commits against
    its base" for a repo it had simply failed to inspect. None forces the caller
    to say it does not know."""
    try:
        result = git_util.run_git(wt, ["diff", "--name-only", f"{base_ref}...HEAD"],
                                  timeout=30)
    except git_util.GitCommandError:
        return None
    return [f for f in result.stdout.splitlines() if f.strip()]


def _is_meaningful_change(files: list[str] | None) -> bool:
    """False for no change AND for an unknown change. Callers that act on the
    difference must check for None before calling this."""
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


def _local_worktree_diff(ts: dict) -> list[dict]:
    """Return one entry per repo with non-empty `git diff origin/<base>...HEAD`.

    Shape matches the post-PR return shape from api_ticket_diff so the
    frontend's per-repo tab UI works identically pre- and post-PR. Pre-PR
    entries have pr_id=None and url='' (no PR yet)."""
    slug = ts.get("slug")
    if not slug:
        return []
    out: list[dict] = []
    for repo in get_repos(_config):
        wt = cfg.ticket_worktree_path(_config, slug, repo["name"])
        if not wt.is_dir():
            continue
        base_branch = cfg.base_branch_for(_config, repo["name"])
        result = subprocess.run(
            ["git", "diff", f"origin/{base_branch}...HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=30)
        diff_text = result.stdout
        if not diff_text.strip():
            continue
        out.append({
            "repo": repo["name"],
            "pr_id": None,
            "url": "",
            "diff": diff_text,
        })
    return out


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
        ticket_dir = ws.get("root", Path(".")) / ws.get("tickets_dir", "tickets") / slug
        docs_dir = ticket_dir / "docs"
        pr_descriptions = ticket.get("pr_descriptions") or {}

        # Fast path: pre-generated descriptions carry the title and description,
        # so the change-manifest haiku call is skipped. The file count is NOT
        # taken from the cache. It was written once at pr_ready entry and says
        # nothing about the branch now: DEV-636's cache still advertised two
        # changed files in websocket-server days after a replan reset wiped that
        # commit, so the modal offered a PR for code the branch no longer had.
        # Recount against the worktree, and mark any repo with nothing to push.
        if pr_descriptions:
            repos_out = []
            for name, entry in pr_descriptions.items():
                row = {
                    "name": name,
                    "branch": entry.get("branch") or ticket.get("branch", ""),
                    "files_changed": int(entry.get("files_changed", 0)),
                    "title": entry.get("title", f"{ticket_key}: {name}"),
                    "description": entry.get("description", ""),
                    "generated": True,
                    "generated_at": entry.get("generated_at", ""),
                    "has_changes": True,
                    "stale_reason": "",
                }
                wt = _tickets_mod.ticket_worktree_path(_config, slug, name)
                if not wt.is_dir():
                    row.update(files_changed=0, has_changes=False,
                               stale_reason="no worktree for this repo")
                else:
                    files = _changed_files(wt, f"origin/{cfg.base_branch_for(_config, name)}")
                    if files is None:
                        row.update(files_changed=0, has_changes=False,
                                   stale_reason="could not inspect this repo, so its "
                                                "state is unknown")
                    else:
                        row["files_changed"] = len(files)
                        if not _is_meaningful_change(files):
                            row.update(has_changes=False,
                                       stale_reason="branch has no commits against its base")
                repos_out.append(row)
            if any(not r["has_changes"] for r in repos_out):
                log.emit("pr_info_stale_repo",
                         f"{ticket_key}: PR cache lists "
                         f"{[r['name'] for r in repos_out if not r['has_changes']]} "
                         "with nothing to push",
                         meta={"ticket": ticket_key})
            return {"repos": repos_out}

        # Slow fallback: pre-feature tickets, or pr_ready entered moments ago
        # before the task ran. Do the per-repo git fetch + summary haiku and
        # serve the default title/description.
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
            base_branch = cfg.base_branch_for(_config, repo["name"])
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
                "generated": False,
            })

        return {"repos": repos_out}
    except Exception as e:
        log.emit("pr_info_error", f"Error generating PR info: {e}", meta={"ticket": ticket_key})
        return JSONResponse({"error": str(e)}, status_code=500)


@router.post("/api/tickets/{ticket_key}/pr-info/regenerate")
def api_regenerate_pr_info(ticket_key: str):
    instance_key = _config.get("job", {}).get("key", "")
    ts = state.load_ticket(ticket_key)
    if not ts:
        return JSONResponse({"error": "Ticket not found"}, status_code=404)
    if ts.get("status") != "pr_ready":
        return JSONResponse({"error": f"Ticket is {ts.get('status')}, not pr_ready"}, status_code=400)

    def _clear(t):
        if t is None:
            return None
        t.pop("pr_descriptions", None)
        t.pop("pr_descriptions_generated_at", None)
        return t

    state.update_ticket(ticket_key, _clear)
    job_id = q.enqueue_job(instance_key, "generate_pr_descriptions", ticket_key=ticket_key)
    log.emit("pr_descriptions_regenerate", f"Manual PR body regenerate for {ticket_key}",
             meta={"ticket": ticket_key, "job_id": job_id})
    return {"status": "queued", "job_id": job_id}


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

    slug = ticket.get("slug", "")
    if not slug:
        return JSONResponse({"error": "No slug found"}, status_code=400)

    platform = make_platform(_config)
    prs = []

    for r in repos_in:
        repo_name = r["name"]
        wt = _tickets_mod.ticket_worktree_path(_config, slug, repo_name)
        if not wt.is_dir():
            return JSONResponse({"error": f"worktree missing for {repo_name}"}, status_code=400)
        base_branch = cfg.base_branch_for(_config, repo_name)

        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "commit", "--no-verify", "-m", f"{ticket_key}: {ticket.get('summary', '')}"],
                       cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "fetch", "origin", base_branch], cwd=str(wt), capture_output=True, timeout=60)

        files = _changed_files(wt, f"origin/{base_branch}")
        if files is None:
            return JSONResponse(
                {"error": f"could not inspect {repo_name}; refusing to open a PR "
                          f"from a state that could not be read"}, status_code=400)
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
        state.transition_ticket(ticket_key, "in_review", reason="manual create-pr", prs=prs)
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
    expired = [k for k, v in tickets.items() if v.get("status") == "done" and v.get("done_at") and v.get("done_at") < cutoff]
    for k in expired:
        state.delete_ticket(k)
        del tickets[k]
    instance_key = _config.get("job", {}).get("key", "")
    pm_counts: dict[str, int] = {}
    val_badges: dict[str, str] = {}
    if instance_key:
        if (_config.get("pm_agent") or {}).get("enabled", True):
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


def _pr_comment_breakdown(entries: list[dict], repo: str, pr_id) -> dict:
    done = not_done = discuss = 0
    for e in entries:
        if e.get("pr_repo") != repo or e.get("pr_id") != pr_id:
            continue
        st = e.get("status")
        if st in ("addressed", "replied"):
            done += 1
        elif st == "needs_reply":
            discuss += 1
        else:
            not_done += 1
    return {"done": done, "not_done": not_done, "discuss": discuss}


def _pr_court(breakdown: dict, ci: str, mergeable: str) -> str:
    if breakdown["not_done"] or breakdown["discuss"]:
        return "your_court"
    if ci == "failing" or mergeable == "CONFLICTING":
        return "your_court"
    return "reviewer"


@router.get("/api/tickets/pr-board")
def api_tickets_pr_board():
    tickets = state.list_tickets()
    out = []
    for key, ts in tickets.items():
        if ts.get("status") not in ("in_review", "pr_failed"):
            continue
        prs = ts.get("prs") or []
        if not prs:
            continue
        slug = ts.get("slug", "")
        entries = _tickets_mod._load_pr_comments(_config, slug) if slug else []
        rows = []
        for pr in prs:
            if pr.get("pr_state") == "MERGED":
                continue
            ci = pr.get("ci", "unknown")
            mergeable = pr.get("mergeable", "UNKNOWN")
            breakdown = _pr_comment_breakdown(entries, pr["repo"], pr["id"])
            rows.append({
                "repo": pr["repo"],
                "id": pr["id"],
                "url": pr.get("url", ""),
                "ci": ci,
                "mergeable": mergeable,
                "approvers": pr.get("approvers", []),
                "comments": breakdown,
                "unresolved": [u for u in (pr.get("unresolved_comments") or []) if u.get("url")],
                "court": _pr_court(breakdown, ci, mergeable),
                "poll_count": pr.get("poll_count", 0),
                "last_polled_at": pr.get("last_polled_at", ""),
            })
        if rows:
            out.append({
                "ticket_key": key,
                "summary": ts.get("summary", ""),
                "status": ts.get("status"),
                "prs": rows,
            })
    out.sort(key=lambda t: t["ticket_key"])
    return {"tickets": out}


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
            if not (f.is_file() and not f.name.startswith(".")
                    and f.suffix in (".md", ".json", ".txt")):
                continue
            try:
                if f.stat().st_size <= 512 * 1024:
                    docs[f.name] = f.read_text()
                else:
                    docs[f.name] = f"[artifact too large to inline: {f.stat().st_size} bytes]"
            except (OSError, UnicodeDecodeError):
                pass

    history = log.get_events_for_ticket(key, limit=200)

    terminal_alive = False
    if ts.get("status") in ("planning", "reviewing", "in_review"):
        health = terminal.session_healthy(key)
        terminal_alive = health["alive"] and health["agent_running"]

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
    proof_videos = _list_proof_videos(docs_dir)
    has_explainer = (docs_dir / "change-explainer.html").is_file()
    ts["repo_count"] = _ticket_repo_count(slug)
    release_block = None
    if (_config.get("features") or {}).get("releases"):
        release_block = _release_block_for_ticket(key, ts)
    instance_key = _config.get("job", {}).get("key", "")
    llm_invocations = _llm_breakdown_for_slug(instance_key, slug)
    active_key = state.active_instance_key()
    transitions = _load_ticket_transitions(active_key, key)
    scheduled_rows = scheduler.list_for_ticket(active_key, key)
    return {"key": key, "state": ts, "docs": docs, "history": history, "summary": summary, "terminal_alive": terminal_alive, "all_statuses": all_statuses, "proof_videos": proof_videos, "docs_dir": str(docs_dir), "has_explainer": has_explainer, "release": release_block, "llm_invocations": llm_invocations, "transitions": transitions, "scheduled_rows": scheduled_rows}


PROOF_VIDEO_EXTS = {".webm", ".mp4", ".mov", ".mkv"}


def _list_proof_videos(docs_dir: Path) -> list[dict]:
    """Return video artifacts in the ticket's docs/ dir, sorted by filename.
    Each entry is {"name", "created_at"}. Used by the proving step to
    surface recordings on the ticket detail page. created_at is the file mtime
    as an ISO string — a re-run overwrites the recording in place, so the write
    time is when that recording was made. It tells a stale proof from a fresh
    one, which the filename alone cannot."""
    if not docs_dir.is_dir():
        return []
    out: list[dict] = []
    for f in docs_dir.iterdir():
        if not (f.is_file() and f.suffix.lower() in PROOF_VIDEO_EXTS):
            continue
        try:
            st = f.stat()
        except OSError:
            continue
        out.append({
            "name": f.name,
            "created_at": datetime.fromtimestamp(st.st_mtime, timezone.utc).isoformat(),
        })
    return sorted(out, key=lambda v: v["name"])


def _load_ticket_transitions(instance_key: str, key: str) -> list[dict]:
    rows = db.query_all(
        "SELECT prior_status, new_status, rejected, rejection_reason,"
        " actor, reason, co_field_diff, ts"
        " FROM ticket_transitions WHERE instance_key=? AND ticket_key=?"
        " ORDER BY ts ASC, id ASC",
        (instance_key, key),
    )
    out: list[dict] = []
    for r in rows:
        d = dict(r)
        try:
            d["co_field_diff"] = json.loads(d.get("co_field_diff") or "{}")
        except Exception:
            d["co_field_diff"] = {}
        d["rejected"] = bool(d["rejected"])
        out.append(d)
    return out


@router.post("/api/tickets/{key}/classify")
def api_classify_ticket(key: str, body: dict):
    """Manually set a held ticket's work type and route it: 'code' enters the
    normal pipeline, 'research' enters the research path."""
    work_type = (body.get("work_type") or "").strip().lower()
    if work_type not in ("code", "research"):
        return JSONResponse({"error": "work_type must be 'code' or 'research'"}, status_code=400)
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    ts["work_type"] = work_type
    state.save_ticket(key, ts)
    instance_key = state.active_instance_key()
    if work_type == "research":
        _tickets_mod._enqueue_stage(instance_key, key, "do_research")
    else:
        _tickets_mod._enqueue_stage(instance_key, key, "start_planning")
    log.emit("ticket_classified_manual", f"{key}: work_type set to {work_type} (manual)",
             meta={"ticket": key, "work_type": work_type})
    return {"status": "classified", "work_type": work_type}


@router.get("/api/tickets/{key}/transitions")
def api_ticket_transitions(key: str):
    active_key = state.active_instance_key()
    return {"key": key, "transitions": _load_ticket_transitions(active_key, key)}


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


_DOC_MIME = {
    ".webm": "video/webm",
    ".mp4": "video/mp4",
    ".mov": "video/quicktime",
    ".mkv": "video/x-matroska",
    ".html": "text/html",
}


@router.get("/api/tickets/{key}/docs/{filename}")
def api_ticket_docs_file(key: str, filename: str):
    """Serve an artifact (video or html page) from a ticket's docs/
    directory. Whitelisted by extension; filename cannot contain path
    separators."""
    if "/" in filename or "\\" in filename or ".." in filename:
        return JSONResponse({"error": "invalid filename"}, status_code=400)
    suffix = Path(filename).suffix.lower()
    if suffix not in _DOC_MIME:
        return JSONResponse({"error": "unsupported file type"}, status_code=400)
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "ticket not found"}, status_code=404)
    slug = ts.get("slug", "")
    ws = _config["workspace"]
    docs_dir = (ws["root"] / ws["tickets_dir"] / slug / "docs").resolve()
    target = (docs_dir / filename).resolve()
    if docs_dir not in target.parents and target != docs_dir:
        return JSONResponse({"error": "invalid path"}, status_code=400)
    if not target.is_file():
        return JSONResponse({"error": "file not found"}, status_code=404)
    return FileResponse(str(target), media_type=_DOC_MIME[suffix])


@router.websocket("/ws/terminal/{key}")
async def ws_terminal(websocket: WebSocket, key: str):
    from web.state import host_is_disabled, multi_apply_host, multi_reset
    if host_is_disabled(websocket.headers.get("host")):
        await websocket.close(code=1011, reason="instance disabled")
        return
    tokens = multi_apply_host(websocket.headers.get("host"))
    try:
        suffix = key[len("work-"):] if key.startswith("work-") else ""
        if suffix.isdigit():
            work_launch.resume_session(int(suffix))
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
    terminal.launch_pane_command(key, str(ticket_dir), terminal.claude_cmd(_config))
    return {"status": "ok"}


DISCUSS_KEY_SUFFIX = "-discuss"
DISCUSS_LIFETIME_SECONDS = 8 * 3600

_discuss_timers: dict[str, threading.Timer] = {}
_discuss_timers_lock = threading.Lock()


def _schedule_discuss_kill(discuss_key: str) -> None:
    """Kill the discuss tmux session after DISCUSS_LIFETIME_SECONDS. Timer
    is started once per session lifetime — repeat starts don't reset the
    clock. Frshty restarts drop the timer (the session would then live
    until the next start call observes it and re-arms)."""
    with _discuss_timers_lock:
        if discuss_key in _discuss_timers:
            return
        def _kill():
            try:
                terminal.kill_terminal(discuss_key)
                log.emit("ticket_discuss_expired",
                         f"discuss session {discuss_key} killed after "
                         f"{DISCUSS_LIFETIME_SECONDS // 3600}h",
                         meta={"discuss_key": discuss_key,
                               "lifetime_seconds": DISCUSS_LIFETIME_SECONDS})
            except Exception:
                pass
            with _discuss_timers_lock:
                _discuss_timers.pop(discuss_key, None)
        t = threading.Timer(DISCUSS_LIFETIME_SECONDS, _kill)
        t.daemon = True
        t.start()
        _discuss_timers[discuss_key] = t


@router.post("/api/tickets/{key}/discuss/start")
def api_start_discuss(key: str, cont: bool = False):
    """Ensure a separate `<key>-discuss` tmux session in the ticket's
    worktree dir and seed it with `cl` (the user's claude alias). Keyed off
    a distinct suffix so it never collides with the main dev terminal that
    might already be running planning/reviewing claude in the same dir.
    Session auto-expires after DISCUSS_LIFETIME_SECONDS (default 8h)."""
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    slug = ts.get("slug", "")
    if not slug:
        return JSONResponse({"error": "no slug"}, status_code=400)
    ws = _config["workspace"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    if not ticket_dir.is_dir():
        return JSONResponse({"error": f"ticket dir missing: {ticket_dir}"}, status_code=400)
    discuss_key = f"{key}{DISCUSS_KEY_SUFFIX}"
    health = terminal.session_healthy(discuss_key)
    if health.get("alive") and health.get("agent_running"):
        _schedule_discuss_kill(discuss_key)
        return {"status": "running", "discuss_key": discuss_key}
    if not health.get("agent_running"):
        base = terminal.claude_cmd(_config)
        terminal.launch_pane_command(
            discuss_key, str(ticket_dir), f"{base} --continue" if cont else base)
    _schedule_discuss_kill(discuss_key)
    return {"status": "ok", "discuss_key": discuss_key}


@router.get("/api/tickets/{key}/diff")
def api_ticket_diff(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return {"repos": [], "diff": ""}
    prs = ts.get("prs") or []
    if not prs:
        repos_out = _local_worktree_diff(ts)
        first_diff = repos_out[0]["diff"] if repos_out else ""
        return {"repos": repos_out, "diff": first_diff}
    platform = make_platform(_config)
    repos_out = []
    for pr in prs:
        try:
            d = platform.get_pr_diff(pr["repo"], pr["id"])
        except Exception:
            d = ""
        repos_out.append({
            "repo": pr["repo"],
            "pr_id": pr["id"],
            "url": pr.get("url", ""),
            "diff": d or "",
        })
    return {"repos": repos_out, "diff": (repos_out[0]["diff"] if repos_out else "")}


@router.post("/api/tickets/{key}/presentation/preprocess")
def api_ticket_presentation_preprocess(key: str):
    tickets = state.load("tickets")
    if not tickets.get(key):
        return JSONResponse({"error": "ticket not found"}, status_code=404)
    result = presentation.spawn_ticket_build(_config, key)
    if result == "unavailable":
        return JSONResponse({"error": "ticket has no slug"}, status_code=400)
    if result == "cached":
        return {"status": "ok", "cached": True}
    if result == "in_progress":
        return {"status": "in_progress", "cached": False}
    return {"status": "ok", "cached": False}


@router.get("/api/tickets/{key}/presentation")
def api_ticket_presentation(key: str):
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return {"status": "not_found"}
    cache_file = presentation.ticket_cache_file(_config, ts)
    building = presentation.is_building(("ticket", key))
    if not cache_file or not cache_file.exists():
        return {"status": "building" if building else "missing"}
    try:
        data = json.loads(cache_file.read_text())
    except (OSError, ValueError):
        return {"status": "missing"}
    return {"status": "ok", "presentation": data, "building": building}


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


@router.get("/api/tickets/{key}/pr-comments/{comment_id}/timeline")
def api_ticket_comment_timeline(key: str, comment_id: int):
    return {"timeline": log.get_events_for_comment(str(comment_id), limit=100)}


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
        ts.pop("pr_failed_reason", None)
        return ts

    ts = state.update_ticket(key, _reset)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") == TicketStatus.pr_failed.value:
        target = "in_review" if ts.get("prs") else "pr_ready"
        try:
            ts = state.transition_ticket(key, target, reason="manual restart")
        except state.TicketStateError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
    if ts.get("status") == TicketStatus.blocked.value:
        try:
            ts = state.transition_ticket(key, "new", reason="manual restart")
        except state.TicketStateError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
    status = ts.get("status", "")
    instance_key = _config.get("job", {}).get("key", "")
    if instance_key and status in ("new", "planning"):
        q.enqueue_job(instance_key, "start_planning", ticket_key=key)
    elif instance_key and status == "reviewing":
        q.enqueue_job(instance_key, "start_reviewing", ticket_key=key)
    return {"status": "restarted"}


@router.post("/api/tickets/{key}/redo-proof")
def api_redo_proof(key: str, body: dict):
    feedback = (body.get("feedback") or "").strip()
    tickets = state.load("tickets")
    ts = tickets.get(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    ws = _config["workspace"]
    slug = ts.get("slug", "")
    docs_dir = ws["root"] / ws["tickets_dir"] / slug / "docs"
    proof = docs_dir / "proof.md"
    if proof.exists():
        try:
            proof.replace(docs_dir / "proof.prev.md")
        except OSError:
            pass

    def _set(t: dict) -> dict:
        t["proof_feedback"] = feedback
        sess = dict(t.get("llm_sessions") or {})
        sess.pop("prove", None)
        t["llm_sessions"] = sess
        return t

    state.update_ticket(key, _set)
    try:
        state.transition_ticket(key, "proving", reason="manual redo proof")
    except state.TicketStateError as e:
        return JSONResponse({"error": str(e)}, status_code=400)
    return {"status": "redoing", "feedback": bool(feedback)}


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
        state.transition_ticket(key, target, reason="manual status override", **fields)
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
            try:
                shutil.rmtree(ticket_dir)
            except PermissionError as e:
                sudo_res = subprocess.run(
                    ["sudo", "-n", "rm", "-rf", str(ticket_dir)],
                    capture_output=True, timeout=60,
                )
                if sudo_res.returncode != 0 or ticket_dir.exists():
                    log.emit("ticket_discard_cleanup_failed",
                        f"Could not remove ticket dir for {key}: {e}",
                        meta={"ticket": key, "path": str(ticket_dir),
                              "sudo_rc": sudo_res.returncode,
                              "sudo_stderr": sudo_res.stderr.decode("utf-8", "replace")[:500]})
        for repo in repos:
            subprocess.run(["git", "worktree", "prune"], cwd=str(repo["path"]), capture_output=True, timeout=60)
    instance_key = _config.get("job", {}).get("key", "")
    if instance_key:
        scheduler.delete(instance_key, key)
    state.delete_ticket(key)
    return {"status": "discarded"}


@router.post("/api/tickets/{key}/ignore")
def api_ignore_ticket(key: str):
    from datetime import datetime, timezone
    import core.scheduler as scheduler
    ts = state.load_ticket(key)
    if ts is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    terminal.kill_terminal(key)
    old_status = ts.get("status", "")
    ts["status"] = "ignored"
    ts["ignored_at"] = datetime.now(timezone.utc).isoformat()
    state.save_ticket(key, ts)
    instance_key = _config.get("job", {}).get("key", "")
    if instance_key:
        scheduler.delete(instance_key, key)
    log.emit("ticket_ignored", f"Ignored {key}",
        links={"detail": f"{_config['_base_url']}/tickets/{key}"},
        meta={"ticket": key, "old_status": old_status})
    return {"status": "ignored", "old_status": old_status}


@router.post("/api/tickets/{key}/unignore")
def api_unignore_ticket(key: str):
    ts = state.load_ticket(key)
    if ts is None:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") != "ignored":
        return JSONResponse({"error": "ticket is not ignored"}, status_code=400)
    ts["status"] = "new"
    ts.pop("ignored_at", None)
    state.save_ticket(key, ts)
    log.emit("ticket_unignored", f"Un-ignored {key}",
        links={"detail": f"{_config['_base_url']}/tickets/{key}"},
        meta={"ticket": key})
    return {"status": "new"}


@router.post("/api/tickets/{key}/approve")
def api_approve_ticket(key: str):
    ts = state.load_ticket(key)
    if not ts:
        return JSONResponse({"error": "not found"}, status_code=404)
    if ts.get("status") != "pending_approval":
        return JSONResponse({"error": f"cannot approve from status {ts.get('status')}"}, status_code=400)
    try:
        state.transition_ticket(key, "new", reason="manual approve", approval_status="approved")
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
                                reason="manual reject",
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
    if not (_config.get("pm_agent") or {}).get("enabled", True):
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
