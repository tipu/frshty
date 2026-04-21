import json
import re
import subprocess
from pathlib import Path

import httpx

import core.log as log
import core.queue as q
import core.state as state
from core.config import get_repos, ticket_worktree_path, resolve_env
from core.claude_runner import run_haiku, run_claude_code, extract_json
from core.ticket_status import TicketStatus, transition
from features.platforms import make_platform
from features.ticket_systems import make_ticket_system


STATES = ["new", "planning", "reviewing", "pr_ready", "pr_created", "in_review", "merged"]


def _label(key: str, ts: dict) -> str:
    return ts.get("slug", key)


_VERDICT_RE = re.compile(r"^VERDICT:\s*(PASS|FAIL)\b", re.MULTILINE | re.IGNORECASE)


def _enqueue_stage(instance_key: str, ticket_key: str, task_name: str) -> None:
    existing = q.jobs_for_ticket(instance_key, ticket_key, limit=20)
    if any(j["task"] == task_name and j["status"] in ("queued", "running") for j in existing):
        return
    q.enqueue_job(instance_key, task_name, ticket_key=ticket_key)


def _image_filename(alt: str, url: str, seen: set | None = None) -> str:
    filename = re.sub(r'[^\w.\-]', '_', alt) if alt else url.split("/")[-1]
    if not filename.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp')):
        filename += '.png'
    if seen is not None and filename in seen:
        base, ext = filename.rsplit('.', 1)
        i = 2
        while f"{base}_{i}.{ext}" in seen:
            i += 1
        filename = f"{base}_{i}.{ext}"
    if seen is not None:
        seen.add(filename)
    return filename


def _download_attachments(config, ticket, docs_path):
    attachments = ticket.get("attachments", [])
    att_dir = docs_path / "attachments"

    headers = {}
    auth = None
    ticket_system = config["job"].get("ticket_system", "")
    if ticket_system == "jira":
        user = resolve_env(config, "jira", "user_env")
        token = resolve_env(config, "jira", "token_env")
        if user and token:
            auth = (user, token)
    elif ticket_system == "linear":
        token = resolve_env(config, "linear", "token_env")
        if token:
            headers["Authorization"] = token

    inline_images = re.findall(r'!\[([^\]]*)\]\((https?://[^)]+)\)', ticket.get("description", ""))
    all_downloads = [(a.get("filename", ""), a.get("url", "")) for a in attachments]
    seen = set()
    for alt, url in inline_images:
        filename = _image_filename(alt, url, seen)
        all_downloads.append((filename, url))

    if not all_downloads:
        return

    att_dir.mkdir(exist_ok=True)
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for filename, url in all_downloads:
            if not url or not filename:
                continue
            try:
                resp = client.get(url, auth=auth, headers=headers)
                if resp.status_code == 200:
                    (att_dir / filename).write_bytes(resp.content)
            except Exception:
                pass


def _localize_images(md: str, docs_path: Path) -> str:
    att_dir = docs_path / "attachments"
    seen = set()
    def _replace(m):
        alt, url = m.group(1), m.group(2)
        filename = _image_filename(alt, url, seen)
        local = att_dir / filename
        if local.exists():
            return f"![{alt}](attachments/{filename})"
        return m.group(0)
    return re.sub(r'!\[([^\]]*)\]\((https?://[^)]+)\)', _replace, md)


def _resolve_status(config: dict, external_status: str) -> str | None:
    system = config["job"].get("ticket_system", "")
    status_map = config.get(system, {}).get("status_map", {})
    if not status_map:
        return None
    return status_map.get(external_status)


def _fetch_ticket_comments(config: dict, key: str) -> list[dict]:
    ts = make_ticket_system(config)
    if not ts:
        return []
    try:
        return ts.fetch_comments(key)
    except Exception:
        return []


def _comment_snapshot(comments: list[dict]) -> dict:
    dates = [c["created_at"] for c in comments if c.get("created_at")]
    return {
        "count": len(comments),
        "latest_created_at": max(dates) if dates else None,
    }


def _write_comments_md(docs_path: Path, comments: list[dict]) -> None:
    if not comments:
        (docs_path / "comments.md").write_text("# Comments\n\nNo upstream comments.\n")
        return
    parts = ["# Comments\n"]
    for c in comments:
        author = c.get("author") or "Unknown"
        created = c.get("created_at") or ""
        body = (c.get("body") or "").strip()
        parts.append(f"\n## {author} — {created}\n\n{body}\n")
    (docs_path / "comments.md").write_text("".join(parts))


def _mark_ticket_merged(config: dict, ticket: dict, ts: dict) -> dict:
    from datetime import datetime, timezone
    comments = _fetch_ticket_comments(config, ticket["key"])
    ts["status"] = transition(ts["status"], "merged")
    ts["merged_at"] = datetime.now(timezone.utc).isoformat()
    ts["merged_comment_snapshot"] = _comment_snapshot(comments)
    if "merged_external_status" not in ts:
        ts["merged_external_status"] = ticket.get("status", "") or ts.get("external_status", "")
    ts.pop("ci_passed", None)
    return ts


def _clear_reingest_docs(config: dict, slug: str) -> list[str]:
    ws = config["workspace"]
    docs = ws["root"] / ws["tickets_dir"] / slug / "docs"
    deleted = []
    for name in ("ticket.md", "technical-plan.md", "change-manifest.md", "tri-review.md"):
        p = docs / name
        if p.exists():
            p.unlink()
            deleted.append(name)
    return deleted


def _reingest_merged_ticket(config: dict, ticket: dict, ts: dict, base_url: str) -> dict:
    from datetime import datetime, timezone
    key = ticket["key"]
    slug = ts.get("slug")
    snapshot = ts.get("merged_comment_snapshot") or {}
    comments = _fetch_ticket_comments(config, key)
    current = _comment_snapshot(comments)

    deleted = _clear_reingest_docs(config, slug) if slug else []

    cur_latest = current["latest_created_at"]
    snap_latest = snapshot.get("latest_created_at")
    has_new = current["count"] > snapshot.get("count", 0) or (
        cur_latest is not None and snap_latest is not None and cur_latest > snap_latest
    )

    for field in ("prs", "ci_fix_attempts", "pr_attempts", "ci_passed",
                  "checks_started_at", "_ci_failed_pending", "pr_scheduled_at",
                  "conflict_resolution_attempts", "last_comment_ids", "done_at"):
        ts.pop(field, None)

    ts["status"] = "new"
    ts["external_status"] = ticket.get("status", "")
    ts["requeued_at"] = datetime.now(timezone.utc).isoformat()
    ts["reopened_count"] = ts.get("reopened_count", 0) + 1
    ts["last_merged_at"] = ts.pop("merged_at", None)
    ts["last_merged_comment_snapshot"] = ts.pop("merged_comment_snapshot", None)
    ts["last_merged_external_status"] = ts.pop("merged_external_status", None)

    comment_check = "ok" if snapshot else "skipped_no_merge_snapshot"

    log.emit(
        "ticket_requeued",
        f"Re-queued ticket: {key} — {ticket['summary']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={
            "ticket": key, "slug": slug, "deleted_docs": deleted,
            "reopened_count": ts["reopened_count"],
            "comment_check": comment_check,
            "merged_comment_count": snapshot.get("count"),
            "current_comment_count": current["count"],
        },
    )

    if snapshot and not has_new:
        log.emit(
            "ticket_requeued_without_comment",
            f"Re-queued without new upstream comment: {key}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
            meta={
                "ticket": key, "slug": slug,
                "merged_comment_count": snapshot.get("count"),
                "current_comment_count": current["count"],
                "merged_latest_at": snapshot.get("latest_created_at"),
                "current_latest_at": current["latest_created_at"],
            },
        )

    new_ts = _setup_ticket(config, ticket, base_url, comments=comments)
    for k in ("slug", "branch", "discovered_at"):
        if new_ts.get(k):
            ts[k] = new_ts[k]
    ts["status"] = new_ts.get("status", "new")
    return ts


def check(config: dict, instance_key: str = ""):
    from datetime import datetime, timezone
    assigned = _fetch_tickets(config)
    if not assigned:
        return

    ticket_state = state.load("tickets")
    base_url = config["_base_url"]
    assigned_keys = {t["key"] for t in assigned}
    discovery_only = not get_repos(config)

    open_prs = [] if discovery_only else _fetch_open_prs(config)

    platform = None
    for key, ts in list(ticket_state.items()):
        if key in assigned_keys or ts.get("status") == TicketStatus.done:
            continue
        prs = ts.get("prs", [])
        if prs and not discovery_only:
            if platform is None:
                platform = make_platform(config)
            try:
                if any(platform.get_pr_state(p["repo"], p["id"]) == "OPEN" for p in prs):
                    continue
            except Exception:
                continue
        ts["status"] = transition(ts.get("status", "new"), "done")
        ts["done_at"] = datetime.now(timezone.utc).isoformat()
        state.save_ticket(key, ts)

    for ticket in assigned:
        key = ticket.get("key", "?")
        try:
            if instance_key:
                running = [j for j in q.jobs_for_ticket(instance_key, key) if j["status"] == "running"]
                if running:
                    continue
            existing = key in ticket_state
            ts = ticket_state.get(key, {"status": "new"})
            ts["external_status"] = ticket.get("status", "")
            if ts.get("status") == "done":
                ts.pop("done_at", None)
                if ts.get("prs"):
                    ts["status"] = "pr_created"
                elif ts.get("slug"):
                    ts["status"] = "pr_ready"
                else:
                    ts["status"] = "new"

            if discovery_only:
                if not existing:
                    log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
                        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
                        meta={"ticket": key})
                    ts["slug"] = _make_slug(key, ticket["summary"])
                    ts["branch"] = _make_branch(config, key, ticket)
                    ts["url"] = ticket.get("url", "")
                state.save_ticket(key, ts)
                continue

            mapped = _resolve_status(config, ticket.get("status", ""))
            if mapped and "slug" not in ts:
                ts["slug"] = _make_slug(key, ticket["summary"])
                ts["branch"] = _make_branch(config, key, ticket)
                ts["url"] = ticket.get("url", "")
                ts["status"] = TicketStatus(mapped).value
                if mapped != "new":
                    state.save_ticket(key, ts)
                    continue

            if ts["status"] == TicketStatus.merged:
                curr_ext = ticket.get("status", "")
                if "merged_external_status" not in ts:
                    ts["merged_external_status"] = curr_ext
                    state.save_ticket(key, ts)
                    continue
                if curr_ext == ts["merged_external_status"]:
                    continue
                ts = _reingest_merged_ticket(config, ticket, ts, base_url)
                state.save_ticket(key, ts)
                if instance_key:
                    _enqueue_stage(instance_key, key, "start_planning")
                continue

            if ts["status"] == TicketStatus.pr_failed:
                continue

            if ts.get("branch"):
                ts = _reconcile_prs(ts, open_prs)

            if ts["status"] == "new":
                ts = _setup_ticket(config, ticket, base_url)
                if ts.get("discovered_at") and instance_key:
                    _enqueue_stage(instance_key, key, "start_planning")

            if ts["status"] == "planning" and instance_key:
                _enqueue_stage(instance_key, key, "start_planning")

            if ts["status"] == "reviewing" and instance_key:
                ws = config["workspace"]
                slug = ts.get("slug", "")
                review_file = ws["root"] / ws["tickets_dir"] / slug / "docs" / "tri-review.md"
                if review_file.exists():
                    verdict = _VERDICT_RE.search(review_file.read_text())
                    if verdict and verdict.group(1).upper() == "PASS":
                        _enqueue_stage(instance_key, key, "mark_ready")
                    elif verdict and verdict.group(1).upper() == "FAIL":
                        _enqueue_stage(instance_key, key, "fix_review_findings")
                    else:
                        _enqueue_stage(instance_key, key, "start_reviewing")
                else:
                    _enqueue_stage(instance_key, key, "start_reviewing")

            if ts["status"] == "pr_ready" and config.get("pr", {}).get("auto_pr") and not ts.get("pr_scheduled_at"):
                ts = _create_pr(config, ticket, ts, base_url)

            if ts["status"] in ("pr_created", "in_review"):
                ts = _resolve_conflicts(config, ticket, ts, base_url)

            if ts["status"] in ("pr_created", "in_review"):
                platform = make_platform(config)
                result = platform.monitor_ci(ticket, ts, base_url)
                if result.get("_ci_failed"):
                    ts = _handle_ci_failure(ticket, ts, result["pr"], result["checks"], base_url, instance_key)
                else:
                    ts = result

            if ts["status"] in ("pr_created", "in_review") and ts.get("ci_passed") and config.get("pr", {}).get("auto_merge"):
                ts = _merge(config, ticket, ts, base_url)

            if ts["status"] in ("pr_created", "in_review"):
                ts = _check_in_review(config, ticket, ts, base_url)

            state.save_ticket(key, ts)
        except Exception as e:
            log.emit("ticket_check_error", f"[{key}] {type(e).__name__}: {e}",
                links={"detail": f"{base_url}/tickets/{key}"},
                meta={"ticket": key, "error": type(e).__name__})


def _fetch_tickets(config: dict) -> list[dict]:
    ts = make_ticket_system(config)
    if not ts:
        return []
    return ts.fetch_tickets()


def _setup_ticket(config, ticket, base_url, comments=None) -> dict:
    from datetime import datetime, timezone
    ws = config["workspace"]
    repos = get_repos(config)
    key = ticket["key"]
    slug = _make_slug(key, ticket["summary"])
    branch = _make_branch(config, key, ticket)

    log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key})

    any_worktree = False
    for repo in repos:
        wt_path = ticket_worktree_path(config, slug, repo["name"])
        if (wt_path / ".git").is_file():
            any_worktree = True
            subprocess.run(["git", "fetch", "origin"], cwd=str(wt_path), capture_output=True, timeout=60)
            subprocess.run(["git", "checkout", branch], cwd=str(wt_path), capture_output=True, timeout=60)
            subprocess.run(["git", "reset", "--hard", f"origin/{ws['base_branch']}"], cwd=str(wt_path), capture_output=True, timeout=60)
            subprocess.run(["git", "clean", "-fd"], cwd=str(wt_path), capture_output=True, timeout=60)
            continue
        wt_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "worktree", "prune"], cwd=str(repo["path"]), capture_output=True, timeout=60)
        subprocess.run(["git", "fetch", "origin"], cwd=str(repo["path"]), capture_output=True, timeout=60)
        branches = subprocess.run(["git", "branch", "--list"], cwd=str(repo["path"]), capture_output=True, text=True, timeout=60).stdout
        if branch not in branches.replace("* ", "").replace("  ", " ").split():
            result = subprocess.run(["git", "branch", branch, f"origin/{ws['base_branch']}"], cwd=str(repo["path"]), capture_output=True, timeout=60)
            if result.returncode != 0:
                subprocess.run(["git", "branch", branch, ws["base_branch"]], cwd=str(repo["path"]), capture_output=True, timeout=60)
        wt_result = subprocess.run(["git", "worktree", "add", str(wt_path), branch], cwd=str(repo["path"]), capture_output=True, text=True, timeout=60)
        if wt_result.returncode != 0:
            log.emit("ticket_worktree_error", f"Failed to create worktree for {slug} in {repo['name']}: {wt_result.stderr.strip()}",
                meta={"ticket": key, "repo": repo["name"]})
            continue
        any_worktree = True
        subprocess.run(["chown", "-R", "1000:1000", str(wt_path)], capture_output=True, timeout=60)
        git_dir = repo["path"] / ".git"
        if git_dir.is_dir():
            subprocess.run(["chown", "-R", "1000:1000", str(git_dir)], capture_output=True, timeout=60)

        for dep in ws.get("dep_commands", []):
            if dep["match"] == repo["name"]:
                try:
                    subprocess.run(dep["cmd"].split(), cwd=str(wt_path), capture_output=True, timeout=300)
                except FileNotFoundError:
                    pass

    if not any_worktree:
        log.emit("ticket_worktree_error", f"No worktrees created for {slug}, staying at new",
            meta={"ticket": key})
        return {"status": TicketStatus.new.value, "slug": slug, "branch": branch}

    docs_path = ws["root"] / ws["tickets_dir"] / slug / "docs"
    docs_path.mkdir(parents=True, exist_ok=True)

    md = f"# {key}: {ticket['summary']}\n\n**Status:** {ticket['status']}\n\n## Description\n\n{ticket.get('description', 'No description')}\n"

    if ticket.get("related"):
        md += "\n## Related Tickets\n\n"
        for r in ticket["related"]:
            md += f"- **{r['key']}** ({r['relation']}): {r['summary']}\n"

    if ticket.get("subtasks"):
        md += "\n## Subtasks\n\n"
        for s in ticket["subtasks"]:
            md += f"- **{s['key']}**: {s['summary']}\n"

    if ticket.get("attachments"):
        md += "\n## Attachments\n\n"
        for a in ticket["attachments"]:
            md += f"- {a['filename']} ({a['url']})\n"

    if ticket.get("project"):
        p = ticket["project"]
        (docs_path / "epic.md").write_text(f"# Epic: {p['name']}\n\n{p.get('description', '')}\n")
    elif ticket.get("parent"):
        p = ticket["parent"]
        pkey = p.get("identifier") or p.get("key", "")
        ptitle = p.get("title") or p.get("summary", "")
        pdesc = p.get("description", "")
        (docs_path / "epic.md").write_text(f"# Parent: {pkey}: {ptitle}\n\n{pdesc}\n")

    _download_attachments(config, ticket, docs_path)
    md = _localize_images(md, docs_path)
    (docs_path / "ticket.md").write_text(md)
    if comments is None:
        comments = _fetch_ticket_comments(config, key)
    _write_comments_md(docs_path, comments)
    subprocess.run(["chown", "-R", "1000:1000", str(docs_path.parent)], capture_output=True, timeout=60)

    log.emit("ticket_worktree_created", f"Workspace ready for {slug}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key, "slug": slug, "branch": branch})

    return {"status": "new", "slug": slug, "branch": branch,
            "discovered_at": datetime.now(timezone.utc).isoformat()}


def _summarize_pr_body(raw_body: str, ticket: dict) -> str:
    if not raw_body or len(raw_body) < 200:
        return raw_body
    result = run_haiku(
        f"Summarize this PR description in 3-5 plain sentences. No bullet points, no headers, no markdown formatting. "
        f"Just say what changed and why.\n\nTicket: {ticket['key']} — {ticket['summary']}\n\n{raw_body[:3000]}"
    )
    return result if result else raw_body[:500]


def _create_pr(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    repos = get_repos(config)
    slug = ts["slug"]
    branch = ts["branch"]
    ws = config["workspace"]

    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    manifest = ticket_dir / "docs" / "change-manifest.md"
    raw_body = manifest.read_text() if manifest.exists() else ticket.get("description", "")
    pr_body = _summarize_pr_body(raw_body, ticket)

    prs = []
    any_diff = False
    for repo in repos:
        wt = ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "commit", "--no-verify", "-m", f"{ticket['key']}: {ticket['summary']}"], cwd=str(wt), capture_output=True, timeout=60)

        subprocess.run(["git", "fetch", "origin", ws["base_branch"]], cwd=str(wt), capture_output=True, timeout=60)
        diff_check = subprocess.run(
            ["git", "diff", f"origin/{ws['base_branch']}..HEAD", "--stat"],
            cwd=str(wt), capture_output=True, text=True, timeout=30)
        if not diff_check.stdout.strip():
            continue
        any_diff = True

        actual_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=10).stdout.strip()
        push_branch = actual_branch or branch

        pushed = platform.push_branch(wt, push_branch)
        if not pushed["ok"]:
            log.emit("ticket_pr_error", f"Failed to push branch for {_label(ticket['key'], ts)} in {repo['name']}: {pushed.get('error', 'unknown')}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": repo["name"], "branch": branch, "error": pushed.get("error", "")})
            continue

        title = f"{ticket['key']}: {ticket['summary']}"
        result = platform.create_pr(repo["name"], wt, push_branch, title, pr_body, ws["base_branch"])

        if result.get("error"):
            err = result["error"]
            if "no changes to be pulled" in err.lower():
                continue
            log.emit("ticket_pr_error", f"Failed to create PR for {_label(ticket['key'], ts)} in {repo['name']}: {err}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": repo["name"], "error": err})
            continue

        pr_url = result.get("url", "")
        pr_id = result.get("id")
        pr_links = {"detail": f"{base_url}/tickets/{ticket['key']}"}
        if ticket.get("url"):
            pr_links["ticket"] = ticket["url"]
        if pr_url:
            pr_links["pr"] = pr_url
        log.emit("ticket_pr_created", f"PR created for {_label(ticket['key'], ts)} in {repo['name']}",
            links=pr_links, meta={"ticket": ticket["key"], "repo": repo["name"], "pr_url": pr_url})

        if pr_id:
            author = ""
            try:
                author = platform.get_pr_info(repo["name"], pr_id).get("author", "")
            except Exception:
                pass
            prs.append({"repo": repo["name"], "id": pr_id, "url": pr_url, "author": author})

    if not any_diff:
        log.emit("ticket_no_changes", f"No code changes needed for {_label(ticket['key'], ts)}, marking as merged",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        return _mark_ticket_merged(config, ticket, ts)

    if not prs:
        ts["pr_attempts"] = ts.get("pr_attempts", 0) + 1
        if ts["pr_attempts"] >= 3:
            log.emit("ticket_pr_error", f"No PRs created for {_label(ticket['key'], ts)} after {ts['pr_attempts']} attempts, giving up",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"]})
            ts["status"] = transition(ts["status"], "pr_failed")
        else:
            log.emit("ticket_pr_error", f"No PRs created for {_label(ticket['key'], ts)}, attempt {ts['pr_attempts']}/3",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"]})
        return ts

    ts["prs"] = prs
    ts["last_comment_ids"] = {f"{p['repo']}/{p['id']}": 0 for p in prs}
    ts["status"] = transition(ts["status"], "pr_created")
    return ts


def _pr_comments_path(config, slug):
    ws = config["workspace"]
    return ws["root"] / ws["tickets_dir"] / slug / "pr_comments.json"


def _load_pr_comments(config, slug) -> list[dict]:
    path = _pr_comments_path(config, slug)
    if path.exists():
        return json.loads(path.read_text())
    return []


def _save_pr_comments(config, slug, comments: list[dict]):
    path = _pr_comments_path(config, slug)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(comments, indent=2, default=str))


def _check_in_review(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    all_merged = True
    for pr in prs:
        pr_state = platform.get_pr_state(pr["repo"], pr["id"])
        if pr_state != "MERGED":
            all_merged = False
            break

    if all_merged:
        log.emit("ticket_merged", f"All PRs merged for {_label(ticket['key'], ts)}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        return _mark_ticket_merged(config, ticket, ts)

    ts["status"] = transition(ts["status"], "in_review")
    platform_name = config["job"].get("platform", "")
    if platform_name == "bitbucket":
        user_id = config.get("bitbucket", {}).get("user_account_id", "")
    elif platform_name == "github":
        user_id = config.get("github", {}).get("user", "")
    else:
        user_id = ""
    slug = ts["slug"]
    last_comment_ids = ts.get("last_comment_ids", {})
    pr_comments = _load_pr_comments(config, slug)

    for pr in prs:
        pr_key = f"{pr['repo']}/{pr['id']}"
        last_seen = last_comment_ids.get(pr_key, 0)
        comments = platform.get_pr_comments(pr["repo"], pr["id"])
        new_comments = [
            c for c in comments
            if c["id"] > last_seen and c["author_id"] != user_id and not c.get("parent_id")
        ]

        if not new_comments:
            continue

        for comment in new_comments:
            classification = run_haiku(
                f"Is this PR review comment actionable (clear code change requested) or ambiguous (vague, question, opinion)?\n\n"
                f"Comment: {comment['body']}\n\n"
                f"Reply with JSON: {{\"actionable\": true/false, \"reason\": \"brief reason\"}}"
            )
            parsed = extract_json(classification) if classification else None
            actionable = parsed.get("actionable", False) if parsed else False

            entry = {
                "id": comment["id"],
                "pr_repo": pr["repo"],
                "pr_id": pr["id"],
                "body": comment["body"],
                "path": comment.get("path"),
                "line": comment.get("line"),
                "status": "new",
                "suggested_reply": "",
            }

            if actionable:
                repos = get_repos(config)
                repo_match = next((r for r in repos if r["name"] == pr["repo"]), None)
                if repo_match:
                    wt = ticket_worktree_path(config, slug, pr["repo"])
                    if wt.is_dir():
                        subprocess.run(["git", "pull", "--rebase", "origin", ts["branch"]], cwd=str(wt), capture_output=True, timeout=60)
                        context = f"File: {comment.get('path', 'unknown')}\nLine: {comment.get('line', 'unknown')}\n\nReview comment: {comment['body']}\n\nFix this review comment."
                        run_claude_code(context, wt)
                        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
                        subprocess.run(["git", "commit", "-m", f"fix: address review comment on {comment.get('path', 'unknown')}"], cwd=str(wt), capture_output=True, timeout=60)
                        platform.push_branch(wt, ts["branch"])
                        ts.pop("ci_passed", None)
                        ts.pop("checks_started_at", None)
                        platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                        entry["status"] = "addressed"
                        log.emit("ticket_pr_comment_fixed", f"{_label(ticket['key'], ts)}: Fixed {comment['body'][:80]}",
                            links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                            meta={"ticket": ticket["key"]})
            else:
                suggested = run_haiku(
                    f"A reviewer left this comment on a PR:\n\n{comment['body']}\n\n"
                    f"Write a brief, direct reply that addresses their concern. 1-2 sentences max."
                )
                entry["status"] = "needs_reply"
                entry["suggested_reply"] = suggested or ""
                log.emit("ticket_pr_comment_needs_reply", f"{_label(ticket['key'], ts)}: Reply needed {comment['body'][:80]}",
                    links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                    meta={"ticket": ticket["key"]})

            pr_comments.append(entry)

        last_comment_ids[pr_key] = max(c["id"] for c in new_comments)

    ts["last_comment_ids"] = last_comment_ids
    _save_pr_comments(config, slug, pr_comments)
    return ts


MAX_CONFLICT_ATTEMPTS = 2


def _fetch_open_prs(config) -> list[dict]:
    platform = make_platform(config)
    try:
        return platform.list_my_open_prs()
    except Exception:
        return []


def _reconcile_prs(ts: dict, open_prs: list[dict]) -> dict:
    matches = [p for p in open_prs if p.get("branch") == ts.get("branch")]
    if not matches:
        return ts

    prior_ids = {(p["repo"], p["id"]) for p in ts.get("prs", [])}
    current_ids = {(p["repo"], p["id"]) for p in matches}
    pr_changed = prior_ids != current_ids
    status_regressed = ts["status"] in ("new", "planning", "reviewing", "pr_ready")

    ts["prs"] = matches

    if status_regressed:
        ts["status"] = "pr_created"

    if pr_changed or status_regressed:
        ts["conflict_resolution_attempts"] = 0
        ts["ci_fix_attempts"] = 0
        ts.pop("ci_passed", None)
        ts.pop("checks_started_at", None)

    return ts


def _resolve_conflicts(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    base_branch = config["workspace"].get("base_branch", "main")

    for pr in prs:
        info = platform.get_pr_info(pr["repo"], pr["id"])
        if info.get("mergeable") != "CONFLICTING":
            continue

        attempts = ts.get("conflict_resolution_attempts", 0)
        if attempts >= MAX_CONFLICT_ATTEMPTS:
            log.emit("ticket_conflict_failed", f"Conflict resolution failed for {_label(ticket['key'], ts)} PR #{pr['id']} after {attempts} attempts",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
            ts["status"] = transition(ts["status"], "pr_failed")
            return ts

        wt = ticket_worktree_path(config, ts["slug"], pr["repo"])
        if not wt.is_dir():
            continue

        result = platform.merge_base(wt, base_branch)
        if not result["ok"]:
            log.emit("ticket_conflict_failed", f"Merge failed for {_label(ticket['key'], ts)} PR #{pr['id']}: {result.get('error', '')[:100]}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"], "error": result.get("error", "")})
            ts["conflict_resolution_attempts"] = attempts + 1
            if attempts + 1 >= MAX_CONFLICT_ATTEMPTS:
                ts["status"] = transition(ts["status"], "pr_failed")
            return ts

        pushed = platform.push_branch(wt, ts["branch"])
        if not pushed["ok"]:
            log.emit("ticket_conflict_push_failed", f"Push failed for {_label(ticket['key'], ts)}: {pushed.get('error', '')[:100]}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": pr["repo"]})
            ts["conflict_resolution_attempts"] = attempts + 1
            return ts

        ts.pop("ci_passed", None)
        ts.pop("checks_started_at", None)
        ts["conflict_resolution_attempts"] = attempts + 1

        log.emit("ticket_conflict_resolved", f"Merged {base_branch} into {_label(ticket['key'], ts)} PR #{pr['id']}",
            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
            meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})

    return ts


MAX_CI_FIX_ATTEMPTS = 2


def _merge(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    all_merged = True
    for pr in prs:
        result = platform.merge_pr(pr["repo"], pr["id"])
        if result.get("error"):
            log.emit("ticket_merge_error", f"{_label(ticket['key'], ts)}: Failed to merge PR #{pr['id']} in {pr['repo']}: {result['error']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"], "error": result["error"]})
            all_merged = False
        else:
            log.emit("ticket_pr_merged", f"{_label(ticket['key'], ts)}: Merged PR #{pr['id']} in {pr['repo']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})

    if all_merged:
        ts = _mark_ticket_merged(config, ticket, ts)
    return ts


def _handle_ci_failure(ticket, ts, pr, checks, base_url, instance_key="") -> dict:
    failed_names = [c["name"] for c in checks if c["state"].upper() in ("FAILURE", "FAILED")]
    fix_attempts = ts.get("ci_fix_attempts", 0)

    if fix_attempts >= MAX_CI_FIX_ATTEMPTS:
        log.emit("ticket_checks_failed", f"CI failed for {_label(ticket['key'], ts)} after {fix_attempts} fix attempts: {', '.join(failed_names)}",
            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
            meta={"ticket": ticket["key"], "failed_checks": failed_names})
        ts["status"] = transition(ts["status"], "pr_failed")
        ts.pop("_ci_failed_pending", None)
        return ts

    if not ts.get("_ci_failed_pending"):
        ts["_ci_failed_pending"] = True
        if instance_key:
            _enqueue_stage(instance_key, ticket["key"], "fix_ci_failures")
    return ts


def _make_slug(key: str, summary: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", summary.lower()).strip("-")
    words = slug.split("-")[:7]
    slug = "-".join(words)
    return f"{key}-{slug}" if slug else key


def _make_branch(config, key: str, ticket: dict) -> str:
    ws = config["workspace"]
    prefix = ws.get("branch_prefix", "")
    slug = _make_slug(key, ticket["summary"])

    if prefix:
        branch_type = run_haiku(
            f"Ticket summary: {ticket['summary']}\nDescription: {ticket.get('description', '')[:500]}\n\n"
            "Is this a bugfix or a feature? Reply with exactly one word: bugfix or feature"
        )
        bt = "bugfix" if branch_type and "bugfix" in branch_type.lower() else "feature"
        return f"{prefix}/{bt}/{slug}"
    return slug


