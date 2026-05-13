import json
import re
import subprocess
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import httpx

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
import core.comments as comments
from core.config import get_repos, ticket_worktree_path, resolve_env
from core.claude_runner import run_haiku, run_claude_code, extract_json
from core.ticket_status import TicketStatus, transition
from features.platforms import make_platform
from features.ticket_systems import make_ticket_system


STATES = ["new", "planning", "reviewing", "pr_ready", "in_review", "merged"]


def _label(key: str, ts: dict) -> str:
    return ts.get("slug", key)


_VERDICT_RE = re.compile(r"^VERDICT:\s*(PASS|FAIL)\b", re.MULTILINE | re.IGNORECASE)


MAX_STAGE_RETRIES = 5
STAGE_RETRY_WINDOW_HOURS = 2

_LLM_BACKED_TASKS = frozenset({
    "start_planning", "start_reviewing", "fix_review_findings",
    "fix_ci_failures", "setup_prd_ticket", "fix_reported_bug",
    "address_pm_findings", "validate_merged_ticket", "resolve_conflicts",
})

_REPO_GATED_TASKS = frozenset({
    "setup_prd_ticket", "start_planning", "mark_ready", "create_pr",
})

_GATE_OCCUPYING_STATUSES = ("planning", "reviewing", "pr_ready", "in_review")

_repo_gate_locks: dict[str, threading.Lock] = {}
_repo_gate_locks_guard = threading.Lock()


def _gate_lock_for(instance_key: str) -> threading.Lock:
    with _repo_gate_locks_guard:
        lock = _repo_gate_locks.get(instance_key)
        if lock is None:
            lock = threading.Lock()
            _repo_gate_locks[instance_key] = lock
        return lock


def _repo_gate_blocked(instance_key: str, ticket_key: str) -> str | None:
    """Per-repo serialization gate. Returns the ticket_key of another ticket
    in the same instance that is currently materialized (slug set, source=prd)
    OR whose status occupies the pipeline (planning, reviewing, pr_ready,
    in_review). Returns None if the gate is clear.

    Includes "new but slug set" so a ticket whose worktree has been created
    via setup_prd_ticket (but whose start_planning has not yet flipped status)
    still holds the gate. Without that, a second setup_prd_ticket between
    the first ticket's setup completion and its start_planning entry would
    see no occupant and slip through.
    """
    import core.db as _db
    rows = _db.query_all(
        "SELECT ticket_key FROM tickets"
        " WHERE instance_key=? AND ticket_key<>?"
        f"      AND ( status IN ({','.join('?' for _ in _GATE_OCCUPYING_STATUSES)})"
        "             OR (status='new' AND slug IS NOT NULL AND slug<>'') )"
        " ORDER BY updated_at ASC LIMIT 1",
        (instance_key, ticket_key, *_GATE_OCCUPYING_STATUSES),
    )
    return rows[0]["ticket_key"] if rows else None


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _enqueue_stage(instance_key: str, ticket_key: str, task_name: str) -> None:
    if task_name in _LLM_BACKED_TASKS:
        from core.llm import _guard_status
        try:
            blocked, _, _ = _guard_status()
        except Exception:
            blocked = False
        if blocked:
            return
    if task_name in _REPO_GATED_TASKS:
        if _repo_gate_blocked(instance_key, ticket_key):
            return
    existing = q.jobs_for_ticket(instance_key, ticket_key, limit=max(200, MAX_STAGE_RETRIES + 1))
    if any(j["task"] == task_name and j["status"] in ("queued", "running") for j in existing):
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=STAGE_RETRY_WINDOW_HOURS)
    consecutive = 0
    for j in existing:
        if j["task"] != task_name:
            continue
        if j["status"] in ("ok", "skipped"):
            break
        if j["status"] == "failed":
            finished = _parse_iso(j.get("finished_at"))
            if finished is None or finished < cutoff:
                break
            consecutive += 1
    if consecutive >= MAX_STAGE_RETRIES:
        return
    q.enqueue_job(instance_key, task_name, ticket_key=ticket_key)


def _ticket_source(config: dict) -> str:
    return config.get("job", {}).get("ticket_system") or "manual"


def _approval_required(config: dict, source: str) -> bool:
    cfg = config.get("ticket_approval") or {}
    if not cfg.get("required"):
        return False
    sources = cfg.get("sources")
    if sources is None:
        return True
    return source in sources


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
            except Exception as e:
                log.emit("download_attachment_failed", f"Failed to download {filename}: {e}")


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
    except AttributeError as e:
        log.emit("fetch_comments_method_missing", f"Method not found: {e}", meta={"ticket": key})
        raise
    except Exception as e:
        log.emit("fetch_ticket_comments_error", f"Unexpected error fetching comments: {e}", meta={"ticket": key})
        raise


def _comment_snapshot(comments: list[dict]) -> dict:
    dates = [c["created_at"] for c in comments if c.get("created_at")]
    comment_ids = [c.get("id") for c in comments if c.get("id")]
    return {
        "count": len(comments),
        "latest_created_at": max(dates) if dates else None,
        "comment_ids": comment_ids,
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
    if not ts.get("merged_external_status"):
        ts["merged_external_status"] = ticket.get("status", "") or ts.get("external_status", "") or "_merged_"
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
                  "conflict_resolution_attempts", "last_comment_ids",
                  "comment_fix_attempts", "done_at"):
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
    if new_ts:
        for k in ("slug", "branch", "discovered_at"):
            if new_ts.get(k):
                ts[k] = new_ts[k]
        ts["status"] = new_ts.get("status", "new")
    return ts


def _is_issue_comment(body: str, ticket_summary: str, last_comments: list[dict]) -> bool:
    if not body:
        return False
    context = f"Ticket: {ticket_summary}\n\nRecent comments:\n"
    for c in last_comments[-3:]:
        context += f"- {c.get('body', '')}\n"
    context += f"\nNew comment: {body}\n\n"

    prompt = context + (
        "Does this comment report a bug, regression, or issue that needs fixing? "
        "Answer 'yes' or 'no' only. Examples of yes: 'still broken on staging', "
        "'getting timeout now', 'regression after merge'. Examples of no: 'looks good', "
        "'thanks for fixing', 'deployed successfully'."
    )
    result = run_haiku(prompt)
    if not result:
        return False
    return result.strip().lower().startswith("yes")


def _ensure_worktree(config: dict, ticket_key: str, slug: str) -> dict | None:
    ws = config["workspace"]
    repos = get_repos(config)
    base_branch = ws.get("base_branch", "main")

    created = False
    synced = False
    repos_status = {}

    for repo in repos:
        wt_path = ticket_worktree_path(config, slug, repo["name"])
        try:
            if (wt_path / ".git").is_file():
                subprocess.run(["git", "fetch", "origin"], cwd=str(wt_path), capture_output=True, timeout=60)
                result = subprocess.run(
                    ["git", "merge", f"origin/{base_branch}"],
                    cwd=str(wt_path), capture_output=True, text=True, timeout=60
                )
                if result.returncode != 0:
                    subprocess.run(["git", "merge", "--abort"], cwd=str(wt_path), capture_output=True, timeout=60)
                    subprocess.run(
                        ["git", "reset", "--hard", f"origin/{base_branch}"],
                        cwd=str(wt_path), capture_output=True, timeout=60
                    )
                    subprocess.run(["git", "clean", "-fd"], cwd=str(wt_path), capture_output=True, timeout=60)
                synced = True
            else:
                wt_path.parent.mkdir(parents=True, exist_ok=True)
                subprocess.run(["git", "worktree", "prune"], cwd=str(repo["path"]), capture_output=True, timeout=60)
                subprocess.run(["git", "fetch", "origin"], cwd=str(repo["path"]), capture_output=True, timeout=60)

                ts = state.load_ticket(ticket_key) or {}
                branch = ws.get("branch") or _make_branch(config, ticket_key, {"key": ticket_key, "summary": ts.get("summary", ticket_key)})
                branches = subprocess.run(
                    ["git", "branch", "--list"], cwd=str(repo["path"]),
                    capture_output=True, text=True, timeout=60
                ).stdout
                if branch not in branches.replace("* ", "").replace("  ", " ").split():
                    subprocess.run(
                        ["git", "branch", branch, f"origin/{base_branch}"],
                        cwd=str(repo["path"]), capture_output=True, timeout=60
                    )

                wt_result = subprocess.run(
                    ["git", "worktree", "add", str(wt_path), branch],
                    cwd=str(repo["path"]), capture_output=True, text=True, timeout=60
                )
                if wt_result.returncode != 0:
                    log.emit("worktree_creation_failed", f"Failed to create worktree for {slug} in {repo['name']}",
                        meta={"ticket": ticket_key, "repo": repo["name"]})
                    continue
                created = True
                subprocess.run(["chown", "-R", "1000:1000", str(wt_path)], capture_output=True, timeout=60)

            head = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=str(wt_path), capture_output=True, text=True, timeout=10
            ).stdout.strip()
            repos_status[repo["name"]] = {"path": wt_path, "head": head}
        except Exception as e:
            log.emit("worktree_ensure_error", f"Error ensuring worktree for {repo['name']}: {e}",
                meta={"ticket": ticket_key, "repo": repo["name"]})
            continue

    if not repos_status:
        return None

    return {
        "created": created,
        "synced": synced,
        "repos": repos_status,
    }


class _TicketCommentAdapter:
    """Adapter to make ticket comment list compatible with fetch_and_detect_comments."""
    def __init__(self, comments_list):
        self.comments_list = comments_list

    def get_ticket_comments(self, ticket_key: str = ""):
        return self.comments_list


def _process_ticket_comments(config: dict, key: str, ts: dict, ticket: dict, base_url: str, instance_key: str = "") -> None:
    if not instance_key:
        return
    if not get_repos(config):
        return

    slug = ts.get("slug")
    if not slug:
        return

    comments_data = _fetch_ticket_comments(config, key)
    if not comments_data:
        ts["ticket_comment_snapshot"] = _comment_snapshot([])
        return

    # Use stateful comment tracking for idempotency and edit detection
    detection = comments.fetch_and_detect_comments(instance_key, _TicketCommentAdapter(comments_data), "ticket", key)
    all_to_process = detection["new"] + detection["edited"]

    # Keep legacy snapshot for backward compatibility
    old_snapshot = ts.get("ticket_comment_snapshot", {})
    old_ids = set(old_snapshot.get("comment_ids", []))
    new_snapshot = _comment_snapshot(comments_data)
    new_ids = set(new_snapshot.get("comment_ids", []))
    new_comment_ids = new_ids - old_ids

    if not all_to_process:
        ts["ticket_comment_snapshot"] = new_snapshot
        return

    to_process = [c for c in all_to_process if c.get("id") in new_comment_ids]
    if not to_process:
        ts["ticket_comment_snapshot"] = new_snapshot
        return

    last_comments = sorted(comments_data, key=lambda c: c.get("created_at", ""))[-5:]

    for comment in to_process:
        comment_id = str(comment["id"])
        edited_at = comment.get("updated_at") or comment.get("created_at")
        body = comment.get("body", "")

        comments.mark_comment_processing(instance_key, "ticket", key, comment_id, edited_at)

        if not _is_issue_comment(body, ticket.get("summary", ""), last_comments):
            comments.mark_comment_processed(instance_key, "ticket", key, comment_id)
            continue

        log.emit("ticket_issue_detected", f"Issue detected in comment for {key}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
            meta={"ticket": key, "comment_id": comment_id})

        wt_result = _ensure_worktree(config, key, slug)
        if not wt_result:
            log.emit("worktree_ensure_failed", f"Failed to ensure worktree for {key}",
                meta={"ticket": key})
            comments.mark_comment_error(instance_key, "ticket", key, comment_id, "Failed to ensure worktree")
            continue

        _enqueue_stage(instance_key, key, "fix_reported_bug")
        comments.mark_comment_processed(instance_key, "ticket", key, comment_id)

    ts["ticket_comment_snapshot"] = new_snapshot


def _handle_new_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if "source" not in ts:
        ts["source"] = _ticket_source(config)
    source = ts.get("source", _ticket_source(config))
    if _approval_required(config, source) and ts.get("approval_status") not in ("approved", "rejected"):
        ts["status"] = TicketStatus.pending_approval.value
        ts["approval_status"] = "pending"
        if "discovered_at" not in ts:
            ts["discovered_at"] = datetime.now(timezone.utc).isoformat()
        if "summary" not in ts:
            ts["summary"] = ticket.get("summary", "")
        if "description" not in ts:
            ts["description"] = ticket.get("description", "")
        state.save_ticket(key, ts)
        log.emit("ticket_pending_approval",
                 f"Ticket {key} awaiting approval (source={source})",
                 links={"detail": f"{base_url}/tickets/{key}"},
                 meta={"ticket": key, "source": source})
        if instance_key and (config.get("pm_agent") or {}).get("enabled", True):
            _enqueue_stage(instance_key, key, "pm_pre_approval")
        return ts, True
    if source == "prd":
        state.save_ticket(key, ts)
        return ts, True
    if not existing:
        log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
            meta={"ticket": key})
    if not ts.get("discovered_at") and not ts.get("setup_failed_at"):
        ts = _setup_ticket(config, ticket, base_url)
    if "source" not in ts:
        ts["source"] = source
    if ts.get("discovered_at") and instance_key:
        if (config.get("pm_agent") or {}).get("enabled", True):
            _enqueue_stage(instance_key, key, "pm_pre_approval")
        _enqueue_stage(instance_key, key, "start_planning")
    return ts, False


def _handle_pending_approval_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    return ts, True


def _handle_planning_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if instance_key:
        _enqueue_stage(instance_key, ticket["key"], "start_planning")
    return ts, False


def _handle_reviewing_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if not instance_key:
        return ts, False
    key = ticket["key"]
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
    return ts, False


def _handle_pr_ready_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if config.get("pr", {}).get("auto_pr") and not ts.get("pr_scheduled_at"):
        if instance_key and _repo_gate_blocked(instance_key, key):
            state.save_ticket(key, ts)
            return ts, True
        ts = _create_pr(config, ticket, ts, base_url)
    return ts, False


def _handle_in_review_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if instance_key:
        if _resolve_conflicts_pending(instance_key, key):
            state.save_ticket(key, ts)
            return ts, True
        if _has_conflicting_pr(config, ts):
            _enqueue_stage(instance_key, key, "resolve_conflicts")
            state.save_ticket(key, ts)
            return ts, True
    else:
        ts = _resolve_conflicts(config, ticket, ts, base_url)

    if ts["status"] == "in_review":
        platform = make_platform(config)
        result = platform.monitor_ci(ticket, ts, base_url)
        if result.get("_ci_failed"):
            ts = _handle_ci_failure(ticket, ts, result["pr"], result["checks"], base_url, instance_key)
        elif result.get("_ci_stalled"):
            pr = result.get("pr") or {}
            log.emit("ticket_checks_stall_repolling",
                f"CI stalled for {_label(ticket['key'], ts)} PR #{pr.get('id', '?')}; "
                f"resetting checks window and re-polling on next cycle",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr.get("repo"), "pr_id": pr.get("id")})
            ts.pop("_ci_timeout_state", None)
            ts.pop("checks_started_at", None)
        else:
            ts = result

    if ts["status"] == "in_review" and ts.get("ci_passed") and config.get("pr", {}).get("auto_merge"):
        ts = _merge(config, ticket, ts, base_url)

    if ts["status"] == "in_review":
        ts = _check_in_review(config, ticket, ts, base_url)
    return ts, False


def _handle_merged_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    curr_ext = ticket.get("status", "")
    if not ts.get("merged_external_status"):
        ts["merged_external_status"] = curr_ext or "_merged_"
        state.save_ticket(key, ts)
        return ts, True
    if curr_ext and curr_ext != ts["merged_external_status"]:
        ts = _reingest_merged_ticket(config, ticket, ts, base_url)
        state.save_ticket(key, ts)
        if instance_key:
            _enqueue_stage(instance_key, key, "start_planning")
        return ts, True
    if instance_key:
        _enqueue_stage(instance_key, key, "validate_merged_ticket")
    return ts, True


def _handle_validation_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if instance_key:
        _enqueue_stage(instance_key, ticket["key"], "validate_merged_ticket")
    return ts, True


def _handle_pr_failed_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if ts.get("prs"):
        ts = _recheck_pr_failed(config, ticket, ts, base_url)
        state.save_ticket(key, ts)
    if ts["status"] == TicketStatus.pr_failed:
        return ts, True
    return ts, False


_PRE_DISPATCH_HANDLERS = (
    (TicketStatus.merged, _handle_merged_ticket),
    (TicketStatus.validation, _handle_validation_ticket),
    (TicketStatus.pr_failed, _handle_pr_failed_ticket),
)


_STATUS_HANDLERS = (
    ("new", _handle_new_ticket),
    ("pending_approval", _handle_pending_approval_ticket),
    ("planning", _handle_planning_ticket),
    ("reviewing", _handle_reviewing_ticket),
    ("pr_ready", _handle_pr_ready_ticket),
    ("in_review", _handle_in_review_ticket),
)


def check(config: dict, instance_key: str = ""):
    """Poll assigned tickets and dispatch each ticket to its status handler.

    Ticket state remains a JSON-serializable dict. Valid state markers used by
    this dispatcher and its handlers:
    - status: current TicketStatus value.
    - external_status: latest upstream ticket-system status text.
    - url: latest upstream ticket URL.
    - done_at: timestamp set when an unassigned ticket is marked done.
    - slug: local ticket/worktree directory slug.
    - branch: VCS branch name for ticket work.
    - source: upstream source such as jira, linear, manual, or prd.
    - approval_status: pending/approved/rejected ticket approval marker.
    - discovered_at: successful worktree/materialization timestamp.
    - setup_failed_at: failed worktree/materialization timestamp.
    - summary: cached upstream ticket summary.
    - description: cached upstream ticket description.
    - prs: tracked pull requests for the ticket branch.
    - merged_at: timestamp when all tracked PRs were marked merged.
    - merged_comment_snapshot: upstream comment snapshot captured at merge.
    - merged_external_status: upstream status observed at merge.
    - requeued_at: timestamp when a merged ticket was reopened upstream.
    - reopened_count: number of times a merged ticket was requeued.
    - last_merged_at: previous merged_at retained after requeue.
    - last_merged_comment_snapshot: previous merge comment snapshot.
    - last_merged_external_status: previous merge external status.
    - ticket_comment_snapshot: upstream ticket-comment id/date snapshot.
    - pr_scheduled_at: marker for scheduled/manual PR creation.
    - pr_attempts: failed PR creation attempt count.
    - pr_failed_reason: reason tag for pr_failed status.
    - conflict_resolution_attempts: merge-conflict fix attempt count.
    - last_conflict_error: previous conflict error passed to the resolver.
    - ci_fix_attempts: CI fix attempt count.
    - ci_passed: marker from monitor_ci that checks passed.
    - checks_started_at: CI monitoring window start timestamp.
    - _ci_failed_pending: queued/running CI-fix marker.
    - _ci_timeout_state: transient CI-stall state cleared before re-poll.
    - last_comment_ids: per-PR review-comment cursor.
    - comment_fix_attempts: per-review-comment fix attempt counts.
    - llm_sessions: per-task Claude session ids used by task workers.
    """
    from datetime import datetime, timezone
    if instance_key:
        enqueue_prd_backfill(instance_key)
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
            except Exception as e:
                log.emit("check_pr_state_failed", f"Failed to check PR state: {e}", meta={"ticket": key})
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
            fresh_url = ticket.get("url") or ""
            if fresh_url:
                ts["url"] = fresh_url
            if ts.get("status") == "done":
                ts.pop("done_at", None)
                if ts.get("prs"):
                    ts["status"] = "in_review"
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

            _process_ticket_comments(config, key, ts, ticket, base_url, instance_key)

            mapped = _resolve_status(config, ticket.get("status", ""))
            if mapped and "slug" not in ts:
                ts["slug"] = _make_slug(key, ticket["summary"])
                ts["branch"] = _make_branch(config, key, ticket)
                ts["url"] = ticket.get("url", "")
                ts["status"] = TicketStatus(mapped).value
                if mapped not in ("new", "planning", "reviewing"):
                    state.save_ticket(key, ts)
                    continue

            pre_stop = False
            for status, handler in _PRE_DISPATCH_HANDLERS:
                if ts["status"] != status:
                    continue
                ts, pre_stop = handler(config, ticket, ts, base_url, instance_key, existing)
                break
            if pre_stop:
                continue

            if ts.get("branch"):
                ts = _reconcile_prs(ts, open_prs)

            if ts["status"] in ("planning", "reviewing") and ts.get("slug"):
                ws = config["workspace"]
                ticket_dir = ws["root"] / ws["tickets_dir"] / ts["slug"]
                if not ticket_dir.is_dir():
                    log.emit("ticket_dir_rebuild",
                        f"Ticket dir missing for {key}, rebuilding",
                        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
                        meta={"ticket": key, "slug": ts["slug"], "status": ts["status"]})
                    new_ts = _setup_ticket(config, ticket, base_url)
                    for k in ("slug", "branch", "discovered_at"):
                        if new_ts.get(k):
                            ts[k] = new_ts[k]

            for status, handler in _STATUS_HANDLERS:
                if ts["status"] != status:
                    continue
                ts, stop_ticket = handler(config, ticket, ts, base_url, instance_key, existing)
                if stop_ticket:
                    break

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
        return {"status": TicketStatus.new.value, "slug": slug, "branch": branch,
                "setup_failed_at": datetime.now(timezone.utc).isoformat()}

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


def render_prd_ticket_md(ts: dict, instance_key: str | None = None,
                         ticket_key: str | None = None) -> str:
    """Deterministic markdown for docs/ticket.md from a saved PRD ticket dict.
    When the ts has no usable content, fall back to the linked prd_section
    so a downstream planner has something to work against. Section is
    located via ts['prd_section_id'] first; if absent, via the
    prd_section_ticket link table using (instance_key, ticket_key).
    Observed live: tickets created via _create_generated_ticket landed in
    planning with docs/ticket.md == '# Untitled' because their content
    fields were empty and prd_section_id had been dropped from tickets.data
    but the link in prd_section_ticket was intact."""
    summary = ts.get("summary") or ""
    description = ts.get("description") or ""
    ac = ts.get("acceptance_criteria_json") or {}
    source_text = ts.get("acceptance_criteria_source_text") or ""
    section_id = ts.get("prd_section_id")
    has_content = (summary.strip() or description.strip()
                   or (isinstance(ac, dict) and ac.get("criteria"))
                   or source_text.strip())
    if not has_content and not section_id and instance_key and ticket_key:
        try:
            link = db.query_one(
                "SELECT prd_section_id FROM prd_section_ticket "
                "WHERE instance_key=? AND ticket_key=?",
                (instance_key, ticket_key),
            )
            if link:
                section_id = link["prd_section_id"]
        except Exception as e:
            log.emit("prd_ticket_md_link_lookup_failed",
                     f"could not resolve prd_section via link table for "
                     f"{instance_key}/{ticket_key}: {e}",
                     meta={"instance_key": instance_key, "ticket_key": ticket_key})
    if not has_content and section_id:
        try:
            row = db.query_one(
                "SELECT header, content FROM prd_section WHERE id=?",
                (section_id,),
            )
        except Exception as e:
            log.emit("prd_ticket_md_section_lookup_failed",
                     f"could not hydrate empty ts from prd_section {section_id}: {e}",
                     meta={"prd_section_id": section_id})
            row = None
        if row:
            summary = row["header"]
            description = row["content"]
            source_text = f"## {row['header']}\n{row['content']}"
            log.emit("prd_ticket_md_hydrated_from_section",
                     f"hydrated empty PRD ticket data from prd_section {section_id} "
                     f"({len(row['content'])} chars)",
                     meta={"prd_section_id": section_id,
                           "ticket_key": ticket_key})
    title = summary or "Untitled"
    parts: list[str] = [f"# {title}", ""]
    if description:
        parts += ["## Description", "", description, ""]
    criteria = (ac.get("criteria") or []) if isinstance(ac, dict) else []
    if criteria:
        parts += ["## Acceptance Criteria", ""]
        for i, c in enumerate(criteria, 1):
            parts.append(f"{i}. {c.get('criterion', '').strip()}")
            for step in c.get("playwright") or []:
                parts.append(f"    - playwright: {step}")
            for t in c.get("tests_required") or []:
                parts.append(f"    - test: {t}")
        parts.append("")
    if source_text:
        parts += ["## Source PRD section", "", source_text.rstrip(), ""]
    return "\n".join(parts)


def materialize_prd_ticket(config: dict, ticket_key: str, ts: dict, base_url: str,
                           instance_key: str | None = None) -> dict:
    """Create slug + branch + per-repo worktree(s) + docs/ticket.md for an approved
    PRD ticket. Returns updated ts dict (caller saves). Raises RuntimeError if no
    worktree could be created, so the caller's task fails and the sweep retries."""
    from datetime import datetime, timezone
    summary = ts.get("summary") or ticket_key
    description = ts.get("description") or ""
    synthetic = {"key": ticket_key, "summary": summary, "description": description,
                 "status": ts.get("status", "new"), "url": ""}
    ws = config["workspace"]
    repos = get_repos(config)
    slug = _make_slug(ticket_key, summary)
    branch = _make_branch(config, ticket_key, synthetic)

    log.emit("prd_ticket_setup_started",
             f"Materializing PRD ticket {ticket_key}",
             links={"detail": f"{base_url}/tickets/{ticket_key}"},
             meta={"ticket": ticket_key, "slug": slug})

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
        existing_branches = subprocess.run(["git", "branch", "--list"], cwd=str(repo["path"]),
                                            capture_output=True, text=True, timeout=60).stdout
        if branch not in existing_branches.replace("* ", "").replace("  ", " ").split():
            res = subprocess.run(["git", "branch", branch, f"origin/{ws['base_branch']}"],
                                 cwd=str(repo["path"]), capture_output=True, timeout=60)
            if res.returncode != 0:
                subprocess.run(["git", "branch", branch, ws["base_branch"]],
                               cwd=str(repo["path"]), capture_output=True, timeout=60)
        wt_result = subprocess.run(["git", "worktree", "add", str(wt_path), branch],
                                   cwd=str(repo["path"]), capture_output=True, text=True, timeout=60)
        if wt_result.returncode != 0:
            log.emit("prd_ticket_worktree_error",
                     f"Failed to create worktree for {slug} in {repo['name']}: {wt_result.stderr.strip()}",
                     meta={"ticket": ticket_key, "repo": repo["name"]})
            continue
        any_worktree = True
        subprocess.run(["chown", "-R", "1000:1000", str(wt_path)], capture_output=True, timeout=60)
        for dep in ws.get("dep_commands", []):
            if dep["match"] == repo["name"]:
                try:
                    subprocess.run(dep["cmd"].split(), cwd=str(wt_path),
                                   capture_output=True, timeout=300)
                except FileNotFoundError:
                    pass

    if not any_worktree:
        raise RuntimeError(f"no worktrees created for {slug}")

    docs_path = ws["root"] / ws["tickets_dir"] / slug / "docs"
    docs_path.mkdir(parents=True, exist_ok=True)
    (docs_path / "ticket.md").write_text(
        render_prd_ticket_md(ts, instance_key=instance_key, ticket_key=ticket_key)
    )
    subprocess.run(["chown", "-R", "1000:1000", str(docs_path.parent)], capture_output=True, timeout=60)

    out = dict(ts)
    out["slug"] = slug
    out["branch"] = branch
    out.setdefault("discovered_at", datetime.now(timezone.utc).isoformat())

    log.emit("prd_ticket_ready",
             f"Workspace ready for PRD ticket {slug}",
             links={"detail": f"{base_url}/tickets/{ticket_key}"},
             meta={"ticket": ticket_key, "slug": slug, "branch": branch})
    return out


def enqueue_prd_backfill(instance_key: str) -> list[str]:
    """Find approved PRD tickets with no slug; enqueue setup_prd_ticket per ticket.
    Dedupes via _enqueue_stage. Returns list of newly-considered ticket keys."""
    import core.db as _db
    rows = _db.query_all(
        "SELECT ticket_key FROM tickets"
        " WHERE instance_key=? AND source='prd' AND status='new'"
        "       AND approval_status='approved'"
        "       AND (slug IS NULL OR slug='')"
        " ORDER BY ticket_key",
        (instance_key,),
    )
    enqueued: list[str] = []
    for r in rows:
        key = r["ticket_key"]
        _enqueue_stage(instance_key, key, "setup_prd_ticket")
        enqueued.append(key)
    return enqueued


def _summarize_pr_body(raw_body: str, ticket: dict) -> str:
    if not raw_body or len(raw_body) < 200:
        return f"Implemented {ticket['key']}: {ticket.get('summary', '')}"
    result = run_haiku(
        f"Summarize this PR description in 3-5 plain sentences. No bullet points, no headers, no markdown formatting. "
        f"Just say what changed and why.\n\nTicket: {ticket['key']} — {ticket['summary']}\n\n{raw_body[:3000]}"
    )
    return result if result and len(result) > 10 else f"Implemented {ticket['key']}: {ticket.get('summary', '')}"


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
            except Exception as e:
                log.emit("get_pr_info_failed", f"Failed to get PR info: {e}", meta={"repo": repo["name"], "pr_id": pr_id})
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
            ts["pr_failed_reason"] = "create_failed"
        else:
            log.emit("ticket_pr_error", f"No PRs created for {_label(ticket['key'], ts)}, attempt {ts['pr_attempts']}/3",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"]})
        return ts

    ts["prs"] = prs
    ts["last_comment_ids"] = {f"{p['repo']}/{p['id']}": 0 for p in prs}
    ts["status"] = transition(ts["status"], "in_review")
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


MAX_PR_COMMENT_FIX_ATTEMPTS = 2


def _recheck_pr_failed(config, ticket, ts, base_url) -> dict:
    prs = ts.get("prs") or []
    if not prs:
        return ts
    platform = make_platform(config)
    all_merged = True
    any_open = False
    for pr in prs:
        try:
            info = platform.get_pr_info(pr["repo"], pr["id"]) or {}
        except Exception:
            return ts
        pr_state = info.get("state", "")
        if pr_state == "MERGED":
            continue
        all_merged = False
        if pr_state == "OPEN":
            any_open = True

    if all_merged:
        log.emit("ticket_pr_failed_recovered_merged",
            f"{_label(ticket['key'], ts)}: tracked PR(s) now MERGED; recovering pr_failed → merged",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts.pop("pr_failed_reason", None)
        return _mark_ticket_merged(config, ticket, ts)

    if any_open:
        log.emit("ticket_pr_failed_recovered_open",
            f"{_label(ticket['key'], ts)}: tracked PR now OPEN; recovering pr_failed → in_review",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["status"] = transition(ts["status"], "in_review")
        ts.pop("pr_failed_reason", None)
        return ts

    return ts


def _check_in_review(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    all_merged = True
    closed_unmerged = []
    for pr in prs:
        try:
            info = platform.get_pr_info(pr["repo"], pr["id"]) or {}
        except Exception:
            info = {}
        pr_state = info.get("state", "OPEN")
        pr["approvers"] = info.get("approvers") or []
        pr["approvers_checked_at"] = datetime.now(timezone.utc).isoformat()
        if pr_state == "MERGED":
            continue
        all_merged = False
        if pr_state in ("CLOSED", "DECLINED", "DELETED"):
            closed_unmerged.append((pr, pr_state))

    if all_merged:
        log.emit("ticket_merged", f"All PRs merged for {_label(ticket['key'], ts)}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        return _mark_ticket_merged(config, ticket, ts)

    if closed_unmerged:
        for pr, pr_state in closed_unmerged:
            log.emit("ticket_pr_closed_unmerged",
                f"{_label(ticket['key'], ts)}: PR #{pr['id']} in {pr['repo']} is {pr_state} without merge; parking ticket at pr_failed",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"], "pr_state": pr_state})
        ts["status"] = transition(ts["status"], "pr_failed")
        ts["pr_failed_reason"] = "pr_rejected"
        return ts

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
    comment_fix_attempts = ts.setdefault("comment_fix_attempts", {})
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

        batch_prompt = (
            "Classify each PR review comment as actionable (clear code change requested) "
            "or ambiguous (vague, question, opinion).\n\n"
            "Comments:\n"
            + "\n".join(f"[{i}] {c['body']}" for i, c in enumerate(new_comments))
            + '\n\nReply with JSON: {"results":[{"i":0,"actionable":true|false}, ...]}'
        )
        batch_raw = run_haiku(batch_prompt)
        batch_parsed = extract_json(batch_raw) if batch_raw else None
        classifications: dict[int, bool] = {}
        if isinstance(batch_parsed, dict) and isinstance(batch_parsed.get("results"), list):
            for r in batch_parsed["results"]:
                if isinstance(r, dict) and isinstance(r.get("i"), int):
                    classifications[r["i"]] = bool(r.get("actionable"))

        for idx, comment in enumerate(new_comments):
            actionable = classifications.get(idx, False)

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
                        fix_result = run_claude_code(context, cwd=wt)
                        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
                        commit = subprocess.run(["git", "commit", "-m", f"fix: address review comment on {comment.get('path', 'unknown')}"], cwd=str(wt), capture_output=True, timeout=60)
                        if fix_result and commit.returncode == 0:
                            platform.push_branch(wt, ts["branch"])
                            ts.pop("ci_passed", None)
                            ts.pop("checks_started_at", None)
                            platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                            entry["status"] = "addressed"
                            log.emit("ticket_pr_comment_fixed", f"{_label(ticket['key'], ts)}: Fixed {comment['body'][:80]}",
                                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                                meta={"ticket": ticket["key"]})
                        else:
                            entry["status"] = "fix_failed"
                            attempt_key = f"{pr_key}/{comment['id']}"
                            attempts = comment_fix_attempts.get(attempt_key, 0) + 1
                            comment_fix_attempts[attempt_key] = attempts
                            entry["attempts"] = attempts
                            log.emit("ticket_pr_comment_fix_failed",
                                f"{_label(ticket['key'], ts)}: No code change produced for {comment['body'][:80]}",
                                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                                meta={"ticket": ticket["key"],
                                      "claude_returned": bool(fix_result),
                                      "commit_rc": commit.returncode,
                                      "attempts": attempts,
                                      "max_attempts": MAX_PR_COMMENT_FIX_ATTEMPTS})
                            if attempts >= MAX_PR_COMMENT_FIX_ATTEMPTS:
                                log.emit("ticket_pr_comment_fix_capped",
                                    f"{_label(ticket['key'], ts)}: Giving up on review comment after {attempts} attempts: {comment['body'][:80]}",
                                    links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                                    meta={"ticket": ticket["key"],
                                          "comment_id": comment["id"],
                                          "attempts": attempts})
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

        batch_entries = pr_comments[-len(new_comments):]
        retryable_failures = [
            e for e in batch_entries
            if e["status"] == "fix_failed" and e.get("attempts", 0) < MAX_PR_COMMENT_FIX_ATTEMPTS
        ]
        if not retryable_failures:
            last_comment_ids[pr_key] = max(c["id"] for c in new_comments)

    ts["last_comment_ids"] = last_comment_ids
    _save_pr_comments(config, slug, pr_comments)
    return ts


MAX_CONFLICT_ATTEMPTS = 2


def _fetch_open_prs(config) -> list[dict]:
    platform = make_platform(config)
    try:
        return platform.list_my_open_prs()
    except Exception as e:
        log.emit("fetch_open_prs_failed", f"Failed to fetch open PRs: {e}")
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
        ts["status"] = "in_review"

    if pr_changed or status_regressed:
        ts["conflict_resolution_attempts"] = 0
        ts["ci_fix_attempts"] = 0
        ts.pop("ci_passed", None)
        ts.pop("checks_started_at", None)

    return ts


def _has_conflicting_pr(config: dict, ts: dict) -> bool:
    prs = ts.get("prs", [])
    if not prs:
        return False
    platform = make_platform(config)
    for pr in prs:
        try:
            info = platform.get_pr_info(pr["repo"], pr["id"])
        except Exception as e:
            log.emit("conflict_check_failed",
                f"get_pr_info failed for {pr['repo']}#{pr['id']}: {e}",
                meta={"repo": pr["repo"], "pr_id": pr["id"]})
            continue
        if info.get("mergeable") == "CONFLICTING":
            return True
    return False


def _resolve_conflicts_pending(instance_key: str, ticket_key: str) -> bool:
    if not instance_key:
        return False
    return any(
        j["task"] == "resolve_conflicts" and j["status"] in ("queued", "running")
        for j in q.jobs_for_ticket(instance_key, ticket_key)
    )


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
            ts["pr_failed_reason"] = "conflict_failed"
            return ts

        wt = ticket_worktree_path(config, ts["slug"], pr["repo"])
        if not wt.is_dir():
            continue

        prev_error = ts.get("last_conflict_error")
        result = platform.merge_base(wt, base_branch, prev_error=prev_error)
        if not result["ok"]:
            error = result.get("error", "")
            log.emit("ticket_conflict_failed", f"Merge failed for {_label(ticket['key'], ts)} PR #{pr['id']}: {error[:100]}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"], "error": error})
            ts["conflict_resolution_attempts"] = attempts + 1
            ts["last_conflict_error"] = error
            if attempts + 1 >= MAX_CONFLICT_ATTEMPTS:
                ts["status"] = transition(ts["status"], "pr_failed")
                ts["pr_failed_reason"] = "conflict_failed"
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
        ts.pop("last_conflict_error", None)
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
        ts["pr_failed_reason"] = "ci_failed"
        ts.pop("_ci_failed_pending", None)
        return ts

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
    summary = ticket.get("summary", key)
    slug = _make_slug(key, summary)

    if prefix:
        branch_type = run_haiku(
            f"Ticket summary: {summary}\nDescription: {ticket.get('description', '')[:500]}\n\n"
            "Is this a bugfix or a feature? Reply with exactly one word: bugfix or feature"
        )
        bt = "bugfix" if branch_type and "bugfix" in branch_type.lower() else "feature"
        return f"{prefix}/{bt}/{slug}"
    return slug
