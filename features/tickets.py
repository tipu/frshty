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
from core import external_log
from core.config import get_repos, ticket_worktree_path, resolve_env
from core.deps import run_dep_command, relink_shared_venv
from core.claude_runner import run_haiku, run_sonnet, run_claude_code, extract_json
from core.ticket_status import TicketStatus, transition
from features.platforms import make_platform
from features.ticket_systems import make_ticket_system


STATES = ["new", "planning", "reviewing", "testing", "proving", "pr_ready", "in_review", "merged"]


def _label(key: str, ts: dict) -> str:
    return ts.get("slug", key)


_VERDICT_RE = re.compile(r"^VERDICT:\s*(PASS|FAIL)\b", re.MULTILINE | re.IGNORECASE)


def emit_once(
    ts: dict,
    marker_field: str,
    event: str,
    summary: str,
    *,
    links: dict | None = None,
    meta: dict | None = None,
) -> bool:
    """Emit a log event exactly once per ticket state, guarded by a marker.

    If ts[marker_field] is truthy, this is a no-op (returns False). Otherwise
    log.emit(event, summary, ...) fires and ts[marker_field] is set to the
    current ISO timestamp. The caller is responsible for persisting ts via
    state.save_ticket() so the marker survives across poll cycles.

    Use this for any side-effect-emitting line whose semantics are "this
    event should fire once per (ticket, marker) lifetime". The recurring
    bug shape that produced ed95f08 / 8510474 / c1b2022 was exactly the
    absence of this pattern — emits fired unconditionally inside helpers
    that were called every poll cycle.
    """
    if ts.get(marker_field):
        return False
    log.emit(event, summary, links=links or {}, meta=meta or {})
    ts[marker_field] = datetime.now(timezone.utc).isoformat()
    return True


MAX_STAGE_RETRIES = 5
STAGE_RETRY_WINDOW_HOURS = 2

_LLM_BACKED_TASKS = frozenset({
    "start_planning", "start_reviewing", "fix_review_findings",
    "fix_ci_failures", "setup_prd_ticket", "fix_reported_bug",
    "address_pm_findings", "validate_merged_ticket", "resolve_conflicts",
    "plan_tests", "write_tests", "run_tests_and_fix",
    "prove",
    "generate_pr_descriptions",
})

_REPO_GATED_TASKS = frozenset({
    "setup_prd_ticket", "start_planning", "mark_ready", "create_pr",
    "plan_tests", "write_tests", "run_tests_and_fix",
    "prove",
})

_GATE_OCCUPYING_STATUSES = ("planning", "reviewing", "testing", "proving")
# When the instance auto-merges, pr_ready/in_review are SHORT-LIVED transitional
# states (PR opens, CI runs, merge fires within a poll cycle), so we extend the
# gate through them. This prevents a sibling ticket from refreshing its
# worktree off origin/main while a held ticket's branch is mid-merge — without
# this, two tickets editing the same hot-spot file race the merge and the
# second one's PR can't fast-forward. With auto_merge=false the gate intentionally
# stops at "proving" (see docstring on _repo_gate_blocked) because in_review
# can linger indefinitely waiting for a human to merge.
_GATE_OCCUPYING_AUTO_MERGE = _GATE_OCCUPYING_STATUSES + ("pr_ready", "in_review")

MAX_TEST_FIX_ATTEMPTS = 3

_TERMINAL_STATUSES = frozenset({"merged", "done"})
_BLOCKED_BY_TIMEOUT_HOURS = 24

_repo_gate_locks: dict[str, threading.Lock] = {}
_repo_gate_locks_guard = threading.Lock()


def _gate_lock_for(instance_key: str) -> threading.Lock:
    with _repo_gate_locks_guard:
        lock = _repo_gate_locks.get(instance_key)
        if lock is None:
            lock = threading.Lock()
            _repo_gate_locks[instance_key] = lock
        return lock


def _repo_gate_blocked(instance_key: str, ticket_key: str, config: dict | None = None) -> str | None:
    """Per-repo serialization gate. Returns the ticket_key of another ticket
    in the same instance whose status occupies the pipeline. Returns None if
    the gate is clear.

    Active-LLM-modifying states (planning, reviewing, testing, proving) always
    occupy the gate. pr_ready and in_review additionally occupy the gate when
    the instance is configured with auto_merge=true — those states are
    short-lived under auto_merge (PR opens → CI passes → merge fires within a
    poll cycle), and excluding them lets a sibling ticket refresh its worktree
    from origin/main while the held ticket's branch is mid-merge, which races
    the merge on shared hot-spot files.

    Without auto_merge the gate stops at "proving" because in_review can sit
    indefinitely waiting for a human to merge — extending the gate would cause
    deadlock where every new ticket accumulated in "new+slug" behind a
    permanently-in_review ticket.

    The "new but slug set" clause was removed for the same reason: it created
    mutual-blocking deadlock when multiple new tickets were discovered in a
    single scan cycle. Concurrent start_planning is prevented at runtime by
    core.tasks.tickets.start_planning's gate-lock + status re-check (only one
    ticket can transition into "planning" inside the threading.Lock), so the
    enqueue-time gate only needs to block on already-active pipeline states.
    """
    import core.db as _db
    auto_merge = bool((config or {}).get("pr", {}).get("auto_merge"))
    statuses = _GATE_OCCUPYING_AUTO_MERGE if auto_merge else _GATE_OCCUPYING_STATUSES
    rows = _db.query_all(
        "SELECT ticket_key FROM tickets"
        " WHERE instance_key=? AND ticket_key<>?"
        f"      AND status IN ({','.join('?' for _ in statuses)})"
        " ORDER BY updated_at ASC LIMIT 1",
        (instance_key, ticket_key, *statuses),
    )
    return rows[0]["ticket_key"] if rows else None


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def _dependency_blocked(instance_key: str, ticket_key: str) -> str | None:
    """Return the key of a non-terminal blocker for this ticket, or None.

    Reads ticket.data.blocked_by. Auto-clears the field if it's older than
    _BLOCKED_BY_TIMEOUT_HOURS to prevent permanent stalls from hallucinated
    dependencies. Mutates the ticket row when clearing.
    """
    try:
        ts = state.load_ticket(ticket_key) or {}
    except RuntimeError:
        return None
    blocked_by = ts.get("blocked_by") or []
    if not blocked_by:
        return None
    ranked_at = ts.get("blocked_by_ranked_at") or ""
    if ranked_at:
        ranked_dt = _parse_iso(ranked_at)
        if ranked_dt is not None:
            age_hours = (datetime.now(timezone.utc) - ranked_dt).total_seconds() / 3600.0
            if age_hours > _BLOCKED_BY_TIMEOUT_HOURS:
                ts["blocked_by"] = []
                ts["blocked_by_cleared_at"] = datetime.now(timezone.utc).isoformat()
                state.save_ticket(ticket_key, ts)
                log.emit("blocked_by_auto_cleared",
                         f"{ticket_key}: cleared stale blocked_by={blocked_by} after {age_hours:.0f}h",
                         meta={"ticket": ticket_key, "stale_blockers": list(blocked_by), "age_hours": round(age_hours, 1)})
                return None
    for blocker_key in blocked_by:
        blocker = state.load_ticket(blocker_key) or {}
        if blocker.get("status") not in _TERMINAL_STATUSES:
            return blocker_key
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
    if task_name == "start_planning":
        if _dependency_blocked(instance_key, ticket_key):
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


_RANKER_PROMPT = """You are ranking software tickets for execution order. Each ticket below shows its key, title, and brief description.

A ticket should be "blocked_by" another ONLY if the blocker is a structural foundation that must merge first — e.g., the blocker creates a service/table/schema/abstraction/migration that the dependent extends, or fixes a bug the dependent's tests would catch.

Tickets that are independent or only loosely related (same area, similar topic) must NOT be marked blocked_by — false dependencies stall work.

Tickets:
{ticket_list}

Reply with JSON only, no prose. Include ONLY tickets that have real dependencies; omit independent tickets:
{{"dependencies": [{{"key": "<dependent_key>", "blocked_by": ["<blocker_key>"], "reason": "brief"}}]}}
"""


def _rank_new_tickets(instance_key: str) -> dict:
    """Run a single haiku call to assign blocked_by on status=new tickets in this instance.

    Considers all non-terminal tickets as potential blockers (because a foundation that's
    already in planning still blocks a leaf that just arrived). Only mutates blocked_by on
    tickets currently at status=new — once a ticket leaves new, its blocked_by is frozen.

    Returns a result dict with counts for logging. Safe to no-op when nothing to rank.
    """
    import core.db as _db
    rows = _db.query_all(
        "SELECT ticket_key, status, data FROM tickets"
        " WHERE instance_key=? AND status NOT IN ('merged','done','pending_approval')"
        " ORDER BY updated_at ASC",
        (instance_key,),
    )
    if not rows:
        return {"ranked": 0, "reason": "no_non_terminal_tickets"}
    in_scope = []
    new_keys: set[str] = set()
    for r in rows:
        key = r["ticket_key"]
        try:
            data = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, TypeError):
            data = {}
        summary = (data.get("summary") or "").strip()
        description = (data.get("description") or "").strip()[:400]
        in_scope.append({"key": key, "status": r["status"], "summary": summary, "description": description})
        if r["status"] == "new":
            new_keys.add(key)
    if not new_keys:
        return {"ranked": 0, "reason": "no_new_tickets"}
    if len(in_scope) < 2:
        for key in new_keys:
            ts = state.load_ticket(key) or {}
            ts["blocked_by"] = []
            ts["blocked_by_ranked_at"] = datetime.now(timezone.utc).isoformat()
            state.save_ticket(key, ts)
        return {"ranked": len(new_keys), "reason": "single_ticket_no_blockers"}
    lines = [f"- {t['key']} [{t['status']}]: {t['summary']}\n  {t['description']}" for t in in_scope]
    prompt = _RANKER_PROMPT.format(ticket_list="\n".join(lines))
    raw = run_haiku(prompt, timeout=60)
    parsed = extract_json(raw) if raw else None
    deps_by_key: dict[str, list[str]] = {}
    if isinstance(parsed, dict):
        deps = parsed.get("dependencies") or []
        if isinstance(deps, list):
            valid_keys = {t["key"] for t in in_scope}
            for entry in deps:
                if not isinstance(entry, dict):
                    continue
                key = entry.get("key")
                blockers = entry.get("blocked_by") or []
                if key not in valid_keys or not isinstance(blockers, list):
                    continue
                filtered = [b for b in blockers if b in valid_keys and b != key]
                if filtered:
                    deps_by_key[key] = filtered
    deps_by_key = _break_cycles(deps_by_key)
    ranked_at = datetime.now(timezone.utc).isoformat()
    mutated = 0
    for key in new_keys:
        ts = state.load_ticket(key) or {}
        if ts.get("status") != "new":
            continue
        new_blockers = deps_by_key.get(key, [])
        if ts.get("blocked_by") != new_blockers or not ts.get("blocked_by_ranked_at"):
            ts["blocked_by"] = new_blockers
            ts["blocked_by_ranked_at"] = ranked_at
            state.save_ticket(key, ts)
            mutated += 1
            if new_blockers:
                log.emit("ticket_blocked_by_assigned",
                         f"{key}: blocked_by={new_blockers}",
                         meta={"ticket": key, "blocked_by": new_blockers,
                               "reason": next((e.get("reason", "") for e in (parsed or {}).get("dependencies") or []
                                               if isinstance(e, dict) and e.get("key") == key), "")})
    return {"ranked": mutated, "in_scope": len(in_scope), "new": len(new_keys), "deps": deps_by_key}


def _break_cycles(deps: dict[str, list[str]]) -> dict[str, list[str]]:
    """Remove edges that introduce cycles. Keeps the first edge seen during DFS."""
    out: dict[str, list[str]] = {}
    def has_path(start: str, target: str, seen: set) -> bool:
        if start == target:
            return True
        if start in seen:
            return False
        seen.add(start)
        for nxt in out.get(start, []):
            if has_path(nxt, target, seen):
                return True
        return False
    for key, blockers in deps.items():
        kept = []
        for b in blockers:
            if has_path(b, key, set()):
                continue
            kept.append(b)
        if kept:
            out[key] = kept
    return out


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
    with external_log.client(ticket_system or "unknown", timeout=60, follow_redirects=True) as client:
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


_logged_invalid_status_map: set[tuple[str, str, str]] = set()


def _resolve_status(config: dict, external_status: str) -> str | None:
    system = config["job"].get("ticket_system", "")
    status_map = config.get(system, {}).get("status_map", {})
    if not status_map:
        return None
    mapped = status_map.get(external_status)
    if mapped is None:
        return None
    try:
        return TicketStatus(mapped).value
    except ValueError:
        key = (system, external_status, mapped)
        if key not in _logged_invalid_status_map:
            _logged_invalid_status_map.add(key)
            valid = ", ".join(s.value for s in TicketStatus)
            log.emit("invalid_status_map_entry",
                f"status_map[{system}][{external_status!r}]={mapped!r} is not a valid TicketStatus; treating as unmapped (valid: {valid})",
                meta={"system": system, "external_status": external_status, "mapped": mapped})
        return None


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


_POST_PR_STATES = frozenset(s.lower() for s in (
    "QA", "In Review", "Ready for Release", "Ready For Release",
    "Done", "Cancelled", "Canceled", "Closed", "Released",
))


def _has_human_reopen_after(history: list[dict], merged_at: str) -> dict | None:
    if not merged_at or not history:
        return None
    for h in history:
        ts = h.get("created_at") or ""
        if not ts or ts <= merged_at:
            continue
        if not h.get("actor_email"):
            continue
        to_state = (h.get("to_state") or "").strip().lower()
        if not to_state or to_state in _POST_PR_STATES:
            continue
        return h
    return None


def _find_pre_merged_pr(config: dict, ticket: dict) -> dict | None:
    """Guard: on first discovery, check if an external PR for this ticket key already merged.

    Returns the PR dict if frshty should short-circuit to merged status. Returns None if:
    - no matching merged PR exists
    - the platform doesn't expose a merged-PR finder (e.g., Bitbucket)
    - the ticket has a post-merge human-actor state transition into an active-work state (reopen)
    """
    key = ticket["key"]
    platform = make_platform(config)
    finder = getattr(type(platform), "find_merged_pr_by_key", None)
    if finder is None:
        return None
    try:
        pr = finder(platform, key)
    except Exception as e:
        log.emit("merged_pr_guard_error", f"find_merged_pr_by_key failed for {key}: {e!r}",
                 meta={"ticket": key, "error": str(e)})
        return None
    if not isinstance(pr, dict):
        return None
    merged_at = pr.get("merged_at", "")
    if not isinstance(merged_at, str) or not merged_at:
        return None
    ticket_system = make_ticket_system(config)
    history_fn = getattr(type(ticket_system), "fetch_state_history", None)
    history: list[dict] = []
    if history_fn is not None:
        try:
            raw = history_fn(ticket_system, key)
            if isinstance(raw, list):
                history = raw
        except Exception as e:
            log.emit("merged_pr_guard_error", f"fetch_state_history failed for {key}: {e!r}",
                     meta={"ticket": key, "error": str(e)})
    reopen = _has_human_reopen_after(history, merged_at)
    if reopen:
        log.emit("merged_pr_guard_skipped_reopen",
                 f"{key}: found merged PR #{pr.get('id')} but {reopen.get('actor_email')} moved to "
                 f"{reopen.get('to_state')} after merge — treating as fresh",
                 links={"pr": pr.get("url", "")},
                 meta={"ticket": key, "pr_id": pr.get("id"), "merged_at": merged_at,
                       "reopen_actor": reopen.get("actor_email"),
                       "reopen_to_state": reopen.get("to_state"),
                       "reopen_at": reopen.get("created_at")})
        return None
    return pr


def _short_circuit_to_merged(config: dict, ticket: dict, ts: dict, pr: dict, base_url: str) -> dict:
    key = ticket["key"]
    ts.setdefault("status", TicketStatus.new.value)
    ts["status"] = transition(ts["status"], TicketStatus.merged.value)
    ts["merged_at"] = pr.get("merged_at", datetime.now(timezone.utc).isoformat())
    ts["merged_external_status"] = ticket.get("status", "") or "_merged_"
    ts["source"] = ts.get("source", _ticket_source(config))
    ts["external_status"] = ticket.get("status", "")
    ts["url"] = ticket.get("url", ts.get("url", ""))
    ts["prs"] = [{
        "id": pr.get("id"),
        "repo": pr.get("repo", ""),
        "title": pr.get("title", ""),
        "branch": pr.get("branch", ""),
        "base": pr.get("base", ""),
        "author": pr.get("author", ""),
        "url": pr.get("url", ""),
        "merged_at": pr.get("merged_at", ""),
    }]
    if not ts.get("discovered_at"):
        ts["discovered_at"] = datetime.now(timezone.utc).isoformat()
    comments_list = _fetch_ticket_comments(config, key)
    ts["merged_comment_snapshot"] = _comment_snapshot(comments_list)
    log.emit("ticket_already_merged",
             f"{key}: existing merged PR #{pr.get('id')} found at discovery — skipping fresh setup",
             links={"ticket": ticket.get("url", ""), "pr": pr.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
             meta={"ticket": key, "pr_id": pr.get("id"), "pr_branch": pr.get("branch", ""),
                   "merged_at": pr.get("merged_at", ""),
                   "merged_external_status": ts["merged_external_status"]})
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
                relink_shared_venv(config, repo["name"], wt_path)
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

    last_checked = ts.get("comments_checked_issue_updated_at", "")
    current_updated = ticket.get("updated_at", "")
    if last_checked and current_updated and current_updated == last_checked:
        return

    comments_data = _fetch_ticket_comments(config, key)
    if current_updated:
        ts["comments_checked_issue_updated_at"] = current_updated
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



def advance_ticket(config: dict, instance_key: str = "", key: str = "") -> None:
    """Run the forward stage dispatch for a single ticket immediately.

    Chains a finished stage into the next one for just this ticket, without
    waiting for the next cron_tick scan of the whole instance. Operates on
    stored state only (no external fetch) and runs only the forward status
    handlers (new..in_review); merged/validation/pr_failed reconciliation
    stays poll-owned because it depends on freshly fetched upstream PR state.
    """
    from features.ticket_states import _STATUS_HANDLERS
    if not instance_key or not key:
        return
    ts = state.load_ticket(key)
    if not ts:
        return
    status = ts.get("status", "")
    if status in (TicketStatus.done.value, "done", "ignored", "epic",
                  TicketStatus.pending_approval.value, TicketStatus.blocked.value):
        return
    if status in (TicketStatus.new.value, "new") and not ts.get("discovered_at"):
        return
    if any(j["status"] == "running" and j["task"] != "advance_ticket"
           for j in q.jobs_for_ticket(instance_key, key)):
        return
    base_url = config["_base_url"]
    ticket = {
        "key": key,
        "summary": ts.get("summary", ""),
        "description": ts.get("description", ""),
        "url": ts.get("url", ""),
        "status": ts.get("external_status", ""),
    }
    for st, handler in _STATUS_HANDLERS:
        if ts["status"] != st:
            continue
        ts, _stop = handler(config, ticket, ts, base_url, instance_key, True)
        break
    state.save_ticket(key, ts)


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

    Idempotency framework: any side-effect emit inside a handler whose
    semantics are "fire once per ticket lifetime" should be issued via
    emit_once(ts, marker_field, event, summary, ...) (defined above), which
    consults ts[marker_field] and short-circuits subsequent calls. The
    handler is responsible for picking a marker field name and persisting
    ts via state.save_ticket() (the dispatcher does the final save). The
    meta-invariant test at tests/features/test_tickets.py:TestCheckIdempotentSecondCycle
    asserts a second check() cycle produces zero new log_events for any
    ticket in any non-terminal status — that test is the structural backstop.

    Handler implementations live in features.ticket_states; the deferred
    import below avoids a circular dependency (ticket_states.py imports
    features.tickets to access private helpers via the `_t` alias, so
    test patches at `features.tickets.<symbol>` flow through to handler
    call sites).
    """
    from datetime import datetime, timezone
    from features.ticket_states import _PRE_DISPATCH_HANDLERS, _STATUS_HANDLERS
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
        if key in assigned_keys or ts.get("status") in (TicketStatus.done, "done", "ignored"):
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
            if ts.get("status") == "ignored":
                continue
            ts["external_status"] = ticket.get("status", "")
            fresh_url = ticket.get("url") or ""
            if fresh_url:
                ts["url"] = fresh_url
            if ticket.get("summary"):
                ts["summary"] = ticket["summary"]
            if ticket.get("description"):
                ts["description"] = ticket["description"]
            issue_type = ticket.get("issue_type", "")
            if issue_type:
                ts["issue_type"] = issue_type
            parent = ticket.get("parent")
            if parent and parent.get("key"):
                ts["parent_key"] = parent["key"]
                ts["parent_summary"] = parent.get("summary", "")
            if ts.get("status") == "done":
                merged_ext = ts.get("merged_external_status")
                if merged_ext and ticket.get("status", "") == merged_ext:
                    state.save_ticket(key, ts)
                    continue
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
                ts["status"] = mapped
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

    if instance_key:
        _maybe_enqueue_ranker(instance_key)


def _maybe_enqueue_ranker(instance_key: str) -> None:
    """Enqueue rank_new_tickets when any status=new ticket has never been ranked.

    Idempotent: skips if a queued/running rank_new_tickets job for this instance exists.
    """
    import core.db as _db
    unranked = _db.query_one(
        "SELECT 1 FROM tickets WHERE instance_key=? AND status='new'"
        " AND (data NOT LIKE '%blocked_by_ranked_at%' OR json_extract(data,'$.blocked_by_ranked_at') IS NULL)"
        " LIMIT 1",
        (instance_key,),
    )
    if not unranked:
        return
    pending = _db.query_one(
        "SELECT 1 FROM jobs WHERE instance_key=? AND task='rank_new_tickets'"
        " AND status IN ('queued','running') LIMIT 1",
        (instance_key,),
    )
    if pending:
        return
    q.enqueue_job(instance_key, "rank_new_tickets", ticket_key=None)


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
            relink_shared_venv(config, repo["name"], wt_path)
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
                run_dep_command(config, repo["name"], wt_path, dep["cmd"])

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
            relink_shared_venv(config, repo["name"], wt_path)
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
                run_dep_command(config, repo["name"], wt_path, dep["cmd"])

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
    pr_descriptions = ts.get("pr_descriptions") or {}

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

        per_repo = pr_descriptions.get(repo["name"]) or {}
        title = per_repo.get("title") or f"{ticket['key']}: {ticket['summary']}"
        body_for_repo = per_repo.get("description") or pr_body
        result = platform.create_pr(repo["name"], wt, push_branch, title, body_for_repo, ws["base_branch"])

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


def _draft_comment_reply(config, slug, ticket, comment, pr) -> str:
    """Draft a reply to a PR review comment grounded in the ticket, the actual
    branch code the comment points at, and the comment itself — so the reply is
    specific instead of "I'd need to see the code"."""
    ws = config["workspace"]
    ticket_md = ws["root"] / ws["tickets_dir"] / slug / "docs" / "ticket.md"
    try:
        ticket_ctx = ticket_md.read_text()[:2000]
    except OSError:
        ticket_ctx = f"{ticket.get('summary', '')}\n{ticket.get('description', '')}"[:2000]

    path = comment.get("path")
    code_ctx = "(no file path on this comment)"
    if path:
        wt = ticket_worktree_path(config, slug, pr["repo"])
        try:
            lines = (wt / path).read_text().splitlines()
            ln = comment.get("line")
            if isinstance(ln, int) and ln > 0:
                lo, hi = max(0, ln - 40), min(len(lines), ln + 20)
            else:
                lo, hi = 0, min(len(lines), 120)
            snippet = "\n".join(f"{i + 1}: {lines[i]}" for i in range(lo, hi))
            code_ctx = f"{path} ({pr['repo']}):\n{snippet}"
        except OSError:
            code_ctx = f"(could not read {path} in {pr['repo']})"

    hunk = comment.get("diff_hunk") or ""
    prompt = (
        "You are the PR author replying to a code-review comment. Write a SPECIFIC, "
        "accurate reply grounded in the ticket and the actual code below. If the comment "
        "requests or suggests a change, state clearly whether you will make it and why; if "
        "it is a question, answer it directly from the code. 2-4 sentences. Do NOT say you "
        "need more context.\n\n"
        f"TICKET:\n{ticket_ctx}\n\n"
        f"CODE:\n{code_ctx}\n\n"
        + (f"REVIEWED HUNK:\n{hunk}\n\n" if hunk else "")
        + f"REVIEWER COMMENT (on {path}:{comment.get('line')}):\n{comment['body']}\n\n"
        "Reply:"
    )
    return run_sonnet(prompt) or ""


MAX_PR_COMMENT_FIX_ATTEMPTS = 2


def _recheck_pr_failed(config, ticket, ts, base_url) -> dict:
    prs = ts.get("prs") or []
    if not prs:
        return ts
    platform = make_platform(config)
    all_merged = True
    any_open = False
    any_open_healthy = False
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
            if info.get("mergeable") != "CONFLICTING":
                any_open_healthy = True

    if all_merged:
        log.emit("ticket_pr_failed_recovered_merged",
            f"{_label(ticket['key'], ts)}: tracked PR(s) now MERGED; recovering pr_failed → merged",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts.pop("pr_failed_reason", None)
        return _mark_ticket_merged(config, ticket, ts)

    if any_open:
        if ts.get("pr_failed_reason") == "conflict_failed" and not any_open_healthy:
            return ts

        log.emit("ticket_pr_failed_recovered_open",
            f"{_label(ticket['key'], ts)}: tracked PR now OPEN; recovering pr_failed → in_review",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["status"] = transition(ts["status"], "in_review")
        ts.pop("pr_failed_reason", None)
        ts["conflict_resolution_attempts"] = 0
        ts.pop("last_conflict_error", None)
        return ts

    return ts


def _get_pr_info(platform, pr_info_map, repo: str, pr_id) -> dict:
    if pr_info_map is not None:
        return pr_info_map.get((repo, pr_id)) or {}
    try:
        return platform.get_pr_info(repo, pr_id) or {}
    except Exception:
        return {}


def _build_pr_info_map(platform, prs: list[dict]) -> dict:
    out = {}
    for pr in prs:
        key = (pr["repo"], pr["id"])
        if key in out:
            continue
        try:
            out[key] = platform.get_pr_info(pr["repo"], pr["id"]) or {}
        except Exception:
            out[key] = {}
    return out


def _check_in_review(config, ticket, ts, base_url, pr_info_map=None) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    all_merged = True
    closed_unmerged = []
    for pr in prs:
        info = _get_pr_info(platform, pr_info_map, pr["repo"], pr["id"])
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
            "Triage each PR review comment using the CODE it is anchored to. "
            "actionable=true when it requests a concrete code change — including terse "
            "directives grounded in the code, e.g. 'Move to global.', 'Sanitize this', "
            "'Use X instead', 'Add a test for Y', 'Rename to Z', 'This should be debug'. "
            "actionable=false ONLY for genuine open questions, opinions, or discussion with "
            "no concrete change to make, e.g. 'Why did we pick this approach?', 'Looks good'. "
            "When a comment implies a specific change to the code shown, choose true.\n\n"
            + "\n\n".join(
                f"[{i}] {c.get('path', '')}:{c.get('line', '')}\n"
                f"CODE:\n{(c.get('diff_hunk') or '(no hunk)')[:600]}\n"
                f"COMMENT: {c['body']}"
                for i, c in enumerate(new_comments)
            )
            + '\n\nReply with JSON: {"results":[{"i":0,"actionable":true|false}, ...]}'
        )
        batch_raw = run_sonnet(batch_prompt)
        batch_parsed = extract_json(batch_raw) if batch_raw else None
        classifications: dict[int, bool] = {}
        if isinstance(batch_parsed, dict) and isinstance(batch_parsed.get("results"), list):
            for r in batch_parsed["results"]:
                if isinstance(r, dict) and isinstance(r.get("i"), int):
                    classifications[r["i"]] = bool(r.get("actionable"))

        if new_comments and not classifications:
            log.emit("ticket_pr_comment_classify_failed",
                f"{_label(ticket['key'], ts)}: Could not classify {len(new_comments)} review comment(s) (LLM empty/unparseable), will retry",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "pr_id": pr["id"], "count": len(new_comments)})
            continue

        for idx, comment in enumerate(new_comments):
            actionable = classifications.get(idx, False)

            entry = {
                "id": comment["id"],
                "pr_repo": pr["repo"],
                "pr_id": pr["id"],
                "body": comment["body"],
                "path": comment.get("path"),
                "line": comment.get("line"),
                "diff_hunk": comment.get("diff_hunk", ""),
                "created_at": comment.get("created_at") or comment.get("created_on") or "",
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
                        from core.git_util import commit_with_hooks
                        commit = commit_with_hooks(
                            wt,
                            message=f"fix: address review comment on {comment.get('path', 'unknown')}",
                            timeout=900,
                        )
                        if fix_result and commit.returncode == 0:
                            platform.push_branch(wt, ts["branch"])
                            ts.pop("ci_passed", None)
                            ts.pop("checks_started_at", None)
                            platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                            entry["status"] = "addressed"
                            sha = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(wt), capture_output=True, text=True, timeout=30).stdout.strip()
                            stat = subprocess.run(["git", "show", "--stat", "--format=", "HEAD"], cwd=str(wt), capture_output=True, text=True, timeout=30).stdout.strip()
                            changed_files = [ln.split("|")[0].strip() for ln in stat.splitlines() if "|" in ln]
                            files_label = ", ".join(changed_files[:5]) + (f" +{len(changed_files) - 5} more" if len(changed_files) > 5 else "")
                            commit_url = pr["url"].split("/pull/")[0] + f"/commit/{sha}" if sha and "/pull/" in pr.get("url", "") else ""
                            entry["fix_commit"] = sha
                            entry["fix_files"] = changed_files
                            log.emit("ticket_pr_comment_fixed",
                                f"{_label(ticket['key'], ts)}: Fixed \"{comment['body'][:60]}\" — changed {files_label or 'files'} ({sha[:7]})",
                                links={"detail": f"{base_url}/tickets/{ticket['key']}", "commit": commit_url},
                                meta={"ticket": ticket["key"], "comment_id": comment["id"], "commit": sha, "files": changed_files})
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
                suggested = _draft_comment_reply(config, slug, ticket, comment, pr)
                entry["status"] = "needs_reply"
                entry["suggested_reply"] = suggested
                log.emit("ticket_pr_comment_needs_reply", f"{_label(ticket['key'], ts)}: Reply needed {comment['body'][:80]}",
                    links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                    meta={"ticket": ticket["key"]})

            pr_comments.append(entry)

        batch_entries = pr_comments[-len(new_comments):]
        retryable_failures = [
            e for e in batch_entries
            if e["status"] == "fix_failed" and e.get("attempts", 0) < MAX_PR_COMMENT_FIX_ATTEMPTS
        ]
        all_classified = all(i in classifications for i in range(len(new_comments)))
        if not retryable_failures and all_classified:
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


def _has_conflicting_pr(config: dict, ts: dict, pr_info_map=None) -> bool:
    prs = ts.get("prs", [])
    if not prs:
        return False
    platform = make_platform(config) if pr_info_map is None else None
    for pr in prs:
        if pr_info_map is not None:
            info = pr_info_map.get((pr["repo"], pr["id"])) or {}
        else:
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


def _resolve_conflicts(config, ticket, ts, base_url, pr_info_map=None) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    base_branch = config["workspace"].get("base_branch", "main")

    for pr in prs:
        if pr_info_map is not None:
            info = pr_info_map.get((pr["repo"], pr["id"])) or {}
        else:
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
