import subprocess
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import core.log as log
import core.queue as q
import core.state as state
import core.comments as comments
import core.git_util as git_util
import core.branch_sync as branch_sync
from core.claude_runner import run_claude_code, run_haiku, run_sonnet, extract_json
from core.config import get_repos
from features.platforms import make_platform

RECLAIM_STALE_SECONDS = 1200
MAX_COMMENT_RETRIES = 3
NEW_COMMENT_BASELINE_HOURS = 24


def check(config: dict):
    instance_key = config["job"]["key"]
    platform = make_platform(config)
    my_prs = platform.list_my_open_prs()
    if not my_prs:
        return

    ticket_state = state.load("tickets")
    ticket_prs = {}
    for ticket_key, ts in ticket_state.items():
        for p in ts.get("prs", []):
            if p.get("repo") and p.get("id"):
                ticket_prs[(p["repo"], p["id"])] = ticket_key

    pr_state = state.load("own_prs")
    base_url = config["_base_url"]

    active_keys: set[str] = set()
    for pr in my_prs:
        if (pr["repo"], pr["id"]) in ticket_prs:
            continue
        pr_key = f"{pr['repo']}/{pr['id']}"
        active_keys.add(pr_key)
        seen = pr_state.get(pr_key, {})

        _cache_pr_metadata(platform, pr, seen)
        _check_comments(config, instance_key, platform, pr, base_url)
        _check_base_fresh(config, platform, pr, seen, base_url)
        _check_ci(config, platform, pr, seen, base_url)
        _check_stale(pr, seen, base_url)

        pr_state[pr_key] = seen

    for stale_key in [k for k in pr_state if k not in active_keys]:
        del pr_state[stale_key]

    state.save("own_prs", pr_state)


def _cache_pr_metadata(platform, pr: dict, seen: dict) -> None:
    """Persist PR display + review state into the kv blob so the manager's
    stale_own_prs aggregator has data to query without a live platform call.
    """
    seen["title"] = pr.get("title", "") or seen.get("title", "")
    seen["url"] = pr.get("url", "") or seen.get("url", "")
    seen["author"] = pr.get("author", "") or seen.get("author", "")
    if pr.get("created_on"):
        seen["created_on"] = pr["created_on"]
    if pr.get("updated_on"):
        seen["updated_on"] = pr["updated_on"]
    try:
        info = platform.get_pr_info(pr["repo"], pr["id"]) or {}
    except Exception:
        info = {}
    approvers = info.get("approvers") or []
    seen["approvers"] = approvers
    seen["review_state"] = "approved" if approvers else "pending"


def _check_comments(config, instance_key, platform, pr, base_url, ticket_key=None):
    user_id = config.get("bitbucket", {}).get("user_account_id", "")
    pr_key = f"{pr['repo']}/{pr['id']}"
    pr_ref = f"{pr['repo']}#{pr['id']}"

    platform_comments = platform.get_pr_comments(pr["repo"], pr["id"])
    by_id = {str(c["id"]): c for c in platform_comments}
    first_sight = not comments.has_comment_state(instance_key, "pr", pr_key)
    detection = comments.fetch_and_detect_comments(instance_key, platform, "pr", pr_key, platform_comments=platform_comments)
    all_to_process = [c for c in detection["new"] + detection["edited"]
                      if c.get("author_id") != user_id and not c.get("resolved")]
    if first_sight:
        all_to_process = _baseline_existing_comments(instance_key, pr_key, all_to_process)

    handled = set()
    if all_to_process:
        _process_detected_comments(config, instance_key, platform, pr, pr_ref, base_url, all_to_process, handled, ticket_key)

    _reclaim_stuck_comments(instance_key, pr, pr_key, pr_ref, base_url, by_id, user_id, handled, ticket_key)


def _baseline_existing_comments(instance_key, pr_key, candidates):
    now = datetime.now(timezone.utc)
    window = NEW_COMMENT_BASELINE_HOURS * 3600
    recent = []
    for c in candidates:
        edited_at = c.get("updated_at") or c.get("created_at")
        if _is_stale(edited_at, now, window):
            comments.mark_comment_seen(instance_key, "pr", pr_key, str(c["id"]), edited_at)
        else:
            recent.append(c)
    return recent


def _process_detected_comments(config, instance_key, platform, pr, pr_ref, base_url, all_to_process, handled, ticket_key=None):
    pr_key = f"{pr['repo']}/{pr['id']}"
    detected_meta = [
        {
            "comment_id": str(c["id"]),
            "author": c.get("author_id", ""),
            "loc": f"{c.get('path', '')}:{c.get('line', '')}".strip(":"),
            "snippet": c["body"][:120],
        }
        for c in all_to_process
    ]
    log.emit(
        "pr_comments_detected",
        f"{pr_ref}: {len(all_to_process)} new comment(s)",
        links={"pr": pr["url"], "detail": f"{base_url}/"},
        meta={"repo": pr["repo"], "pr_id": pr["id"], "count": len(all_to_process), "comments": detected_meta},
    )

    comment_list = "\n\n".join(
        f"[{i}] {c.get('path', '')}:{c.get('line', '')}\n"
        f"CODE:\n{(c.get('diff_hunk') or '(no hunk)')[:600]}\n"
        f"COMMENT: {c['body'][:300]}"
        for i, c in enumerate(all_to_process)
    )
    batch_prompt = (
        "Triage each PR review comment using the CODE it is anchored to. "
        "actionable=true when it requests a concrete code change — including terse directives "
        "grounded in the code, e.g. 'Move to global.', 'Sanitize this', 'Use X instead', "
        "'Add a test for Y', 'Rename to Z', 'This should be debug'. actionable=false ONLY for "
        "genuine open questions, opinions, or discussion with no concrete change to make. "
        "When a comment implies a specific change to the code shown, choose true.\n\n"
        f"{comment_list}\n\n"
        'Reply with JSON: {"results": [{"id": 0, "actionable": true, "reason": "brief reason"}, ...]}'
    )
    batch_raw = run_sonnet(batch_prompt, timeout=120)
    classifications = {}
    if batch_raw:
        parsed_batch = extract_json(batch_raw)
        if isinstance(parsed_batch, list):
            for item in parsed_batch:
                classifications[item.get("id", -1)] = item
        elif isinstance(parsed_batch, dict) and any(isinstance(v, list) for v in parsed_batch.values()):
            for item in next(v for v in parsed_batch.values() if isinstance(v, list)):
                classifications[item.get("id", -1)] = item

    for i, comment in enumerate(all_to_process):
        comment_id = str(comment["id"])
        edited_at = comment.get("updated_at") or comment.get("created_at")
        handled.add(comment_id)

        classified = i in classifications
        cls = classifications.get(i, {})
        actionable = cls.get("actionable", False)
        reason = cls.get("reason", "")

        links = {
            "pr": pr["url"],
            "detail": f"{base_url}/",
        }
        meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment_id}

        comments.mark_comment_processing(instance_key, "pr", pr_key, comment_id, edited_at)

        log.emit("pr_comment_registered", f"{pr_ref}: Comment registered — {comment['body'][:80]}", links=links, meta=meta)

        if not classified:
            log.emit("pr_comment_classify_failed",
                     f"{pr_ref}: Could not classify (LLM empty/unparseable), will retry — {comment['body'][:80]}",
                     links=links, meta=meta)
            comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "classification failed")
            continue

        if actionable:
            q.enqueue_job(instance_key, "fix_pr_comment", payload={"pr": pr, "comment": comment}, ticket_key=ticket_key)
            log.emit("pr_comment_queued", f"{pr_ref}: Queued fix — {comment['body'][:80]}", links=links, meta=meta)
        else:
            log.emit("pr_comment_flagged_manual", f"{pr_ref}: Ambiguous ({reason}) — {comment['body'][:80]}", links=links, meta=meta)
            comments.mark_comment_processed(instance_key, "pr", pr_key, comment_id)


def _is_stale(ts, now, seconds) -> bool:
    if not ts:
        return True
    try:
        t = datetime.fromisoformat(ts)
    except ValueError:
        return True
    if t.tzinfo is None:
        t = t.replace(tzinfo=timezone.utc)
    return (now - t).total_seconds() > seconds


def _reclaim_stuck_comments(instance_key, pr, pr_key, pr_ref, base_url, by_id, user_id, handled, ticket_key=None):
    now = datetime.now(timezone.utc)
    for row in comments.get_unprocessed_comments(instance_key, "pr", pr_key):
        comment_id = str(row["comment_id"])
        if comment_id in handled:
            continue
        comment = by_id.get(comment_id)
        if comment is None:
            comments.mark_comment_deleted(instance_key, "pr", pr_key, comment_id)
            continue
        if comment.get("author_id") == user_id:
            continue
        if comment.get("resolved"):
            comments.mark_comment_processed(instance_key, "pr", pr_key, comment_id)
            continue

        error_count = row.get("error_count") or 0
        if row.get("state") == "processing":
            if not _is_stale(row.get("last_checked_at"), now, RECLAIM_STALE_SECONDS):
                continue
            note = "orphaned mid-fix"
        elif error_count > MAX_COMMENT_RETRIES:
            continue
        else:
            note = f"retry {error_count}/{MAX_COMMENT_RETRIES}"

        links = {"pr": pr["url"], "detail": f"{base_url}/"}
        meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment_id, "reason": note}
        edited_at = comment.get("updated_at") or comment.get("created_at")
        comments.mark_comment_processing(instance_key, "pr", pr_key, comment_id, edited_at)
        q.enqueue_job(instance_key, "fix_pr_comment", payload={"pr": pr, "comment": comment}, ticket_key=ticket_key)
        log.emit("pr_comment_reclaimed", f"{pr_ref}: Re-queued ({note}) — {comment['body'][:80]}", links=links, meta=meta)


def fix_comment(config, payload) -> tuple[bool, str | None]:
    instance_key = config["job"]["key"]
    platform = make_platform(config)
    pr = payload["pr"]
    comment = payload["comment"]
    pr_key = f"{pr['repo']}/{pr['id']}"
    pr_ref = f"{pr['repo']}#{pr['id']}"
    comment_id = str(comment["id"])
    links = {"pr": pr["url"], "detail": f"{config['_base_url']}/"}
    meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment_id}

    try:
        worktree = _ensure_worktree(config, pr)
        if not worktree:
            log.emit("pr_comment_blocked", f"{pr_ref}: Could not create worktree — {comment['body'][:80]}", links=links, meta={**meta, "reason": "Could not create worktree"})
            comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "Could not create worktree")
            return False, "Could not create worktree"

        context = f"File: {comment.get('path', 'unknown')}\nLine: {comment.get('line', 'unknown')}\n\nReview comment: {comment['body']}\n\nFix this review comment."
        result = run_claude_code(context, cwd=worktree, timeout=600)
        if result is None:
            log.emit("pr_comment_blocked", f"{pr_ref}: Claude failed to fix — {comment['body'][:80]}", links=links, meta={**meta, "reason": "Claude failed to fix"})
            comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "Claude failed to fix")
            return False, "Claude failed to fix"

        log.emit("pr_comment_code_written", f"{pr_ref}: Code written — {comment['body'][:80]}", links=links, meta=meta)
        push = platform.push_branch(worktree, pr["branch"])
        if isinstance(push, dict) and not push.get("ok", True):
            log.emit("pr_comment_blocked", f"{pr_ref}: Push failed — {comment['body'][:80]}", links=links, meta={**meta, "reason": "push failed"})
            comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "push failed")
            return False, "push failed"

        resolution = platform.resolve_comment(pr["repo"], pr["id"], int(comment_id))
        if isinstance(resolution, dict) and resolution.get("status") != "resolved":
            detail = str(resolution.get("detail", ""))[:200]
            log.emit("pr_comment_blocked", f"{pr_ref}: Resolve failed — {comment['body'][:80]}", links=links, meta={**meta, "reason": "resolve failed", "detail": detail})
            comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "resolve failed")
            return False, "resolve failed"

        log.emit("pr_comment_addressed", f"{pr_ref}: Fixed & pushed — {comment['body'][:80]}", links=links, meta=meta)
        comments.mark_comment_processed(instance_key, "pr", pr_key, comment_id)
        return True, None
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        log.emit("pr_comment_error",
                 f"{pr_ref}: Unexpected error fixing comment — {reason[:120]}",
                 links=links,
                 meta={**meta, "reason": reason[:200], "traceback": traceback.format_exc()[-1500:]})
        comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, reason[:200])
        return False, reason


def _check_ci(config, platform, pr, seen, base_url):
    from features.tickets import MAX_CI_FIX_ATTEMPTS
    from features.pr_ci import triage_and_fix_pr, FAILED_STATES

    checks = platform.get_pr_checks(pr["repo"], pr["id"])
    if checks is None:
        return
    failing = [c for c in checks if c.get("state", "").upper() in FAILED_STATES]
    if not failing:
        # CI is clean on this PR — reset the per-sha dedup AND attempt counter
        # so a future failure on a new push gets a fresh budget.
        seen.pop("ci_fix_sha", None)
        seen.pop("ci_unrelated_sha", None)
        seen.pop("ci_fix_attempts", None)
        seen.pop("ci_cap_emitted", None)
        return

    worktree = _ensure_worktree(config, pr)
    if not worktree:
        return

    head = subprocess.run(["git", "rev-parse", "HEAD"],
                          cwd=str(worktree), capture_output=True, text=True, timeout=10).stdout.strip()
    # Don't re-triage the same commit we already acted on (either fixed or
    # classified as unrelated). New commits reset this.
    if seen.get("ci_fix_sha") == head or seen.get("ci_unrelated_sha") == head:
        return

    attempts = seen.get("ci_fix_attempts", 0)
    pr_ref = f"{pr['repo']}#{pr['id']}"
    pr_link = {"pr": pr["url"], "detail": f"{base_url}/"}
    meta = {"repo": pr["repo"], "pr_id": pr["id"],
            "failed_checks": [c["name"] for c in failing]}

    outcome = triage_and_fix_pr(platform, pr["repo"], pr["id"], label=pr_ref,
                                  worktree=worktree,
                                  attempts=attempts,
                                  max_attempts=MAX_CI_FIX_ATTEMPTS)
    kind = outcome["result"]
    failed_names = outcome.get("failed_names", [])

    if kind == "capped":
        if not seen.get("ci_cap_emitted"):
            log.emit("pr_checks_failed",
                     f"CI failed on {pr_ref} after {attempts} fix attempts: {', '.join(failed_names)}",
                     links=pr_link, meta=meta)
            seen["ci_cap_emitted"] = True
        return

    if kind == "unrelated":
        log.emit("pr_checks_unrelated",
                 f"CI failure on {pr_ref} not caused by our changes: {outcome.get('reason','')[:100]}",
                 links=pr_link,
                 meta={**meta, "reason": outcome.get("reason", "")})
        seen["ci_unrelated_sha"] = head
        return

    if kind == "fixed":
        platform.push_branch(worktree, pr["branch"])
        new_head = subprocess.run(["git", "rev-parse", "HEAD"],
                                    cwd=str(worktree), capture_output=True, text=True, timeout=10).stdout.strip()
        seen["ci_fix_sha"] = new_head
        seen["ci_fix_attempts"] = outcome["attempts"]
        seen.pop("ci_cap_emitted", None)
        log.emit("pr_ci_fix_sent",
                 f"{pr_ref}: Sent CI fix (attempt {outcome['attempts']}/{MAX_CI_FIX_ATTEMPTS}): {outcome.get('fix_hint','')[:80]}",
                 links=pr_link,
                 meta={**meta, "fix_hint": outcome.get("fix_hint", "")})
        return

    # haiku_empty, haiku_parse_error, fix_failed, no_failing, worktree_missing:
    # no action — we'll retry next cycle (possibly against a new sha).


def _repo_path_for(config, pr):
    for r in get_repos(config):
        if r["name"] == pr["repo"]:
            return r["path"]
    return None


def _check_base_fresh(config, platform, pr, seen, base_url):
    if not config.get("pr", {}).get("auto_update_branch"):
        return
    base_branch = pr.get("base") or config.get("workspace", {}).get("base_branch", "")
    repo_path = _repo_path_for(config, pr)
    pr_ref = f"{pr['repo']}#{pr['id']}"
    links = {"pr": pr["url"], "detail": f"{base_url}/"}
    meta = {"repo": pr["repo"], "pr_id": pr["id"], "base": base_branch}

    outcome = branch_sync.sync_branch_with_base(
        platform, repo_path, base_branch, pr["branch"], seen,
        lambda: _ensure_worktree(config, pr))
    result = outcome["result"]

    if result == "synced":
        seen.pop("ci_fix_sha", None)
        seen.pop("ci_unrelated_sha", None)
        log.emit("pr_base_synced", f"{pr_ref}: merged {base_branch} into PR branch",
                 links=links, meta=meta)
    elif result == "merge_failed" and outcome.get("capped"):
        log.emit("pr_base_sync_failed",
                 f"{pr_ref}: could not merge {base_branch} after {outcome['attempts']} attempts: {outcome.get('error', '')[:100]}",
                 links=links, meta={**meta, "error": outcome.get("error", "")})
    elif result == "push_failed":
        log.emit("pr_base_sync_push_failed",
                 f"{pr_ref}: merged {base_branch} but push failed: {outcome.get('error', '')[:100]}",
                 links=links, meta={**meta, "error": outcome.get("error", "")})


def _check_stale(pr, seen, base_url):
    created = datetime.fromisoformat(pr["created_on"].replace("Z", "+00:00"))
    if datetime.now(timezone.utc) - created > timedelta(hours=24) and not seen.get("stale_notified"):
        log.emit("pr_stale_no_review", f"PR #{pr['id']} has had no review for 24h+",
            links={"pr": pr["url"]},
            meta={"repo": pr["repo"], "pr_id": pr["id"]})
        seen["stale_notified"] = True


def _ensure_worktree(config, pr) -> Path | None:
    state_dir = config["_state_dir"]
    branch_slug = pr["branch"].replace("/", "-")
    worktree_path = state_dir / "pr_worktrees" / branch_slug

    repos = get_repos(config)
    if not repos:
        return None

    matching = [r for r in repos if r["name"] == pr["repo"]]
    if not matching:
        return None
    repo_path = matching[0]["path"]

    if (worktree_path / ".git").is_file():
        if not worktree_path.resolve().is_relative_to(state_dir.resolve()):
            return None
        subprocess.run(["git", "fetch", "origin", pr["branch"]], cwd=str(worktree_path), capture_output=True, timeout=60)
        subprocess.run(["git", "reset", "--hard", f"origin/{pr['branch']}"], cwd=str(worktree_path), capture_output=True, timeout=60)
        return worktree_path

    base_branch = config.get("workspace", {}).get("base_branch", "main")
    return git_util.add_or_reuse_worktree(repo_path, worktree_path, pr["branch"], base_branch=base_branch)
