import subprocess
import threading
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import core.log as log
import core.queue as q
import core.state as state
import core.comments as comments
import core.git_util as git_util
import core.branch_sync as branch_sync
from core.claude_runner import run_claude_code, run_haiku, run_balanced, extract_json
from core.config import base_branch_for, get_repos
from features.platforms import make_platform

RECLAIM_STALE_SECONDS = 1200
MAX_COMMENT_RETRIES = 3
NEW_COMMENT_BASELINE_HOURS = 24
COMMENT_DEBOUNCE_MINUTES = 30


def _debounce_seconds(config) -> int:
    return int(config.get("pr", {}).get("comment_debounce_minutes", COMMENT_DEBOUNCE_MINUTES)) * 60


def _push_fix_deadline(config, seen) -> None:
    deadline = datetime.now(timezone.utc) + timedelta(seconds=_debounce_seconds(config))
    seen["fix_deadline"] = deadline.isoformat()

# _ensure_worktree resets the shared PR worktree to origin, so concurrent
# jobs on the same PR silently destroy each other's uncommitted work.
_worktree_locks: dict[str, threading.Lock] = {}
_worktree_locks_guard = threading.Lock()


def _worktree_lock(pr_key: str) -> threading.Lock:
    with _worktree_locks_guard:
        return _worktree_locks.setdefault(pr_key, threading.Lock())


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
        _check_comments(config, instance_key, platform, pr, base_url, seen=seen)
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


def _self_id(config: dict) -> str:
    """The account frshty posts as, in the id space the adapter reports.

    GitHub comments carry a login, Bitbucket comments carry an account id.
    Reading only the Bitbucket key leaves this empty on a GitHub instance, and
    the self-authored guard then matches nothing."""
    if (config.get("job") or {}).get("platform") == "github":
        return (config.get("github") or {}).get("user", "")
    return (config.get("bitbucket") or {}).get("user_account_id", "")


def _thread_key(comment: dict, by_id: dict) -> str:
    if comment.get("thread_id"):
        return str(comment["thread_id"])
    cur = comment
    walked: set[str] = set()
    while True:
        cid = str(cur["id"])
        if cid in walked:
            return cid
        walked.add(cid)
        parent_id = cur.get("parent_id")
        parent = by_id.get(str(parent_id)) if parent_id is not None else None
        if parent is None:
            return cid
        cur = parent


def _reopen_answered_threads(platform_comments: list, settled_ids: set, self_id: str = "") -> list[str]:
    """Clear the resolved flag on threads a reviewer answered after they closed.

    Both adapters stamp the thread's resolution onto every comment in it, so a
    reply written after the thread was resolved arrives with resolved=True and
    every caller drops it — at intake, at reclaim, and at flush — without ever
    writing it to comment_state. The reviewer's follow-up is then lost for good.

    A resolved thread that mixes comments frshty owes an answer with comments
    it does not is one the reviewer reopened. A resolved thread that owes an
    answer on every comment was closed by a human who wants it left alone, so
    that one keeps its flag. Only a reviewer's comment can be owed an answer:
    our own reply never enters comment_state, so counting it would reopen its
    thread on every poll forever.

    Mutates the comments in place and returns the reopened thread keys."""
    by_id = {str(c["id"]): c for c in platform_comments}
    threads: dict[str, list[dict]] = {}
    for c in platform_comments:
        if c.get("resolved"):
            threads.setdefault(_thread_key(c, by_id), []).append(c)

    reopened = []
    for thread_key, thread in threads.items():
        ids = {str(c["id"]) for c in thread}
        owed = {
            str(c["id"]) for c in thread
            if str(c["id"]) not in settled_ids and c.get("author_id") != self_id
        }
        if not owed or not (ids - owed):
            continue
        for c in thread:
            c["resolved"] = False
        reopened.append(thread_key)
    return reopened


def _check_comments(config, instance_key, platform, pr, base_url, seen=None, ticket_key=None):
    user_id = _self_id(config)
    pr_key = f"{pr['repo']}/{pr['id']}"
    pr_ref = f"{pr['repo']}#{pr['id']}"
    if seen is None:
        seen = {}

    platform_comments = platform.get_pr_comments(pr["repo"], pr["id"])
    first_sight = not comments.has_comment_state(instance_key, "pr", pr_key)
    for thread_key in _reopen_answered_threads(
            platform_comments, comments.settled_comment_ids(instance_key, "pr", pr_key), user_id):
        log.emit("pr_thread_reopened",
                 f"{pr_ref}: reviewer replied after the thread was resolved",
                 links={"pr": pr["url"], "detail": f"{base_url}/"},
                 meta={"repo": pr["repo"], "pr_id": pr["id"], "thread_id": thread_key})
    by_id = {str(c["id"]): c for c in platform_comments}
    detection = comments.fetch_and_detect_comments(instance_key, platform, "pr", pr_key, platform_comments=platform_comments)
    all_to_process = [c for c in detection["new"] + detection["edited"]
                      if c.get("author_id") != user_id and not c.get("resolved")]
    if first_sight:
        all_to_process = _baseline_existing_comments(instance_key, pr_key, all_to_process)
    # General GitHub review bodies were added to the adapter after inline
    # comment tracking had already been live. Baseline just the old bodies on
    # the first poll with the expanded source so rollout does not replay an
    # entire PR's review history. Recent bodies remain actionable, including
    # ones submitted before this version was deployed.
    if any(c.get("comment_kind") == "review_body" for c in platform_comments) \
            and not seen.get("review_bodies_baselined"):
        review_bodies = [c for c in all_to_process if c.get("comment_kind") == "review_body"]
        recent_review_bodies = _baseline_existing_comments(instance_key, pr_key, review_bodies)
        keep_ids = {str(c["id"]) for c in recent_review_bodies}
        all_to_process = [
            c for c in all_to_process
            if c.get("comment_kind") != "review_body" or str(c["id"]) in keep_ids
        ]
        seen["review_bodies_baselined"] = True

    handled = set()
    if all_to_process:
        _process_detected_comments(config, instance_key, platform, pr, pr_ref, base_url, all_to_process, handled, seen, ticket_key)

    _reclaim_stuck_comments(instance_key, pr, pr_key, pr_ref, base_url, by_id, user_id, handled, seen, ticket_key)

    _flush_deferred_comments(config, instance_key, pr, pr_key, pr_ref, base_url, by_id, seen, ticket_key)


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


def _process_detected_comments(config, instance_key, platform, pr, pr_ref, base_url, all_to_process, handled, seen, ticket_key=None):
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
    batch_raw = run_balanced(batch_prompt, timeout=120)
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
            comments.mark_comment_retryable(instance_key, "pr", pr_key, comment_id, "classification failed")
            continue

        if actionable:
            comments.mark_comment_deferred(instance_key, "pr", pr_key, comment_id, edited_at)
            _push_fix_deadline(config, seen)
            log.emit("pr_comment_deferred", f"{pr_ref}: Deferred for batch fix (window pushed to {seen['fix_deadline']}) — {comment['body'][:80]}", links=links, meta=meta)
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


def _reclaim_stuck_comments(instance_key, pr, pr_key, pr_ref, base_url, by_id, user_id, handled, seen, ticket_key=None):
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
            announced = seen.setdefault("abandoned_comments", [])
            if comment_id not in announced:
                announced.append(comment_id)
                log.emit("pr_comment_abandoned",
                         f"{pr_ref}: Gave up after {error_count} failed attempt(s) — {comment['body'][:80]}",
                         links={"pr": pr["url"], "detail": f"{base_url}/"},
                         meta={"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment_id,
                               "error_count": error_count, "last_error": row.get("last_error")})
            continue
        else:
            note = f"retry {error_count}/{MAX_COMMENT_RETRIES}"

        links = {"pr": pr["url"], "detail": f"{base_url}/"}
        meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment_id, "reason": note}
        edited_at = comment.get("updated_at") or comment.get("created_at")
        comments.mark_comment_deferred(instance_key, "pr", pr_key, comment_id, edited_at)
        # Reclaimed comments already waited a full window; flush on the next pass
        # instead of restarting the debounce clock.
        seen.setdefault("fix_deadline", datetime.now(timezone.utc).isoformat())
        log.emit("pr_comment_reclaimed", f"{pr_ref}: Deferred for batch retry ({note}) — {comment['body'][:80]}", links=links, meta=meta)


def _flush_deferred_comments(config, instance_key, pr, pr_key, pr_ref, base_url, by_id, seen, ticket_key=None):
    deferred = comments.get_deferred_comments(instance_key, "pr", pr_key)
    if not deferred:
        seen.pop("fix_deadline", None)
        return

    deadline = seen.get("fix_deadline")
    if deadline:
        try:
            if datetime.now(timezone.utc) < datetime.fromisoformat(deadline):
                return
        except ValueError:
            pass

    comment_ids = []
    for row in deferred:
        comment_id = str(row["comment_id"])
        comment = by_id.get(comment_id)
        if comment is None:
            comments.mark_comment_deleted(instance_key, "pr", pr_key, comment_id)
            continue
        if comment.get("resolved"):
            comments.mark_comment_processed(instance_key, "pr", pr_key, comment_id)
            continue
        edited_at = comment.get("updated_at") or comment.get("created_at")
        comments.mark_comment_processing(instance_key, "pr", pr_key, comment_id, edited_at)
        comment_ids.append(comment_id)

    seen.pop("fix_deadline", None)
    if not comment_ids:
        return

    q.enqueue_job(instance_key, "fix_pr_comments", payload={"pr": pr, "comment_ids": comment_ids}, ticket_key=ticket_key)
    log.emit("pr_comments_batch_queued",
             f"{pr_ref}: Debounce window closed — queued one batch fix for {len(comment_ids)} comment(s)",
             links={"pr": pr["url"], "detail": f"{base_url}/"},
             meta={"repo": pr["repo"], "pr_id": pr["id"], "comment_ids": comment_ids})


def _comment_fix_tools(worktree: Path) -> list[str]:
    scope = str(worktree.resolve())
    return [
        f"Read(/{scope}/**)",
        "Grep",
        "Glob",
        f"Edit(/{scope}/**)",
        f"Write(/{scope}/**)",
        f"MultiEdit(/{scope}/**)",
        f"NotebookEdit(/{scope}/**)",
    ]


def _commit_fix(worktree, message) -> tuple[bool, str]:
    subprocess.run(["git", "add", "-A"], cwd=str(worktree), capture_output=True, timeout=60)
    staged = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=str(worktree), capture_output=True, timeout=60).returncode != 0
    if not staged:
        return False, "no changes produced"
    commit = git_util.commit_with_hooks(worktree, message=message, timeout=900)
    if commit.returncode != 0:
        detail = (commit.stderr or commit.stdout or "").strip()[:200]
        return False, f"commit failed: {detail}"
    return True, ""


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
        with _worktree_lock(pr_key):
            worktree = _ensure_worktree(config, pr)
            if not worktree:
                log.emit("pr_comment_blocked", f"{pr_ref}: Could not create worktree — {comment['body'][:80]}", links=links, meta={**meta, "reason": "Could not create worktree"})
                comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "Could not create worktree")
                return False, "Could not create worktree"

            context = f"File: {comment.get('path', 'unknown')}\nLine: {comment.get('line', 'unknown')}\n\nReview comment: {comment['body']}\n\nFix this review comment."
            result = run_claude_code(context, cwd=worktree, timeout=600,
                                     allowed_tools=_comment_fix_tools(worktree))
            if result is None:
                reason = "Claude did not run to completion"
                log.emit("pr_comment_provider_failed", f"{pr_ref}: {reason} — {comment['body'][:80]}", links=links, meta={**meta, "reason": reason})
                comments.mark_comment_deferred(instance_key, "pr", pr_key, comment_id,
                                               comment.get("updated_at") or comment.get("created_at"))
                return False, reason

            committed, commit_reason = _commit_fix(worktree, f"fix: address review comment on {comment.get('path', 'unknown')}")
            if not committed:
                log.emit("pr_comment_blocked", f"{pr_ref}: {commit_reason} — {comment['body'][:80]}", links=links, meta={**meta, "reason": commit_reason})
                comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, commit_reason)
                return False, commit_reason

            log.emit("pr_comment_code_written", f"{pr_ref}: Code written — {comment['body'][:80]}", links=links, meta=meta)
            push = platform.push_branch(worktree, pr["branch"])
            if isinstance(push, dict) and not push.get("ok", True):
                log.emit("pr_comment_blocked", f"{pr_ref}: Push failed — {comment['body'][:80]}", links=links, meta={**meta, "reason": "push failed"})
                comments.mark_comment_error(instance_key, "pr", pr_key, comment_id, "push failed")
                return False, "push failed"

        if comment.get("resolvable", True):
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


def fix_comments_batch(config, payload) -> tuple[bool, str | None]:
    instance_key = config["job"]["key"]
    platform = make_platform(config)
    pr = payload["pr"]
    comment_ids = [str(c) for c in payload["comment_ids"]]
    pr_key = f"{pr['repo']}/{pr['id']}"
    pr_ref = f"{pr['repo']}#{pr['id']}"
    links = {"pr": pr["url"], "detail": f"{config['_base_url']}/"}
    meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_ids": comment_ids}

    def _fail_all(ids, reason, event="pr_comment_blocked"):
        log.emit(event, f"{pr_ref}: {reason} — batch of {len(ids)} comment(s)", links=links, meta={**meta, "reason": reason})
        for cid in ids:
            comments.mark_comment_error(instance_key, "pr", pr_key, cid, reason)
        return False, reason

    def _defer_all(pending_comments, reason):
        """Park a batch the model never got to, without spending its budget.

        run_claude_code returns None for a guard block, a timeout, and a
        non-zero exit alike, so None says the run did not finish — it says
        nothing about the comment. Charging that to error_count exhausts
        MAX_COMMENT_RETRIES inside one outage and abandons the comment for
        good. Deferring puts it back in the debounce pool for the next poll."""
        ids = [str(c["id"]) for c in pending_comments]
        log.emit("pr_comment_provider_failed",
                 f"{pr_ref}: {reason} — batch of {len(ids)} comment(s) held for retry",
                 links=links, meta={**meta, "reason": reason})
        for c in pending_comments:
            comments.mark_comment_deferred(instance_key, "pr", pr_key, str(c["id"]),
                                           c.get("updated_at") or c.get("created_at"))
        return False, reason

    try:
        with _worktree_lock(pr_key):
            fetched = platform.get_pr_comments(pr["repo"], pr["id"])
            _reopen_answered_threads(fetched, comments.settled_comment_ids(instance_key, "pr", pr_key),
                                     _self_id(config))
            by_id = {str(c["id"]): c for c in fetched}
            pending = []
            for cid in comment_ids:
                comment = by_id.get(cid)
                if comment is None:
                    comments.mark_comment_deleted(instance_key, "pr", pr_key, cid)
                elif comment.get("resolved"):
                    comments.mark_comment_processed(instance_key, "pr", pr_key, cid)
                else:
                    pending.append(comment)
            if not pending:
                return True, "nothing left to fix"
            pending_ids = [str(c["id"]) for c in pending]

            worktree = _ensure_worktree(config, pr)
            if not worktree:
                return _fail_all(pending_ids, "Could not create worktree")

            comment_list = "\n\n".join(
                f"[{i + 1}] File: {c.get('path', 'unknown')}\nLine: {c.get('line', 'unknown')}\nReview comment: {c['body']}"
                for i, c in enumerate(pending)
            )
            context = (
                f"The following {len(pending)} review comments were left on this PR. "
                f"Address ALL of them.\n\n{comment_list}\n\nFix every review comment above."
            )
            result = run_claude_code(context, cwd=worktree, timeout=900,
                                     allowed_tools=_comment_fix_tools(worktree))
            if result is None:
                return _defer_all(pending, "Claude did not run to completion")

            committed, commit_reason = _commit_fix(worktree, f"fix: address {len(pending)} review comments")
            if not committed:
                return _fail_all(pending_ids, commit_reason)

            log.emit("pr_comment_code_written", f"{pr_ref}: Code written for batch of {len(pending)} comment(s)", links=links, meta=meta)
            push = platform.push_branch(worktree, pr["branch"])
            if isinstance(push, dict) and not push.get("ok", True):
                return _fail_all(pending_ids, "push failed")

        unresolved = []
        pending_by_id = {str(c["id"]): c for c in pending}
        for cid in pending_ids:
            comment = pending_by_id[cid]
            if not comment.get("resolvable", True):
                comments.mark_comment_processed(instance_key, "pr", pr_key, cid)
                continue
            resolution = platform.resolve_comment(pr["repo"], pr["id"], int(cid))
            if isinstance(resolution, dict) and resolution.get("status") != "resolved":
                comments.mark_comment_error(instance_key, "pr", pr_key, cid, "resolve failed")
                unresolved.append(cid)
            else:
                comments.mark_comment_processed(instance_key, "pr", pr_key, cid)

        if unresolved:
            log.emit("pr_comment_blocked", f"{pr_ref}: Resolve failed for {len(unresolved)} of {len(pending_ids)} comment(s)",
                     links=links, meta={**meta, "reason": "resolve failed", "unresolved": unresolved})
            return False, "resolve failed"

        log.emit("pr_comment_addressed", f"{pr_ref}: Fixed & pushed batch of {len(pending_ids)} comment(s) in one commit", links=links, meta=meta)
        return True, None
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        log.emit("pr_comment_error",
                 f"{pr_ref}: Unexpected error fixing comment batch — {reason[:120]}",
                 links=links,
                 meta={**meta, "reason": reason[:200], "traceback": traceback.format_exc()[-1500:]})
        for cid in comment_ids:
            comments.mark_comment_error(instance_key, "pr", pr_key, cid, reason[:200])
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

    lock = _worktree_lock(f"{pr['repo']}/{pr['id']}")
    if not lock.acquire(blocking=False):
        return
    try:
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
    finally:
        lock.release()


def _repo_path_for(config, pr):
    for r in get_repos(config):
        if r["name"] == pr["repo"]:
            return r["path"]
    return None


def _check_base_fresh(config, platform, pr, seen, base_url):
    if not config.get("pr", {}).get("auto_update_branch"):
        return
    base_branch = pr.get("base") or base_branch_for(config, pr["repo"])
    repo_path = _repo_path_for(config, pr)
    pr_ref = f"{pr['repo']}#{pr['id']}"
    links = {"pr": pr["url"], "detail": f"{base_url}/"}
    meta = {"repo": pr["repo"], "pr_id": pr["id"], "base": base_branch}

    lock = _worktree_lock(f"{pr['repo']}/{pr['id']}")
    if not lock.acquire(blocking=False):
        return
    try:
        outcome = branch_sync.sync_branch_with_base(
            platform, repo_path, base_branch, pr["branch"], seen,
            lambda: _ensure_worktree(config, pr))
    finally:
        lock.release()
    result = outcome["result"]

    if result == "synced":
        seen.pop("ci_fix_sha", None)
        seen.pop("ci_unrelated_sha", None)
        log.emit("pr_base_synced", f"{pr_ref}: merged {base_branch} into PR branch",
                 links=links, meta=meta)
    elif result == "dirty_worktree":
        log.emit("pr_base_sync_blocked",
                 f"{pr_ref}: cannot merge {base_branch}: {outcome.get('error', '')[:160]}",
                 links=links, meta={**meta, "error": outcome.get("error", "")})
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

    base_branch = base_branch_for(config, pr["repo"])
    return git_util.add_or_reuse_worktree(repo_path, worktree_path, pr["branch"], base_branch=base_branch)
