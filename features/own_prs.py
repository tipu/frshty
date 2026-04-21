import subprocess
from datetime import datetime, timezone, timedelta
from pathlib import Path

import core.log as log
import core.state as state
from core.claude_runner import run_claude_code, run_haiku, extract_json
from core.config import get_repos
from features.platforms import make_platform


def check(config: dict):
    platform = make_platform(config)
    my_prs = platform.list_my_open_prs()
    if not my_prs:
        return

    ticket_state = state.load("tickets")
    ticket_prs = set()
    for ts in ticket_state.values():
        for p in ts.get("prs", []):
            if p.get("repo") and p.get("id"):
                ticket_prs.add((p["repo"], p["id"]))

    pr_state = state.load("own_prs")
    base_url = config["_base_url"]

    for pr in my_prs:
        if (pr["repo"], pr["id"]) in ticket_prs:
            continue
        pr_key = f"{pr['repo']}/{pr['id']}"
        seen = pr_state.get(pr_key, {})

        _check_comments(config, platform, pr, seen, base_url)
        _check_ci(config, platform, pr, seen, base_url)
        _check_stale(pr, seen, base_url)

        pr_state[pr_key] = seen

    state.save("own_prs", pr_state)


def _check_comments(config, platform, pr, seen, base_url):
    comments = platform.get_pr_comments(pr["repo"], pr["id"])
    last_seen_id = seen.get("last_comment_id", 0)
    user_id = config.get("bitbucket", {}).get("user_account_id", "")

    new_comments = [c for c in comments if c["id"] > last_seen_id and c["author_id"] != user_id]
    if not new_comments:
        return

    comment_list = "\n".join(f"[{i}] {c['body'][:200]}" for i, c in enumerate(new_comments))
    batch_prompt = (
        "Classify each PR review comment as actionable (clear code change requested) or ambiguous (vague, question, opinion).\n\n"
        f"{comment_list}\n\n"
        'Reply with JSON array: [{"id": 0, "actionable": true/false, "reason": "brief reason"}, ...]'
    )
    batch_raw = run_haiku(batch_prompt, timeout=60)
    classifications = {}
    if batch_raw:
        parsed_batch = extract_json(batch_raw)
        if isinstance(parsed_batch, list):
            for item in parsed_batch:
                classifications[item.get("id", -1)] = item
        elif isinstance(parsed_batch, dict) and any(isinstance(v, list) for v in parsed_batch.values()):
            for item in next(v for v in parsed_batch.values() if isinstance(v, list)):
                classifications[item.get("id", -1)] = item

    for i, comment in enumerate(new_comments):
        cls = classifications.get(i, {})
        actionable = cls.get("actionable", False)
        reason = cls.get("reason", "failed to classify")

        links = {
            "pr": pr["url"],
            "detail": f"{base_url}/",
        }
        meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment["id"]}

        pr_ref = f"{pr['repo']}#{pr['id']}"
        if actionable:
            worktree = _ensure_worktree(config, pr)
            if worktree:
                context = f"File: {comment.get('path', 'unknown')}\nLine: {comment.get('line', 'unknown')}\n\nReview comment: {comment['body']}\n\nFix this review comment."
                result = run_claude_code(context, worktree)
                if result is None:
                    log.emit("pr_comment_flagged_manual", f"{pr_ref}: Claude failed to fix — {comment['body'][:80]}", links=links, meta=meta)
                    continue
                platform.push_branch(worktree, pr["branch"])
                platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                log.emit("pr_comment_addressed", f"{pr_ref}: Fixed — {comment['body'][:80]}", links=links, meta=meta)
            else:
                log.emit("pr_comment_flagged_manual", f"{pr_ref}: Could not create worktree — {comment['body'][:80]}", links=links, meta=meta)
        else:
            log.emit("pr_comment_flagged_manual", f"{pr_ref}: Ambiguous ({reason}) — {comment['body'][:80]}", links=links, meta=meta)

    if new_comments:
        seen["last_comment_id"] = max(c["id"] for c in new_comments)


def _check_ci(config, platform, pr, seen, base_url):
    from features.tickets import MAX_CI_FIX_ATTEMPTS
    from features.pr_ci import triage_and_fix_pr, FAILED_STATES

    checks = platform.get_pr_checks(pr["repo"], pr["id"])
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

    worktree_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "fetch", "origin", pr["branch"]], cwd=str(repo_path), capture_output=True, timeout=60)
    subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=60)
    result = subprocess.run(
        ["git", "worktree", "add", str(worktree_path), pr["branch"]],
        cwd=str(repo_path), capture_output=True, text=True, timeout=60,
    )
    if result.returncode == 0:
        return worktree_path
    return None
