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

    for comment in new_comments:
        classification = run_haiku(
            f"Is this PR review comment actionable (clear code change requested) or ambiguous (vague, question, opinion)?\n\n"
            f"Comment: {comment['body']}\n\n"
            f"Reply with JSON: {{\"actionable\": true/false, \"reason\": \"brief reason\"}}"
        )

        parsed = extract_json(classification) if classification else None
        actionable = parsed.get("actionable", False) if parsed else False
        reason = parsed.get("reason", "") if parsed else "failed to classify"

        links = {
            "pr": pr["url"],
            "detail": f"{base_url}/",
        }
        meta = {"repo": pr["repo"], "pr_id": pr["id"], "comment_id": comment["id"]}

        if actionable:
            worktree = _ensure_worktree(config, pr)
            if worktree:
                context = f"File: {comment.get('path', 'unknown')}\nLine: {comment.get('line', 'unknown')}\n\nReview comment: {comment['body']}\n\nFix this review comment."
                result = run_claude_code(context, worktree)
                if result is None:
                    log.emit("pr_comment_flagged_manual", f"Claude failed to fix: {comment['body'][:80]}", links=links, meta=meta)
                    continue
                platform.push_branch(worktree, pr["branch"])
                platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                log.emit("pr_comment_addressed", f"Fixed: {comment['body'][:80]}", links=links, meta=meta)
            else:
                log.emit("pr_comment_flagged_manual", f"Could not create worktree: {comment['body'][:80]}", links=links, meta=meta)
        else:
            log.emit("pr_comment_flagged_manual", f"Ambiguous: {reason} — {comment['body'][:80]}", links=links, meta=meta)

    if new_comments:
        seen["last_comment_id"] = max(c["id"] for c in new_comments)


def _check_ci(config, platform, pr, seen, base_url):
    checks = platform.get_pr_checks(pr["repo"], pr["id"])
    failing = [c for c in checks if c["state"] in ("FAILED", "STOPPED", "failure")]
    if not failing:
        seen.pop("ci_fix_sha", None)
        return

    worktree = _ensure_worktree(config, pr)
    if not worktree:
        return

    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(worktree), capture_output=True, text=True, timeout=10).stdout.strip()
    if seen.get("ci_fix_sha") == head:
        return

    failure_names = ", ".join(c["name"] for c in failing)
    prompt = f"CI checks are failing: {failure_names}. Investigate and fix the failures."
    result = run_claude_code(prompt, worktree)
    if result is None:
        return
    platform.push_branch(worktree, pr["branch"])
    seen["ci_fix_sha"] = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(worktree), capture_output=True, text=True, timeout=10).stdout.strip()

    log.emit("pr_ci_fix_pushed", f"Fixed failing CI: {failure_names}",
        links={"pr": pr["url"], "detail": f"{base_url}/"},
        meta={"repo": pr["repo"], "pr_id": pr["id"], "checks": failure_names})


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
