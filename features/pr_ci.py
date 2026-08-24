"""Shared CI-failure triage+fix for any PR, regardless of origin.

Both the ticket pipeline (frshty-created PRs) and own_prs (user-opened PRs)
call through triage_and_fix_pr so behavior is identical: causality check via
Haiku first, fix via headless claude only if the failure is caused by our
changes, attempt cap enforced by the caller.
"""
import json
import re
from pathlib import Path

from core.claude_runner import run_balanced, run_claude_code, extract_json


FIX_TIMEOUT = 1800
# Single source of truth for the CI failure set. _CIMonitorMixin._evaluate_checks
# in features/platforms.py imports this — if the sets ever diverge again,
# monitor_ci flags a PR as CI-failed and enqueues fix_ci_failures, but triage
# sees no "failed" checks (no_failing), never fixes or counts an attempt, and
# the ticket loops in_review forever holding the repo gate.
# CANCELLED/TIMED_OUT are real failures GH reports; STOPPED is Bitbucket's.
FAILED_STATES = ("FAILURE", "FAILED", "STOPPED", "CANCELLED", "TIMED_OUT")
PENDING_STATES = ("PENDING", "QUEUED", "IN_PROGRESS", "INPROGRESS", "WAITING", "REQUESTED", "EXPECTED")


def ci_summary(checks) -> str:
    """Collapse a get_pr_checks() result into one display state for the PR board:
    'unknown' (fetch failed), 'none' (no checks), 'failing', 'pending', 'passing'."""
    if checks is None:
        return "unknown"
    if not checks:
        return "none"
    states = [c.get("state", "").upper() for c in checks]
    if any(s in FAILED_STATES for s in states):
        return "failing"
    if any(s in PENDING_STATES for s in states):
        return "pending"
    return "passing"


def triage_and_fix_pr(platform, repo: str, pr_id: int, label: str,
                       worktree: Path | None, attempts: int, max_attempts: int) -> dict:
    """Return a result dict describing what (if anything) happened:
        {"result": "no_failing" | "capped" | "worktree_missing" |
                    "haiku_empty" | "haiku_parse_error" |
                    "unrelated" | "fix_failed" | "fixed",
         "failed_names": [...], "attempts": int,
         "reason": str (when unrelated), "fix_hint": str (when fixed)}
    Caller owns logging and state mutation based on result.
    """
    checks = platform.get_pr_checks(repo, pr_id) or []
    failed_names = [c["name"] for c in checks if c.get("state", "").upper() in FAILED_STATES]
    if not failed_names:
        return {"result": "no_failing", "attempts": attempts, "failed_names": []}

    if attempts >= max_attempts:
        return {"result": "capped", "attempts": attempts, "failed_names": failed_names}

    if worktree is None or not Path(worktree).is_dir():
        return {"result": "worktree_missing", "attempts": attempts, "failed_names": failed_names}

    failure_logs = platform.get_failed_logs(repo, pr_id)
    if not isinstance(failure_logs, str):
        failure_logs = ""
    pr_diff = platform.get_pr_diff(repo, pr_id)
    if not isinstance(pr_diff, str):
        pr_diff = ""
    changed_files = re.findall(r"^diff --git a/.* b/(.*)$", pr_diff, flags=re.MULTILINE)
    diff_note = f" (truncated: first 4000 of {len(pr_diff)} chars)" if len(pr_diff) > 4000 else ""
    logs_note = f" (truncated: first 4000 of {len(failure_logs)} chars)" if len(failure_logs) > 4000 else ""
    causality_prompt = (
        "CI checks failed on a PR. Determine if this is caused by the changes in the PR "
        "or is pre-existing/unrelated.\n\n"
        f"PR: {label}\n"
        f"Failed checks: {', '.join(failed_names)}\n"
        f"Fix attempt: {attempts + 1}/{max_attempts}\n\n"
        f"All {len(changed_files)} changed files in the PR:\n"
        + "\n".join(f"  {f}" for f in changed_files[:200]) + "\n\n"
        f"PR diff{diff_note}:\n{pr_diff[:4000]}\n\n"
        f"Failure logs{logs_note}:\n{failure_logs[:4000]}\n\n"
        "Analyze causality:\n"
        "1. Could the diff have caused these failures? Consider both direct changes and indirect effects. "
        "Judge from the full changed-file list, not only the diff excerpt — the excerpt may cover "
        "only the first files.\n"
        "2. Or are these pre-existing failures, flaky tests, or infra issues unrelated to the changes?\n\n"
        "Reply with EXACTLY one JSON object:\n"
        '{"caused_by_us": true/false, "reason": "brief explanation", "fix_hint": "what to change if caused_by_us"}'
    )
    classification = run_balanced(causality_prompt, timeout=180)
    if not classification:
        return {"result": "haiku_empty", "attempts": attempts, "failed_names": failed_names}
    try:
        analysis = extract_json(classification) or json.loads(classification.strip())
    except (json.JSONDecodeError, TypeError):
        return {"result": "haiku_parse_error", "attempts": attempts, "failed_names": failed_names}

    caused = bool(analysis.get("caused_by_us", False))
    reason = analysis.get("reason", "")
    fix_hint = analysis.get("fix_hint", "")

    if not caused:
        return {"result": "unrelated", "attempts": attempts,
                "failed_names": failed_names, "reason": reason}

    fix_prompt = (
        f"CI failed: {', '.join(failed_names)}. Caused by our changes. Hint: {fix_hint}\n\n"
        "Reproduce the failure locally first. Read `gh run view --log-failed` for the exact "
        "errors, then find the local equivalent of each failing step (check "
        ".github/workflows/, Makefile, package.json scripts, pyproject.toml). Run that "
        "command locally and confirm you see the same failure. Fix the code. Re-run the "
        "same command — it must pass locally before you commit. Only then commit and push. "
        "If you cannot get it green locally, do not push: say what you tried and stop."
    )
    ran = run_claude_code(fix_prompt, cwd=worktree, timeout=FIX_TIMEOUT)
    if ran is None:
        return {"result": "fix_failed", "attempts": attempts,
                "failed_names": failed_names, "fix_hint": fix_hint}

    return {"result": "fixed", "attempts": attempts + 1,
            "failed_names": failed_names, "fix_hint": fix_hint}
