"""Shared CI-failure triage+fix for any PR, regardless of origin.

Both the ticket pipeline (frshty-created PRs) and own_prs (user-opened PRs)
call through triage_and_fix_pr so behavior is identical: causality check via
Haiku first, fix via headless claude only if the failure is caused by our
changes, attempt cap enforced by the caller.
"""
import json
from pathlib import Path

from core.claude_runner import run_haiku, run_claude_code, extract_json


FIX_TIMEOUT = 1800
FAILED_STATES = ("FAILURE", "FAILED", "STOPPED")


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

    failure_logs = platform.get_failed_logs(repo, pr_id) or ""
    pr_diff = platform.get_pr_diff(repo, pr_id) or ""
    causality_prompt = (
        "CI checks failed on a PR. Determine if this is caused by the changes in the PR "
        "or is pre-existing/unrelated.\n\n"
        f"PR: {label}\n"
        f"Failed checks: {', '.join(failed_names)}\n"
        f"Fix attempt: {attempts + 1}/{max_attempts}\n\n"
        f"PR diff (what changed):\n{pr_diff[:4000]}\n\n"
        f"Failure logs:\n{failure_logs[:4000]}\n\n"
        "Analyze causality:\n"
        "1. Could the diff have caused these failures? Consider both direct changes and indirect effects.\n"
        "2. Or are these pre-existing failures, flaky tests, or infra issues unrelated to the changes?\n\n"
        "Reply with EXACTLY one JSON object:\n"
        '{"caused_by_us": true/false, "reason": "brief explanation", "fix_hint": "what to change if caused_by_us"}'
    )
    classification = run_haiku(causality_prompt, timeout=120)
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
        f"CI checks failed: {', '.join(failed_names)}. This is caused by our changes. "
        f"Fix the issue: {fix_hint}. Run the failing tests locally if you can, "
        f"then commit with --no-verify and push."
    )
    ran = run_claude_code(fix_prompt, cwd=worktree, timeout=FIX_TIMEOUT)
    if ran is None:
        return {"result": "fix_failed", "attempts": attempts,
                "failed_names": failed_names, "fix_hint": fix_hint}

    return {"result": "fixed", "attempts": attempts + 1,
            "failed_names": failed_names, "fix_hint": fix_hint}
