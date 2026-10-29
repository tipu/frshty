"""A spent fix budget still triages a red check, so an unrelated check is not
reported as a failure the fixer gave up on."""
from unittest.mock import MagicMock, patch

from features import pr_ci


def _platform():
    platform = MagicMock()
    platform.get_pr_checks.return_value = [{"name": "evals", "state": "FAILURE"}]
    platform.get_failed_logs.return_value = "judge score 0.61 < 0.7"
    platform.get_pr_diff.return_value = "diff --git a/a.py b/a.py\n"
    return platform


def _run(verdict, worktree):
    with patch("features.pr_ci.run_balanced", return_value=verdict), \
         patch("features.pr_ci.run_claude_code") as fixer:
        out = pr_ci.triage_and_fix_pr(_platform(), "repo", 1, label="X-1",
                                      worktree=worktree, attempts=2, max_attempts=2)
    fixer.assert_not_called()
    return out


def test_capped_unrelated_check_is_reported_unrelated():
    out = _run('{"caused_by_us": false, "reason": "flaky judge", "fix_hint": ""}', None)
    assert out["result"] == "unrelated"
    assert out["failed_names"] == ["evals"]


def test_capped_caused_check_is_reported_capped_without_a_fix(tmp_path):
    out = _run('{"caused_by_us": true, "reason": "our diff", "fix_hint": "x"}', tmp_path)
    assert out["result"] == "capped"
    assert out["attempts"] == 2
