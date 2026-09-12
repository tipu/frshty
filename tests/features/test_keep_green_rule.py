"""Every fixer that pushes to a live PR branch must hold the green checks green.

Observed on LSC-78 (atroposhealth/text-to-tql-service#184). The PR-comment
fixer pushed 3fd23633, which broke the `build` check (two ruff errors) while
`unit-tests` stayed green. CI fix attempt 1 pushed 11b3e083: `build` went
green, `unit-tests` went red. CI fix attempt 2 pushed 7576ca84: `unit-tests`
went green, `build` went red again with the same two ruff errors. Two
oscillations spent MAX_CI_FIX_ATTEMPTS and parked the ticket in pr_failed.
Each agent verified only the check it was told about, so each fix landed with
a check it never ran now red.
"""
from unittest.mock import MagicMock, patch

from features import own_prs, pr_autofix, pr_ci, tickets
from tests.conftest import make_comment, make_pr, make_ticket_state


def _checks(*pairs):
    return [{"name": name, "state": state} for name, state in pairs]


class TestCiFixPromptNamesTheGreenChecks:
    def _fix_prompt(self, checks, tmp_path):
        platform = MagicMock()
        platform.get_pr_checks.return_value = checks
        platform.get_failed_logs.return_value = "ruff: ASYNC240"
        platform.get_pr_diff.return_value = "diff --git a/a.py b/a.py\n"
        analysis = '{"caused_by_us": true, "reason": "our diff", "fix_hint": "fix ruff"}'
        with patch("features.pr_ci.run_balanced", return_value=analysis), \
             patch("features.pr_ci.run_claude_code", return_value="done") as fixer:
            pr_ci.triage_and_fix_pr(platform, "repo", 184, label="LSC-78",
                                    worktree=tmp_path, attempts=0, max_attempts=2)
        return fixer.call_args[0][0]

    def test_a_passing_check_is_named_and_must_stay_green(self, tmp_path):
        prompt = self._fix_prompt(
            _checks(("build", "FAILURE"), ("unit-tests", "SUCCESS"),
                    ("integration-tests", "SUCCESS")),
            tmp_path)
        assert "unit-tests" in prompt and "integration-tests" in prompt, (
            "the CI fixer must be told which checks are green, or it fixes the "
            "red one and pushes with a green one newly red — the LSC-78 "
            f"oscillation. prompt was: {prompt}")
        assert "still pass before you push" in prompt

    def test_a_running_check_is_not_claimed_to_be_green(self, tmp_path):
        prompt = self._fix_prompt(
            _checks(("build", "FAILURE"), ("review", "IN_PROGRESS")), tmp_path)
        assert "review" not in prompt, (
            "a check still running has not passed; naming it green tells the "
            f"agent to keep something green that may already be red. prompt: {prompt}")

    def test_no_green_clause_when_every_other_check_is_red_or_running(self, tmp_path):
        prompt = self._fix_prompt(
            _checks(("build", "FAILURE"), ("unit-tests", "FAILURE"),
                    ("review", "QUEUED")), tmp_path)
        assert "pass on this PR right now" not in prompt


class TestGreenCheckNames:
    def test_only_a_reported_pass_counts(self):
        names = pr_ci.green_check_names(
            _checks(("build", "FAILURE"), ("unit-tests", "SUCCESS"),
                    ("lint", "NEUTRAL"), ("docs", "SKIPPED"),
                    ("review", "IN_PROGRESS"), ("e2e", "CANCELLED")))
        assert names == ["unit-tests", "lint", "docs"]

    def test_a_state_outside_every_known_set_is_not_green(self):
        """gh maps a check to ERROR, ACTION_REQUIRED, STALE or STARTUP_FAILURE
        as well. None of those is a pass, and none is in FAILED_STATES or
        PENDING_STATES, so reading green as 'not failed and not pending' would
        tell the fixer to hold a red check green."""
        names = pr_ci.green_check_names(
            _checks(("build", "ERROR"), ("sign-off", "ACTION_REQUIRED"),
                    ("docs", "STALE"), ("e2e", "STARTUP_FAILURE"),
                    ("unit-tests", "SUCCESS")))
        assert names == ["unit-tests"]

    def test_no_checks_is_no_names(self):
        assert pr_ci.green_check_names(None) == []
        assert pr_ci.green_check_names([]) == []


class TestCommentFixersCarryTheRule:
    def _config(self, tmp_path):
        return {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

    def test_own_pr_single_comment_fix(self, tmp_path):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": True}
        payload = {"pr": make_pr(), "comment": make_comment(id=10, author_id="reviewer1")}
        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done") as fixer, \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments.mark_comment_processed"), \
             patch("features.own_prs.log.emit"):
            own_prs.fix_comment(self._config(tmp_path), payload)
        assert pr_ci.KEEP_GREEN_RULE in fixer.call_args[0][0]

    def test_own_pr_batch_comment_fix(self, tmp_path):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": True}
        platform.get_pr_comments.return_value = [make_comment(id=10, author_id="reviewer1")]
        payload = {"pr": make_pr(), "comment_ids": ["10"]}
        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done") as fixer, \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit"):
            mock_comments.settled_comment_ids.return_value = set()
            own_prs.fix_comments_batch(self._config(tmp_path), payload)
        assert pr_ci.KEEP_GREEN_RULE in fixer.call_args[0][0]

    def test_pr_autofix_finding_fix(self, tmp_path):
        findings = [{"severity": "critical", "path": "a.py", "line": 3,
                     "title": "empty page raises", "body": "guard the empty list"}]
        platform = MagicMock()
        platform.get_pr_diff.return_value = "diff --git a/a.py b/a.py\n"
        platform.push_branch.return_value = {"ok": True}
        config = {"job": {"key": "clarivis"}, "features": {"pr_autofix": True},
                  "_state_dir": tmp_path, "_base_url": "http://base"}
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix._ensure_worktree", return_value=tmp_path), \
             patch("features.pr_autofix._claude_review", return_value={"findings": []}), \
             patch("features.pr_autofix._codex_review", return_value={"findings": []}), \
             patch("features.pr_autofix._normalize_findings", return_value=findings), \
             patch("features.pr_autofix._consolidate", return_value=findings), \
             patch("features.pr_autofix._write_artifacts"), \
             patch("features.pr_autofix.run_claude_code", return_value="fixed") as fixer, \
             patch("features.pr_autofix.git_util.run_git") as run_git, \
             patch("features.pr_autofix.commit_subject", return_value="fix: guard"), \
             patch("features.pr_autofix.git_util.commit_with_hooks",
                   return_value=MagicMock(returncode=0)), \
             patch("features.pr_autofix.state.load", return_value={}), \
             patch("features.pr_autofix.state.save"), \
             patch("features.pr_autofix.log.emit"):
            run_git.return_value = MagicMock(returncode=1, stdout="abc1234\n")
            pr_autofix.run(config, {"pr": make_pr()})
        assert pr_ci.KEEP_GREEN_RULE in fixer.call_args[0][0]

    def test_ticket_pr_comment_fix(self, fresh_db, fake_config, tmp_state):
        slug = "PROJ-1-do-the-thing"
        wt = fake_config["workspace"]["root"] / "tickets" / slug / "repo"
        wt.mkdir(parents=True, exist_ok=True)
        (wt / ".git").mkdir(exist_ok=True)
        ts = make_ticket_state(
            status="in_review", slug=slug, branch=slug,
            prs=[{"repo": "repo", "id": 99, "branch": slug, "url": "http://u"}],
        )
        ticket = {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1"}
        comment = {"id": 100, "body": "Please rename this variable",
                   "author_id": "reviewer1", "author_name": "Bob",
                   "path": "src/main.py", "line": 42, "parent_id": None,
                   "created_on": "2026-01-01T12:00:00Z",
                   "created_at": "2026-01-01T12:00:00Z",
                   "updated_at": "2026-01-01T12:00:00Z"}
        platform = MagicMock()
        platform.get_pr_state.return_value = "OPEN"
        platform.get_pr_comments.return_value = [comment]
        bb_config = {
            **fake_config,
            "job": {**fake_config["job"], "platform": "bitbucket"},
            "bitbucket": {"org": "x", "user_account_id": "bot-self"},
        }
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.get_repos",
                   return_value=[{"name": "repo", "path": wt.parent}]), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.run_balanced",
                   return_value='{"results": [{"i": 0, "actionable": true}]}'), \
             patch("features.tickets.run_claude_code", return_value=None) as fixer, \
             patch("features.tickets.subprocess.run",
                   return_value=MagicMock(returncode=0)):
            tickets._check_in_review(bb_config, ticket, ts, "http://base")
        assert pr_ci.KEEP_GREEN_RULE in fixer.call_args[0][0]
