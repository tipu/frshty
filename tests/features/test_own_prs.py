from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock

from features import own_prs
from tests.conftest import make_pr, make_comment


class TestCheckStale:
    def test_stale_pr_emits(self):
        pr = make_pr(created_on="2020-01-01T00:00:00Z")
        seen = {}
        with patch("features.own_prs.log.emit") as mock_emit:
            own_prs._check_stale(pr, seen, "http://base")
        mock_emit.assert_called_once()
        assert seen["stale_notified"] is True

    def test_already_notified_skips(self):
        pr = make_pr(created_on="2020-01-01T00:00:00Z")
        seen = {"stale_notified": True}
        with patch("features.own_prs.log.emit") as mock_emit:
            own_prs._check_stale(pr, seen, "http://base")
        mock_emit.assert_not_called()

    def test_recent_pr_skips(self):
        recent = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        pr = make_pr(created_on=recent)
        seen = {}
        with patch("features.own_prs.log.emit") as mock_emit:
            own_prs._check_stale(pr, seen, "http://base")
        mock_emit.assert_not_called()


class TestCheckCi:
    def test_no_failing_clears_sha(self):
        platform = MagicMock()
        platform.get_pr_checks.return_value = [{"state": "SUCCESS", "name": "build"}]
        pr = make_pr()
        seen = {"ci_fix_sha": "abc"}
        config = {"_state_dir": "/tmp"}
        own_prs._check_ci(config, platform, pr, seen, "http://base")
        assert "ci_fix_sha" not in seen

    def test_skips_when_head_unchanged(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_checks.return_value = [{"state": "FAILED", "name": "lint"}]
        pr = make_pr()
        seen = {"ci_fix_sha": "deadbeef"}
        config = {"_state_dir": tmp_path}
        worktree = tmp_path / "wt"
        worktree.mkdir()

        with patch("features.own_prs._ensure_worktree", return_value=worktree), \
             patch("features.own_prs.subprocess.run") as mock_run, \
             patch("features.own_prs.run_claude_code") as mock_cc:
            mock_run.return_value = MagicMock(returncode=0, stdout="deadbeef\n")
            own_prs._check_ci(config, platform, pr, seen, "http://base")
        mock_cc.assert_not_called()

    def test_no_push_when_claude_fails(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_checks.return_value = [{"state": "FAILED", "name": "lint"}]
        pr = make_pr()
        seen = {}
        config = {"_state_dir": tmp_path}
        worktree = tmp_path / "wt"
        worktree.mkdir()

        with patch("features.own_prs._ensure_worktree", return_value=worktree), \
             patch("features.own_prs.subprocess.run") as mock_run, \
             patch("features.own_prs.run_claude_code", return_value=None):
            mock_run.return_value = MagicMock(returncode=0, stdout=b"abc123\n")
            own_prs._check_ci(config, platform, pr, seen, "http://base")
        platform.push_branch.assert_not_called()


class TestCheckComments:
    def test_no_new_comments_noop(self):
        platform = MagicMock()
        platform.get_pr_comments.return_value = []
        pr = make_pr()
        seen = {}
        config = {"bitbucket": {"user_account_id": "me"}}
        own_prs._check_comments(config, platform, pr, seen, "http://base")

    def test_actionable_comment_triggers_fix(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="reviewer1", body="Fix this function"),
        ]
        pr = make_pr()
        seen = {"last_comment_id": 0}
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.run_haiku", return_value='[{"id": 0, "actionable": true, "reason": "clear"}]'), \
             patch("features.own_prs.extract_json", return_value=[{"id": 0, "actionable": True, "reason": "clear"}]), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs.log"):
            own_prs._check_comments(config, platform, pr, seen, "http://base")
        platform.push_branch.assert_called_once()
        platform.resolve_comment.assert_called_once()
        assert seen["last_comment_id"] == 10


class TestEnsureWorktree:
    def test_uses_correct_repo(self, tmp_path):
        repo_a = tmp_path / "repo-a"
        repo_b = tmp_path / "repo-b"
        repo_a.mkdir(); repo_b.mkdir()

        config = {"_state_dir": tmp_path}
        repos = [{"name": "repo-a", "path": repo_a}, {"name": "repo-b", "path": repo_b}]
        pr = make_pr(repo="repo-b", branch="fix/thing")

        calls = []
        def fake_run(cmd, *a, **kw):
            calls.append((cmd, kw.get("cwd", "")))
            return MagicMock(returncode=0, stdout=b"", stderr=b"")

        with patch("features.own_prs.get_repos", return_value=repos), \
             patch("features.own_prs.subprocess.run", side_effect=fake_run):
            own_prs._ensure_worktree(config, pr)

        for cmd, cwd in calls:
            assert str(repo_a) not in str(cwd)

    def test_no_repos_returns_none(self, tmp_path):
        config = {"_state_dir": tmp_path}
        pr = make_pr()
        with patch("features.own_prs.get_repos", return_value=[]):
            assert own_prs._ensure_worktree(config, pr) is None

    def test_no_matching_repo_returns_none(self, tmp_path):
        config = {"_state_dir": tmp_path}
        pr = make_pr(repo="nonexistent")
        with patch("features.own_prs.get_repos", return_value=[{"name": "other", "path": "/x"}]):
            assert own_prs._ensure_worktree(config, pr) is None
