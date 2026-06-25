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
        config = {"bitbucket": {"user_account_id": "me"}}
        with patch("features.own_prs.comments") as mock_comments:
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        platform.push_branch.assert_not_called()

    def test_actionable_comment_enqueues_fix_job(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this function")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_sonnet", return_value='{"results": [{"id": 0, "actionable": true, "reason": "clear"}]}'), \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [comment], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_comments.mark_comment_processing.assert_called_once()
        mock_enqueue.assert_called_once()
        assert mock_enqueue.call_args[0][1] == "fix_pr_comment"
        assert str(mock_enqueue.call_args[1]["payload"]["comment"]["id"]) == "10"
        platform.push_branch.assert_not_called()

    def test_classifier_failure_retries_not_finalize(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Restore the pluralize please")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_sonnet", return_value=""), \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [comment], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_comments.mark_comment_error.assert_called_once()
        mock_comments.mark_comment_processed.assert_not_called()
        platform.push_branch.assert_not_called()

    def test_emits_detection_event_with_count(self, tmp_path):
        platform = MagicMock()
        c1 = make_comment(id=10, author_id="r1", body="one")
        c2 = make_comment(id=11, author_id="r2", body="two")
        platform.get_pr_comments.return_value = [c1, c2]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_sonnet", return_value='{"results": [{"id": 0, "actionable": false, "reason": "q"}, {"id": 1, "actionable": false, "reason": "q"}]}'), \
             patch("features.own_prs.log.emit") as mock_emit:
            mock_comments.fetch_and_detect_comments.return_value = {"new": [c1, c2], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")

        detected = [c for c in mock_emit.call_args_list if c[0][0] == "pr_comments_detected"]
        assert len(detected) == 1
        assert detected[0][1]["meta"]["count"] == 2
        assert len(detected[0][1]["meta"]["comments"]) == 2

    def test_reclaims_orphaned_processing_comment(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        stale = (datetime.now(timezone.utc) - timedelta(seconds=own_prs.RECLAIM_STALE_SECONDS + 60)).isoformat()

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = [
                {"comment_id": "10", "state": "processing", "error_count": 0, "last_checked_at": stale},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_enqueue.assert_called_once()
        assert mock_enqueue.call_args[0][1] == "fix_pr_comment"

    def test_skips_fresh_processing_comment(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        fresh = datetime.now(timezone.utc).isoformat()

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = [
                {"comment_id": "10", "state": "processing", "error_count": 0, "last_checked_at": fresh},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_enqueue.assert_not_called()

    def test_stops_retrying_past_cap(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = [
                {"comment_id": "10", "state": "new", "error_count": own_prs.MAX_COMMENT_RETRIES + 1, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_enqueue.assert_not_called()

    def test_reclaim_marks_deleted_when_gone_upstream(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = []
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = [
                {"comment_id": "99", "state": "new", "error_count": 1, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_comments.mark_comment_deleted.assert_called_once()
        mock_enqueue.assert_not_called()

class TestFixComment:
    def _payload(self, **comment_kw):
        return {
            "pr": make_pr(),
            "comment": make_comment(id=10, author_id="reviewer1", body="Fix this function", **comment_kw),
        }

    def test_emits_code_written_before_addressed(self, tmp_path):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": True}
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "instance_key": "test"}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs.comments.mark_comment_processed"), \
             patch("features.own_prs.log.emit") as mock_emit:
            ok, _ = own_prs.fix_comment(config, self._payload())

        assert ok is True
        platform.push_branch.assert_called_once()
        platform.resolve_comment.assert_called_once()
        events = [c[0][0] for c in mock_emit.call_args_list]
        assert events.index("pr_comment_code_written") < events.index("pr_comment_addressed")

    def test_no_push_when_claude_fails(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "instance_key": "test"}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value=None), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "Claude failed to fix"
        platform.push_branch.assert_not_called()
        mock_err.assert_called_once()

    def test_worktree_failure_marks_error(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "instance_key": "test"}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=None), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "Could not create worktree"
        mock_err.assert_called_once()

    def test_push_failure_marks_error_not_resolved(self, tmp_path):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": False, "error": "remote rejected"}
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "instance_key": "test"}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "push failed"
        platform.resolve_comment.assert_not_called()
        mock_err.assert_called_once()

    def test_exception_marks_error(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "instance_key": "test"}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", side_effect=RuntimeError("boom")), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert "RuntimeError" in reason
        mock_err.assert_called_once()


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
