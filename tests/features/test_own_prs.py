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


class TestCheckBaseFresh:
    def _cfg(self, enabled=True):
        return {"_state_dir": "/tmp", "pr": {"auto_update_branch": enabled},
                "workspace": {"repos": [], "base_branch": "main"}}

    def test_disabled_flag_noop(self):
        platform = MagicMock()
        with patch("features.own_prs.branch_sync.sync_branch_with_base") as mock_sync:
            own_prs._check_base_fresh(self._cfg(enabled=False), platform, make_pr(), {}, "http://base")
        mock_sync.assert_not_called()

    def test_synced_logs_and_clears_ci_sha(self):
        platform = MagicMock()
        seen = {"ci_fix_sha": "old", "ci_unrelated_sha": "old"}
        with patch("features.own_prs.branch_sync.sync_branch_with_base",
                   return_value={"result": "synced", "base": "main"}), \
             patch("features.own_prs._repo_path_for", return_value="/repo"), \
             patch("features.own_prs.log.emit") as mock_emit:
            own_prs._check_base_fresh(self._cfg(), platform, make_pr(), seen, "http://base")
        assert "ci_fix_sha" not in seen and "ci_unrelated_sha" not in seen
        assert any(c.args[0] == "pr_base_synced" for c in mock_emit.call_args_list)

    def test_capped_merge_failure_logs(self):
        platform = MagicMock()
        with patch("features.own_prs.branch_sync.sync_branch_with_base",
                   return_value={"result": "merge_failed", "error": "boom", "attempts": 2, "capped": True}), \
             patch("features.own_prs._repo_path_for", return_value="/repo"), \
             patch("features.own_prs.log.emit") as mock_emit:
            own_prs._check_base_fresh(self._cfg(), platform, make_pr(), {}, "http://base")
        assert any(c.args[0] == "pr_base_sync_failed" for c in mock_emit.call_args_list)

    def test_skip_result_no_log(self):
        platform = MagicMock()
        with patch("features.own_prs.branch_sync.sync_branch_with_base", return_value={"result": "skip"}), \
             patch("features.own_prs._repo_path_for", return_value="/repo"), \
             patch("features.own_prs.log.emit") as mock_emit:
            own_prs._check_base_fresh(self._cfg(), platform, make_pr(), {}, "http://base")
        assert not any("base_sync" in str(c.args[0]) for c in mock_emit.call_args_list)


class TestCheckComments:
    def test_no_new_comments_noop(self):
        platform = MagicMock()
        platform.get_pr_comments.return_value = []
        pr = make_pr()
        config = {"bitbucket": {"user_account_id": "me"}}
        with patch("features.own_prs.comments") as mock_comments:
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        platform.push_branch.assert_not_called()

    def test_actionable_comment_defers_and_starts_window(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this function")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        seen = {}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_balanced", return_value='{"results": [{"id": 0, "actionable": true, "reason": "clear"}]}'), \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [comment], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = [
                {"comment_id": "10", "state": "deferred", "error_count": 0, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen)
        mock_comments.mark_comment_deferred.assert_called_once()
        assert mock_comments.mark_comment_deferred.call_args[0][3] == "10"
        mock_enqueue.assert_not_called()
        assert datetime.fromisoformat(seen["fix_deadline"]) > datetime.now(timezone.utc)
        platform.push_branch.assert_not_called()

    def test_new_review_body_source_baselines_old_but_processes_recent(self, tmp_path):
        platform = MagicMock()
        old = make_comment(
            id=10,
            author_id="reviewer1",
            body="Old general feedback",
            created_at=(datetime.now(timezone.utc) - timedelta(days=3)).isoformat(),
            comment_kind="review_body",
            resolvable=False,
        )
        recent = make_comment(
            id=11,
            author_id="reviewer1",
            body="Add the missing fingerprint",
            created_at=(datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            comment_kind="review_body",
            resolvable=False,
        )
        platform.get_pr_comments.return_value = [old, recent]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        seen = {}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_balanced", return_value='{"results": [{"id": 0, "actionable": true, "reason": "clear"}]}'), \
             patch("features.own_prs.q.enqueue_job"), \
             patch("features.own_prs.log"):
            mock_comments.has_comment_state.return_value = True
            mock_comments.fetch_and_detect_comments.return_value = {"new": [old, recent], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen)

        mock_comments.mark_comment_seen.assert_called_once()
        assert mock_comments.mark_comment_seen.call_args.args[3] == "10"
        assert mock_comments.mark_comment_deferred.call_args.args[3] == "11"
        assert seen["review_bodies_baselined"] is True

    def test_new_comment_pushes_existing_window(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=11, author_id="reviewer1", body="Also fix this")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        old_deadline = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
        seen = {"fix_deadline": old_deadline}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_balanced", return_value='{"results": [{"id": 0, "actionable": true, "reason": "clear"}]}'), \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [comment], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = [
                {"comment_id": "10", "state": "deferred", "error_count": 0, "last_checked_at": None},
                {"comment_id": "11", "state": "deferred", "error_count": 0, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen)
        assert datetime.fromisoformat(seen["fix_deadline"]) > datetime.fromisoformat(old_deadline)
        mock_enqueue.assert_not_called()

    def test_expired_window_flushes_single_batch_job(self, tmp_path):
        platform = MagicMock()
        c1 = make_comment(id=10, author_id="reviewer1", body="Fix this")
        c2 = make_comment(id=11, author_id="reviewer1", body="Fix that")
        platform.get_pr_comments.return_value = [c1, c2]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        seen = {"fix_deadline": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = [
                {"comment_id": "10", "state": "deferred", "error_count": 0, "last_checked_at": None},
                {"comment_id": "11", "state": "deferred", "error_count": 0, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen)
        mock_enqueue.assert_called_once()
        assert mock_enqueue.call_args[0][1] == "fix_pr_comments"
        assert mock_enqueue.call_args[1]["payload"]["comment_ids"] == ["10", "11"]
        assert "fix_deadline" not in seen

    def test_unexpired_window_does_not_flush(self, tmp_path):
        platform = MagicMock()
        c1 = make_comment(id=10, author_id="reviewer1", body="Fix this")
        platform.get_pr_comments.return_value = [c1]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        seen = {"fix_deadline": (datetime.now(timezone.utc) + timedelta(minutes=10)).isoformat()}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = [
                {"comment_id": "10", "state": "deferred", "error_count": 0, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen)
        mock_enqueue.assert_not_called()
        assert seen["fix_deadline"]

    def test_flush_carries_ticket_key(self, tmp_path):
        platform = MagicMock()
        c1 = make_comment(id=10, author_id="reviewer1", body="Fix this")
        platform.get_pr_comments.return_value = [c1]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        seen = {"fix_deadline": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = [
                {"comment_id": "10", "state": "deferred", "error_count": 0, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen, ticket_key="DEV-512")
        assert mock_enqueue.call_args[1]["ticket_key"] == "DEV-512"

    def test_first_sight_baselines_old_keeps_recent(self, tmp_path):
        platform = MagicMock()
        old = make_comment(id=1, author_id="reviewer1", body="old", created_at="2020-01-01T00:00:00+00:00")
        recent_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        recent = make_comment(id=2, author_id="reviewer1", body="recent", created_at=recent_ts)
        platform.get_pr_comments.return_value = [old, recent]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_balanced", return_value='{"results": [{"id": 0, "actionable": false, "reason": "q"}]}'), \
             patch("features.own_prs.q.enqueue_job"), \
             patch("features.own_prs.log"):
            mock_comments.has_comment_state.return_value = False
            mock_comments.fetch_and_detect_comments.return_value = {"new": [old, recent], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")

        seen_ids = [c[0][3] for c in mock_comments.mark_comment_seen.call_args_list]
        processing_ids = [c[0][3] for c in mock_comments.mark_comment_processing.call_args_list]
        assert seen_ids == ["1"]
        assert "2" in processing_ids
        assert "1" not in processing_ids

    def test_classifier_failure_retries_not_finalize(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Restore the pluralize please")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_balanced", return_value=""), \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [comment], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        # The classifier not answering says nothing about the comment, so it
        # must not spend the retry budget that decides when frshty gives up.
        mock_comments.mark_comment_error.assert_not_called()
        mock_comments.mark_comment_retryable.assert_called_once()
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
             patch("features.own_prs.run_balanced", return_value='{"results": [{"id": 0, "actionable": false, "reason": "q"}, {"id": 1, "actionable": false, "reason": "q"}]}'), \
             patch("features.own_prs.log.emit") as mock_emit:
            mock_comments.fetch_and_detect_comments.return_value = {"new": [c1, c2], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = []
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
            mock_comments.get_deferred_comments.return_value = [
                {"comment_id": "10", "state": "deferred", "error_count": 0, "last_checked_at": None},
            ]
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_comments.mark_comment_deferred.assert_called_once()
        mock_enqueue.assert_called_once()
        assert mock_enqueue.call_args[0][1] == "fix_pr_comments"
        assert mock_enqueue.call_args[1]["payload"]["comment_ids"] == ["10"]

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
            mock_comments.get_deferred_comments.return_value = []
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
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_enqueue.assert_not_called()

    def test_abandoned_comment_is_announced_once(self, tmp_path):
        """A comment past the cap is dropped with a bare continue.

        Nothing reaches the event feed, so a comment that frshty gave up on
        looks the same as a comment nobody ever left. Announce it once, then
        stay quiet."""
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this")
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}
        seen = {}

        def _run():
            with patch("features.own_prs.comments") as mock_comments, \
                 patch("features.own_prs.q.enqueue_job"), \
                 patch("features.own_prs.log.emit") as mock_emit:
                mock_comments.fetch_and_detect_comments.return_value = {"new": [], "edited": []}
                mock_comments.get_unprocessed_comments.return_value = [
                    {"comment_id": "10", "state": "new",
                     "error_count": own_prs.MAX_COMMENT_RETRIES + 1, "last_checked_at": None},
                ]
                mock_comments.get_deferred_comments.return_value = []
                own_prs._check_comments(config, "test", platform, pr, "http://base", seen=seen)
            return [c.args[0] for c in mock_emit.call_args_list]

        assert "pr_comment_abandoned" in _run()
        assert "pr_comment_abandoned" not in _run()

    def test_resolved_comment_not_processed(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this", resolved=True)
        platform.get_pr_comments.return_value = [comment]
        pr = make_pr()
        config = {"_state_dir": tmp_path, "bitbucket": {"user_account_id": "me"}, "workspace": {"repos": []}}

        with patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.run_balanced") as mock_sonnet, \
             patch("features.own_prs.q.enqueue_job") as mock_enqueue, \
             patch("features.own_prs.log"):
            mock_comments.fetch_and_detect_comments.return_value = {"new": [comment], "edited": []}
            mock_comments.get_unprocessed_comments.return_value = []
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_sonnet.assert_not_called()
        mock_enqueue.assert_not_called()
        mock_comments.mark_comment_processing.assert_not_called()

    def test_reclaim_marks_resolved_processed(self, tmp_path):
        platform = MagicMock()
        comment = make_comment(id=10, author_id="reviewer1", body="Fix this", resolved=True)
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
            mock_comments.get_deferred_comments.return_value = []
            own_prs._check_comments(config, "test", platform, pr, "http://base")
        mock_comments.mark_comment_processed.assert_called_once()
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
            mock_comments.get_deferred_comments.return_value = []
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
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments.mark_comment_processed"), \
             patch("features.own_prs.log.emit") as mock_emit:
            ok, _ = own_prs.fix_comment(config, self._payload())

        assert ok is True
        platform.push_branch.assert_called_once()
        platform.resolve_comment.assert_called_once()
        events = [c[0][0] for c in mock_emit.call_args_list]
        assert events.index("pr_comment_code_written") < events.index("pr_comment_addressed")

    def test_resolve_failure_marks_error_not_processed(self, tmp_path):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": True}
        platform.resolve_comment.return_value = {"status": "error", "detail": "boom"}
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.comments.mark_comment_processed") as mock_proc, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "resolve failed"
        mock_err.assert_called_once()
        mock_proc.assert_not_called()

    def test_general_review_body_is_processed_without_thread_resolution(self, tmp_path):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": True}
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}
        payload = self._payload(resolvable=False, comment_kind="review_body")

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments.mark_comment_processed") as mock_proc, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, payload)

        assert ok is True and reason is None
        platform.resolve_comment.assert_not_called()
        mock_proc.assert_called_once()

    def test_no_push_when_claude_fails(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value=None), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.comments.mark_comment_deferred") as mock_defer, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "Claude did not run to completion"
        platform.push_branch.assert_not_called()
        mock_err.assert_not_called()
        mock_defer.assert_called_once()

    def test_worktree_failure_marks_error(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

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
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "push failed"
        platform.resolve_comment.assert_not_called()
        mock_err.assert_called_once()

    def test_no_changes_marks_error_not_resolved(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(False, "no changes produced")), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.comments.mark_comment_processed") as mock_proc, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "no changes produced"
        platform.push_branch.assert_not_called()
        platform.resolve_comment.assert_not_called()
        mock_err.assert_called_once()
        mock_proc.assert_not_called()

    def test_commit_failure_marks_error_not_resolved(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(False, "commit failed: hook rejected")), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert reason == "commit failed: hook rejected"
        platform.push_branch.assert_not_called()
        platform.resolve_comment.assert_not_called()
        mock_err.assert_called_once()

    def test_exception_marks_error(self, tmp_path):
        platform = MagicMock()
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", side_effect=RuntimeError("boom")), \
             patch("features.own_prs.comments.mark_comment_error") as mock_err, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comment(config, self._payload())

        assert ok is False
        assert "RuntimeError" in reason
        mock_err.assert_called_once()


class TestFixCommentsBatch:
    def _payload(self, ids=("10", "11")):
        return {"pr": make_pr(), "comment_ids": list(ids)}

    def _config(self, tmp_path):
        return {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}

    def test_single_commit_resolves_all(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="r1", body="Fix this"),
            make_comment(id=11, author_id="r1", body="Fix that"),
        ]
        platform.push_branch.return_value = {"ok": True}
        platform.resolve_comment.return_value = {"status": "resolved"}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done") as mock_claude, \
             patch("features.own_prs._commit_fix", return_value=(True, "")) as mock_commit, \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comments_batch(self._config(tmp_path), self._payload())

        assert ok is True
        mock_claude.assert_called_once()
        assert "Fix this" in mock_claude.call_args[0][0]
        assert "Fix that" in mock_claude.call_args[0][0]
        mock_commit.assert_called_once()
        assert "2 review comments" in mock_commit.call_args[0][1]
        platform.push_branch.assert_called_once()
        assert platform.resolve_comment.call_count == 2
        assert mock_comments.mark_comment_processed.call_count == 2

    def test_general_review_body_skips_thread_resolution(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="r1", body="Add a fingerprint",
                         resolvable=False, comment_kind="review_body"),
        ]
        platform.push_branch.return_value = {"ok": True}

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comments_batch(
                self._config(tmp_path), self._payload(ids=("10",)),
            )

        assert ok is True and reason is None
        platform.resolve_comment.assert_not_called()
        mock_comments.mark_comment_processed.assert_called_once()

    def test_provider_failure_does_not_spend_the_fix_budget(self, tmp_path):
        """A provider outage is not a verdict on the comment.

        run_claude_code returns None for a guard block, a timeout, and a
        non-zero exit alike, so the caller cannot tell a two-second API error
        from a model that read the code and gave up. Counting the outage as a
        failed attempt burns the retry budget in minutes and abandons the
        comment for good."""
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="r1", body="Fix this"),
        ]

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value=None), \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit") as mock_emit:
            ok, reason = own_prs.fix_comments_batch(self._config(tmp_path), self._payload(ids=("10",)))

        assert ok is False
        mock_comments.mark_comment_error.assert_not_called()
        mock_comments.mark_comment_deferred.assert_called_once()
        assert any(call.args[0] == "pr_comment_provider_failed" for call in mock_emit.call_args_list)

    def test_no_changes_marks_all_error(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="r1", body="Fix this"),
            make_comment(id=11, author_id="r1", body="Fix that"),
        ]

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(False, "no changes produced")), \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comments_batch(self._config(tmp_path), self._payload())

        assert ok is False
        assert reason == "no changes produced"
        platform.push_branch.assert_not_called()
        platform.resolve_comment.assert_not_called()
        assert mock_comments.mark_comment_error.call_count == 2

    def test_skips_resolved_and_deleted_comments(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="r1", body="Fix this", resolved=True),
        ]

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code") as mock_claude, \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comments_batch(self._config(tmp_path), self._payload(ids=("10", "99")))

        assert ok is True
        assert reason == "nothing left to fix"
        mock_claude.assert_not_called()
        platform.push_branch.assert_not_called()
        mock_comments.mark_comment_processed.assert_called_once()
        mock_comments.mark_comment_deleted.assert_called_once()

    def test_partial_resolve_failure(self, tmp_path):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [
            make_comment(id=10, author_id="r1", body="Fix this"),
            make_comment(id=11, author_id="r1", body="Fix that"),
        ]
        platform.push_branch.return_value = {"ok": True}
        platform.resolve_comment.side_effect = [
            {"status": "resolved"},
            {"status": "error", "detail": "boom"},
        ]

        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs.run_claude_code", return_value="done"), \
             patch("features.own_prs._commit_fix", return_value=(True, "")), \
             patch("features.own_prs.comments") as mock_comments, \
             patch("features.own_prs.log.emit"):
            ok, reason = own_prs.fix_comments_batch(self._config(tmp_path), self._payload())

        assert ok is False
        assert reason == "resolve failed"
        mock_comments.mark_comment_processed.assert_called_once()
        mock_comments.mark_comment_error.assert_called_once()


class TestCommitFix:
    def _init_repo(self, path):
        import subprocess
        path.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=str(path), check=True)
        subprocess.run(["git", "config", "user.email", "t@t"], cwd=str(path), check=True)
        subprocess.run(["git", "config", "user.name", "t"], cwd=str(path), check=True)
        (path / "a.txt").write_text("one\n")
        subprocess.run(["git", "add", "-A"], cwd=str(path), check=True)
        subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=str(path), check=True)
        return path

    def test_commits_dirty_worktree(self, tmp_path):
        import subprocess
        repo = self._init_repo(tmp_path / "repo")
        (repo / "a.txt").write_text("two\n")

        ok, reason = own_prs._commit_fix(repo, "fix: address review comment on a.txt")

        assert ok is True
        assert reason == ""
        status = subprocess.run(["git", "status", "--porcelain"], cwd=str(repo), capture_output=True, text=True)
        assert status.stdout.strip() == ""
        msg = subprocess.run(["git", "log", "-1", "--format=%s"], cwd=str(repo), capture_output=True, text=True).stdout
        assert "a.txt" in msg

    def test_clean_worktree_reports_no_changes(self, tmp_path):
        repo = self._init_repo(tmp_path / "repo")

        ok, reason = own_prs._commit_fix(repo, "fix: address review comment on a.txt")

        assert ok is False
        assert reason == "no changes produced"


class TestEnsureWorktree:
    def test_uses_correct_repo(self, tmp_path):
        repo_a = tmp_path / "repo-a"
        repo_b = tmp_path / "repo-b"
        repo_a.mkdir(); repo_b.mkdir()

        config = {"_state_dir": tmp_path, "workspace": {"base_branch": "main"}}
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
