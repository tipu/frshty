from unittest.mock import patch, MagicMock

import features.tickets as tickets


def _cfg(enabled=True):
    return {"pr": {"auto_update_branch": enabled},
            "workspace": {"base_branch": "main"}, "_base_url": "http://base"}


def _ts(**over):
    ts = {"status": "in_review", "slug": "t-1-slug", "branch": "t-1-branch",
          "summary": "Do the thing", "prs": [{"repo": "myrepo", "id": 7, "url": "http://pr/7"}]}
    ts.update(over)
    return ts


class TestPrBaseMoved:
    def test_true_when_never_synced(self):
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="sha1"):
            assert tickets._pr_base_moved(_cfg(), _ts()) is True

    def test_false_when_synced_to_same_sha(self):
        ts = _ts(base_sync={"myrepo/7": {"base_synced": True, "base_sync_sha": "sha1"}})
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="sha1"):
            assert tickets._pr_base_moved(_cfg(), ts) is False

    def test_true_when_base_advanced(self):
        ts = _ts(base_sync={"myrepo/7": {"base_synced": True, "base_sync_sha": "old"}})
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="new"):
            assert tickets._pr_base_moved(_cfg(), ts) is True


class TestSyncPrBase:
    def test_disabled_flag_noop(self):
        with patch("features.tickets.branch_sync.sync_branch_with_base") as mock_sync:
            tickets._sync_pr_base(_cfg(enabled=False), {"key": "T-1"}, _ts(), "http://base")
        mock_sync.assert_not_called()

    def test_synced_clears_ci_and_logs(self):
        ts = _ts(ci_passed=True, checks_started_at="2026-01-01T00:00:00Z")
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=MagicMock()), \
             patch("features.tickets.branch_sync.sync_branch_with_base",
                   return_value={"result": "synced", "base": "main"}), \
             patch("features.tickets.log.emit") as mock_emit:
            tickets._sync_pr_base(_cfg(), {"key": "T-1", "summary": "x"}, ts, "http://base")
        assert "ci_passed" not in ts and "checks_started_at" not in ts
        assert any(c.args[0] == "ticket_base_synced" for c in mock_emit.call_args_list)

    def test_pushes_pr_branch_when_pr_has_own_branch(self):
        ts = _ts(prs=[{"repo": "myrepo", "id": 7, "url": "http://pr/7",
                       "branch": "t-1-other-branch"},
                      {"repo": "repo2", "id": 8, "url": "http://pr/8"}])
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=MagicMock()), \
             patch("features.tickets.branch_sync.sync_branch_with_base",
                   return_value={"result": "skip"}) as mock_sync, \
             patch("features.tickets.log.emit"):
            tickets._sync_pr_base(_cfg(), {"key": "T-1", "summary": "x"}, ts, "http://base")
        branches = [c.args[3] for c in mock_sync.call_args_list]
        assert branches == ["t-1-other-branch", "t-1-branch"]

    def test_capped_merge_failure_logs(self):
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=MagicMock()), \
             patch("features.tickets.branch_sync.sync_branch_with_base",
                   return_value={"result": "merge_failed", "error": "x", "attempts": 2, "capped": True}), \
             patch("features.tickets.log.emit") as mock_emit:
            tickets._sync_pr_base(_cfg(), {"key": "T-1", "summary": "x"}, _ts(), "http://base")
        assert any(c.args[0] == "ticket_base_sync_failed" for c in mock_emit.call_args_list)
