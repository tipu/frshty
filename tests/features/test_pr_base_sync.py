import subprocess
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

    def test_false_when_capped_on_current_sha(self):
        ts = _ts(base_sync={"myrepo/7": {"base_sync_sha": "sha1", "base_sync_attempts": 2}})
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="sha1"):
            assert tickets._pr_base_moved(_cfg(), ts) is False

    def test_true_when_capped_but_base_advanced(self):
        ts = _ts(base_sync={"myrepo/7": {"base_sync_sha": "old", "base_sync_attempts": 2}})
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="new"):
            assert tickets._pr_base_moved(_cfg(), ts) is True

    def test_true_when_under_attempt_cap(self):
        ts = _ts(base_sync={"myrepo/7": {"base_sync_sha": "sha1", "base_sync_attempts": 1}})
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="sha1"):
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


def _dev_cfg():
    return {"pr": {"auto_update_branch": True},
            "workspace": {"base_branch": "main", "base_branches": {"myrepo": "development"}},
            "_base_url": "http://base"}


def _main_pr_ts(**over):
    return _ts(prs=[{"repo": "myrepo", "id": 25, "url": "http://pr/25", "base": "main"}], **over)


class TestPrTargetBase:
    def test_base_moved_reads_the_pr_base_not_the_configured_base(self):
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="sha1") as mock_sha:
            tickets._pr_base_moved(_dev_cfg(), _main_pr_ts())
        assert mock_sha.call_args.args[1] == "main"

    def test_base_moved_falls_back_to_the_configured_base(self):
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.ls_remote_sha", return_value="sha1") as mock_sha:
            tickets._pr_base_moved(_dev_cfg(), _ts())
        assert mock_sha.call_args.args[1] == "development"

    def test_sync_merges_the_pr_base_not_the_configured_base(self):
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.sync_branch_with_base",
                   return_value={"result": "synced", "base": "main"}) as mock_sync, \
             patch("features.tickets.log.emit") as mock_emit:
            tickets._sync_pr_base(_dev_cfg(), {"key": "T-1", "summary": "x"}, _main_pr_ts(), "http://base")
        assert mock_sync.call_args.args[2] == "main"
        synced = [c for c in mock_emit.call_args_list if c.args[0] == "ticket_base_synced"]
        assert synced and synced[0].kwargs["meta"]["base"] == "main"

    def test_sync_falls_back_to_the_configured_base(self):
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.sync_branch_with_base",
                   return_value={"result": "skip"}) as mock_sync, \
             patch("features.tickets.log.emit"):
            tickets._sync_pr_base(_dev_cfg(), {"key": "T-1", "summary": "x"}, _ts(), "http://base")
        assert mock_sync.call_args.args[2] == "development"

    def test_conflict_resolution_merges_the_pr_base(self, tmp_path):
        platform = MagicMock()
        platform.sync_remote_branch.return_value = {"ok": True}
        platform.merge_base.return_value = {"ok": True}
        platform.push_branch.return_value = {"ok": True}
        info = {("myrepo", 25): {"mergeable": "CONFLICTING"}}
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.ticket_worktree_path", return_value=tmp_path), \
             patch("features.tickets.subprocess.run"), \
             patch("features.tickets.log.emit"):
            tickets._resolve_conflicts(_dev_cfg(), {"key": "T-1", "summary": "x"},
                                       _main_pr_ts(), "http://base", pr_info_map=info)
        assert platform.merge_base.call_args.args[1] == "main"

    def test_missing_worktree_is_created_from_the_pr_base(self, tmp_path):
        ts = _main_pr_ts()
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=tmp_path / "wt"), \
             patch("features.tickets.git_util.add_or_reuse_worktree",
                   return_value=tmp_path / "wt") as mock_add:
            tickets._ensure_pr_worktree(_dev_cfg(), {"key": "T-1"}, ts, ts["prs"][0], "http://base")
        assert mock_add.call_args.args[3] == "main"


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True)


class TestEnsurePrWorktree:
    def test_creates_a_worktree_on_the_pr_branch_when_the_ticket_has_none(self, tmp_path):
        origin = tmp_path / "origin.git"
        seed = tmp_path / "seed"
        clone = tmp_path / "clone"
        _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
        _git(tmp_path, "clone", "-q", str(origin), str(seed))
        _git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "base")
        _git(seed, "push", "-q", "origin", "HEAD:main")
        _git(tmp_path, "clone", "-q", str(origin), str(clone))
        _git(seed, "checkout", "-q", "-b", "pr-branch")
        _git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "pr")
        _git(seed, "push", "-q", "origin", "pr-branch")
        wt = tmp_path / "tickets" / "t-1-slug" / "myrepo"
        ts = _ts(prs=[{"repo": "myrepo", "id": 7, "url": "http://pr/7", "branch": "pr-branch"}])

        with patch("features.tickets._ticket_repo_path", return_value=str(clone)), \
             patch("features.tickets.ticket_worktree_path", return_value=wt):
            got = tickets._ensure_pr_worktree(_cfg(), {"key": "T-1"}, ts, ts["prs"][0], "http://base")

        assert got == wt
        head = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(wt),
                              capture_output=True, text=True).stdout.strip()
        assert head == "pr-branch"

    def test_a_failed_creation_is_reported(self, tmp_path):
        wt = tmp_path / "tickets" / "t-1-slug" / "myrepo"
        ts = _ts()
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.git_util.add_or_reuse_worktree", return_value=None), \
             patch("features.tickets.log.emit") as mock_emit:
            got = tickets._ensure_pr_worktree(_cfg(), {"key": "T-1"}, ts, ts["prs"][0], "http://base")
        assert got is None
        assert any(c.args[0] == "ticket_pr_worktree_failed" for c in mock_emit.call_args_list)
