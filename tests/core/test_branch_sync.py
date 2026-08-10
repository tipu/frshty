from unittest.mock import patch, MagicMock

import core.branch_sync as branch_sync


def _platform(merge_ok=True, push_ok=True, error=""):
    p = MagicMock()
    p.merge_base.return_value = {"ok": merge_ok, "error": error}
    p.push_branch.return_value = {"ok": push_ok, "error": error}
    return p


def _sync(platform, st, base_sha="basesha", behind="3", worktree="/wt", fetch_rc=0, dirty=""):
    fetch = MagicMock(returncode=fetch_rc, stdout="")
    rev = MagicMock(returncode=0, stdout=f"{behind}\n")
    status = MagicMock(returncode=0, stdout=dirty)
    with patch("core.branch_sync.ls_remote_sha", return_value=base_sha), \
         patch("core.branch_sync.subprocess.run", side_effect=[fetch, rev, status]):
        return branch_sync.sync_branch_with_base(
            platform, "/repo", "main", "feature", st, lambda: worktree)


class TestLsRemoteSha:
    def test_parses_first_field(self):
        with patch("core.branch_sync.subprocess.run",
                   return_value=MagicMock(returncode=0, stdout="abc123\trefs/heads/main\n")):
            assert branch_sync.ls_remote_sha("/repo", "main") == "abc123"

    def test_failure_returns_empty(self):
        with patch("core.branch_sync.subprocess.run",
                   return_value=MagicMock(returncode=1, stdout="")):
            assert branch_sync.ls_remote_sha("/repo", "main") == ""


class TestSyncBranchWithBase:
    def test_no_base(self):
        assert branch_sync.sync_branch_with_base(_platform(), "/repo", "", "f", {}, lambda: "/wt")["result"] == "no_base"

    def test_ls_remote_failure(self):
        with patch("core.branch_sync.ls_remote_sha", return_value=""):
            out = branch_sync.sync_branch_with_base(_platform(), "/repo", "main", "f", {}, lambda: "/wt")
        assert out["result"] == "ls_remote_failed"

    def test_skip_when_already_synced(self):
        st = {"base_sync_sha": "basesha", "base_synced": True}
        with patch("core.branch_sync.ls_remote_sha", return_value="basesha"):
            out = branch_sync.sync_branch_with_base(_platform(), "/repo", "main", "f", st, lambda: "/wt")
        assert out["result"] == "skip"

    def test_uptodate_marks_synced_without_merge(self):
        platform = _platform()
        st = {}
        out = _sync(platform, st, behind="0")
        assert out["result"] == "uptodate"
        assert st["base_synced"] is True
        platform.merge_base.assert_not_called()

    def test_behind_merges_and_pushes(self):
        platform = _platform()
        st = {}
        out = _sync(platform, st, behind="4")
        assert out["result"] == "synced"
        assert st["base_synced"] is True
        platform.merge_base.assert_called_once()
        platform.push_branch.assert_called_once()

    def test_merge_failure_increments_and_caps(self):
        platform = _platform(merge_ok=False, error="conflict")
        st = {"base_sync_sha": "basesha", "base_sync_attempts": 1}
        out = _sync(platform, st, behind="4")
        assert out["result"] == "merge_failed"
        assert out["capped"] is True
        assert st["base_sync_attempts"] == 2
        platform.push_branch.assert_not_called()

    def test_push_failure_increments(self):
        platform = _platform(push_ok=False, error="non-ff")
        st = {}
        out = _sync(platform, st, behind="4")
        assert out["result"] == "push_failed"
        assert st["base_sync_attempts"] == 1
        assert st.get("base_synced") is not True

    def test_capped_skips_without_worktree(self):
        st = {"base_sync_sha": "basesha", "base_sync_attempts": branch_sync.MAX_BASE_SYNC_ATTEMPTS}
        called = []
        with patch("core.branch_sync.ls_remote_sha", return_value="basesha"):
            out = branch_sync.sync_branch_with_base(
                _platform(), "/repo", "main", "f", st, lambda: called.append(1) or "/wt")
        assert out["result"] == "capped"
        assert called == []

    def test_base_advance_resets_attempts(self):
        platform = _platform()
        st = {"base_sync_sha": "oldsha", "base_sync_attempts": 2, "base_synced": True}
        out = _sync(platform, st, base_sha="newsha", behind="0")
        assert out["result"] == "uptodate"
        assert st["base_sync_sha"] == "newsha"
        assert st["base_sync_attempts"] == 0


class TestDirtyWorktree:
    def test_dirty_worktree_blocks_merge_and_caps(self, tmp_path):
        import core.branch_sync as bs
        platform = MagicMock()
        st = {}

        def fake_run(cmd, **kw):
            out = MagicMock(returncode=0, stdout="", stderr="")
            if cmd[:2] == ["git", "rev-list"]:
                out.stdout = "3"
            elif cmd[:3] == ["git", "status", "--porcelain"]:
                out.stdout = " M package.json\n M pnpm-lock.yaml\n"
            return out

        with patch("core.branch_sync.ls_remote_sha", return_value="sha1"), \
             patch("core.branch_sync.subprocess.run", side_effect=fake_run):
            out = bs.sync_branch_with_base(platform, "/repo", "main", "br", st,
                                           lambda: tmp_path)

        assert out["result"] == "dirty_worktree"
        assert out["capped"] is True
        assert "package.json" in out["error"]
        assert st["base_sync_attempts"] == bs.MAX_BASE_SYNC_ATTEMPTS
        assert not st.get("base_synced")
        platform.merge_base.assert_not_called()
