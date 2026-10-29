import subprocess
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import core.branch_sync as branch_sync


def _platform(merge_ok=True, push_ok=True, error=""):
    p = MagicMock()
    p.merge_base.return_value = {"ok": merge_ok, "error": error}
    p.push_branch.return_value = {"ok": push_ok, "error": error}
    return p


def _sync(platform, st, base_sha="basesha", behind="3", worktree="/wt", fetch_rc=0, dirty=""):
    fetch = MagicMock(returncode=fetch_rc, stdout="", stderr="")
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
        st = {"base_sync_sha": "basesha", "base_sync_attempts": branch_sync.MAX_BASE_SYNC_ATTEMPTS,
              "base_sync_head": "basesha",
              "base_sync_blocked_at": datetime.now(timezone.utc).isoformat()}
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


class TestIdentityPark:
    def _ls(self, heads):
        return lambda repo, ref: heads.get(ref, "")

    def test_identity_refusal_parks_on_the_branch_head(self):
        platform = _platform(push_ok=False, error="1 commit(s) on feature are not authored by me")
        platform.push_branch.return_value["identity_blocked"] = True
        st = {}
        out = _sync(platform, st, behind="4")
        assert out["result"] == "push_failed"
        assert st["base_sync_parked"] == {"head": "basesha", "base": "main"}

    def test_other_push_failure_does_not_park(self):
        st = {}
        _sync(_platform(push_ok=False, error="non-ff"), st, behind="4")
        assert "base_sync_parked" not in st

    def test_parked_sync_ignores_a_moved_base(self):
        platform = _platform()
        st = {"base_sync_sha": "oldbase", "base_sync_parked": {"head": "h1", "base": "main"}}
        with patch("core.branch_sync.ls_remote_sha",
                   side_effect=self._ls({"refs/heads/feature": "h1", "main": "newbase"})):
            out = branch_sync.sync_branch_with_base(platform, "/repo", "main", "feature", st, lambda: "/wt")
        assert out["result"] == "parked"
        assert st["base_sync_sha"] == "oldbase"
        platform.merge_base.assert_not_called()

    def test_new_branch_head_unparks(self):
        st = {"base_sync_parked": {"head": "h1", "base": "main"}}
        with patch("core.branch_sync.ls_remote_sha", side_effect=self._ls({"refs/heads/feature": "h2"})):
            assert branch_sync.parked("/repo", "feature", "main", st) is False
        assert "base_sync_parked" not in st

    def test_new_base_branch_unparks(self):
        st = {"base_sync_parked": {"head": "h1", "base": "stack-1"}}
        with patch("core.branch_sync.ls_remote_sha", side_effect=self._ls({"refs/heads/feature": "h1"})):
            assert branch_sync.parked("/repo", "feature", "main", st) is False
        assert "base_sync_parked" not in st


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


class TestASyncThatCannotStartSpendsAnAttempt:
    def test_no_worktree_caps_after_max_attempts(self):
        st = {}
        with patch("core.branch_sync.ls_remote_sha", return_value="basesha"):
            outs = [branch_sync.sync_branch_with_base(_platform(), "/repo", "main", "f", st,
                                                      lambda: None)
                    for _ in range(branch_sync.MAX_BASE_SYNC_ATTEMPTS + 1)]
        assert [o["result"] for o in outs] == ["no_worktree"] * branch_sync.MAX_BASE_SYNC_ATTEMPTS + ["capped"]
        assert outs[branch_sync.MAX_BASE_SYNC_ATTEMPTS - 1]["capped"] is True

    def test_fetch_failure_spends_an_attempt(self):
        st = {}
        out = _sync(_platform(), st, fetch_rc=1)
        assert out["result"] == "fetch_failed"
        assert st["base_sync_attempts"] == 1


class TestDirtyBlockClears:
    def _repo(self, tmp_path):
        wt = tmp_path / "wt"
        wt.mkdir()
        for cmd in (["init", "-q"], ["config", "user.email", "t@t"], ["config", "user.name", "t"],
                    ["commit", "-q", "--allow-empty", "-m", "init"]):
            subprocess.run(["git", *cmd], cwd=wt, check=True, capture_output=True)
        return wt

    def _capped(self):
        return {"base_sync_sha": "basesha", "base_sync_attempts": branch_sync.MAX_BASE_SYNC_ATTEMPTS,
                "last_base_error": f"{"worktree has uncommitted changes"} (a.py); merge would overwrite them",
                "base_sync_blocked_at": datetime.now(timezone.utc).isoformat()}

    def test_clean_worktree_resets_a_dirty_cap_and_merges(self, tmp_path):
        wt = self._repo(tmp_path)
        platform = _platform()
        st = self._capped()
        real_run = subprocess.run

        def fake_run(cmd, **kw):
            if cmd[:2] == ["git", "fetch"]:
                return MagicMock(returncode=0, stdout="", stderr="")
            if cmd[:2] == ["git", "rev-list"]:
                return MagicMock(returncode=0, stdout="3\n", stderr="")
            return real_run(cmd, **kw)

        with patch("core.branch_sync.ls_remote_sha", return_value="basesha"), \
             patch("core.branch_sync.subprocess.run", side_effect=fake_run):
            out = branch_sync.sync_branch_with_base(platform, "/repo", "main", "f", st, lambda: wt)
        assert out["result"] == "synced"
        assert st["base_sync_attempts"] == 0
        assert "last_base_error" not in st
        platform.merge_base.assert_called_once()

    def test_still_dirty_worktree_stays_capped(self, tmp_path):
        wt = self._repo(tmp_path)
        (wt / "a.py").write_text("x")
        platform = _platform()
        st = self._capped()
        with patch("core.branch_sync.ls_remote_sha", return_value="basesha"):
            out = branch_sync.sync_branch_with_base(platform, "/repo", "main", "f", st, lambda: wt)
        assert out["result"] == "capped"
        assert st["base_sync_attempts"] == branch_sync.MAX_BASE_SYNC_ATTEMPTS
        platform.merge_base.assert_not_called()

    def test_merge_failure_cap_is_not_reset(self, tmp_path):
        wt = self._repo(tmp_path)
        platform = _platform()
        st = {"base_sync_sha": "basesha", "base_sync_attempts": branch_sync.MAX_BASE_SYNC_ATTEMPTS,
              "last_base_error": "CONFLICT in a.py", "base_sync_head": "basesha",
              "base_sync_blocked_at": datetime.now(timezone.utc).isoformat()}
        calls = []
        with patch("core.branch_sync.ls_remote_sha", return_value="basesha"):
            out = branch_sync.sync_branch_with_base(platform, "/repo", "main", "f", st,
                                                    lambda: calls.append(1) or wt)
        assert out["result"] == "capped"
        assert calls == []
        platform.merge_base.assert_not_called()


class TestSharedHeadsListing:
    def _sync(self, local_refs, heads):
        calls = []

        def fake_run(cmd, **kw):
            calls.append(cmd)
            return MagicMock(returncode=0, stdout="0\n", stderr="")

        with patch("core.branch_sync.ls_remote_sha") as per_branch, \
             patch("core.branch_sync.git_util.remote_ref_sha",
                   side_effect=lambda wt, ref: local_refs.get(ref, "")), \
             patch("core.branch_sync.subprocess.run", side_effect=fake_run):
            out = branch_sync.sync_branch_with_base(_platform(), "/repo", "main", "f", {},
                                                    lambda: "/wt", heads=heads)
        per_branch.assert_not_called()
        return out, [c for c in calls if c[:2] == ["git", "fetch"]]

    def test_refs_already_fetched_are_not_fetched_again(self):
        out, fetches = self._sync({"main": "basesha", "f": "headsha"},
                                  {"main": "basesha", "f": "headsha"})
        assert out["result"] == "uptodate"
        assert fetches == []

    def test_moved_base_is_fetched(self):
        out, fetches = self._sync({"main": "oldsha", "f": "headsha"},
                                  {"main": "basesha", "f": "headsha"})
        assert out["result"] == "uptodate"
        assert fetches == [["git", "fetch", "origin", "main", "f"]]

    def test_moved_branch_is_fetched(self):
        out, fetches = self._sync({"main": "basesha", "f": "oldhead"},
                                  {"main": "basesha", "f": "headsha"})
        assert out["result"] == "uptodate"
        assert fetches == [["git", "fetch", "origin", "main", "f"]]

    def test_base_missing_from_the_listing_reads_as_a_failed_ls_remote(self):
        out, _ = self._sync({"main": "basesha"}, {})
        assert out["result"] == "ls_remote_failed"

    def test_parked_reads_the_branch_head_from_the_listing(self):
        st = {"base_sync_parked": {"head": "h1", "base": "main"}}
        with patch("core.branch_sync.ls_remote_sha") as per_branch:
            assert branch_sync.parked("/repo", "feature", "main", st, {"feature": "h1"}) is True
        per_branch.assert_not_called()
