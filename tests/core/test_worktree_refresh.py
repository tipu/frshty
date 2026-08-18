import subprocess
from pathlib import Path

import pytest

from core import git_util


def _run(cwd, *args):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True, check=True)


@pytest.fixture()
def repo_pair(tmp_path):
    origin = tmp_path / "origin"
    origin.mkdir()
    _run(origin, "init", "-q", "-b", "main")
    _run(origin, "config", "user.email", "t@t")
    _run(origin, "config", "user.name", "t")
    (origin / "base.txt").write_text("one\n")
    _run(origin, "add", "-A")
    _run(origin, "commit", "-qm", "base")

    wt = tmp_path / "wt"
    _run(tmp_path, "clone", "-q", str(origin), str(wt))
    _run(wt, "config", "user.email", "t@t")
    _run(wt, "config", "user.name", "t")

    (origin / "base.txt").write_text("one\ntwo\n")
    _run(origin, "add", "-A")
    _run(origin, "commit", "-qm", "base moves on")
    return origin, wt


class TestRefreshWorktreeOntoBase:
    def test_resets_when_branch_has_no_own_commits(self, repo_pair):
        _origin, wt = repo_pair
        out = git_util.refresh_worktree_onto_base(wt, "main")
        assert out["result"] == "reset"
        assert out["ahead"] == 0
        assert (wt / "base.txt").read_text() == "one\ntwo\n"

    def test_preserves_unpushed_commits_instead_of_resetting(self, repo_pair):
        _origin, wt = repo_pair
        (wt / "feature.txt").write_text("my work\n")
        _run(wt, "add", "-A")
        _run(wt, "commit", "-qm", "DEV-636 implementation")
        sha = _run(wt, "rev-parse", "HEAD").stdout.strip()

        out = git_util.refresh_worktree_onto_base(wt, "main")

        assert out["result"] == "merged"
        assert out["ahead"] == 1
        assert (wt / "feature.txt").read_text() == "my work\n", "unpushed work must survive"
        assert (wt / "base.txt").read_text() == "one\ntwo\n", "base must still be refreshed"
        log = _run(wt, "log", "--format=%H").stdout
        assert sha in log, "the original commit must remain reachable"


class TestUnreadableCountNeverResets:
    """A failed rev-list returns empty stdout, which reads as a genuine zero.

    This function resets when the count is zero, so treating the failure as
    "nothing to lose" would destroy the commits it exists to protect. It is the
    same defect this function was written to fix.
    """

    def test_a_failing_count_does_not_reset(self, tmp_path):
        import subprocess
        from unittest.mock import patch
        wt = tmp_path / "wt"
        wt.mkdir()

        real = subprocess.run
        def fake(cmd, *a, **k):
            if cmd[:3] == ["git", "rev-list", "--count"]:
                return subprocess.CompletedProcess(cmd, 128, "", "fatal: bad revision")
            if cmd[:2] == ["git", "fetch"]:
                return subprocess.CompletedProcess(cmd, 0, "", "")
            raise AssertionError(f"must not run {cmd[:3]} after an unreadable count")

        with patch.object(git_util.subprocess, "run", side_effect=fake):
            out = git_util.refresh_worktree_onto_base(wt, "main")
        assert out["result"] == "unknown_ahead"
        assert "bad revision" in out["error"]

    def test_a_readable_zero_still_resets(self, tmp_path):
        import subprocess
        from unittest.mock import patch
        wt = tmp_path / "wt"
        wt.mkdir()
        ran = []

        def fake(cmd, *a, **k):
            ran.append(cmd[:3])
            if cmd[:3] == ["git", "rev-list", "--count"]:
                return subprocess.CompletedProcess(cmd, 0, "0\n", "")
            return subprocess.CompletedProcess(cmd, 0, "", "")

        with patch.object(git_util.subprocess, "run", side_effect=fake):
            out = git_util.refresh_worktree_onto_base(wt, "main")
        assert out["result"] == "reset"
        assert ["git", "reset", "--hard"] in ran
