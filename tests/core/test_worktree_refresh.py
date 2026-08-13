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
