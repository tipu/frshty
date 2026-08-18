"""A failed git command must never be handed back as a fact.

subprocess.run returns an object whose .stdout is empty both when a command had
nothing to report and when it failed. Reading only .stdout turned failures into
facts across this codebase: a failed `git diff` became "no changes" and marked a
ticket merged, a failed `git status` became "clean" and skipped committing work,
a failed `git rev-list` became "no commits to lose" and hard-reset the branch.

These use real repositories. The reset path destroys data, so it is not worth
asserting against a mock of the thing being tested.
"""
import subprocess
from unittest.mock import patch

import pytest

from core import git_util


def _run(cwd, *args):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True,
                          text=True, check=True)


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


class TestRunGit:
    def test_a_failure_raises_instead_of_returning_empty_output(self, repo_pair):
        _o, wt = repo_pair
        with pytest.raises(git_util.GitCommandError) as e:
            git_util.run_git(wt, ["rev-list", "--count", "origin/nope..HEAD"])
        assert "rev-list" in str(e.value)

    def test_success_returns_stdout(self, repo_pair):
        _o, wt = repo_pair
        assert git_util.run_git(wt, ["rev-list", "--count", "HEAD"]).stdout.strip() == "1"

    def test_an_expected_nonzero_can_be_allowed(self, repo_pair):
        """diff --quiet exits 1 to mean "there are changes". That is information,
        not failure, so a caller opts into the code it expects."""
        _o, wt = repo_pair
        (wt / "base.txt").write_text("edited\n")
        assert git_util.run_git(wt, ["diff", "--quiet"], allowed_codes=(0, 1)).returncode == 1

    def test_is_dirty_reports_real_state(self, repo_pair):
        _o, wt = repo_pair
        assert git_util.is_dirty(wt) is False
        (wt / "base.txt").write_text("edited\n")
        assert git_util.is_dirty(wt) is True
        (wt / "untracked.txt").write_text("new\n")
        assert git_util.is_dirty(wt) is True

    def test_is_dirty_raises_rather_than_reporting_clean(self, tmp_path):
        not_a_repo = tmp_path / "plain"
        not_a_repo.mkdir()
        with pytest.raises(git_util.GitCommandError):
            git_util.is_dirty(not_a_repo)


class TestRefreshNeverDestroysUncommittedWork:
    def test_a_dirty_worktree_is_not_reset(self, repo_pair):
        """ahead == 0 says nothing about the working tree. reset --hard plus
        clean -fd would delete tracked edits and untracked files alike."""
        _o, wt = repo_pair
        (wt / "base.txt").write_text("uncommitted edit\n")
        (wt / "scratch.txt").write_text("untracked\n")

        out = git_util.refresh_worktree_onto_base(wt, "main")

        assert out["result"] == "dirty"
        assert (wt / "base.txt").read_text() == "uncommitted edit\n"
        assert (wt / "scratch.txt").exists()

    def test_a_clean_worktree_with_no_commits_still_resets(self, repo_pair):
        _o, wt = repo_pair
        out = git_util.refresh_worktree_onto_base(wt, "main")
        assert out["result"] == "reset"
        assert (wt / "base.txt").read_text() == "one\ntwo\n"

    def test_an_unreadable_count_touches_nothing(self, repo_pair):
        _o, wt = repo_pair
        (wt / "base.txt").write_text("uncommitted edit\n")
        real = git_util.run_git

        def fail_count(cwd, args, **kw):
            if args[:2] == ["rev-list", "--count"]:
                raise git_util.GitCommandError(
                    args, subprocess.CompletedProcess(["git"], 128, "", "bad revision"))
            return real(cwd, args, **kw)

        with patch.object(git_util, "run_git", side_effect=fail_count):
            out = git_util.refresh_worktree_onto_base(wt, "main")

        assert out["result"] == "unknown"
        assert "bad revision" in out["error"]
        assert (wt / "base.txt").read_text() == "uncommitted edit\n"

    def test_a_failed_reset_is_not_reported_as_success(self, repo_pair):
        _o, wt = repo_pair
        real = git_util.run_git

        def fail_reset(cwd, args, **kw):
            if args[0] == "reset":
                raise git_util.GitCommandError(
                    args, subprocess.CompletedProcess(["git"], 1, "", "reset failed"))
            return real(cwd, args, **kw)

        with patch.object(git_util, "run_git", side_effect=fail_reset):
            out = git_util.refresh_worktree_onto_base(wt, "main")
        assert out["result"] == "unknown"
        assert "reset failed" in out["error"]
