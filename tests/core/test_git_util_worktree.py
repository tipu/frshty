"""Tests for git_util.add_or_reuse_worktree — the resolution path when a PR
branch is already checked out by another worktree."""

import subprocess
from pathlib import Path

import core.git_util as git_util


def _git(cwd: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, check=True)


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    _git(path, "init", "-b", "main")
    _git(path, "config", "user.email", "t@t")
    _git(path, "config", "user.name", "t")
    (path / "f.txt").write_text("x")
    _git(path, "add", "-A")
    _git(path, "commit", "-m", "init")


def test_reuses_worktree_already_holding_branch(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _git(repo, "branch", "feature/x")
    holder = tmp_path / "ticket_wt"
    _git(repo, "worktree", "add", str(holder), "feature/x")

    pr_wt = tmp_path / "pr_wt"
    result = git_util.add_or_reuse_worktree(repo, pr_wt, "feature/x", base_branch="main")

    assert result is not None
    assert result.resolve() == holder.resolve()
    assert not pr_wt.exists()


def test_frees_canonical_checkout_then_adds_worktree(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _git(repo, "checkout", "-b", "feature/y")

    pr_wt = tmp_path / "pr_wt"
    result = git_util.add_or_reuse_worktree(repo, pr_wt, "feature/y", base_branch="main")

    assert result is not None
    assert result.resolve() == pr_wt.resolve()
    head = subprocess.run(
        ["git", "rev-parse", "--abbrev-ref", "HEAD"],
        cwd=str(repo), capture_output=True, text=True,
    )
    assert head.stdout.strip() == "main"


def test_adds_fresh_worktree_when_branch_free(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _git(repo, "branch", "feature/z")

    pr_wt = tmp_path / "pr_wt"
    result = git_util.add_or_reuse_worktree(repo, pr_wt, "feature/z", base_branch="main")

    assert result is not None
    assert result.resolve() == pr_wt.resolve()
    assert (pr_wt / ".git").exists()
