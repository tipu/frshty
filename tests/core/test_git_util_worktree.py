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


def test_returns_none_when_holder_is_not_visible(tmp_path):
    repo = tmp_path / "repo"
    _init_repo(repo)
    _git(repo, "branch", "feature/w")
    holder = tmp_path / "host_wt"
    _git(repo, "worktree", "add", str(holder), "feature/w")
    holder.rename(tmp_path / "elsewhere")

    pr_wt = tmp_path / "pr_wt"
    result = git_util.add_or_reuse_worktree(repo, pr_wt, "feature/w", base_branch="main")

    assert result is None
    assert not pr_wt.exists()


def _clone_with_origin(tmp_path: Path, branch: str) -> Path:
    origin = tmp_path / "origin.git"
    seed = tmp_path / "seed"
    _init_repo(seed)
    _git(seed, "branch", branch)
    subprocess.run(["git", "clone", "-q", "--bare", str(seed), str(origin)], check=True)
    repo = tmp_path / "repo"
    subprocess.run(["git", "clone", "-q", str(origin), str(repo)], check=True)
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    return repo


def test_detached_worktree_does_not_borrow_a_dirty_holder(tmp_path):
    repo = _clone_with_origin(tmp_path, "feature/d")
    holder = tmp_path / "agent_wt"
    _git(repo, "worktree", "add", str(holder), "feature/d")
    (holder / "f.txt").write_text("agent edit in progress")

    pr_wt = tmp_path / "pr_wt"
    result = git_util.add_detached_worktree(repo, pr_wt, "feature/d")

    assert result is not None
    assert result.resolve() == pr_wt.resolve()
    assert (pr_wt / "f.txt").read_text() == "x"
    status = subprocess.run(["git", "status", "--porcelain"], cwd=str(pr_wt),
                            capture_output=True, text=True, check=True)
    assert status.stdout == ""
    origin_tip = subprocess.run(["git", "rev-parse", "origin/feature/d"], cwd=str(repo),
                                capture_output=True, text=True, check=True).stdout.strip()
    assert git_util.head_sha(pr_wt) == origin_tip
    assert (holder / "f.txt").read_text() == "agent edit in progress"


def test_detached_worktree_returns_none_for_a_missing_remote_branch(tmp_path):
    repo = _clone_with_origin(tmp_path, "feature/e")

    pr_wt = tmp_path / "pr_wt"
    assert git_util.add_detached_worktree(repo, pr_wt, "no/such-branch") is None
    assert not pr_wt.exists()
