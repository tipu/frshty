"""Hooks that a tool writes into an in-tree core.hooksPath are not ticket work.

The failure this guards: quartz-ios sets core.hooksPath to `.husky/_`, and
git-lfs wrote its hooks there in every ticket worktree. The untracked
directory made enter_testing fail with "worktree has uncommitted changes",
and a leftover commit would have put the hooks on the pull request.
"""
import subprocess
from unittest.mock import patch

from core.tasks import tickets as T


def _git(repo, *args):
    return subprocess.run(["git", *args], cwd=str(repo), check=True,
                          capture_output=True, text=True).stdout


def _workspace_repo(ticket_dir, hooks_path=".husky/_"):
    repo = ticket_dir / "workspace" / "app"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "a.py").write_text("x = 1\n")
    (repo / ".husky").mkdir()
    (repo / ".husky" / "pre-commit").write_text("true\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")
    if hooks_path:
        _git(repo, "config", "core.hooksPath", hooks_path)
    return repo


def _lfs_hooks(repo):
    hooks = repo / ".husky" / "_"
    hooks.mkdir(parents=True, exist_ok=True)
    for name in ("post-checkout", "post-commit", "post-merge"):
        (hooks / name).write_text("#!/bin/sh\nexit 0\n")
        (hooks / name).chmod(0o755)


def test_untracked_hooks_dir_is_not_dirty(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    _lfs_hooks(repo)
    assert T._dirty_workspace_repos(ticket_dir) == []


def test_real_change_next_to_hooks_dir_is_dirty(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    _lfs_hooks(repo)
    (repo / "a.py").write_text("x = 2\n")
    assert T._dirty_workspace_repos(ticket_dir) == ["app"]


def test_untracked_file_beside_hooks_dir_is_dirty(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    (repo / ".husky" / "commit-msg").write_text("true\n")
    assert T._dirty_workspace_repos(ticket_dir) == ["app"]


def test_hooks_dir_counts_without_in_tree_hooks_path(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir, hooks_path=None)
    _lfs_hooks(repo)
    assert T._dirty_workspace_repos(ticket_dir) == ["app"]


def test_leftover_commit_leaves_hooks_out(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    _lfs_hooks(repo)
    (repo / "a.py").write_text("x = 2\n")

    committed = T._commit_workspace_changes(ticket_dir, "PROJ-1",
                                            message="fix: PROJ-1 raise x")

    assert committed == ["app"]
    files = _git(repo, "show", "--name-only", "--format=", "HEAD").split()
    assert files == ["a.py"]
    assert "?? .husky/_/" in _git(repo, "status", "--porcelain")


def test_describe_commit_leaves_hooks_out(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    _lfs_hooks(repo)
    (repo / "a.py").write_text("x = 2\n")

    with patch("core.commit_message.run_haiku", return_value="Raise x") as haiku:
        T._commit_workspace_changes(ticket_dir, "PROJ-1", describe=True)

    assert "post-checkout" not in haiku.call_args[0][0]
    files = _git(repo, "show", "--name-only", "--format=", "HEAD").split()
    assert files == ["a.py"]


def test_only_hooks_dir_commits_nothing(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    _lfs_hooks(repo)
    head = _git(repo, "rev-parse", "HEAD")

    assert T._commit_workspace_changes(ticket_dir, "PROJ-1") == []
    assert _git(repo, "rev-parse", "HEAD") == head


def _bare_hooks_repo(ticket_dir):
    repo = ticket_dir / "workspace" / "app"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "a.py").write_text("x = 1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "base")
    _git(repo, "config", "core.hooksPath", ".husky/_")
    return repo


def test_collapsed_parent_of_hooks_dir_is_not_dirty(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _bare_hooks_repo(ticket_dir)
    _lfs_hooks(repo)
    assert "?? .husky/\n" in _git(repo, "status", "--porcelain")
    assert T._dirty_workspace_repos(ticket_dir) == []


def test_collapsed_parent_keeps_hooks_out_of_commit(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _bare_hooks_repo(ticket_dir)
    _lfs_hooks(repo)
    (repo / "a.py").write_text("x = 2\n")

    T._commit_workspace_changes(ticket_dir, "PROJ-1", message="fix: PROJ-1 raise x")

    files = _git(repo, "show", "--name-only", "--format=", "HEAD").split()
    assert files == ["a.py"]


def test_collapsed_parent_with_other_file_is_dirty(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _bare_hooks_repo(ticket_dir)
    _lfs_hooks(repo)
    (repo / ".husky" / "pre-commit").write_text("true\n")
    assert T._dirty_workspace_repos(ticket_dir) == ["app"]


def test_tracked_deletion_in_hooks_dir_is_committed(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir)
    (repo / ".husky" / "_").mkdir()
    (repo / ".husky" / "_" / "tracked").write_text("x\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "tracked hook")
    _lfs_hooks(repo)
    (repo / ".husky" / "_" / "tracked").unlink()
    (repo / "a.py").write_text("x = 2\n")

    T._commit_workspace_changes(ticket_dir, "PROJ-1", message="fix: PROJ-1 drop hook")

    files = sorted(_git(repo, "show", "--name-only", "--format=", "HEAD").split())
    assert files == [".husky/_/tracked", "a.py"]
    assert _git(repo, "status", "--porcelain").splitlines() == ["?? .husky/_/"]


def test_quoted_hooks_dir_is_not_dirty_and_stays_out_of_commit(tmp_path):
    ticket_dir = tmp_path / "PROJ-1-x"
    repo = _workspace_repo(ticket_dir, hooks_path="hüsky/_")
    hooks = repo / "hüsky" / "_"
    hooks.mkdir(parents=True)
    (hooks / "post-checkout").write_text("#!/bin/sh\nexit 0\n")
    assert _git(repo, "status", "--porcelain").startswith('?? "h')
    assert T._dirty_workspace_repos(ticket_dir) == []

    (repo / "a.py").write_text("x = 2\n")
    T._commit_workspace_changes(ticket_dir, "PROJ-1", message="fix: PROJ-1 raise x")
    files = _git(repo, "show", "--name-only", "--format=", "HEAD").split()
    assert files == ["a.py"]
