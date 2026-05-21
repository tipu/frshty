"""Tests for the changed-files-based test-selection helper used by
run_tests_and_fix to skip repos the current ticket never touched.

The helper shells out to `git diff --name-only origin/<base>...HEAD`, so each
test builds a tiny git repo on disk, simulates an "origin" via a bare clone or
explicit ref, and asserts on what `_repo_has_changes_vs_base` returns.
"""
import subprocess

import pytest

from core.tasks.tickets import _repo_has_changes_vs_base


def _init_repo(path):
	path.mkdir(parents=True, exist_ok=True)
	subprocess.run(["git", "init", "-q", "-b", "main"], cwd=str(path), check=True)
	subprocess.run(["git", "config", "user.email", "test@test"], cwd=str(path), check=True)
	subprocess.run(["git", "config", "user.name", "test"], cwd=str(path), check=True)
	(path / "README.md").write_text("base\n")
	subprocess.run(["git", "add", "."], cwd=str(path), check=True)
	subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=str(path), check=True)


def _create_origin(path):
	"""Create an `origin` remote pointing at the current main, so
	`origin/main` resolves the way `_repo_has_changes_vs_base` expects."""
	origin = path.parent / f"{path.name}.git"
	subprocess.run(["git", "clone", "-q", "--bare", str(path), str(origin)],
	               check=True)
	subprocess.run(["git", "remote", "add", "origin", str(origin)],
	               cwd=str(path), check=True)
	subprocess.run(["git", "fetch", "-q", "origin"], cwd=str(path), check=True)


class TestRepoHasChangesVsBase:
	def test_no_commits_since_base_returns_false(self, tmp_path):
		repo = tmp_path / "repo"
		_init_repo(repo)
		_create_origin(repo)
		# Branch off main but make no changes
		subprocess.run(["git", "checkout", "-q", "-b", "feature"],
		               cwd=str(repo), check=True)
		assert _repo_has_changes_vs_base(repo, "main") is False

	def test_new_commit_with_added_file_returns_true(self, tmp_path):
		repo = tmp_path / "repo"
		_init_repo(repo)
		_create_origin(repo)
		subprocess.run(["git", "checkout", "-q", "-b", "feature"],
		               cwd=str(repo), check=True)
		(repo / "new.py").write_text("x = 1\n")
		subprocess.run(["git", "add", "."], cwd=str(repo), check=True)
		subprocess.run(["git", "commit", "-q", "-m", "add file"],
		               cwd=str(repo), check=True)
		assert _repo_has_changes_vs_base(repo, "main") is True

	def test_new_commit_with_modified_file_returns_true(self, tmp_path):
		repo = tmp_path / "repo"
		_init_repo(repo)
		_create_origin(repo)
		subprocess.run(["git", "checkout", "-q", "-b", "feature"],
		               cwd=str(repo), check=True)
		(repo / "README.md").write_text("changed\n")
		subprocess.run(["git", "add", "."], cwd=str(repo), check=True)
		subprocess.run(["git", "commit", "-q", "-m", "modify"],
		               cwd=str(repo), check=True)
		assert _repo_has_changes_vs_base(repo, "main") is True

	def test_uncommitted_changes_do_not_count(self, tmp_path):
		"""The helper compares COMMITTED state vs base; staging-area /
		working-tree noise should not be treated as ticket scope."""
		repo = tmp_path / "repo"
		_init_repo(repo)
		_create_origin(repo)
		subprocess.run(["git", "checkout", "-q", "-b", "feature"],
		               cwd=str(repo), check=True)
		(repo / "uncommitted.py").write_text("# not committed\n")
		assert _repo_has_changes_vs_base(repo, "main") is False

	def test_missing_origin_falls_back_to_true(self, tmp_path):
		"""If `origin/<base>` can't be resolved, treat as touched (safer to
		run tests than to silently skip a repo we couldn't introspect)."""
		repo = tmp_path / "repo"
		_init_repo(repo)
		assert _repo_has_changes_vs_base(repo, "main") is True

	def test_non_git_directory_falls_back_to_true(self, tmp_path):
		"""Same safety net for a directory that isn't a git repo at all
		(git diff exits non-zero, helper returns True so the caller's
		downstream test run is attempted rather than silently skipped)."""
		not_a_repo = tmp_path / "not_a_repo"
		not_a_repo.mkdir()
		assert _repo_has_changes_vs_base(not_a_repo, "main") is True

	def test_base_branch_name_is_used(self, tmp_path):
		"""When base_branch is configured to something other than 'main',
		the helper diffs against that branch (use case: repos that use
		`master` or `develop` as the integration branch)."""
		repo = tmp_path / "repo"
		_init_repo(repo)
		subprocess.run(["git", "checkout", "-q", "-b", "develop"],
		               cwd=str(repo), check=True)
		(repo / "x.py").write_text("x\n")
		subprocess.run(["git", "add", "."], cwd=str(repo), check=True)
		subprocess.run(["git", "commit", "-q", "-m", "on-develop"],
		               cwd=str(repo), check=True)
		_create_origin(repo)
		# Branch off develop and add another commit
		subprocess.run(["git", "checkout", "-q", "-b", "feature"],
		               cwd=str(repo), check=True)
		(repo / "y.py").write_text("y\n")
		subprocess.run(["git", "add", "."], cwd=str(repo), check=True)
		subprocess.run(["git", "commit", "-q", "-m", "on-feature"],
		               cwd=str(repo), check=True)
		# Against develop, feature has changes; against main, feature has changes too
		assert _repo_has_changes_vs_base(repo, "develop") is True
