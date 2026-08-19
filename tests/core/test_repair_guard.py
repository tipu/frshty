"""The repair agent runs with permissions bypassed, so the guard has to hold.

Two ways past the earlier version, both reachable without writing anything a
worktree check can see:

`git status --porcelain` prints a rename as `R  old -> new`, and the guard read
only the half after the arrow. `git mv .pre-commit-config.yaml pre-commit.disabled`
therefore presented its harmless destination and the hook config left the repo.

Nothing under `.git` is tracked, and every repo gitignores `.venv`, so a planted
`.venv/bin/pre-commit` that exits 0 does not appear in `git status` at all. The
guard now fingerprints what decides whether the hook runs and compares it, which
is answerable, instead of asking whether the diff looks honest, which is not.
"""
import subprocess
from pathlib import Path

import core.tasks.tickets as T


def _git(d: Path, *a):
    return subprocess.run(["git", "-C", str(d), *a], capture_output=True, text=True, check=True)


def _repo(tmp_path: Path) -> Path:
    r = tmp_path / "wt"
    r.mkdir()
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    _git(r, "config", "user.email", "t@t")
    _git(r, "config", "user.name", "t")
    (r / ".pre-commit-config.yaml").write_text("repos: []\n")
    (r / ".gitignore").write_text(".venv/\n")
    (r / "pyproject.toml").write_text("[tool.ruff]\n")
    (r / "app.py").write_text("x = 1\n")
    (r / "pkg").mkdir()
    (r / "pkg" / "mod.py").write_text("y = 2\n")
    _git(r, "add", "-A")
    _git(r, "commit", "-qm", "base", "--no-verify")
    return r


class TestRenamesAreInspectedOnBothSides:
    def test_moving_the_hook_config_away_is_rejected(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        _git(r, "mv", ".pre-commit-config.yaml", "pre-commit.disabled")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok, "a rename that removes the hook config must be rejected"

    def test_moving_a_manifest_away_is_rejected(self, tmp_path):
        """pyproject.toml is not part of the hook fingerprint, so this case is
        caught by reading the source of the rename and nothing else."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        _git(r, "mv", "pyproject.toml", "settings.toml")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "pyproject.toml" in why

    def test_the_parser_keeps_both_paths(self, tmp_path):
        r = _repo(tmp_path)
        _git(r, "mv", "pkg/mod.py", "pkg/mod2.py")
        raw = _git(r, "status", "--porcelain", "-z").stdout
        entries = T._porcelain_entries(raw)
        assert len(entries) == 1
        code, paths = entries[0]
        assert "R" in code
        assert set(paths) == {"pkg/mod2.py", "pkg/mod.py"}

    def test_renaming_ordinary_code_is_rejected(self, tmp_path):
        """A lint fix never moves a file, and a move is how a module gets
        shadowed."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        _git(r, "mv", "pkg/mod.py", "pkg/mod2.py")
        ok, _ = T._repair_touched_only_code(r, before)
        assert not ok


class TestHookIntegrityCoversWhatStatusCannotSee:
    def _plant_runner(self, r: Path) -> None:
        (r / ".venv" / "bin").mkdir(parents=True)
        pc = r / ".venv" / "bin" / "pre-commit"
        pc.write_text("#!/bin/sh\nexit 0\n")
        pc.chmod(0o755)

    def test_a_gitignored_fake_runner_is_rejected(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        self._plant_runner(r)
        assert _git(r, "status", "--porcelain").stdout.strip() == "", (
            "the point of this test is that the worktree looks clean")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_the_worktree_check_alone_would_accept_it(self, tmp_path):
        """Without the fingerprint the guard reports success on a repo whose hook
        has been replaced. A check that cannot fail here is not a check."""
        r = _repo(tmp_path)
        self._plant_runner(r)
        ok, _ = T._repair_touched_only_code(r, None)
        assert ok

    def test_hiding_a_file_via_info_exclude_is_rejected(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / ".git" / "info").mkdir(exist_ok=True)
        (r / ".git" / "info" / "exclude").write_text("shadow.py\n")
        (r / "shadow.py").write_text("FileExplorerAction = object\n")
        assert _git(r, "status", "--porcelain").stdout.strip() == ""
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_rewriting_the_native_git_hook_is_rejected(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        hook = r / ".git" / "hooks" / "pre-commit"
        hook.parent.mkdir(exist_ok=True)
        hook.write_text("#!/bin/sh\nexit 0\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_an_ordinary_edit_is_accepted(self, tmp_path):
        """The guard has to let the repair it exists to allow through."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / "app.py").write_text("x = 1\n")
        (r / "pkg" / "mod.py").write_text("y = 3\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why

    def test_redirecting_hooks_through_git_config_is_rejected(self, tmp_path):
        """core.hooksPath is how the hook is disabled without touching a hook."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        _git(r, "config", "core.hooksPath", str(tmp_path / "empty-hooks"))
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_the_watched_hook_follows_a_configured_hooks_path(self, tmp_path):
        """When core.hooksPath is already set, the hook git runs is the one
        there. Watching the default location watches a file that does not
        exist, so rewriting the real hook goes unnoticed."""
        r = _repo(tmp_path)
        elsewhere = tmp_path / "hooks"
        elsewhere.mkdir()
        (elsewhere / "pre-commit").write_text("#!/bin/sh\nexec real-check\n")
        _git(r, "config", "core.hooksPath", str(elsewhere))
        before = T._hook_integrity(r)
        (elsewhere / "pre-commit").write_text("#!/bin/sh\nexit 0\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why


class TestOnlyEditsAreAllowed:
    def test_deleting_the_offending_file_is_rejected(self, tmp_path):
        """`git rm bad.py` satisfies any hook that complained about bad.py. The
        guard listed the ways in rather than the one way through, so a deletion
        matched nothing and the missing file then read as unreadable and was
        skipped."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        _git(r, "rm", "-q", "app.py")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok, "a repair that deletes the file the hook named must be rejected"

    def test_replacing_a_file_with_a_symlink_is_rejected(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / "app.py").unlink()
        (r / "app.py").symlink_to(r / "pkg" / "mod.py")
        _git(r, "add", "-A")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok

    def test_a_staged_edit_is_still_accepted(self, tmp_path):
        """Staging is what the repair loop does next, so `M ` has to pass."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / "app.py").write_text("x = 2\n")
        _git(r, "add", "-A")
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why


class TestRejectionLeavesNoWorkingHole:
    def test_a_planted_runner_is_removed_not_merely_noticed(self, tmp_path):
        """_restore_repo lists untracked files with --exclude-standard, so the
        planted runner survived it. Noticing a bypass and leaving it in place
        means the next attempt runs the fake hook and passes."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / ".venv" / "bin").mkdir(parents=True)
        fake = r / ".venv" / "bin" / "pre-commit"
        fake.write_text("#!/bin/sh\nexit 0\n")
        fake.chmod(0o755)
        failed = T._restore_hook_setup(r, before)
        assert failed == []
        assert not fake.exists(), "the planted runner must not survive the rejection"

    def test_a_real_runner_is_put_back_after_being_overwritten(self, tmp_path):
        r = _repo(tmp_path)
        (r / ".venv" / "bin").mkdir(parents=True)
        real = r / ".venv" / "bin" / "pre-commit"
        real.write_text("#!/bin/sh\nexec the-real-thing \"$@\"\n")
        real.chmod(0o755)
        before = T._hook_integrity(r)
        real.write_text("#!/bin/sh\nexit 0\n")
        assert T._restore_hook_setup(r, before) == []
        assert "the-real-thing" in real.read_text()
        assert real.stat().st_mode & 0o111, "the runner must stay executable"

    def test_an_edited_exclude_is_put_back(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        excl = r / ".git" / "info" / "exclude"
        excl.parent.mkdir(parents=True, exist_ok=True)
        excl.write_text("shadow.py\n")
        assert T._restore_hook_setup(r, before) == []
        ok, _ = T._repair_touched_only_code(r, before)
        assert ok, "after restoration the fingerprint must match again"

    def test_the_router_removes_a_planted_runner_not_just_the_guard(self, tmp_path):
        """Through _route_hook_failure, not by calling the restore directly. A
        restore that works but is never reached leaves the same hole."""
        from unittest.mock import patch

        from core import git_util as g

        r = _repo(tmp_path)
        fake = r / ".venv" / "bin" / "pre-commit"

        def cheat(*a, **k):
            fake.parent.mkdir(parents=True, exist_ok=True)
            fake.write_text("#!/bin/sh\nexit 0\n")
            fake.chmod(0o755)
            return "done"

        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                  "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
        with patch.object(T, "run_claude_code", side_effect=cheat), patch.object(T, "log"):
            route = T._route_hook_failure(r, outcome, "DEV-1")
        assert route == "block_unknown"
        assert not fake.exists(), "the router must undo the bypass, not only notice it"


class TestLinkedWorktrees:
    """Ticket repos are linked worktrees, so `<repo>/.git` is a file. Watching
    `<repo>/.git/info/exclude` watched a path that cannot exist, which made the
    fingerprint blind in exactly the place it runs."""

    def _worktree(self, tmp_path: Path) -> Path:
        main = _repo(tmp_path)
        wt = tmp_path / "tick"
        _git(main, "worktree", "add", "-q", "-b", "feat", str(wt))
        assert (wt / ".git").is_file()
        return wt

    def test_the_watched_exclude_is_the_one_git_honours(self, tmp_path):
        wt = self._worktree(tmp_path)
        excl = T._hook_paths(wt)["exclude"]
        excl.parent.mkdir(parents=True, exist_ok=True)
        excl.write_text("secret.py\n")
        (wt / "secret.py").write_text("x = 1\n")
        assert _git(wt, "status", "--porcelain").stdout.strip() == "", (
            "the guard must watch the exclude file git actually reads")

    def test_hiding_a_file_in_a_worktree_is_rejected(self, tmp_path):
        wt = self._worktree(tmp_path)
        before = T._hook_integrity(wt)
        excl = T._hook_paths(wt)["exclude"]
        excl.parent.mkdir(parents=True, exist_ok=True)
        excl.write_text("secret.py\n")
        (wt / "secret.py").write_text("x = 1\n")
        ok, why = T._repair_touched_only_code(wt, before)
        assert not ok
        assert "hook setup" in why

    def test_the_watched_hook_is_the_one_git_runs(self, tmp_path):
        wt = self._worktree(tmp_path)
        before = T._hook_integrity(wt)
        hook = T._hook_paths(wt)["git_hook"]
        hook.parent.mkdir(parents=True, exist_ok=True)
        hook.write_text("#!/bin/sh\nexit 0\n")
        ok, why = T._repair_touched_only_code(wt, before)
        assert not ok
        assert "hook setup" in why

    def test_an_ordinary_edit_in_a_worktree_is_accepted(self, tmp_path):
        wt = self._worktree(tmp_path)
        before = T._hook_integrity(wt)
        (wt / "app.py").write_text("x = 9\n")
        ok, why = T._repair_touched_only_code(wt, before)
        assert ok, why
