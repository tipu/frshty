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
