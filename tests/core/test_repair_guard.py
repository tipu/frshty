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
import shutil
import subprocess
from pathlib import Path

import pytest

import core.tasks.tickets as T


def _named(repo: Path, suffix: str) -> Path:
    """The watched path ending in `suffix`, resolved the way the guard resolves it."""
    hits = [p for p in T._hook_paths(repo) if str(p).endswith(suffix)]
    assert len(hits) == 1, f"{suffix} -> {hits}"
    return hits[0]


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
        """Without the fingerprint too, so this pins the rename inspection and
        not the separate check that would also notice the file disappeared."""
        r = _repo(tmp_path)
        _git(r, "mv", ".pre-commit-config.yaml", "pre-commit.disabled")
        ok, why = T._repair_touched_only_code(r, None)
        assert not ok, "a rename that removes the hook config must be rejected"
        assert ".pre-commit-config.yaml" in why

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

    def test_making_the_hook_non_executable_is_rejected(self, tmp_path):
        """Git skips a hook without the executable bit, so `chmod -x` disables it
        without changing a byte. Comparing contents alone reads that as clean."""
        r = _repo(tmp_path)
        hook = _named(r, "hooks/pre-commit")
        hook.parent.mkdir(parents=True, exist_ok=True)
        hook.write_text("#!/bin/sh\nexec real-check\n")
        hook.chmod(0o755)
        before = T._hook_integrity(r)
        hook.chmod(0o644)
        assert hook.read_text() == "#!/bin/sh\nexec real-check\n"
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_replacing_the_commit_msg_hook_is_rejected(self, tmp_path):
        """pre-commit is not the only hook that can stop a commit."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        hook = _named(r, "hooks/commit-msg")
        hook.parent.mkdir(parents=True, exist_ok=True)
        hook.write_text("#!/bin/sh\nexit 0\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok

    def test_hook_paths_raises_rather_than_guessing(self, tmp_path):
        """`git config --get` exits 1 outside a repository, which is an allowed
        code, so a fallback for the failing rev-parse would produce a plausible
        set of paths that do not exist. That fingerprint compares equal to
        itself and lets everything through."""
        from core import git_util

        notarepo = tmp_path / "notarepo"
        notarepo.mkdir()
        with pytest.raises(git_util.GitCommandError):
            T._hook_paths(notarepo)

    def test_editing_what_a_symlinked_runner_points_at_is_rejected(self, tmp_path):
        """`~/.local/bin/pre-commit` is usually a link into a toolchain, so
        recording only where it points leaves rewriting what it points at
        invisible."""
        r = _repo(tmp_path)
        real = tmp_path / "toolchain" / "pre-commit"
        real.parent.mkdir()
        real.write_text("#!/bin/sh\nexec real-check\n")
        real.chmod(0o755)
        (r / ".venv" / "bin").mkdir(parents=True)
        (r / ".venv" / "bin" / "pre-commit").symlink_to(real)
        before = T._hook_integrity(r)
        real.write_text("#!/bin/sh\nexit 0\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_an_included_config_is_watched_too(self, tmp_path):
        """core.hooksPath and core.excludesFile can be set in any config file git
        reads, so the watched set comes from git rather than from a guess."""
        r = _repo(tmp_path)
        extra = tmp_path / "extra.gitconfig"
        extra.write_text("[core]\n\tquotePath = false\n")
        _git(r, "config", "include.path", str(extra))
        watched = T._hook_paths(r)
        assert extra in watched, watched

    def test_a_local_hook_entry_script_is_watched(self, tmp_path):
        """A repo-local hook's entry script is the hook. Editing it to exit zero
        is an ordinary file modification, which the worktree check accepts."""
        r = _repo(tmp_path)
        script = r / "scripts" / "lint.sh"
        script.parent.mkdir()
        script.write_text("#!/bin/sh\nexec real-check\n")
        script.chmod(0o755)
        (r / ".pre-commit-config.yaml").write_text(
            "repos:\n  - repo: local\n    hooks:\n      - id: lint\n"
            "        name: lint\n        entry: scripts/lint.sh\n        language: script\n")
        _git(r, "add", "-A")
        _git(r, "commit", "-qm", "local hook", "--no-verify")
        assert script in T._hook_paths(r)
        before = T._hook_integrity(r)
        script.write_text("#!/bin/sh\nexit 0\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_an_entry_naming_an_installed_command_watches_nothing(self, tmp_path):
        """The real configs run `pipenv --quiet run basedpyright`. Nothing in
        that resolves to a file here, and inventing one would be a false watch."""
        r = _repo(tmp_path)
        (r / ".pre-commit-config.yaml").write_text(
            "repos:\n  - repo: local\n    hooks:\n      - id: types\n"
            "        name: types\n        entry: pipenv --quiet run basedpyright\n")
        assert T._local_hook_entries(r) == []

    def test_only_the_command_is_watched_not_its_arguments(self, tmp_path):
        """`run` is an argument of `pipenv --quiet run basedpyright`, and a repo
        that happens to contain a file called run must not have it watched: an
        ordinary edit to it would then be rejected as tampering."""
        r = _repo(tmp_path)
        (r / "run").write_text("#!/bin/sh\necho hello\n")
        (r / ".pre-commit-config.yaml").write_text(
            "repos:\n  - repo: local\n    hooks:\n      - id: types\n"
            "        name: types\n        entry: pipenv --quiet run basedpyright\n")
        _git(r, "add", "-A")
        _git(r, "commit", "-qm", "has a file called run", "--no-verify")
        assert T._local_hook_entries(r) == []
        before = T._hook_integrity(r)
        (r / "run").write_text("#!/bin/sh\necho hi\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why

    def test_a_tilde_in_the_hooks_path_is_expanded(self, tmp_path):
        """Git expands ~ in pathname values. Leaving it literal makes the watched
        path a directory named "~" inside the repo, which never exists."""
        r = _repo(tmp_path)
        _git(r, "config", "core.hooksPath", "~/frshty-hooks-that-do-not-exist")
        hook = _named(r, "frshty-hooks-that-do-not-exist/pre-commit")
        assert hook.is_relative_to(Path.home())
        assert not hook.is_relative_to(r)

    def test_editing_a_nested_gitignore_is_rejected(self, tmp_path):
        """Adding a path to any .gitignore hides a created file from the status
        check that would otherwise reject it, so only the root one being
        forbidden leaves the nested ones as ordinary edits."""
        r = _repo(tmp_path)
        nested = r / "pkg" / ".gitignore"
        nested.write_text("*.log\n")
        _git(r, "add", "-A")
        _git(r, "commit", "-qm", "nested", "--no-verify")
        before = T._hook_integrity(r)
        nested.write_text("*.log\nshadow.py\n")
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert ".gitignore" in why

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


class TestTheGuardAcceptsWhatFormattersProduce:
    """The cost of tightening this guard is blocked tickets, so the shapes a real
    autofix leaves behind are pinned alongside the ones it must reject."""

    def test_edits_across_several_files_are_accepted(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / "app.py").write_text("x = 11\n")
        (r / "pkg" / "mod.py").write_text("y = 22\n")
        assert _git(r, "status", "--porcelain").stdout.strip(), "the test must edit something"
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why

    def test_a_mix_of_staged_and_unstaged_edits_is_accepted(self, tmp_path):
        """A fix the agent staged and one it did not are `M ` and ` M`, and a
        file touched both ways is `MM`."""
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        (r / "app.py").write_text("x = 2\n")
        _git(r, "add", "app.py")
        (r / "app.py").write_text("x = 3\n")
        (r / "pkg" / "mod.py").write_text("y = 3\n")
        codes = {c for c, _ in T._porcelain_entries(
            _git(r, "status", "--porcelain", "-z").stdout)}
        assert codes == {"MM", " M"}, codes
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why

    def test_a_repair_that_changed_nothing_is_accepted(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why

    def test_real_ruff_output_is_accepted(self, tmp_path):
        ruff = shutil.which("ruff")
        if ruff is None:
            pytest.skip("ruff is not installed")
        r = _repo(tmp_path)
        (r / "app.py").write_text("import os\nimport sys\nx   =    sys.argv\n")
        _git(r, "add", "-A")
        _git(r, "commit", "-qm", "messy", "--no-verify")
        before = T._hook_integrity(r)
        subprocess.run([ruff, "check", "--select", "F401", "--fix", "."],
                       cwd=str(r), capture_output=True)
        subprocess.run([ruff, "format", "."], cwd=str(r), capture_output=True)
        assert (r / "app.py").read_text() != "import os\nimport sys\nx   =    sys.argv\n"
        ok, why = T._repair_touched_only_code(r, before)
        assert ok, why


class TestJudgedAgainstThePreRepairTree:
    """`git status` compares against HEAD, so it reports the ticket's own earlier
    work as well as the repair's. The guard blamed the repair for a manifest the
    ticket had legitimately edited hours before, and no repair could ever pass in
    that checkout again."""

    def _staged(self, tmp_path: Path):
        r = _repo(tmp_path)
        (r / "package.json").write_text('{"name":"a"}\n')
        (r / "run").write_text("#!/bin/sh\n")
        _git(r, "add", "-A")
        _git(r, "commit", "-qm", "seed", "--no-verify")
        return r

    def _judge(self, r: Path, prior, repair):
        prior(r)
        _git(r, "add", "-A")   # what _commit_workspace_changes does before committing
        snapshot = T._snapshot_repo(r)
        before = T._hook_integrity(r)
        repair(r)
        return T._repair_touched_only_code(r, before, snapshot)

    def test_a_manifest_the_ticket_edited_earlier_is_not_the_repairs_doing(self, tmp_path):
        r = self._staged(tmp_path)
        ok, why = self._judge(r,
                              lambda r: (r / "package.json").write_text('{"name":"a","dep":"1"}\n'),
                              lambda r: (r / "app.py").write_text("x = 9\n"))
        assert ok, why

    def test_a_manifest_the_repair_edited_is_still_rejected(self, tmp_path):
        r = self._staged(tmp_path)
        ok, why = self._judge(r, lambda r: None,
                              lambda r: (r / "package.json").write_text('{"name":"cheat"}\n'))
        assert not ok
        assert "package.json" in why

    def test_a_file_the_ticket_added_earlier_is_not_the_repairs_doing(self, tmp_path):
        r = self._staged(tmp_path)
        ok, why = self._judge(r, lambda r: (r / "new.py").write_text("y = 2\n"),
                              lambda r: (r / "app.py").write_text("x = 9\n"))
        assert ok, why

    def test_an_untracked_file_that_was_already_there_is_not_the_repairs_doing(self, tmp_path):
        """A build artifact nobody gitignored is untracked before the repair and
        still untracked after it. Rejecting on its presence blocks every repair
        in that checkout."""
        r = self._staged(tmp_path)
        (r / "build.log").write_text("noise\n")
        snapshot = T._snapshot_repo(r)
        before = T._hook_integrity(r)
        assert "build.log" in snapshot[1]
        (r / "app.py").write_text("x = 9\n")
        ok, why = T._repair_touched_only_code(r, before, snapshot)
        assert ok, why

    def test_a_file_the_repair_added_is_still_rejected(self, tmp_path):
        r = self._staged(tmp_path)
        ok, why = self._judge(r, lambda r: None,
                              lambda r: (r / "stub.py").write_text("FileExplorerAction = object\n"))
        assert not ok

    def test_a_mode_change_the_repair_made_is_rejected(self, tmp_path):
        """Visible only against the pre-repair tree. Against HEAD it is
        indistinguishable from a mode change the ticket made earlier."""
        r = self._staged(tmp_path)
        ok, why = self._judge(r, lambda r: None, lambda r: (r / "app.py").chmod(0o755))
        assert not ok
        assert "mode" in why

    def test_a_mode_change_the_ticket_made_earlier_is_not_the_repairs_doing(self, tmp_path):
        r = self._staged(tmp_path)
        ok, why = self._judge(r, lambda r: (r / "run").chmod(0o755),
                              lambda r: (r / "app.py").write_text("x = 9\n"))
        assert ok, why

    def test_a_deletion_is_still_rejected(self, tmp_path):
        r = self._staged(tmp_path)
        ok, why = self._judge(r, lambda r: None, lambda r: (r / "app.py").unlink())
        assert not ok


class TestSuppressionIsCountedNotMatched:
    """Reading the whole file asked whether it contains a marker. That is a
    different question: a file that already carries a `# noqa` could never be
    formatted again, because the guard reported the pre-existing one as the
    repair's work. Plenty of real files carry one."""

    def _repo_with(self, tmp_path: Path, body: str) -> Path:
        r = _repo(tmp_path)
        (r / "app.py").write_text(body)
        _git(r, "add", "-A")
        _git(r, "commit", "-qm", "seed", "--no-verify")
        return r

    def _route(self, r: Path, mutate) -> str:
        from unittest.mock import patch

        from core import git_util as g

        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                  "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
        with patch.object(T, "run_claude_code", side_effect=lambda *a, **k: mutate() or "done"), \
             patch.object(T, "log"):
            return T._route_hook_failure(r, outcome, "DEV-1")

    def test_a_pre_existing_marker_does_not_block_a_reformat(self, tmp_path):
        r = self._repo_with(tmp_path, "import os  # noqa\nx   =   1\n")
        route = self._route(r, lambda: (r / "app.py").write_text("import os  # noqa\nx = 1\n"))
        assert route == "repair", "an ordinary reformat of a file with a marker must pass"

    def test_reformatting_the_marker_line_itself_does_not_block(self, tmp_path):
        """The line carrying the marker is the one a formatter is most likely to
        rewrite, and reading the added lines presents it as newly added."""
        r = self._repo_with(tmp_path, "x=1  # noqa\n")
        route = self._route(r, lambda: (r / "app.py").write_text("x = 1  # noqa\n"))
        assert route == "repair"

    def test_a_second_marker_on_a_reformatted_line_still_blocks(self, tmp_path):
        """Counting has to notice one more, not merely that one is present."""
        r = self._repo_with(tmp_path, "x=1  # noqa\ny=2\n")
        route = self._route(r, lambda: (r / "app.py").write_text("x = 1  # noqa\ny = 2  # noqa\n"))
        assert route == "block_unknown"

    def test_a_marker_the_repair_added_still_blocks(self, tmp_path):
        r = self._repo_with(tmp_path, "import os\nx   =   1\n")
        route = self._route(r, lambda: (r / "app.py").write_text("import os  # noqa\nx = 1\n"))
        assert route == "block_unknown"
        assert "# noqa" not in (r / "app.py").read_text(), "and it must be undone"

    def test_a_type_ignore_the_repair_added_still_blocks(self, tmp_path):
        r = self._repo_with(tmp_path, "x   =   1\n")
        route = self._route(r, lambda: (r / "app.py").write_text("x = 1  # type: ignore\n"))
        assert route == "block_unknown"


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
        failed = T._restore_hook_setup(before, T._restorable_roots(r))
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
        assert T._restore_hook_setup(before, T._restorable_roots(r)) == []
        assert "the-real-thing" in real.read_text()
        assert real.stat().st_mode & 0o111, "the runner must stay executable"

    def test_an_edited_exclude_is_put_back(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        excl = r / ".git" / "info" / "exclude"
        excl.parent.mkdir(parents=True, exist_ok=True)
        excl.write_text("shadow.py\n")
        assert T._restore_hook_setup(before, T._restorable_roots(r)) == []
        ok, _ = T._repair_touched_only_code(r, before)
        assert ok, "after restoration the fingerprint must match again"

    def test_restoring_a_symlinked_path_does_not_write_through_it(self, tmp_path):
        """Restoration must stay inside the checkout. Writing to a watched path
        that has become a symlink follows the link and overwrites whatever is on
        the other end."""
        r = _repo(tmp_path)
        outside = tmp_path / "outside"
        outside.mkdir()
        victim = outside / "victim"
        victim.write_text("DO NOT OVERWRITE\n")
        before = T._hook_integrity(r)
        cfg = r / ".pre-commit-config.yaml"
        cfg.unlink()
        cfg.symlink_to(victim)
        assert T._restore_hook_setup(before, T._restorable_roots(r)) == []
        assert victim.read_text() == "DO NOT OVERWRITE\n"
        assert not cfg.is_symlink()
        assert cfg.read_text() == "repos: []\n"

    def test_swapping_a_watched_file_for_a_symlink_is_rejected(self, tmp_path):
        """Detection, not just repair. The link may point at content identical to
        the original, so comparing the bytes on the far end sees nothing."""
        r = _repo(tmp_path)
        twin = tmp_path / "twin"
        twin.write_text("repos: []\n")
        before = T._hook_integrity(r)
        cfg = r / ".pre-commit-config.yaml"
        cfg.unlink()
        cfg.symlink_to(twin)
        assert cfg.read_text() == "repos: []\n", "the contents on the far end match"
        ok, why = T._repair_touched_only_code(r, before)
        assert not ok
        assert "hook setup" in why

    def test_restoring_a_path_replaced_by_a_directory(self, tmp_path):
        r = _repo(tmp_path)
        before = T._hook_integrity(r)
        cfg = r / ".pre-commit-config.yaml"
        cfg.unlink()
        cfg.mkdir()
        (cfg / "decoy").write_text("x\n")
        assert T._restore_hook_setup(before, T._restorable_roots(r)) == []
        assert cfg.is_file()
        assert cfg.read_text() == "repos: []\n"

    def test_a_restore_that_cannot_complete_is_reported(self, tmp_path):
        """Silence here means the ticket is retried against a contaminated
        worktree."""
        r = _repo(tmp_path)
        bindir = r / ".venv" / "bin"
        bindir.mkdir(parents=True)
        runner = bindir / "pre-commit"
        runner.write_text("#!/bin/sh\nexec real-check\n")
        runner.chmod(0o755)
        before = T._hook_integrity(r)
        runner.write_text("#!/bin/sh\nexit 0\n")
        bindir.chmod(0o555)
        try:
            failed = T._restore_hook_setup(before, T._restorable_roots(r))
        finally:
            bindir.chmod(0o755)
        assert failed, "an unwritable path must be reported, not skipped"

    def test_restoration_uses_the_paths_recorded_before_the_repair(self, tmp_path):
        """Re-resolving afterwards follows a core.hooksPath the repair had just
        changed, so the old hook is written to the new location and the one git
        now runs is left as the repair left it."""
        r = _repo(tmp_path)
        original = _named(r, "hooks/pre-commit")
        original.parent.mkdir(parents=True, exist_ok=True)
        original.write_text("#!/bin/sh\nexec real-check\n")
        original.chmod(0o755)
        before = T._hook_integrity(r)
        elsewhere = tmp_path / "decoy"
        elsewhere.mkdir()
        _git(r, "config", "core.hooksPath", str(elsewhere))
        original.write_text("#!/bin/sh\nexit 0\n")
        assert T._restore_hook_setup(before, T._restorable_roots(r)) == []
        assert "exec real-check" in original.read_text(), (
            "the hook recorded before the repair is the one that must be put back")
        assert not (elsewhere / "pre-commit").exists(), (
            "restoration must not write to a path the repair introduced")

    def test_the_recorded_mode_is_put_back(self, tmp_path):
        r = _repo(tmp_path)
        bindir = r / ".venv" / "bin"
        bindir.mkdir(parents=True)
        runner = bindir / "pre-commit"
        runner.write_text("#!/bin/sh\nexec real-check\n")
        runner.chmod(0o755)
        before = T._hook_integrity(r)
        runner.write_text("#!/bin/sh\nexit 0\n")
        runner.chmod(0o644)
        assert T._restore_hook_setup(before, T._restorable_roots(r)) == []
        assert runner.stat().st_mode & 0o111, "the runner must be executable again"

    def test_a_config_outside_the_checkout_is_reported_not_rewritten(self, tmp_path):
        """`git config --show-origin` names ~/.gitconfig and /etc/gitconfig, so a
        change to either has to block the ticket. Writing them back would have
        frshty rewriting the operator's own configuration from a recording it
        took minutes earlier."""
        r = _repo(tmp_path)
        outside = tmp_path / "their.gitconfig"
        outside.write_text("[core]\n\tquotePath = false\n")
        _git(r, "config", "include.path", str(outside))
        before = T._hook_integrity(r)
        assert outside in before
        outside.write_text("[core]\n\tquotePath = true\n")
        failed = T._restore_hook_setup(before, T._restorable_roots(r))
        assert str(outside) in failed, "the change must be reported"
        assert outside.read_text() == "[core]\n\tquotePath = true\n", (
            "and the file must not be rewritten")

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

    def test_a_repair_that_returns_nothing_is_still_undone(self, tmp_path):
        """A repair that died or timed out still edited files on its way there.
        Returning early left them in place, unexamined and uncommitted."""
        from unittest.mock import patch

        from core import git_util as g

        r = _repo(tmp_path)
        fake = r / ".venv" / "bin" / "pre-commit"

        def cheat_then_die(*a, **k):
            fake.parent.mkdir(parents=True, exist_ok=True)
            fake.write_text("#!/bin/sh\nexit 0\n")
            fake.chmod(0o755)
            (r / "app.py").write_text("half-done\n")
            return None

        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                  "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
        with patch.object(T, "run_claude_code", side_effect=cheat_then_die), \
             patch.object(T, "log"):
            route = T._route_hook_failure(r, outcome, "DEV-1")
        assert route == "block_unknown"
        assert not fake.exists()
        assert (r / "app.py").read_text() == "x = 1\n"

    def test_the_exclude_is_put_back_before_the_worktree_cleanup_reads_it(self, tmp_path):
        """_restore_repo enumerates untracked files, and that enumeration reads
        info/exclude. Cleaning up while the repair's exclude is still in place
        cannot see the file the repair hid behind it."""
        from unittest.mock import patch

        from core import git_util as g

        r = _repo(tmp_path)
        excl = _named(r, "info/exclude")

        def cheat(*a, **k):
            excl.parent.mkdir(parents=True, exist_ok=True)
            excl.write_text("shadow.py\n")
            (r / "shadow.py").write_text("FileExplorerAction = object\n")
            return "done"

        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                  "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
        with patch.object(T, "run_claude_code", side_effect=cheat), patch.object(T, "log"):
            route = T._route_hook_failure(r, outcome, "DEV-1")
        assert route == "block_unknown"
        assert not (r / "shadow.py").exists(), (
            "the hidden file must not survive; the exclude has to go back first")

    def test_an_unusable_baseline_blocks_before_the_agent_runs(self, tmp_path):
        """A baseline that cannot be put back is not a baseline."""
        from unittest.mock import patch

        from core import git_util as g

        r = _repo(tmp_path)
        (r / ".venv" / "bin").mkdir(parents=True)
        (r / ".venv" / "bin" / "pre-commit").mkdir()
        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                  "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(r, outcome, "DEV-1")
        assert route == "block_unknown"
        agent.assert_not_called()

    def test_an_unreadable_runner_also_blocks(self, tmp_path):
        """A directory is not the only shape that cannot be put back. A regular
        file we cannot read is recorded the same way and must block the same."""
        from unittest.mock import patch

        from core import git_util as g

        r = _repo(tmp_path)
        (r / ".venv" / "bin").mkdir(parents=True)
        runner = r / ".venv" / "bin" / "pre-commit"
        runner.write_text("#!/bin/sh\nexec real-check\n")
        runner.chmod(0o000)
        try:
            assert T._state_of(runner)[0] == T._IRREGULAR
            outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                      "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
            with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
                route = T._route_hook_failure(r, outcome, "DEV-1")
        finally:
            runner.chmod(0o755)
        assert route == "block_unknown"
        agent.assert_not_called()

    def test_a_snapshot_that_cannot_be_taken_blocks_before_the_agent_runs(self, tmp_path):
        """No baseline means nothing to compare against and nothing to put back.
        Guessing one would fingerprint files that do not exist, which compares
        equal to itself."""
        from unittest.mock import patch

        from core import git_util as g

        main = _repo(tmp_path)
        wt = tmp_path / "tick"
        _git(main, "worktree", "add", "-q", "-b", "feat", str(wt))
        (wt / ".git").write_text(f"gitdir: {tmp_path / 'gone'}\n")
        with pytest.raises(g.GitCommandError):
            T._hook_integrity(wt)
        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                  "app.py:1:1: E741 Ambiguous name", "aaa", "aaa")
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(wt, outcome, "DEV-1")
        assert route == "block_unknown"
        agent.assert_not_called()


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
        excl = _named(wt, 'info/exclude')
        excl.parent.mkdir(parents=True, exist_ok=True)
        excl.write_text("secret.py\n")
        (wt / "secret.py").write_text("x = 1\n")
        assert _git(wt, "status", "--porcelain").stdout.strip() == "", (
            "the guard must watch the exclude file git actually reads")

    def test_hiding_a_file_in_a_worktree_is_rejected(self, tmp_path):
        wt = self._worktree(tmp_path)
        before = T._hook_integrity(wt)
        excl = _named(wt, 'info/exclude')
        excl.parent.mkdir(parents=True, exist_ok=True)
        excl.write_text("secret.py\n")
        (wt / "secret.py").write_text("x = 1\n")
        ok, why = T._repair_touched_only_code(wt, before)
        assert not ok
        assert "hook setup" in why

    def test_the_watched_hook_is_the_one_git_runs(self, tmp_path):
        wt = self._worktree(tmp_path)
        before = T._hook_integrity(wt)
        hook = _named(wt, 'hooks/pre-commit')
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
