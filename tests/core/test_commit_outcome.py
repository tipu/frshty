"""commit_with_hooks must say which phase failed and keep the evidence.

Every non-zero result used to look the same to the caller, and the useful text —
the diagnostics from the two explicit `pre-commit run` passes — was logged and
discarded, so only the later `git commit` output survived. Anything deciding what
to do next was reading the wrong thing.

Three real failures reached that one path during DEV-635 and DEV-644: a type
error in a generated test (fixable here), a symbol missing from a published
sibling package (not fixable here), and a missing pre-commit binary (not a code
problem at all). They need different answers, so they must first be told apart.
"""
import subprocess
from pathlib import Path
from unittest.mock import patch

from core import git_util as g


def _repo(tmp_path: Path, *, config: bool = True) -> Path:
    r = tmp_path / "r"
    r.mkdir()
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
    if config:
        (r / ".pre-commit-config.yaml").write_text("repos: []\n")
    (r / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
    return r


class TestOutcomeShape:
    def test_a_clean_commit_reports_committed(self, tmp_path):
        r = _repo(tmp_path, config=False)
        out = g.commit_outcome(r, message="m")
        assert out.status == "committed"
        assert out.phase == "git_commit"
        assert out.after_head and out.after_head != out.before_head

    def test_a_missing_binary_is_tooling_not_hook(self, tmp_path):
        r = _repo(tmp_path)
        with patch.object(g, "_find_pre_commit", return_value=None):
            out = g.commit_outcome(r, message="m")
        assert out.status == "tooling_failed"
        assert out.phase == "locate_runner"
        assert "pre-commit" in out.output.lower()

    def test_hook_diagnostics_are_kept_not_discarded(self, tmp_path):
        """The classifier and the repair agent both need this text."""
        r = _repo(tmp_path)
        fake = tmp_path / "pc"
        fake.write_text("#!/bin/sh\necho 'E741 Ambiguous variable name: l' >&2\nexit 1\n")
        fake.chmod(0o755)
        with patch.object(g, "_find_pre_commit", return_value=fake):
            out = g.commit_outcome(r, message="m")
        assert out.status == "hook_failed"
        assert out.phase.startswith("hook_pass")
        assert "E741" in out.output

    def test_a_failed_hook_does_not_reach_the_commit(self, tmp_path):
        """Running git commit after the explicit pass already failed only
        discards the good diagnostics and repeats the work."""
        r = _repo(tmp_path)
        fake = tmp_path / "pc"
        fake.write_text("#!/bin/sh\necho boom >&2\nexit 1\n")
        fake.chmod(0o755)
        with patch.object(g, "_find_pre_commit", return_value=fake):
            out = g.commit_outcome(r, message="m")
        log = subprocess.run(["git", "-C", str(r), "log", "--oneline"],
                             capture_output=True, text=True).stdout
        assert log.strip() == ""
        assert out.after_head == out.before_head


class TestDeterministicTriage:
    def test_missing_runner_is_environment(self):
        assert g.triage_commit_failure("tooling_failed", "pre-commit not found") == "environment"

    def test_exit_127_style_message_is_environment(self):
        assert g.triage_commit_failure(
            "hook_failed", "/bin/sh: 1: pre-commit: not found") == "environment"

    def test_a_missing_import_symbol_is_a_dependency_question(self):
        """Not fixable by editing this repo; a sibling package must publish it."""
        assert g.triage_commit_failure(
            "hook_failed",
            'error: "FileExplorerAction" is unknown import symbol') == "dependency"

    def test_a_module_not_found_is_a_dependency_question(self):
        assert g.triage_commit_failure(
            "hook_failed", "ModuleNotFoundError: No module named 'rpa_schema'") == "dependency"

    def test_an_ordinary_lint_error_is_ambiguous(self):
        """Ambiguous means: ask, do not assume it is repairable."""
        assert g.triage_commit_failure(
            "hook_failed", "E741 Ambiguous variable name: `l`") == "ambiguous"

    def test_a_git_level_failure_never_reaches_code_repair(self):
        for msg in ("nothing to commit, working tree clean",
                    "Unable to create '.git/index.lock': File exists",
                    "Committer identity unknown"):
            assert g.triage_commit_failure("git_failed", msg) == "git"
