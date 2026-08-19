"""A failed commit must be routed by cause, not always sent to an editing agent.

Today every non-zero result gets one instruction: make the smallest edit that
satisfies the hook. Two of the three real failures cannot be satisfied that way.
The dangerous one is a missing symbol from a published sibling package: the hook
cannot pass from inside this repo, and an agent told to make it pass has cheap
exits — a type-ignore, a locally stubbed class, a loosened linter config. The
agent runs with permissions bypassed, so the prompt is a request, not a
constraint. The guard has to be code.
"""
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

import core.tasks.tickets as T
from core import git_util as g


def _git_repo(tmp_path: Path) -> Path:
    r = tmp_path / "wt"
    r.mkdir()
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
    (r / "src.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(r), "commit", "-qm", "base", "--no-verify"], check=True)
    return r


def _outcome(status, output, phase="hook_pass_2"):
    return g.CommitOutcome(status, phase, "r", 1, output, "aaa", "aaa")


class TestRouting:
    def test_an_ambiguous_lint_failure_may_be_repaired(self, tmp_path):
        r = _git_repo(tmp_path)
        with patch.object(T, "run_claude_code",
                          side_effect=lambda *a, **k: (r / "src.py").write_text("entry = 1\n") or "fixed") as agent, \
             patch.object(T, "log"):
            route = T._route_hook_failure(r, _outcome("hook_failed", "E741 Ambiguous name"), "DEV-1")
        assert route == "repair"
        agent.assert_called_once()

    def test_a_dependency_failure_is_never_repaired(self, tmp_path):
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(
                tmp_path,
                _outcome("hook_failed", 'error: "FileExplorerAction" is unknown import symbol'),
                "DEV-1")
        assert route == "block_dependency"
        agent.assert_not_called()

    def test_an_environment_failure_is_never_repaired(self, tmp_path):
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(
                tmp_path, _outcome("tooling_failed", "pre-commit not found", "locate_runner"),
                "DEV-1")
        assert route == "block_environment"
        agent.assert_not_called()

    def test_a_git_failure_is_never_repaired(self, tmp_path):
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(
                tmp_path, _outcome("git_failed", "index.lock: File exists", "git_commit"), "DEV-1")
        assert route == "block_git"
        agent.assert_not_called()

    def test_an_agent_that_returns_nothing_blocks(self, tmp_path):
        with patch.object(T, "run_claude_code", return_value=None), patch.object(T, "log"):
            route = T._route_hook_failure(_git_repo(tmp_path), _outcome("hook_failed", "E741"), "DEV-1")
        assert route == "block_unknown"


    def test_a_repair_that_edits_config_is_reverted_and_blocks(self, tmp_path):
        """Exercises the guard through the router, not just on its own. Without
        this, deleting the check in _route_hook_failure passes every test."""
        r = _git_repo(tmp_path)
        (r / ".pre-commit-config.yaml").write_text("repos: []\n")
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(r), "commit", "-qm", "cfg", "--no-verify"], check=True)

        def cheat(*a, **k):
            (r / ".pre-commit-config.yaml").write_text("repos: []  # disabled\n")
            return "done"

        with patch.object(T, "run_claude_code", side_effect=cheat), patch.object(T, "log"):
            route = T._route_hook_failure(r, _outcome("hook_failed", "E741"), "DEV-1")
        assert route == "block_unknown"
        assert "# disabled" not in (r / ".pre-commit-config.yaml").read_text(), \
            "the cheating edit must be discarded, not left staged for the retry"


class TestRepairMayNotCheat:
    """The edits that make a hook pass without fixing anything."""

    def _repo(self, tmp_path, touched: dict):
        r = tmp_path / "r"
        r.mkdir()
        subprocess.run(["git", "init", "-q", str(r)], check=True)
        subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
        subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
        (r / ".pre-commit-config.yaml").write_text("repos: []\n")
        (r / "src.py").write_text("x = 1\n")
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        subprocess.run(["git", "-C", str(r), "commit", "-qm", "base", "--no-verify"], check=True)
        for name, body in touched.items():
            (r / name).parent.mkdir(parents=True, exist_ok=True)
            (r / name).write_text(body)
        return r

    def test_editing_the_hook_config_is_rejected(self, tmp_path):
        r = self._repo(tmp_path, {".pre-commit-config.yaml": "repos: []  # disabled\n"})
        ok, why = T._repair_touched_only_code(r)
        assert not ok and "pre-commit-config" in why

    def test_editing_a_dependency_manifest_is_rejected(self, tmp_path):
        r = self._repo(tmp_path, {"Pipfile": "[packages]\nrpa-schema='*'\n"})
        ok, why = T._repair_touched_only_code(r)
        assert not ok and "Pipfile" in why

    def test_adding_a_suppression_directive_is_rejected(self, tmp_path):
        r = self._repo(tmp_path, {"src.py": "x = 1  # type: ignore\n"})
        ok, why = T._repair_touched_only_code(r)
        assert not ok and "suppress" in why.lower()

    def test_an_ordinary_code_edit_is_accepted(self, tmp_path):
        r = self._repo(tmp_path, {"src.py": "entry = 1\n"})
        ok, why = T._repair_touched_only_code(r)
        assert ok, why
