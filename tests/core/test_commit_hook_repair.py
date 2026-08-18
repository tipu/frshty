"""A pre-commit rejection must get one repair attempt, not block the ticket.

DEV-644 was blocked in write_tests because ruff rejected two uses of `l` as a
loop variable in generated tests. commit_with_hooks correctly refused to pass
--no-verify, but the hook output went into a CalledProcessError and nothing acted
on it. The hook already names the file and the line, so it is enough to fix.
"""
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

import core.tasks.tickets as T


def _repo(tmp_path: Path) -> Path:
    ws = tmp_path / "workspace" / "r"
    ws.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(ws)], check=True)
    subprocess.run(["git", "-C", str(ws), "config", "user.email", "t@t.com"], check=True)
    subprocess.run(["git", "-C", str(ws), "config", "user.name", "t"], check=True)
    (ws / "a.py").write_text("x = 1\n")
    return tmp_path


def _fail(rc=1, out="E741 Ambiguous variable name: `l`\n  src/x.py:53:27"):
    return subprocess.CompletedProcess(["git", "commit"], rc, out, "")


def _ok():
    return subprocess.CompletedProcess(["git", "commit"], 0, "", "")


class TestHookFailureIsRepaired:
    def test_a_rejected_commit_is_repaired_and_retried(self, tmp_path):
        td = _repo(tmp_path)
        calls = []
        with patch.object(T, "commit_with_hooks", create=True), \
             patch("core.git_util.commit_with_hooks", side_effect=[_fail(), _ok()]) as cwh, \
             patch.object(T, "run_claude_code", return_value="fixed") as agent, \
             patch.object(T, "log"):
            out = T._commit_workspace_changes(td, "DEV-1", message="m")
        assert out == ["r"], "the repo must still be reported as committed"
        assert cwh.call_count == 2, "the commit must be retried after the repair"
        assert agent.call_count == 1
        calls.append(agent.call_args[0][0])
        assert "E741" in calls[0], "the hook output must be handed to the agent"

    def test_the_agent_is_told_not_to_disable_the_hook(self, tmp_path):
        td = _repo(tmp_path)
        with patch("core.git_util.commit_with_hooks", side_effect=[_fail(), _ok()]), \
             patch.object(T, "run_claude_code", return_value="fixed") as agent, \
             patch.object(T, "log"):
            T._commit_workspace_changes(td, "DEV-1", message="m")
        prompt = agent.call_args[0][0]
        for phrase in ("do not disable", "smallest edit"):
            assert phrase in prompt.lower()

    def test_a_second_failure_still_raises(self, tmp_path):
        """One repair, not a loop. A real problem must still stop the ticket."""
        td = _repo(tmp_path)
        with patch("core.git_util.commit_with_hooks", side_effect=[_fail(), _fail()]), \
             patch.object(T, "run_claude_code", return_value="tried"), \
             patch.object(T, "log"), pytest.raises(subprocess.CalledProcessError):
            T._commit_workspace_changes(td, "DEV-1", message="m")

    def test_no_repair_when_the_agent_returns_nothing(self, tmp_path):
        td = _repo(tmp_path)
        with patch("core.git_util.commit_with_hooks", side_effect=[_fail(), _ok()]), \
             patch.object(T, "run_claude_code", return_value=None), \
             patch.object(T, "log"), pytest.raises(subprocess.CalledProcessError):
            T._commit_workspace_changes(td, "DEV-1", message="m")

    def test_a_clean_commit_never_calls_the_agent(self, tmp_path):
        td = _repo(tmp_path)
        with patch("core.git_util.commit_with_hooks", side_effect=[_ok()]), \
             patch.object(T, "run_claude_code") as agent, \
             patch.object(T, "log"):
            out = T._commit_workspace_changes(td, "DEV-1", message="m")
        assert out == ["r"]
        agent.assert_not_called()
