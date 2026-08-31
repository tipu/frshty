import subprocess
from unittest.mock import MagicMock

import pytest

import core.terminal as terminal


class TestLaunchPaneCommand:
    def test_new_session_runs_agent_as_tmux_command(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SHELL", "/test/shell")
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda name: False)
        run = MagicMock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        monkeypatch.setattr(terminal.subprocess, "run", run)

        terminal.launch_pane_command("work-7", str(tmp_path), "claude --resume abc")

        args = run.call_args.args[0]
        assert args[3:8] == ["new-session", "-d", "-s", "term-work-7", "-c"]
        assert args[-1] == "claude --resume abc; exec /test/shell -l"
        assert "send-keys" not in args
        assert run.call_args.kwargs["env"] == terminal._child_env()

    def test_existing_agentless_pane_is_respawned(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SHELL", "/test/shell")
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda name: True)
        run = MagicMock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        monkeypatch.setattr(terminal.subprocess, "run", run)

        terminal.launch_pane_command("work-8", str(tmp_path), "codex resume --last")

        args = run.call_args.args[0]
        assert args[3:9] == ["respawn-pane", "-k", "-t", "term-work-8", "-c", str(tmp_path)]
        assert args[-1] == "codex resume --last; exec /test/shell -l"
        assert "send-keys" not in args

    def test_tmux_failure_is_reported(self, monkeypatch, tmp_path):
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda name: False)
        monkeypatch.setattr(
            terminal.subprocess,
            "run",
            lambda *args, **kwargs: subprocess.CompletedProcess([], 1, "", "bad cwd"),
        )

        with pytest.raises(RuntimeError, match="could not launch agent pane: bad cwd"):
            terminal.launch_pane_command("work-9", str(tmp_path), "claude")


class TestLaunchClaude:
    def test_launch_bypasses_interactive_shell(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            terminal, "session_healthy", lambda key: {"alive": False, "agent_running": False})
        launch = MagicMock()
        monkeypatch.setattr(terminal, "launch_pane_command", launch)

        terminal.launch_claude("work-10", str(tmp_path), "session-10", "", False)

        launch.assert_called_once_with(
            "work-10", str(tmp_path),
            "claude --dangerously-skip-permissions --resume session-10",
        )
