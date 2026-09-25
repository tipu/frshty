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

    def test_the_state_root_reaches_the_agent_pane(self, monkeypatch):
        monkeypatch.setenv("FRSHTY_ROOT", "/boxes/frshty/state")
        monkeypatch.setenv("FRSHTY_BOARD_FILE", "/boxes/frshty/state/board.json")
        monkeypatch.delenv("FRSHTY_DB", raising=False)
        monkeypatch.setenv("GH_TOKEN", "secret")
        env = terminal._child_env()
        assert env["FRSHTY_ROOT"] == "/boxes/frshty/state"
        assert env["FRSHTY_BOARD_FILE"] == "/boxes/frshty/state/board.json"
        assert "FRSHTY_DB" not in env
        assert "GH_TOKEN" not in env

    def test_existing_agentless_pane_is_respawned(self, monkeypatch, tmp_path):
        monkeypatch.setenv("SHELL", "/test/shell")
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda name: True)
        run = MagicMock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        monkeypatch.setattr(terminal.subprocess, "run", run)

        terminal.launch_pane_command("work-8", str(tmp_path), "codex resume --last")

        args = run.call_args.args[0]
        assert args[3:9] == ["respawn-pane", "-k", "-t", "=term-work-8:", "-c", str(tmp_path)]
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
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "ctx"))
        launch = MagicMock()
        monkeypatch.setattr(terminal, "launch_pane_command", launch)

        terminal.launch_claude("work-10", str(tmp_path), "session-10", "", False)

        launch.assert_called_once_with(
            "work-10", str(tmp_path),
            "claude --dangerously-skip-permissions --resume session-10",
        )

    def test_resume_reapplies_the_launch_context(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            terminal, "session_healthy", lambda key: {"alive": False, "agent_running": False})
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "ctx"))
        launch = MagicMock()
        monkeypatch.setattr(terminal, "launch_pane_command", launch)

        terminal.launch_claude("work-11", str(tmp_path), "session-11", "report rules", True)
        launch.reset_mock()
        terminal.launch_claude("work-11", str(tmp_path), "session-11", "", False)

        ctx = tmp_path / "ctx" / "session-11.md"
        assert ctx.read_text() == "report rules"
        assert launch.call_args.args[2] == (
            "claude --dangerously-skip-permissions --resume session-11 "
            f"--append-system-prompt \"$(cat {ctx})\""
        )

    def test_resume_without_a_launch_context_stays_bare(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            terminal, "session_healthy", lambda key: {"alive": False, "agent_running": False})
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "ctx"))
        launch = MagicMock()
        monkeypatch.setattr(terminal, "launch_pane_command", launch)

        terminal.launch_claude("work-12", str(tmp_path), "session-12", "", False)

        assert launch.call_args.args[2] == (
            "claude --dangerously-skip-permissions --resume session-12")


class TestRecordLaunchCommand:
    def _no_agent(self, monkeypatch, tmp_path):
        monkeypatch.setattr(
            terminal, "session_healthy",
            lambda key, agent="claude": {"alive": False, "agent_running": False})
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "ctx"))
        monkeypatch.setattr(terminal, "launch_pane_command", MagicMock())

    def test_first_launch_records_the_whole_command(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)

        terminal.launch_claude("work-20", str(tmp_path), "session-20", "the brief", True)

        ctx = tmp_path / "ctx" / "session-20.md"
        assert (tmp_path / "ctx" / "session-20.cmd").read_text() == (
            "claude --dangerously-skip-permissions --session-id session-20 "
            f"--append-system-prompt \"$(cat {ctx})\"")

    def test_a_resume_overwrites_the_recorded_command(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)

        terminal.launch_claude("work-21", str(tmp_path), "session-21", "the brief", True)
        terminal.launch_claude("work-21", str(tmp_path), "session-21", "", False)

        ctx = tmp_path / "ctx" / "session-21.md"
        assert (tmp_path / "ctx" / "session-21.cmd").read_text() == (
            "claude --dangerously-skip-permissions --resume session-21 "
            f"--append-system-prompt \"$(cat {ctx})\"")

    def test_a_failed_launch_records_no_command(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)
        monkeypatch.setattr(terminal, "launch_pane_command",
                            MagicMock(side_effect=RuntimeError("bad cwd")))

        with pytest.raises(RuntimeError):
            terminal.launch_claude("work-22", str(tmp_path), "session-22", "brief", True)

        assert not (tmp_path / "ctx" / "session-22.cmd").exists()

    def test_a_secret_env_override_is_masked(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)
        config = {"llm": {"claude": {"config_dir": "/opt/chosen",
                                     "env": {"ANTHROPIC_API_KEY": "sk-secret"}}}}

        terminal.launch_claude("work-23", str(tmp_path), "session-23", "brief", True,
                               config=config)

        recorded = (tmp_path / "ctx" / "session-23.cmd").read_text()
        assert "sk-secret" not in recorded
        assert recorded.startswith(
            "ANTHROPIC_API_KEY=*** CLAUDE_CONFIG_DIR=/opt/chosen claude ")

    def test_an_env_value_that_holds_another_assignment_is_masked(self, monkeypatch,
                                                                  tmp_path):
        """One override's value can read like another override's assignment.
        Masking the values before the command is built keeps the key out."""
        self._no_agent(monkeypatch, tmp_path)
        config = {"llm": {"claude": {"env": {"ANTHROPIC_API_KEY": "sk-secret",
                                             "A": "ANTHROPIC_API_KEY=sk-secret "}}}}

        terminal.launch_claude("work-26", str(tmp_path), "session-26", "brief", True,
                               config=config)

        recorded = (tmp_path / "ctx" / "session-26.cmd").read_text()
        assert "sk-secret" not in recorded
        assert recorded.startswith("A=*** ANTHROPIC_API_KEY=*** claude ")

    def test_a_codex_secret_env_override_is_masked(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)
        config = {"llm": {"codex": {"config_dir": "/opt/chosen",
                                    "env": {"OPENAI_API_KEY": "sk-secret"}}}}

        terminal.launch_codex("work-27", str(tmp_path), "session-27", "brief", True,
                              config=config)

        recorded = (tmp_path / "ctx" / "session-27.cmd").read_text()
        assert "sk-secret" not in recorded
        assert recorded.startswith(
            "CODEX_HOME=/opt/chosen OPENAI_API_KEY=*** codex ")

    def test_the_other_agents_config_variable_is_masked(self, monkeypatch, tmp_path):
        """Only the running agent's own configuration directory is shown. A
        variable that belongs to the other agent is an override like any
        other, so its value is masked."""
        self._no_agent(monkeypatch, tmp_path)
        config = {"llm": {"claude": {"config_dir": "/opt/chosen",
                                     "env": {"CODEX_HOME": "sk-secret"}}}}

        terminal.launch_claude("work-29", str(tmp_path), "session-29", "brief", True,
                               config=config)

        recorded = (tmp_path / "ctx" / "session-29.cmd").read_text()
        assert "sk-secret" not in recorded
        assert recorded.startswith(
            "CLAUDE_CONFIG_DIR=/opt/chosen CODEX_HOME=*** claude ")

    def test_the_pane_still_gets_the_real_environment(self, monkeypatch, tmp_path):
        """Only the recorded copy is masked. The pane needs the real values,
        or the agent runs without the variables its instance is configured
        with."""
        self._no_agent(monkeypatch, tmp_path)
        launch = MagicMock()
        monkeypatch.setattr(terminal, "launch_pane_command", launch)
        config = {"llm": {"claude": {"env": {"ANTHROPIC_API_KEY": "sk-secret"}}}}

        terminal.launch_claude("work-28", str(tmp_path), "session-28", "brief", True,
                               config=config)

        assert launch.call_args.args[2].startswith("ANTHROPIC_API_KEY=sk-secret claude ")

    def test_a_running_agent_keeps_the_recorded_command(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)
        terminal.launch_claude("work-24", str(tmp_path), "session-24", "brief", True)
        monkeypatch.setattr(
            terminal, "session_healthy",
            lambda key, agent="claude": {"alive": True, "agent_running": True})

        terminal.launch_claude("work-24", str(tmp_path), "session-24", "", False)

        assert "--session-id session-24" in (tmp_path / "ctx" / "session-24.cmd").read_text()

    def test_a_codex_launch_records_its_command(self, monkeypatch, tmp_path):
        self._no_agent(monkeypatch, tmp_path)

        terminal.launch_codex("work-25", str(tmp_path), "session-25", "the brief", True)

        recorded = (tmp_path / "ctx" / "session-25.cmd").read_text()
        assert recorded.startswith("codex --dangerously-bypass-approvals-and-sandbox ")
        assert "session-25" in recorded
