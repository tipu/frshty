import os

from core import terminal
from core.terminal import claude_cmd


class TestClaudeCmd:
    def test_default_is_plain_claude(self):
        assert claude_cmd(None) == "claude --dangerously-skip-permissions"
        assert claude_cmd({}) == "claude --dangerously-skip-permissions"

    def test_config_dir_becomes_an_env_prefix(self):
        cmd = claude_cmd({"llm": {"claude": {"config_dir": "~/.quill-claude"}}})
        assert cmd.startswith("CLAUDE_CONFIG_DIR=/")
        assert "/.quill-claude" in cmd
        assert cmd.endswith("claude --dangerously-skip-permissions")

    def test_env_overrides_win_over_config_dir(self):
        cmd = claude_cmd({"llm": {"claude": {
            "config_dir": "/ignored",
            "env": {"CLAUDE_CONFIG_DIR": "/explicit"},
        }}})
        assert "CLAUDE_CONFIG_DIR=/explicit" in cmd
        assert "/ignored" not in cmd

    def test_custom_bin(self):
        cmd = claude_cmd({"llm": {"claude": {"bin": "/opt/claude"}}})
        assert cmd == "/opt/claude --dangerously-skip-permissions"

    def test_value_needing_quotes_is_quoted(self):
        cmd = claude_cmd({"llm": {"claude": {"env": {"FOO": "a b"}}}})
        assert "FOO='a b'" in cmd

    def test_matches_the_headless_provider(self):
        from core.llm import ClaudeProvider
        config = {"llm": {"claude": {"config_dir": "~/.quill-claude"}}}
        provider_dir = ClaudeProvider(config)._env()["CLAUDE_CONFIG_DIR"]
        assert f"CLAUDE_CONFIG_DIR={provider_dir} " in claude_cmd(config)


class TestAgentConfigDir:
    def test_the_env_override_wins_over_config_dir(self):
        config = {"llm": {"claude": {"config_dir": "~/.ignored",
                                     "env": {"CLAUDE_CONFIG_DIR": "~/.chosen"}}}}
        assert terminal.agent_config_dir(config, "claude") == "~/.chosen"

    def test_no_environment_reads_as_empty(self):
        assert terminal.agent_config_dir({}, "claude") == ""
        assert terminal.agent_config_dir(None, "codex") == ""

    def test_codex_reads_its_own_variable(self):
        config = {"llm": {"codex": {"env": {"CODEX_HOME": "~/.alt-codex"}}}}
        assert terminal.agent_config_dir(config, "codex") == "~/.alt-codex"


class TestWithConfigDir:
    def test_a_recorded_dir_replaces_the_live_one(self):
        config = {"llm": {"claude": {"config_dir": "~/.new"}}}
        cmd = terminal.claude_cmd(terminal.with_config_dir(config, "claude", "~/.recorded"))
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser('~/.recorded')} ")

    def test_a_recorded_dir_beats_an_env_override(self):
        config = {"llm": {"claude": {"env": {"CLAUDE_CONFIG_DIR": "~/.new"}}}}
        cmd = terminal.claude_cmd(terminal.with_config_dir(config, "claude", "~/.recorded"))
        assert cmd.startswith(f"CLAUDE_CONFIG_DIR={os.path.expanduser('~/.recorded')} ")

    def test_other_settings_survive(self):
        config = {"llm": {"claude": {"bin": "/opt/claude",
                                     "env": {"ANTHROPIC_BASE_URL": "https://example.test"}}}}
        cmd = terminal.claude_cmd(terminal.with_config_dir(config, "claude", "~/.recorded"))
        assert "ANTHROPIC_BASE_URL=https://example.test" in cmd
        assert "/opt/claude" in cmd

    def test_an_empty_recorded_dir_removes_the_directory(self):
        config = {"llm": {"claude": {"config_dir": "~/.new",
                                     "env": {"CLAUDE_CONFIG_DIR": "~/.newer"}}}}
        assert terminal.claude_cmd(terminal.with_config_dir(config, "claude", "")) == \
            "claude --dangerously-skip-permissions"

    def test_the_source_config_is_not_changed(self):
        config = {"llm": {"claude": {"config_dir": "~/.new"}}}
        terminal.with_config_dir(config, "claude", "~/.recorded")
        assert config["llm"]["claude"]["config_dir"] == "~/.new"


CLAUDE_TRUST_PANE = (
    "─────────────────────────────────────────────\n"
    " Accessing workspace:\n"
    " /Users/danial/dev/frshty\n"
    " Quick safety check: Is this a project you created or one you trust?\n"
    " Claude Code'll be able to read, edit, and execute files here.\n"
    " Security guide\n"
    " ❯ No, exit\n"
    "   Yes, I trust this folder\n"
    " Enter to confirm · Esc to cancel\n"
)
CLAUDE_TRUST_PANE_YES_SELECTED = CLAUDE_TRUST_PANE.replace(
    " ❯ No, exit\n   Yes, I trust this folder\n",
    "   No, exit\n ❯ Yes, I trust this folder\n")
CLAUDE_READY_PANE = "❯ Try \"how do I log an error?\"\n  ? for shortcuts\n"


class TestClaudeTrustPrompt:
    """WB-209, WB-283 and LSC-51 each sat on this question for days. Claude was
    up, so the launch reported a healthy run, and the kickoff prompt's Enter
    took the preselected `No, exit`."""

    def _calls(self, monkeypatch, pane):
        from unittest.mock import MagicMock
        calls = []
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda n: True)
        monkeypatch.setattr(terminal.subprocess, "run",
                            lambda argv, **kw: calls.append(argv) or MagicMock(returncode=0))
        monkeypatch.setattr(terminal, "pane_text", lambda k: pane)
        return calls

    def test_the_decline_is_stepped_over_before_enter(self, monkeypatch):
        calls = self._calls(monkeypatch, CLAUDE_TRUST_PANE)
        assert terminal.answer_claude_trust("work-1") is True
        assert calls[-1][-4:] == ["-t", "=term-work-1:", "Down", "Enter"]

    def test_enter_alone_when_the_answer_is_already_selected(self, monkeypatch):
        calls = self._calls(monkeypatch, CLAUDE_TRUST_PANE_YES_SELECTED)
        assert terminal.answer_claude_trust("work-1") is True
        assert calls[-1][-3:] == ["-t", "=term-work-1:", "Enter"]

    def test_nothing_is_sent_without_the_question(self, monkeypatch):
        calls = self._calls(monkeypatch, CLAUDE_READY_PANE)
        assert terminal.answer_claude_trust("work-1") is False
        assert calls == []

    def test_nothing_is_sent_once_the_question_is_no_longer_the_bottom(self, monkeypatch):
        """The lines stay on screen after Claude exits, above the shell it
        drops back to, and an agent can print them itself."""
        calls = self._calls(monkeypatch, CLAUDE_TRUST_PANE + "~/dev/frshty (main*) \u00bb\n")
        assert terminal.answer_claude_trust("work-1") is False
        assert calls == []

    def test_answer_trust_routes_by_agent(self, monkeypatch):
        from unittest.mock import MagicMock
        claude, codex = MagicMock(return_value=True), MagicMock(return_value=True)
        monkeypatch.setattr(terminal, "answer_claude_trust", claude)
        monkeypatch.setattr(terminal, "answer_codex_trust", codex)
        assert terminal.answer_trust("work-1", "claude") is True
        claude.assert_called_once_with("work-1")
        codex.assert_not_called()
        assert terminal.answer_trust("work-1", "codex") is True
        codex.assert_called_once_with("work-1")


class TestLaunchContextPath:
    def test_a_relaunch_without_context_keeps_the_seed_text(self, tmp_path, monkeypatch):
        """A run that never started is launched again as a first run, and it
        passes no context because the text is already on disk."""
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path))
        path = terminal.launch_context_path("sid-1", "the brief")
        assert terminal.launch_context_path("sid-1", "") == path
        assert open(path).read() == "the brief"

    def test_a_first_launch_writes_the_context(self, tmp_path, monkeypatch):
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path))
        path = terminal.launch_context_path("sid-2", "first text")
        assert open(path).read() == "first text"
        terminal.launch_context_path("sid-2", "second text")
        assert open(path).read() == "second text"
