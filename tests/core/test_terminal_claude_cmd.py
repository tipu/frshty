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
