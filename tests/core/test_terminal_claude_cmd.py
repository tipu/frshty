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
