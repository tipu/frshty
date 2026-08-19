from unittest.mock import patch, MagicMock

from features.platforms import GitHubPlatform


def _cfg(**github):
    return {
        "job": {"key": "t", "platform": "github"},
        "github": {"repo": "org/repo", **github},
        "workspace": {"base_branch": "main"},
    }


class TestGhEnv:
    def test_no_account_inherits_active_gh_account(self):
        assert GitHubPlatform(_cfg())._gh_env() is None

    def test_account_supplies_a_token_in_the_child_env(self):
        p = GitHubPlatform(_cfg(account="carol"))
        with patch("core.preflight._run",
                   return_value=MagicMock(returncode=0, stdout="gho_carol\n")):
            env = p._gh_env()
        assert env["GH_TOKEN"] == "gho_carol"

    def test_token_is_resolved_once(self):
        p = GitHubPlatform(_cfg(account="carol"))
        with patch("core.preflight._run",
                   return_value=MagicMock(returncode=0, stdout="gho_carol\n")) as run:
            p._gh_env(); p._gh_env(); p._gh_env()
        assert run.call_count == 1

    def test_missing_token_falls_back_to_the_active_account(self):
        p = GitHubPlatform(_cfg(account="nobody"))
        with patch("core.preflight._run",
                   return_value=MagicMock(returncode=1, stdout="", stderr="no account")):
            assert p._gh_env() is None

    def test_run_gh_passes_the_env(self):
        p = GitHubPlatform(_cfg(account="carol"))
        with patch("core.preflight._run",
                   return_value=MagicMock(returncode=0, stdout="gho_carol\n")), \
             patch("subprocess.run", return_value=MagicMock(returncode=0)) as run:
            p._run_gh(["api", "user"])
        assert run.call_args.kwargs["env"]["GH_TOKEN"] == "gho_carol"

    def test_recovery_does_not_switch_when_an_account_is_configured(self):
        p = GitHubPlatform(_cfg(account="carol"))
        with patch("core.preflight.gh_switch_to") as switch:
            assert p._try_recover_gh_auth("org/repo", "404") is False
        switch.assert_not_called()
