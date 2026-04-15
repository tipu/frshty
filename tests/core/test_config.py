import os
from pathlib import Path
from unittest.mock import patch

import pytest

from core.config import load_config, resolve_env, get_repos, ticket_worktree_path, save_feature_toggle


class TestLoadConfig:
    def test_loads_valid_toml(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[job]\nkey = "test"\nport = 8000\nplatform = "github"\n\n'
            '[workspace]\nroot = "' + str(tmp_path) + '"\n'
        )
        config = load_config(str(config_file))
        assert config["job"]["key"] == "test"
        assert config["workspace"]["root"] == tmp_path
        assert config["workspace"]["base_branch"] == "main"
        assert config["pr"]["auto_pr"] is True
        assert config["pr"]["merge_strategy"] == "squash"

    def test_defaults_sections(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[job]\nkey = "x"\nport = 9000\nplatform = "github"\n\n'
            '[workspace]\nroot = "' + str(tmp_path) + '"\n'
        )
        config = load_config(str(config_file))
        assert config["features"] == {}
        assert config["slack"] == {}
        assert config["timesheet"] == {}

    def test_state_dir_derived(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[job]\nkey = "mykey"\nport = 8000\nplatform = "github"\n\n'
            '[workspace]\nroot = "' + str(tmp_path) + '"\n'
        )
        config = load_config(str(config_file))
        assert config["_state_dir"] == Path.home() / ".frshty" / "mykey"

    def test_base_url_from_host(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[job]\nkey = "x"\nport = 8000\nhost = "http://custom:3000"\nplatform = "github"\n\n'
            '[workspace]\nroot = "' + str(tmp_path) + '"\n'
        )
        config = load_config(str(config_file))
        assert config["_base_url"] == "http://custom:3000"

    def test_base_url_from_port(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text(
            '[job]\nkey = "x"\nport = 9999\nplatform = "github"\n\n'
            '[workspace]\nroot = "' + str(tmp_path) + '"\n'
        )
        config = load_config(str(config_file))
        assert config["_base_url"] == "http://localhost:9999"


class TestResolveEnv:
    def test_direct_value(self):
        config = {"jira": {"user": "alice", "user_env": "JIRA_USER"}}
        assert resolve_env(config, "jira", "user_env") == "alice"

    def test_env_var_fallback(self):
        config = {"jira": {"user_env": "MY_JIRA_USER"}}
        with patch.dict(os.environ, {"MY_JIRA_USER": "bob"}):
            assert resolve_env(config, "jira", "user_env") == "bob"

    def test_missing_returns_empty(self):
        assert resolve_env({}, "jira", "user_env") == ""

    def test_empty_env_var(self):
        config = {"jira": {"user_env": ""}}
        assert resolve_env(config, "jira", "user_env") == ""


class TestGetRepos:
    def test_explicit_repos(self, tmp_path):
        config = {"workspace": {"root": tmp_path, "repos": ["repo-a", "repo-b"]}}
        repos = get_repos(config)
        assert len(repos) == 2
        assert repos[0]["name"] == "repo-a"
        assert repos[0]["path"] == tmp_path / "repo-a"

    def test_projects_dir_discovery(self, tmp_path):
        projects = tmp_path / "projects"
        (projects / "alpha" / ".git").mkdir(parents=True)
        (projects / "beta" / ".git").mkdir(parents=True)
        (projects / "not-a-repo").mkdir(parents=True)
        config = {"workspace": {"root": tmp_path, "projects_dir": "projects", "exclude": []}}
        repos = get_repos(config)
        names = [r["name"] for r in repos]
        assert "alpha" in names
        assert "beta" in names
        assert "not-a-repo" not in names

    def test_projects_dir_excludes(self, tmp_path):
        projects = tmp_path / "projects"
        (projects / "keep" / ".git").mkdir(parents=True)
        (projects / "skip" / ".git").mkdir(parents=True)
        config = {"workspace": {"root": tmp_path, "projects_dir": "projects", "exclude": ["skip"]}}
        repos = get_repos(config)
        assert len(repos) == 1
        assert repos[0]["name"] == "keep"

    def test_no_repos_or_projects(self):
        config = {"workspace": {"root": Path("/tmp")}}
        assert get_repos(config) == []


class TestTicketWorktreePath:
    def test_flat_layout(self, tmp_path):
        config = {"workspace": {"root": tmp_path, "tickets_dir": "tickets", "ticket_layout": "flat"}}
        result = ticket_worktree_path(config, "PROJ-1-slug", "myrepo")
        assert result == tmp_path / "tickets" / "PROJ-1-slug" / "myrepo"

    def test_workspace_layout(self, tmp_path):
        config = {"workspace": {"root": tmp_path, "tickets_dir": "tickets", "ticket_layout": "workspace"}}
        result = ticket_worktree_path(config, "PROJ-1-slug", "myrepo")
        assert result == tmp_path / "tickets" / "PROJ-1-slug" / "workspace" / "myrepo"


class TestSaveFeatureToggle:
    def test_toggles_feature(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text("[features]\nreviews = true\nslack = false\n")
        config = {"_config_path": config_file, "features": {"reviews": True, "slack": False}}
        save_feature_toggle(config, "reviews", False)
        text = config_file.read_text()
        assert "reviews = false" in text
        assert config["features"]["reviews"] is False

    def test_toggle_on(self, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text("[features]\nslack = false\n")
        config = {"_config_path": config_file, "features": {"slack": False}}
        save_feature_toggle(config, "slack", True)
        assert "slack = true" in config_file.read_text()
