import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import core.state as state
import core.log as log


@pytest.fixture()
def tmp_state(tmp_path):
    state.init(tmp_path)
    return tmp_path


@pytest.fixture()
def tmp_log(tmp_state):
    log.init(tmp_state, "test")
    return tmp_state


@pytest.fixture()
def fake_config(tmp_path):
    ws_root = tmp_path / "workspace"
    ws_root.mkdir()
    config_file = tmp_path / "config.toml"
    config_file.write_text('[job]\nkey = "test"\nport = 8000\nplatform = "github"\nticket_system = "jira"\n\n[workspace]\nroot = "' + str(ws_root) + '"\n\n[features]\n\n[pr]\n\n[github]\nrepo = "org/repo"\n')
    return {
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": ws_root,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
            "branch_prefix": "",
            "exclude": [],
            "dep_commands": [],
        },
        "pr": {"auto_pr": True, "auto_merge": False, "merge_strategy": "squash", "merge_flags": []},
        "features": {},
        "github": {"repo": "org/repo"},
        "bitbucket": {"org": "myorg", "user_account_id": "me123"},
        "jira": {"base_url": "https://jira.example.com", "user_env": "JIRA_USER", "token_env": "JIRA_TOKEN"},
        "slack": {},
        "timesheet": {},
        "_config_path": config_file,
        "_state_dir": tmp_path / ".frshty" / "test",
        "_base_url": "http://localhost:8000",
    }


def make_pr(**overrides):
    base = {
        "id": 1,
        "repo": "myrepo",
        "title": "Fix bug",
        "author": "alice",
        "branch": "fix/bug",
        "base": "main",
        "created_on": "2026-01-01T00:00:00Z",
        "updated_on": "2026-01-02T00:00:00Z",
        "url": "https://example.com/pr/1",
    }
    base.update(overrides)
    return base


def make_ticket(**overrides):
    base = {
        "key": "PROJ-1",
        "summary": "Do the thing",
        "status": "In Progress",
        "description": "Description text",
        "url": "https://jira.example.com/browse/PROJ-1",
        "attachments": [],
        "related": [],
        "parent": None,
        "subtasks": [],
        "estimate_seconds": 28800,
    }
    base.update(overrides)
    return base


def make_ticket_state(**overrides):
    base = {
        "status": "new",
        "slug": "PROJ-1-do-the-thing",
        "branch": "PROJ-1-do-the-thing",
        "url": "https://jira.example.com/browse/PROJ-1",
    }
    base.update(overrides)
    return base


def make_comment(**overrides):
    base = {
        "id": 100,
        "body": "Please fix this",
        "author_id": "reviewer1",
        "author_name": "Bob",
        "path": "src/main.py",
        "line": 42,
        "created_on": "2026-01-01T12:00:00Z",
        "parent_id": None,
    }
    base.update(overrides)
    return base


def make_check(**overrides):
    base = {
        "name": "CI",
        "state": "SUCCESS",
        "url": "https://ci.example.com/run/1",
    }
    base.update(overrides)
    return base


def seed_state(tmp_path, module, data):
    state.init(tmp_path)
    state.save(module, data)
