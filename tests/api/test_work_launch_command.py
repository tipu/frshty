"""The task detail payload carries the command line the agent pane runs.

The page already shows the launch context the agent was seeded with. The
command around it — the binary, the permission flag, the session id and the
configuration directory — is what says which account the pane authenticates
as, and it was not readable anywhere on the board."""
import sys
from unittest.mock import patch

import pytest

import core.log as log
import core.state as state
import core.terminal as terminal
from services import work_store


@pytest.fixture()
def client(tmp_path, fresh_db):
    state.init(tmp_path)
    state._default_instance_key = "test"
    state._instance_key_cv.set("test")
    log.init(tmp_path, "test")
    saved = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        import frshty
    finally:
        sys.argv = saved
    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main"},
        "features": {}, "pr": {}, "slack": {},
        "_config_path": tmp_path / "config.toml", "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000", "repos": [],
    })
    (tmp_path / "config.toml").write_text("[job]\nkey='test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _item_with_run():
    item_id = work_store.create_item("read the crash report", contexts="test")
    work_store.add_run(item_id, "sess-cmd-1", f"work-{item_id}", "/tmp")
    return item_id


def test_the_detail_payload_carries_the_launch_command(client, tmp_path, monkeypatch):
    item_id = _item_with_run()
    launch_dir = tmp_path / "launch"
    launch_dir.mkdir()
    monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(launch_dir))
    (launch_dir / "sess-cmd-1.cmd").write_text(
        "CLAUDE_CONFIG_DIR=/opt/chosen claude --dangerously-skip-permissions "
        "--session-id sess-cmd-1")

    body = client.get(f"/api/work/items/{item_id}/detail").json()

    assert body["launch_command"] == (
        "CLAUDE_CONFIG_DIR=/opt/chosen claude --dangerously-skip-permissions "
        "--session-id sess-cmd-1")


def test_a_run_with_no_recorded_command_reports_an_empty_string(client, tmp_path,
                                                                monkeypatch):
    item_id = _item_with_run()
    monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "launch"))

    body = client.get(f"/api/work/items/{item_id}/detail").json()

    assert body["launch_command"] == ""
