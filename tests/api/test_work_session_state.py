"""The task detail payload reports whether the pane still holds a live agent.

A pane keeps the agent's last screen after the process exits, so the board has
to say which of the two it is. Ask and Correct refuse without a live agent, and
that refusal is unreadable while the page shows nothing about the session."""
import sys
from unittest.mock import patch

import pytest

import core.log as log
import core.state as state
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


def _item_with_run(provider="claude"):
    item_id = work_store.create_item("look at the appimage crash", contexts="test")
    work_store.add_run(item_id, "sess-1", f"work-{item_id}", "/tmp", provider=provider)
    return item_id


def test_a_pane_with_no_agent_is_reported_as_stopped(client):
    item_id = _item_with_run()

    import web.work as work_routes
    with patch.object(work_routes.terminal, "session_healthy",
                      return_value={"alive": True, "agent_running": False}) as health:
        body = client.get(f"/api/work/items/{item_id}/detail").json()

    assert body["session"] == {"alive": True, "agent_running": False}
    assert health.call_args.args[0] == f"work-{item_id}"
    assert health.call_args.kwargs["agent"] == "claude"


def test_a_pane_with_a_live_agent_is_reported_as_running(client):
    item_id = _item_with_run()

    import web.work as work_routes
    with patch.object(work_routes.terminal, "session_healthy",
                      return_value={"alive": True, "agent_running": True}):
        body = client.get(f"/api/work/items/{item_id}/detail").json()

    assert body["session"] == {"alive": True, "agent_running": True}


def test_the_run_provider_is_the_process_looked_for(client):
    item_id = _item_with_run(provider="codex")

    import web.work as work_routes
    with patch.object(work_routes.terminal, "session_healthy",
                      return_value={"alive": True, "agent_running": True}) as health:
        client.get(f"/api/work/items/{item_id}/detail")

    assert health.call_args.kwargs["agent"] == "codex"


def test_a_task_that_never_ran_reports_no_session(client):
    item_id = work_store.create_item("nothing has run yet", contexts="test")

    import web.work as work_routes
    with patch.object(work_routes.terminal, "session_healthy") as health:
        body = client.get(f"/api/work/items/{item_id}/detail").json()

    assert body["session"] is None
    assert health.call_count == 0
