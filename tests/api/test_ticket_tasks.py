"""The ticket detail endpoint lists the tasks coupled to that ticket."""
import sys

import pytest

import core.log as log
import core.state as state
from services import work_store


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        import frshty
    finally:
        sys.argv = saved_argv

    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config(_config_for(tmp_path))
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _config_for(tmp_path):
    return {
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": tmp_path,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
            "repos": ["repo_a"],
        },
        "features": {"tickets": True},
        "pr": {"auto_pr": True},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    }


def _seed_ticket(tmp_path, key="DEV-910", slug="dev-910-thing"):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    (tmp_path / "tickets" / slug / "docs").mkdir(parents=True)
    state.save_ticket(key, {"status": "new", "slug": slug, "summary": "A thing"})
    return key


def test_detail_lists_the_tasks_that_named_the_ticket(client, tmp_path):
    key = _seed_ticket(tmp_path)
    mine = work_store.create_item(f"finish {key} and open the PR", contexts="test")
    work_store.create_item("unrelated cleanup", contexts="test")

    body = client.get(f"/api/tickets/{key}/detail").json()

    assert [t["id"] for t in body["tasks"]] == [mine]
    assert body["tasks"][0]["url"] == f"/tasks/{mine}"
    assert body["tasks"][0]["state"] == "agent_working"


def test_detail_lists_no_tasks_when_none_named_the_ticket(client, tmp_path):
    key = _seed_ticket(tmp_path, key="DEV-911", slug="dev-911-quiet")
    work_store.create_item("unrelated cleanup", contexts="test")

    assert client.get(f"/api/tickets/{key}/detail").json()["tasks"] == []
