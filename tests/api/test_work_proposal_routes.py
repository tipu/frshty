"""The HTTP surface for a proposal: the board lists it, approval launches it,
declining files it. The tmux launch is patched throughout."""
import sys
from unittest.mock import patch

import pytest

import core.db as db
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
    return TestClient(frshty.app, raise_server_exceptions=False), tmp_path


def _proposal():
    return work_store.create_proposal(
        "Move WB-304 to the PLT board and assign it to the TRIAGE sprint.",
        note="Proposed from Slack #wb-alerts: Erik asked for the board move",
        instance_key="atropos", contexts="atropos,slack",
        brief="\n\n## Slack conversation\n\nErik: move it to PLT\n")


def _launch_patches(tmp_path):
    from services import work_launch
    root = tmp_path / "ws"
    root.mkdir(exist_ok=True)
    config = {"job": {"key": "personal"}, "workspace": {"root": str(root)}, "llm": {}}
    return [
        patch.object(work_launch, "personal_config", return_value=config),
        patch.object(work_launch, "project_entries", return_value=[]),
        patch.object(work_launch.terminal, "launch_agent"),
        patch.object(work_launch.terminal, "session_healthy", return_value={"alive": True}),
        patch.object(work_launch.threading, "Thread"),
        patch.object(work_launch.work_tags, "schedule_implicit_tags"),
    ]


def test_the_board_lists_a_proposal_in_its_own_group(client):
    c, _ = client
    item_id = _proposal()

    body = c.get("/api/work/items").json()
    assert [row["id"] for row in body["groups"]["proposed"]] == [item_id]
    assert body["counts"]["proposed"] == 1
    assert body["groups"]["proposed"][0]["state"] == "proposed"


def test_the_detail_page_serves_the_brief(client):
    c, _ = client
    item_id = _proposal()

    body = c.get(f"/api/work/items/{item_id}/detail").json()
    assert body["item"]["state"] == "proposed"
    assert "Erik: move it to PLT" in body["item"]["launch_brief"]
    assert body["runs"] == []


def test_approving_starts_a_run(client):
    c, tmp_path = client
    item_id = _proposal()

    patches = _launch_patches(tmp_path)
    for p in patches:
        p.start()
    try:
        r = c.post(f"/api/work/items/{item_id}/approve", json={})
    finally:
        for p in patches:
            p.stop()

    assert r.status_code == 200, r.text
    assert r.json()["state"] == "agent_working"
    assert len(db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                            (item_id,))) == 1
    assert c.get("/api/work/items").json()["counts"]["proposed"] == 0


def test_approving_twice_is_refused(client):
    c, tmp_path = client
    item_id = _proposal()

    patches = _launch_patches(tmp_path)
    for p in patches:
        p.start()
    try:
        c.post(f"/api/work/items/{item_id}/approve", json={})
        second = c.post(f"/api/work/items/{item_id}/approve", json={})
    finally:
        for p in patches:
            p.stop()

    assert second.status_code == 409, second.text
    assert len(db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                            (item_id,))) == 1


def test_declining_never_starts_a_run(client):
    c, _ = client
    item_id = _proposal()

    r = c.post(f"/api/work/items/{item_id}/action", json={"action": "decline"})

    assert r.status_code == 200, r.text
    assert db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                        (item_id,)) == []
    body = c.get("/api/work/items").json()
    assert body["counts"]["proposed"] == 0
    assert body["counts"]["done"] == 0, "a declined proposal goes straight to the archive"
    assert c.get("/api/work/items?archive=1").json()["counts"]["done"] == 1


def test_approving_an_ordinary_task_is_refused(client):
    c, _ = client
    item_id = work_store.create_item("an ordinary task")
    r = c.post(f"/api/work/items/{item_id}/approve", json={})
    assert r.status_code == 409
    assert "not awaiting approval" in r.json()["error"]
