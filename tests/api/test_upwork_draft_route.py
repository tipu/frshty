"""The route a work-board task records its Upwork reply at.

The reply a client gets is written by a task, not by the scan, and this is
where the task puts it. Recording is not sending: the text lands in the box on
/upwork and goes no further.
"""
import sys

import pytest

import core.db as db
import core.log as log
import core.state as state

ROOM = "room_18eeda9cac2e6bb6a6c540f36c8cf49a"


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
        "job": {"key": "test", "port": 8000, "platform": "github",
                "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main"},
        "features": {"upwork": True}, "pr": {}, "slack": {}, "upwork": {},
        "_config_path": tmp_path / "config.toml", "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000", "repos": [],
    })
    (tmp_path / "config.toml").write_text("[job]\nkey='test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _room(instance_key="test", room_id=ROOM):
    db.execute(
        "INSERT INTO upwork_rooms(instance_key, room_id, first_ts, last_ts,"
        " created_at, updated_at) VALUES (?, ?, '', '', '', '')",
        (instance_key, room_id))


def _draft(room_id=ROOM):
    return db.query_one("SELECT reply_draft, reply_sent_at FROM upwork_rooms"
                        " WHERE room_id = ?", (room_id,))


def test_a_draft_lands_in_the_box_and_is_not_sent(client):
    _room()
    resp = client.post(f"/api/upwork/rooms/{ROOM}/draft",
                       json={"text": "Pagination is on the agreed branch."})
    assert resp.status_code == 200
    assert resp.json() == {"status": "recorded"}
    row = _draft()
    assert row["reply_draft"] == "Pagination is on the agreed branch."
    assert row["reply_sent_at"] is None
    assert [e["event"] for e in log.get_events(limit=50)
            if e["event"] == "upwork_draft_recorded"]


def test_a_draft_for_an_unknown_room_is_refused(client):
    _room()
    resp = client.post("/api/upwork/rooms/room_nobody_indexed/draft",
                       json={"text": "hello"})
    assert resp.status_code == 404
    assert _draft()["reply_draft"] == ""


def test_a_draft_for_another_instances_room_is_refused(client):
    _room(instance_key="other")
    resp = client.post(f"/api/upwork/rooms/{ROOM}/draft", json={"text": "hello"})
    assert resp.status_code == 404
    assert _draft()["reply_draft"] == ""


def test_the_named_instance_decides_which_room_is_written(client):
    """One server answers for several instances and picks between them on the
    Host header. A task reaches it on the port, so it names its instance."""
    _room(instance_key="test")
    _room(instance_key="other")
    resp = client.post(f"/api/upwork/rooms/{ROOM}/draft?instance=other",
                       json={"text": "for the other instance"})
    assert resp.status_code == 200
    rows = db.query_all("SELECT instance_key, reply_draft FROM upwork_rooms"
                        " WHERE room_id = ? ORDER BY instance_key", (ROOM,))
    assert [(r["instance_key"], r["reply_draft"]) for r in rows] == [
        ("other", "for the other instance"), ("test", "")]


def test_a_draft_for_an_instance_that_has_no_such_room_is_refused(client):
    _room(instance_key="test")
    resp = client.post(f"/api/upwork/rooms/{ROOM}/draft?instance=other",
                       json={"text": "hello"})
    assert resp.status_code == 404
    assert _draft()["reply_draft"] == ""


def test_an_empty_draft_is_refused(client):
    _room()
    resp = client.post(f"/api/upwork/rooms/{ROOM}/draft", json={"text": "   "})
    assert resp.status_code == 400
    assert _draft()["reply_draft"] == ""


def test_a_draft_over_the_limit_is_refused(client):
    _room()
    resp = client.post(f"/api/upwork/rooms/{ROOM}/draft",
                       json={"text": "x" * 4001})
    assert resp.status_code == 400
    assert _draft()["reply_draft"] == ""
