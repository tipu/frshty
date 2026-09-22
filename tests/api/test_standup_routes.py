"""The HTTP surface of the standup: draft, open, edit, answer, close.

Every route answers with the whole day, so the page never has to merge a patch
into a list it already holds."""
import sys

import pytest

import core.db as db
import core.log as log
import core.state as state
from services import standup


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
        "standup": {"enabled": True, "days": ["mon", "tue", "wed", "thu", "fri",
                                              "sat", "sun"]},
        "_config_path": tmp_path / "config.toml", "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000", "repos": [],
    })
    (tmp_path / "config.toml").write_text("[job]\nkey='test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def test_the_page_renders(client):
    r = client.get("/standup")
    assert r.status_code == 200
    assert "Standup" in r.text


def test_reading_a_day_never_creates_one(client):
    r = client.get("/api/standup?day=2026-09-16")
    assert r.status_code == 200
    assert r.json()["exists"] is False
    assert db.query_all("SELECT id FROM standups") == []


def test_draft_then_open_then_add_and_check_off(client):
    assert client.post("/api/standup/draft", json={"day": "2026-09-16"}).json()["state"] == "draft"
    assert client.post("/api/standup/open", json={"day": "2026-09-16"}).json()["state"] == "open"
    added = client.post("/api/standup/items",
                        json={"day": "2026-09-16", "text": "Log the week's hours",
                              "contexts": "acme"}).json()
    item = next(i for i in added["items"] if i["text"] == "Log the week's hours")
    assert item["contexts"] == "acme"
    after = client.post(f"/api/standup/items/{item['id']}/state",
                        json={"day": "2026-09-16", "state": "done"}).json()
    assert next(i for i in after["items"] if i["id"] == item["id"])["state"] == "done"


def test_an_unknown_state_is_refused(client):
    client.post("/api/standup/open", json={"day": "2026-09-16"})
    item = client.post("/api/standup/items",
                       json={"day": "2026-09-16", "text": "A line"}).json()["items"][0]
    r = client.post(f"/api/standup/items/{item['id']}/state",
                    json={"day": "2026-09-16", "state": "teleported"})
    assert r.status_code == 409
    assert "teleported" in r.json()["error"]


def test_closing_asks_and_the_answer_lands(client):
    client.post("/api/standup/open", json={"day": "2026-09-16"})
    item = client.post("/api/standup/items",
                       json={"day": "2026-09-16", "text": "A line"}).json()["items"][0]
    closed = client.post("/api/standup/close", json={"day": "2026-09-16"}).json()
    assert closed["state"] == "closed"
    assert next(i for i in closed["items"] if i["id"] == item["id"])["question"]
    answered = client.post(f"/api/standup/items/{item['id']}/answer",
                           json={"day": "2026-09-16", "option": "drop"}).json()
    assert next(i for i in answered["items"] if i["id"] == item["id"])["state"] == "dropped"


def test_an_edit_after_the_close_is_refused(client):
    client.post("/api/standup/open", json={"day": "2026-09-16"})
    item = client.post("/api/standup/items",
                       json={"day": "2026-09-16", "text": "A line"}).json()["items"][0]
    client.post("/api/standup/close", json={"day": "2026-09-16"})
    r = client.post(f"/api/standup/items/{item['id']}",
                    json={"day": "2026-09-16", "text": "A different line"})
    assert r.status_code == 409
    assert r.json()["error"] == "this day is closed"


def test_reorder_moves_the_lines(client):
    client.post("/api/standup/open", json={"day": "2026-09-16"})
    client.post("/api/standup/items", json={"day": "2026-09-16", "text": "First"})
    day = client.post("/api/standup/items", json={"day": "2026-09-16", "text": "Second"}).json()
    ids = [i["id"] for i in day["items"]]
    out = client.post("/api/standup/reorder",
                      json={"day": "2026-09-16", "order": list(reversed(ids))}).json()
    assert [i["id"] for i in out["items"]] == list(reversed(ids))


def test_the_event_log_is_readable(client):
    client.post("/api/standup/open", json={"day": "2026-09-16"})
    item = client.post("/api/standup/items",
                       json={"day": "2026-09-16", "text": "A line"}).json()["items"][0]
    client.post(f"/api/standup/items/{item['id']}/state",
                json={"day": "2026-09-16", "state": "parked"})
    kinds = [e["kind"] for e in client.get(
        f"/api/standup/items/{item['id']}/events").json()["events"]]
    assert "operator_parked" in kinds


def test_the_day_reports_whether_the_standup_is_enabled(client):
    body = client.post("/api/standup/draft", json={"day": "2026-09-16"}).json()
    assert body["enabled"] is True
    assert body["budget"] == standup.DEFAULTS["max_nudges_per_day"]
