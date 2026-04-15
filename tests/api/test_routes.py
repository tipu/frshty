import json
import sys
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import core.state as state
import core.log as log


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    log.init(tmp_path, "test")

    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        if "frshty" in sys.modules:
            frshty = sys.modules["frshty"]
        else:
            import frshty
    finally:
        sys.argv = saved_argv

    from fastapi.testclient import TestClient
    frshty._config = {
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": tmp_path,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
        },
        "features": {"reviews": True, "slack": False},
        "pr": {"auto_pr": True},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    }
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


class TestEvents:
    def test_get_events_empty(self, client):
        resp = client.get("/api/events")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_get_events_with_data(self, client):
        log.emit("test_event", "hello")
        resp = client.get("/api/events")
        data = resp.json()
        assert len(data) == 1
        assert data[0]["event"] == "test_event"

    def test_dismiss_event(self, client):
        record = log.emit("evt", "msg")
        resp = client.post(f"/api/events/{record['id']}/dismiss")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_dismiss_all(self, client):
        log.emit("a", "1")
        log.emit("b", "2")
        resp = client.post("/api/events/dismiss-all")
        assert resp.status_code == 200


class TestStatus:
    def test_returns_shape(self, client):
        resp = client.get("/api/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "job" in data
        assert "features" in data
        assert "unread_total" in data
        assert "counts" in data
        assert "slack_alive" in data


class TestConfig:
    def test_get_config(self, client):
        resp = client.get("/api/config")
        assert resp.status_code == 200
        data = resp.json()
        assert "job" in data
        assert "features" in data

    def test_get_config_raw(self, client):
        resp = client.get("/api/config/raw")
        assert resp.status_code == 200
        data = resp.json()
        assert "content" in data

    def test_save_config_raw(self, client):
        resp = client.post("/api/config/raw", json={"content": "[job]\nkey = 'updated'\n"})
        assert resp.status_code == 200
        assert resp.json()["ok"] is True


class TestSettings:
    def test_update_features(self, client, tmp_path):
        config_file = tmp_path / "config.toml"
        config_file.write_text("[features]\nreviews = true\nslack = false\n")
        resp = client.put("/api/settings", json={"features": {"reviews": False}})
        assert resp.status_code == 200
        assert resp.json()["features"]["reviews"] is False


class TestTickets:
    def test_list_empty(self, client):
        resp = client.get("/api/tickets/list")
        assert resp.status_code == 200
        assert resp.json() == {}

    def test_list_with_tickets(self, client):
        state.save("tickets", {
            "T-1": {"status": "planning", "slug": "T-1-thing"},
            "T-2": {"status": "done", "done_at": "2020-01-01T00:00:00Z"},
        })
        resp = client.get("/api/tickets/list")
        data = resp.json()
        assert "T-1" in data
        assert "T-2" not in data

    def test_detail_not_found(self, client):
        resp = client.get("/api/tickets/NOPE/detail")
        assert resp.status_code == 404

    def test_detail_found(self, client, tmp_path):
        slug = "T-1-slug"
        state.save("tickets", {"T-1": {"status": "pr_ready", "slug": slug}})
        docs_dir = tmp_path / "tickets" / slug / "docs"
        docs_dir.mkdir(parents=True)
        (docs_dir / "ticket.md").write_text("# T-1\n\nDescription")
        with patch("frshty.terminal.session_healthy", return_value={"alive": False, "claude_running": False}):
            resp = client.get("/api/tickets/T-1/detail")
        assert resp.status_code == 200
        data = resp.json()
        assert data["key"] == "T-1"
        assert "ticket.md" in data["docs"]

    def test_demo_not_found(self, client):
        state.save("tickets", {"T-1": {"status": "merged", "slug": "T-1-s"}})
        resp = client.get("/api/tickets/T-1/demo")
        assert resp.status_code == 404

    def test_kill_terminal(self, client):
        with patch("frshty.terminal.kill_terminal") as mock_kill:
            resp = client.delete("/api/tickets/T-1/terminal")
        assert resp.status_code == 200
        mock_kill.assert_called_once_with("T-1")

    def test_pr_comments_empty(self, client):
        state.save("tickets", {"T-1": {"status": "in_review", "slug": "T-1-s"}})
        resp = client.get("/api/tickets/T-1/pr-comments")
        assert resp.status_code == 200
        assert resp.json() == []


class TestScheduled:
    def test_empty(self, client):
        resp = client.get("/api/scheduled")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_with_scheduled_items(self, client):
        state.save("scheduler", {
            "T-1": {"action": "create_pr", "run_at": "2026-04-20T10:00:00Z", "scheduled_at": "2026-04-15T10:00:00Z", "meta": {}}
        })
        resp = client.get("/api/scheduled")
        data = resp.json()
        assert len(data) == 1
        assert data[0]["key"] == "T-1"
        assert data[0]["type"] == "scheduled_pr"


class TestReviews:
    def test_list_empty(self, client):
        resp = client.get("/api/reviews")
        assert resp.status_code == 200

    def test_submit_no_url(self, client):
        resp = client.post("/api/reviews/submit", json={"url": ""})
        assert resp.status_code == 400

    def test_submit_invalid_url(self, client):
        resp = client.post("/api/reviews/submit", json={"url": "https://not-github.com/foo"})
        assert resp.status_code == 400

    def test_submit_valid_url(self, client):
        with patch("frshty.multiprocessing.Process") as mock_proc:
            mock_proc.return_value = MagicMock()
            resp = client.post("/api/reviews/submit", json={"url": "https://github.com/org/repo/pull/123"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["pr_id"] == 123
        assert data["repo"] == "org/repo"
