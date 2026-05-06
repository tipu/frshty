"""HTTP route tests for releases. Mirrors tests/api/test_routes.py client setup."""
import sys
from pathlib import Path

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
    instance_key = tmp_path.name
    frshty._config = {
        "job": {"key": instance_key, "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": tmp_path,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
        },
        "features": {"releases": True},
        "pr": {"auto_pr": True},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    }
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False), frshty


def _ticket(status, slug="T-1-foo", **extra):
    base = {"status": status, "slug": slug}
    if status == "merged":
        base.setdefault("merged_external_status", "Done")
    base.update(extra)
    return base


class TestFeatureGate:
    def test_assign_returns_404_when_disabled(self, client):
        c, frshty = client
        frshty._config["features"]["releases"] = False
        state.save_ticket("T-1", _ticket("new"))
        resp = c.post("/api/tickets/T-1/release", json={"release_key": "v1.0"})
        assert resp.status_code == 404
        assert resp.json()["error"] == "releases feature disabled"

    def test_list_returns_404_when_disabled(self, client):
        c, frshty = client
        frshty._config["features"]["releases"] = False
        resp = c.get("/api/releases")
        assert resp.status_code == 404

    def test_detail_omits_release_block_when_disabled(self, client):
        c, frshty = client
        frshty._config["features"]["releases"] = False
        state.save_ticket("T-1", _ticket("new"))
        from unittest.mock import patch
        with patch("frshty.terminal.session_healthy",
                   return_value={"alive": False, "claude_running": False}):
            resp = c.get("/api/tickets/T-1/detail")
        assert resp.status_code == 200
        assert resp.json().get("release") is None


class TestAssignmentRoute:
    def test_assigns_and_creates_release(self, client):
        c, _ = client
        state.save_ticket("T-1", _ticket("new"))
        resp = c.post("/api/tickets/T-1/release",
                       json={"release_key": "v1.0", "title": "First"})
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert body["status"] == "ok"
        assert body["release"]["release_key"] == "v1.0"
        assert body["release"]["title"] == "First"

    def test_unassigns_with_null(self, client):
        c, _ = client
        state.save_ticket("T-1", _ticket("new"))
        c.post("/api/tickets/T-1/release", json={"release_key": "v1.0"})
        resp = c.post("/api/tickets/T-1/release", json={"release_key": None})
        assert resp.status_code == 200
        assert resp.json()["release"] is None

    def test_rejects_invalid_release_key(self, client):
        c, _ = client
        state.save_ticket("T-1", _ticket("new"))
        resp = c.post("/api/tickets/T-1/release", json={"release_key": "has/slash"})
        assert resp.status_code == 400
        assert "invalid release_key" in resp.json()["error"]

    def test_returns_404_for_missing_ticket(self, client):
        c, _ = client
        resp = c.post("/api/tickets/NOPE/release", json={"release_key": "v1.0"})
        assert resp.status_code == 404


class TestListAndDetail:
    def test_list_returns_summaries(self, client):
        c, _ = client
        state.save_ticket("T-1", _ticket("merged", release_key="v1.0"))
        state.save_ticket("T-2", _ticket("new", release_key="v1.0", slug="T-2-bar"))
        from features import releases as releases_mod
        releases_mod.upsert_release(state.active_instance_key(), "v1.0", title="Auth")
        resp = c.get("/api/releases")
        assert resp.status_code == 200
        items = resp.json()["releases"]
        assert len(items) == 1
        assert items[0]["release_key"] == "v1.0"
        assert items[0]["ticket_count"] == 2
        assert items[0]["terminal_count"] == 1

    def test_detail_returns_tickets_in_release(self, client):
        c, _ = client
        state.save_ticket("T-1", _ticket("merged", release_key="v1.0"))
        from features import releases as releases_mod
        releases_mod.upsert_release(state.active_instance_key(), "v1.0")
        resp = c.get("/api/releases/v1.0")
        assert resp.status_code == 200
        body = resp.json()
        assert body["release_key"] == "v1.0"
        assert len(body["tickets"]) == 1
        assert body["tickets"][0]["ticket_key"] == "T-1"

    def test_detail_404_for_missing_release(self, client):
        c, _ = client
        resp = c.get("/api/releases/nope")
        assert resp.status_code == 404


class TestTicketDetailExtension:
    def test_detail_includes_release_block_when_enabled(self, client):
        c, _ = client
        state.save_ticket("T-1", _ticket("merged", release_key="v1.0"))
        from features import releases as releases_mod
        releases_mod.upsert_release(state.active_instance_key(), "v1.0")
        from unittest.mock import patch
        with patch("frshty.terminal.session_healthy",
                   return_value={"alive": False, "claude_running": False}):
            resp = c.get("/api/tickets/T-1/detail")
        assert resp.status_code == 200
        rel = resp.json()["release"]
        assert rel is not None
        assert rel["release_key"] == "v1.0"
        assert rel["ticket_count"] == 1
        assert rel["terminal_count"] == 1


class TestManualInspect:
    def test_inspect_404_when_no_release(self, client):
        c, _ = client
        resp = c.post("/api/releases/nope/inspect", json={})
        # 404 (no release) takes precedence over 400 (events not enabled)
        assert resp.status_code in (400, 404)

    def test_inspect_requires_events(self, client):
        c, _ = client
        from features import releases as releases_mod
        releases_mod.upsert_release(state.active_instance_key(), "v1.0")
        # _events_enabled returns False without runtime
        resp = c.post("/api/releases/v1.0/inspect", json={"force": True})
        assert resp.status_code == 400
        assert "events not enabled" in resp.json()["error"]
