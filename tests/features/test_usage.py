from datetime import datetime, timezone

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import core.db as db
import web.middleware as middleware
from services import usage
from web.usage import router as usage_router


def _clear():
    db.execute("DELETE FROM usage_counters")


def _today():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


@pytest.fixture
def client():
    _clear()
    app = FastAPI()
    middleware.install(app)
    app.include_router(usage_router)

    @app.get("/tickets/{key}")
    def ticket_page(key: str):
        return {"key": key}

    @app.get("/api/thing")
    def api_thing():
        return {"ok": True}

    @app.get("/api/never-called")
    def api_never_called():
        return {"ok": True}

    @app.get("/api/status")
    def api_status():
        return {"ok": True}

    return TestClient(app)


class TestMigration:
    def test_table_exists(self):
        names = {r["name"] for r in db.query_all(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert "usage_counters" in names


class TestRecord:
    def test_upsert_increments(self):
        _clear()
        usage.record("route", "GET /x", instance="aimyable")
        usage.record("route", "GET /x", instance="aimyable")
        row = db.query_one(
            "SELECT count FROM usage_counters WHERE kind='route' AND name='GET /x'")
        assert row["count"] == 2

    def test_batch_count(self):
        _clear()
        usage.record("ui", "/x button:Save", n=5)
        row = db.query_one(
            "SELECT count FROM usage_counters WHERE kind='ui'")
        assert row["count"] == 5

    def test_instances_kept_separate(self):
        _clear()
        usage.record("route", "GET /x", instance="aimyable")
        usage.record("route", "GET /x", instance="nectar")
        rows = db.query_all(
            "SELECT instance FROM usage_counters WHERE name='GET /x'")
        assert {r["instance"] for r in rows} == {"aimyable", "nectar"}

    def test_empty_name_is_noop(self):
        _clear()
        usage.record("ui", "   ")
        assert db.query_all("SELECT * FROM usage_counters") == []

    def test_name_whitespace_collapsed_and_capped(self):
        _clear()
        usage.record("ui", "a   b\t c" + "x" * 500)
        row = db.query_one("SELECT name FROM usage_counters")
        assert row["name"].startswith("a b c")
        assert len(row["name"]) == 200

    def test_failure_emits_instead_of_raising(self, monkeypatch):
        emitted = []
        monkeypatch.setattr(usage.db, "execute",
                            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
        monkeypatch.setattr(usage.log, "emit",
                            lambda event, summary, **k: emitted.append(event))
        usage.record("route", "GET /x")
        assert emitted == ["usage_record_failed"]


class TestIsTracked:
    def test_skips_polling_and_static(self):
        assert usage.is_tracked("/api/events") is False
        assert usage.is_tracked("/api/status") is False
        assert usage.is_tracked("/static/frshty.css") is False
        assert usage.is_tracked("/api/usage/ui") is False

    def test_tracks_normal_paths(self):
        assert usage.is_tracked("/tickets") is True
        assert usage.is_tracked("/api/poll") is True


class TestRouteMiddleware:
    def test_records_route_template(self, client):
        client.get("/tickets/DEV-123")
        row = db.query_one(
            "SELECT name FROM usage_counters WHERE kind='route'")
        assert row["name"] == "GET /tickets/{key}"

    def test_skip_listed_route_not_recorded(self, client):
        client.get("/api/status")
        assert db.query_all(
            "SELECT * FROM usage_counters WHERE kind='route'") == []

    def test_404_not_recorded(self, client):
        client.get("/no/such/page")
        assert db.query_all(
            "SELECT * FROM usage_counters WHERE kind='route'") == []


class TestUiEndpoint:
    def test_records_normalized_page_and_element(self, client):
        resp = client.post("/api/usage/ui", json={
            "page": "/tickets/DEV-123",
            "events": [{"element": "button:Retry", "count": 3}],
        })
        assert resp.json() == {"ok": True, "recorded": 1}
        row = db.query_one(
            "SELECT name, count FROM usage_counters WHERE kind='ui'")
        assert row["name"] == "/tickets/{key} button:Retry"
        assert row["count"] == 3

    def test_rejects_relative_page(self, client):
        resp = client.post("/api/usage/ui", json={
            "page": "javascript:x",
            "events": [{"element": "button:X", "count": 1}],
        })
        assert resp.json()["ok"] is False

    def test_ignores_bad_events(self, client):
        resp = client.post("/api/usage/ui", json={
            "page": "/tickets/DEV-1",
            "events": [{"element": "", "count": 2},
                       {"element": "a:x", "count": "nope"},
                       {"element": "a:y", "count": 0}],
        })
        assert resp.json() == {"ok": True, "recorded": 0}
        assert db.query_all("SELECT * FROM usage_counters WHERE kind='ui'") == []


class TestReport:
    def test_used_unused_and_excluded(self, client):
        client.get("/api/thing")
        report = client.get("/api/usage/report").json()
        used_names = [r["name"] for r in report["routes"]["used"]]
        assert "GET /api/thing" in used_names
        assert "GET /api/never-called" in report["routes"]["unused"]
        assert "GET /api/status" in report["routes"]["excluded_from_tracking"]
        assert "POST /api/usage/ui" in report["routes"]["excluded_from_tracking"]
        assert report["tracking_since"] == _today()

    def test_includes_ui_and_mcp_sections(self, client):
        usage.record("mcp", "get_tickets")
        usage.record("ui", "/tickets button:Save", n=2)
        report = client.get("/api/usage/report").json()
        assert report["mcp"][0]["name"] == "get_tickets"
        assert report["ui"][0]["count"] == 2
        assert report["ui"][0]["last_day"] == _today()
