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
    from web.state import set_primary_config
    set_primary_config({
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
    })
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
        with patch("web.tickets.terminal.session_healthy", return_value={"alive": False, "claude_running": False}):
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
        with patch("web.tickets.terminal.kill_terminal") as mock_kill:
            resp = client.delete("/api/tickets/T-1/terminal")
        assert resp.status_code == 200
        mock_kill.assert_called_once_with("T-1")

    def test_reset_terminal_kills_and_spawns_with_claude(self, client):
        state.save("tickets", {"T-1": {"status": "in_review", "slug": "T-1-s"}})
        with patch("web.tickets.terminal.kill_terminal") as mock_kill, \
             patch("web.tickets.terminal.ensure_session") as mock_ensure, \
             patch("web.tickets.terminal.send_keys") as mock_send, \
             patch("web.tickets.time.sleep"):
            resp = client.post("/api/tickets/T-1/terminal/reset")
        assert resp.status_code == 200
        mock_kill.assert_called_once_with("T-1")
        mock_ensure.assert_called_once()
        mock_send.assert_called_once_with("T-1", "claude --dangerously-skip-permissions")

    def test_reset_terminal_not_found(self, client):
        resp = client.post("/api/tickets/NOPE/terminal/reset")
        assert resp.status_code == 404

    def test_pr_comments_empty(self, client):
        state.save("tickets", {"T-1": {"status": "in_review", "slug": "T-1-s"}})
        resp = client.get("/api/tickets/T-1/pr-comments")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_status_override_resets_counters(self, client):
        state.save("tickets", {"T-1": {
            "status": "in_review",
            "slug": "T-1-s",
            "ci_fix_attempts": 2,
            "conflict_resolution_attempts": 1,
            "ci_passed": True,
            "checks_started_at": "2026-04-15T00:00:00+00:00",
        }})
        resp = client.post("/api/tickets/T-1/status", json={"status": "pr_failed"})
        assert resp.status_code == 200, resp.text
        ts = state.load("tickets")["T-1"]
        assert ts["status"] == "pr_failed"
        assert ts["ci_fix_attempts"] == 0
        assert ts["conflict_resolution_attempts"] == 0
        assert "ci_passed" not in ts
        assert "checks_started_at" not in ts

    def test_status_override_illegal_transition(self, client):
        state.save("tickets", {"T-1": {"status": "new", "slug": "T-1-s"}})
        resp = client.post("/api/tickets/T-1/status", json={"status": "in_review"})
        assert resp.status_code == 400, resp.text
        ts = state.load("tickets")["T-1"]
        assert ts["status"] == "new"

    def test_status_override_invalid(self, client):
        state.save("tickets", {"T-1": {"status": "pr_failed", "slug": "T-1-s"}})
        resp = client.post("/api/tickets/T-1/status", json={"status": "garbage"})
        assert resp.status_code == 400

    def test_status_override_not_found(self, client):
        resp = client.post("/api/tickets/NOPE/status", json={"status": "in_review"})
        assert resp.status_code == 404


class TestDiscardTicket:
    def test_discard_removes_dir_and_state(self, client, tmp_path):
        slug = "T-1-thing"
        state.save("tickets", {"T-1": {"status": "in_review", "slug": slug}})
        ticket_dir = tmp_path / "tickets" / slug
        ticket_dir.mkdir(parents=True)
        (ticket_dir / "file.txt").write_text("hello")
        with patch("web.tickets.terminal.kill_terminal"), \
             patch("web.tickets.get_repos", return_value=[]), \
             patch("core.scheduler.delete"):
            resp = client.delete("/api/tickets/T-1")
        assert resp.status_code == 200
        assert resp.json() == {"status": "discarded"}
        assert not ticket_dir.exists()
        assert "T-1" not in state.load("tickets")

    def test_discard_not_found(self, client):
        resp = client.delete("/api/tickets/NOPE")
        assert resp.status_code == 404

    def test_discard_falls_back_to_sudo_on_permission_error(self, client, tmp_path):
        slug = "T-2-perm"
        state.save("tickets", {"T-2": {"status": "in_review", "slug": slug}})
        ticket_dir = tmp_path / "tickets" / slug
        ticket_dir.mkdir(parents=True)

        def fake_rmtree(path):
            raise PermissionError(13, "Permission denied", str(path))

        sudo_result = MagicMock(returncode=0, stderr=b"")

        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["sudo", "-n", "rm"]:
                Path(cmd[-1]).rmdir()
                return sudo_result
            return MagicMock(returncode=0, stderr=b"")

        with patch("web.tickets.terminal.kill_terminal"), \
             patch("web.tickets.get_repos", return_value=[]), \
             patch("core.scheduler.delete"), \
             patch("shutil.rmtree", side_effect=fake_rmtree), \
             patch("web.tickets.subprocess.run", side_effect=fake_run) as mock_run:
            resp = client.delete("/api/tickets/T-2")
        assert resp.status_code == 200
        sudo_calls = [c for c in mock_run.call_args_list if c.args[0][:3] == ["sudo", "-n", "rm"]]
        assert len(sudo_calls) == 1
        assert not ticket_dir.exists()
        assert "T-2" not in state.load("tickets")

    def test_discard_emits_event_when_cleanup_fully_fails(self, client, tmp_path):
        slug = "T-3-stuck"
        state.save("tickets", {"T-3": {"status": "in_review", "slug": slug}})
        ticket_dir = tmp_path / "tickets" / slug
        ticket_dir.mkdir(parents=True)
        (ticket_dir / "stuck.txt").write_text("x")

        def fake_rmtree(path):
            raise PermissionError(13, "Permission denied", str(path))

        def fake_run(cmd, **kwargs):
            if cmd[:3] == ["sudo", "-n", "rm"]:
                return MagicMock(returncode=1, stderr=b"sudo: a password is required\n")
            return MagicMock(returncode=0, stderr=b"")

        with patch("web.tickets.terminal.kill_terminal"), \
             patch("web.tickets.get_repos", return_value=[]), \
             patch("core.scheduler.delete"), \
             patch("shutil.rmtree", side_effect=fake_rmtree), \
             patch("web.tickets.subprocess.run", side_effect=fake_run):
            resp = client.delete("/api/tickets/T-3")
        assert resp.status_code == 200
        assert "T-3" not in state.load("tickets")
        events = client.get("/api/events").json()
        cleanup_events = [e for e in events if e["event"] == "ticket_discard_cleanup_failed"]
        assert len(cleanup_events) == 1
        assert cleanup_events[0]["meta"]["ticket"] == "T-3"
        assert cleanup_events[0]["meta"]["sudo_rc"] == 1


class TestScheduled:
    def test_empty(self, client):
        resp = client.get("/api/scheduled")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_with_scheduled_items(self, client):
        import core.scheduler as scheduler
        from datetime import datetime, timezone
        token = state.use("test")
        try:
            with patch("core.scheduler.log"):
                scheduler.schedule(
                    "T-1", "create_pr",
                    datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc),
                    meta={"slug": "s"},
                )
        finally:
            state.reset(token)
        resp = client.get("/api/scheduled")
        assert resp.status_code == 200
        data = resp.json()
        keys = [item.get("key") for item in data]
        assert "T-1" in keys, f"expected T-1 in scheduled list, got {keys}"
        t1 = next(item for item in data if item["key"] == "T-1")
        assert t1["type"] == "scheduled_pr"
        assert t1.get("action") == "create_pr"


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
        with patch("web.reviews.multiprocessing.Process") as mock_proc:
            mock_proc.return_value = MagicMock()
            resp = client.post("/api/reviews/submit", json={"url": "https://github.com/org/repo/pull/123"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["pr_id"] == 123
        assert data["repo"] == "org/repo"
