from datetime import datetime, timedelta, timezone

import core.db as db
from services import work_store


def _mkitem(objective="do the thing", **kw):
    return work_store.create_item(objective, **kw)


class TestMigration:
    def test_tables_exist(self):
        names = {r["name"] for r in db.query_all(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"work_items", "work_runs", "work_events"} <= names

    def test_migration_idempotent(self, tmp_path):
        import pathlib
        sql = pathlib.Path("migrations/014_work_layer.sql").read_text()
        import sqlite3
        conn = sqlite3.connect(tmp_path / "t.db")
        conn.executescript(sql)
        conn.executescript(sql)
        rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        assert len([r for r in rows if r[0].startswith("work_")]) == 3


class TestTransitions:
    def test_stop_flips_to_needs_you(self):
        item_id = _mkitem()
        work_store.add_run(item_id, "sid-stop-1", "work-1", "/tmp")
        ok = work_store.record_event("sid-stop-1", "Stop", {"last_assistant_message": "did the thing"})
        assert ok is True
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "did the thing" in item["stop_reason"]
        run = db.query_one("SELECT status FROM work_runs WHERE session_id = 'sid-stop-1'")
        assert run["status"] == "stopped"

    def test_unknown_session_writes_nothing(self):
        before = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        ok = work_store.record_event("sid-nonexistent", "Stop", {})
        assert ok is False
        after = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        assert after == before

    def test_prompt_resumes_working(self):
        item_id = _mkitem()
        work_store.add_run(item_id, "sid-resume-1", "work-2", "/tmp")
        work_store.record_event("sid-resume-1", "Stop", {})
        work_store.record_event("sid-resume-1", "UserPromptSubmit", {})
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"

    def test_session_end_keeps_done(self):
        item_id = _mkitem()
        work_store.add_run(item_id, "sid-done-1", "work-3", "/tmp")
        work_store.apply_action(item_id, "done")
        work_store.record_event("sid-done-1", "SessionEnd", {"reason": "exit"})
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "done"

    def test_launch_failed_marks_item(self):
        item_id = _mkitem()
        run_id = work_store.add_run(item_id, "sid-fail-1", "work-4", "/tmp")
        work_store.mark_launch_failed(run_id, "tmux exploded")
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"
        assert "tmux exploded" in item["stop_reason"]


class TestGrouping:
    def test_five_groups(self):
        now = datetime.now(timezone.utc)
        fresh = _mkitem("working fresh")
        stale = _mkitem("working stale")
        needs = _mkitem("needs op")
        snoozed = _mkitem("snoozed future")
        expired = _mkitem("snooze expired")
        done = _mkitem("finished")
        db.execute("UPDATE work_items SET state='needs_you' WHERE id = ?", (needs,))
        db.execute("UPDATE work_items SET state='done' WHERE id = ?", (done,))
        db.execute(
            "UPDATE work_items SET state='waiting_external', snoozed_until=? WHERE id = ?",
            ((now + timedelta(hours=2)).isoformat(), snoozed),
        )
        db.execute(
            "UPDATE work_items SET state='waiting_external', snoozed_until=? WHERE id = ?",
            ((now - timedelta(hours=2)).isoformat(), expired),
        )
        db.execute(
            "UPDATE work_items SET updated_at=? WHERE id = ?",
            ((now - timedelta(minutes=31)).isoformat(), stale),
        )
        groups = work_store.grouped_items(now)
        ids = {g: {r["id"] for r in rows} for g, rows in groups.items()}
        assert fresh in ids["agent_working"]
        assert stale in ids["failed_stale"]
        assert needs in ids["needs_you"]
        assert snoozed in ids["waiting_external"]
        assert expired in ids["needs_you"]
        assert done in ids["done"]

    def test_stale_boundary(self):
        now = datetime.now(timezone.utc)
        fresh = _mkitem("29 min old")
        db.execute("UPDATE work_items SET updated_at=? WHERE id = ?",
                   ((now - timedelta(minutes=29)).isoformat(), fresh))
        groups = work_store.grouped_items(now)
        assert fresh in {r["id"] for r in groups["agent_working"]}

    def test_empty_groups_are_lists(self):
        groups = work_store.grouped_items()
        assert set(groups.keys()) == set(work_store.GROUPS)
        for rows in groups.values():
            assert isinstance(rows, list)


class TestIntake:
    def _client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.work import router
        a = FastAPI()
        a.include_router(router)
        return TestClient(a)

    def test_intake_creates_item_and_run(self, tmp_path, monkeypatch):
        from unittest.mock import patch, MagicMock
        reg = MagicMock()
        reg.config = {"workspace": {"root": tmp_path}}
        instances = MagicMock()
        instances.get.return_value = reg
        with patch("web.work.runtime.instances", return_value=instances), \
             patch("web.work.terminal.launch_claude") as mock_launch, \
             patch("web.work.terminal.session_healthy", return_value={"alive": True, "claude_running": True}):
            client = self._client()
            r = client.post("/api/work/intake", json={"text": "ship the widget"})
        assert r.status_code == 200, r.text
        d = r.json()
        mock_launch.assert_called_once()
        args = mock_launch.call_args
        assert args.args[0] == f"work-{d['item_id']}"
        assert args.args[2] == d["session_id"]
        assert d["session_id"] != ""
        assert "ship the widget" in args.args[3]
        assert args.args[4] is True
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (d["item_id"],))
        assert item["state"] == "agent_working"
        run = db.query_one("SELECT status FROM work_runs WHERE id = ?", (d["run_id"],))
        assert run["status"] == "launched"

    def test_intake_launch_failure_marks_item(self, tmp_path):
        from unittest.mock import patch, MagicMock
        reg = MagicMock()
        reg.config = {"workspace": {"root": tmp_path}}
        instances = MagicMock()
        instances.get.return_value = reg
        with patch("web.work.runtime.instances", return_value=instances), \
             patch("web.work.terminal.launch_claude", side_effect=RuntimeError("boom")), \
             patch("web.work.log.emit"):
            client = self._client()
            r = client.post("/api/work/intake", json={"text": "doomed"})
        assert r.status_code == 500
        item_id = r.json()["item_id"]
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"
        assert "boom" in item["stop_reason"]

    def test_intake_without_personal_is_503(self):
        from unittest.mock import patch
        with patch("web.work.runtime.instances", return_value=None):
            client = self._client()
            r = client.post("/api/work/intake", json={"text": "anything"})
        assert r.status_code == 503

    def test_intake_rejects_empty(self):
        client = self._client()
        r = client.post("/api/work/intake", json={"text": "   "})
        assert r.status_code == 400

    def test_items_endpoint_renders_without_personal(self):
        from unittest.mock import patch
        with patch("web.work.runtime.instances", return_value=None):
            client = self._client()
            r = client.get("/api/work/items")
        assert r.status_code == 200
        d = r.json()
        assert set(d["groups"].keys()) == set(work_store.GROUPS)
        assert d["personal_loaded"] is False

    def test_work_page_renders(self):
        client = self._client()
        r = client.get("/work")
        assert r.status_code == 200
        assert "Needs you" in r.text

    def test_concurrent_intakes_serialize_launch(self, tmp_path):
        import threading
        import time
        from unittest.mock import patch, MagicMock
        reg = MagicMock()
        reg.config = {"workspace": {"root": tmp_path}}
        instances = MagicMock()
        instances.get.return_value = reg
        active = []
        overlaps = []

        def slow_launch(*a, **kw):
            active.append(1)
            if len(active) > 1:
                overlaps.append(1)
            time.sleep(0.05)
            active.pop()

        with patch("web.work.runtime.instances", return_value=instances), \
             patch("web.work.terminal.launch_claude", side_effect=slow_launch), \
             patch("web.work.terminal.session_healthy", return_value={"alive": True, "claude_running": True}):
            client = self._client()
            threads = [
                threading.Thread(target=lambda i=i: client.post(
                    "/api/work/intake", json={"text": f"parallel {i}"}))
                for i in range(4)
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
        assert overlaps == [], "launch_claude entered concurrently"
        n = db.query_one("SELECT COUNT(*) AS n FROM work_items WHERE objective LIKE 'parallel %'")["n"]
        assert n == 4


class TestQueueReservation:
    def test_work_run_single_flight_under_personal(self):
        import core.queue as q
        first = q.enqueue_job("personal", "work_run", {"run_id": 1})
        q.enqueue_job("personal", "work_run", {"run_id": 2})
        client_job = q.enqueue_job("someclient", "scan_tickets", {})
        claimed = q.claim_next()
        assert claimed is not None and claimed["id"] == first
        second = q.claim_next()
        assert second is not None
        assert second["id"] == client_job, "second work_run must not run while first is running"
        third = q.claim_next()
        assert third is None


class TestPersonalConfig:
    def test_loads_and_stays_quiet(self):
        import os
        import pytest
        if not os.path.isfile("config/personal.toml"):
            pytest.skip("config/personal.toml is machine-local and absent here; the work layer runs read-only")
        import core.config as cfg
        from core.registry import Instances
        from core.tasks.routes import _cron_routes
        config = cfg.load_config("config/personal.toml")
        assert config["job"]["key"] == "personal"
        assert "git" not in config
        assert cfg.get_repos(config) == []
        instances = Instances()
        reg = instances.add(config)
        jobs = _cron_routes({"instance_key": "personal"}, {"personal": reg})
        tasks = {j["task"] for j in jobs}
        assert tasks == {"scheduler_check", "dep_store_gc"}, tasks

    def test_no_port_or_host_collision(self):
        import pathlib
        import tomllib
        seen_hosts = {}
        for p in sorted(pathlib.Path("config").glob("*.toml")):
            if p.name in ("example.toml", "test.toml", "tipu-test.toml", "discovery.toml"):
                continue
            raw = tomllib.loads(p.read_text())
            host = (raw.get("job") or {}).get("host", "")
            if host:
                assert host not in seen_hosts, f"{p.name} and {seen_hosts[host]} share {host}"
                seen_hosts[host] = p.name


class TestHookInstaller:
    def test_installer_idempotent_and_preserving(self, tmp_path):
        import json
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_work_hooks", "scripts/install_work_hooks.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps({
            "hooks": {"UserPromptSubmit": [{"matcher": "", "hooks": [
                {"type": "command", "command": "echo existing"}]}]},
            "permissions": {"allow": ["Read"]},
        }))
        added = mod.install_into(str(settings))
        assert set(added) == set(mod.EVENTS)
        again = mod.install_into(str(settings))
        assert again == []
        data = json.loads(settings.read_text())
        prompt_hooks = data["hooks"]["UserPromptSubmit"]
        commands = [h["command"] for e in prompt_hooks for h in e["hooks"]]
        assert "echo existing" in commands
        assert data["permissions"] == {"allow": ["Read"]}
        for event in mod.EVENTS:
            cmds = [h["command"] for e in data["hooks"][event] for h in e["hooks"]]
            assert any("work_hook.py" in c for c in cmds)


class TestHookScript:
    def test_hook_records_stop_event(self, tmp_path):
        import json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        item_id = _mkitem("hooked item")
        work_store.add_run(item_id, "sid-hook-1", "work-h1", "/tmp")
        payload = json.dumps({"session_id": "sid-hook-1", "hook_event_name": "Stop"})
        r = subprocess.run(
            [".venv/bin/python", "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0, r.stderr
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"

    def test_hook_foreign_session_fast_noop(self, tmp_path):
        import json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        before = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        payload = json.dumps({"session_id": "sid-foreign-xyz", "hook_event_name": "Stop"})
        r = subprocess.run(
            [".venv/bin/python", "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0
        after = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        assert after == before

    def test_hook_garbage_input_exits_zero(self):
        import subprocess
        r = subprocess.run(
            [".venv/bin/python", "scripts/work_hook.py"],
            input="not json at all", capture_output=True, text=True, timeout=15,
        )
        assert r.returncode == 0
