import base64
import sys
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

    def test_search_filters_by_objective(self):
        hit = _mkitem("merge the billing PR")
        miss = _mkitem("update the readme")
        groups = work_store.grouped_items(q="BILLING")
        ids = {r["id"] for rows in groups.values() for r in rows}
        assert hit in ids
        assert miss not in ids

    def test_search_lifts_done_window(self):
        now = datetime.now(timezone.utc)
        old = _mkitem("ancient billing task")
        db.execute("UPDATE work_items SET state='done', updated_at=? WHERE id = ?",
                   ((now - timedelta(days=30)).isoformat(), old))
        without_q = work_store.grouped_items(now)
        assert old not in {r["id"] for r in without_q["done"]}
        with_q = work_store.grouped_items(now, q="ancient billing")
        assert old in {r["id"] for r in with_q["done"]}

    def test_done_items_are_sorted_by_completion_time_descending(self):
        now = datetime.now(timezone.utc)
        completed_first = _mkitem("completed first")
        completed_last = _mkitem("completed last")
        db.execute(
            "UPDATE work_items SET state='done', priority=10, updated_at=? WHERE id = ?",
            (now.isoformat(), completed_first),
        )
        db.execute(
            "UPDATE work_items SET state='done', priority=0, updated_at=? WHERE id = ?",
            ((now - timedelta(hours=1)).isoformat(), completed_last),
        )
        db.execute(
            "INSERT INTO work_events(work_item_id, kind, created_at) VALUES (?, 'operator_done', ?)",
            (completed_first, (now - timedelta(hours=2)).isoformat()),
        )
        db.execute(
            "INSERT INTO work_events(work_item_id, kind, created_at) VALUES (?, 'self_reported_done', ?)",
            (completed_last, (now - timedelta(hours=1)).isoformat()),
        )

        done_ids = [
            row["id"] for row in work_store.grouped_items(now, q="completed")["done"]
        ]

        assert done_ids == [completed_last, completed_first]


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
        with patch("services.work_launch.runtime.instances", return_value=instances), \
             patch("services.work_launch.terminal.launch_claude") as mock_launch, \
             patch("services.work_launch.terminal.session_healthy", return_value={"alive": True, "agent_running": True}):
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
        with patch("services.work_launch.runtime.instances", return_value=instances), \
             patch("services.work_launch.terminal.launch_claude", side_effect=RuntimeError("boom")), \
             patch("services.work_launch.log.emit"):
            client = self._client()
            r = client.post("/api/work/intake", json={"text": "doomed"})
        assert r.status_code == 500
        item_id = r.json()["item_id"]
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"
        assert "boom" in item["stop_reason"]

    def test_intake_without_personal_is_503(self):
        from unittest.mock import patch
        with patch("services.work_launch.runtime.instances", return_value=None):
            client = self._client()
            r = client.post("/api/work/intake", json={"text": "anything"})
        assert r.status_code == 503

    def test_intake_rejects_empty(self):
        client = self._client()
        r = client.post("/api/work/intake", json={"text": "   "})
        assert r.status_code == 400

    def test_items_endpoint_renders_without_personal(self):
        from unittest.mock import patch
        with patch("services.work_launch.runtime.instances", return_value=None):
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

    def test_work_detail_collapses_consecutive_tool_calls_by_default(self):
        r = self._client().get("/work/1")

        assert r.status_code == 200
        assert '<details v-else-if="e.kind === \'tools\'" class="tl-tools">' in r.text
        assert "e.entries.length }} tool call" in r.text
        assert "previous.kind === \"tools\"" in r.text
        assert '<details v-else-if="e.kind === \'tools\'" class="tl-tools" open>' not in r.text

    def test_work_detail_reports_load_and_render_failures(self):
        r = self._client().get("/work/1")

        assert r.status_code == 200
        assert "Loading work item…" in r.text
        assert "Unable to load this work item:" in r.text
        assert 'id="work-fatal"' in r.text
        assert "setInterval(() => this.refresh(), 15000)" in r.text

    def test_transcript_image_endpoint_serves_lazy_image(self, tmp_path):
        import json as _json
        image_bytes = b"\x89PNG\r\n\x1a\nwork timeline"
        transcript = tmp_path / "image.jsonl"
        transcript.write_text(_json.dumps({
            "type": "user", "timestamp": "2026-08-25T03:51:39.000Z",
            "message": {"content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/png",
                                                   "data": base64.b64encode(image_bytes).decode()}},
                {"type": "text", "text": "[Image: original 20x10]"},
            ]},
        }))
        item_id = _mkitem("view transcript image")
        work_store.add_run(item_id, f"sid-img-{item_id}", f"work-{item_id}", "/tmp")
        db.execute("UPDATE work_runs SET transcript_path = ? WHERE work_item_id = ?",
                   (str(transcript), item_id))
        image_ref = work_store.item_detail(item_id)["timeline"][0]["images"][0]

        r = self._client().get(
            f"/api/work/items/{item_id}/transcript-image/{image_ref['id']}")
        assert r.status_code == 200
        assert r.headers["content-type"] == "image/png"
        assert r.headers["content-security-policy"] == "sandbox"
        assert r.content == image_bytes

    def test_items_endpoint_paginates_done(self):
        from unittest.mock import patch
        from web.work import DONE_PAGE_SIZE
        ids = []
        for n in range(DONE_PAGE_SIZE + 3):
            item = _mkitem(f"paged task {n}")
            work_store.apply_action(item, "done")
            ids.append(item)
        with patch("services.work_launch.runtime.instances", return_value=None):
            client = self._client()
            p1 = client.get("/api/work/items", params={"q": "paged task"}).json()
            p2 = client.get("/api/work/items",
                            params={"q": "paged task", "done_page": 2}).json()
            clamped = client.get("/api/work/items",
                                 params={"q": "paged task", "done_page": 99}).json()
        assert p1["counts"]["done"] == DONE_PAGE_SIZE + 3
        assert p1["done_pages"] == 2
        assert len(p1["groups"]["done"]) == DONE_PAGE_SIZE
        assert len(p2["groups"]["done"]) == 3
        page_ids = {r["id"] for r in p1["groups"]["done"]} | {r["id"] for r in p2["groups"]["done"]}
        assert page_ids == set(ids)
        assert clamped["done_page"] == 2

    def test_items_endpoint_search_filters(self):
        from unittest.mock import patch
        hit = _mkitem("unique needle objective")
        _mkitem("unrelated haystack")
        with patch("services.work_launch.runtime.instances", return_value=None):
            client = self._client()
            d = client.get("/api/work/items", params={"q": "needle"}).json()
        all_ids = {r["id"] for rows in d["groups"].values() for r in rows}
        assert all_ids == {hit}

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

        with patch("services.work_launch.runtime.instances", return_value=instances), \
             patch("services.work_launch.terminal.launch_claude", side_effect=slow_launch), \
             patch("services.work_launch.terminal.session_healthy", return_value={"alive": True, "agent_running": True}):
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
        db.execute("DELETE FROM jobs")
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
        expected = {e for e in mod.EVENTS if e != "PreToolUse"}
        expected |= {"PreToolUse[AskUserQuestion]", "PreToolUse[Bash]"}
        assert set(added) == expected
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
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0, r.stderr
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"
        assert item["stop_reason"] == "tmux session gone"
        kinds = [e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ? ORDER BY id", (item_id,))]
        assert "Stop" in kinds

    def test_hook_foreign_session_fast_noop(self, tmp_path):
        import json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        before = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        payload = json.dumps({"session_id": "sid-foreign-xyz", "hook_event_name": "Stop"})
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0
        after = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        assert after == before

    def test_hook_garbage_input_exits_zero(self):
        import subprocess
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input="not json at all", capture_output=True, text=True, timeout=15,
        )
        assert r.returncode == 0


class TestAutocontinue:
    def _setup(self, monkeypatch, tail="Progress made. Continuing next step.", send_ok=True):
        from unittest.mock import MagicMock
        item_id = _mkitem("auto item")
        run_id = work_store.add_run(item_id, f"sid-auto-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-auto-{item_id}", "Stop", {})
        monkeypatch.setattr(work_store, "last_assistant_text", lambda p: tail)
        sender = MagicMock(return_value=send_ok)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        return item_id, run_id, sender

    def _state(self, item_id):
        return db.query_one(
            "SELECT state, continues_used, stop_reason FROM work_items WHERE id = ?", (item_id,))

    def test_continues_when_no_question(self, monkeypatch):
        item_id, _, sender = self._setup(monkeypatch)
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "continued"
        sender.assert_called_once()
        s = self._state(item_id)
        assert s["state"] == "agent_working"
        assert s["continues_used"] == 1

    def test_question_blocks_continue(self, monkeypatch):
        item_id, _, sender = self._setup(monkeypatch, tail="Should I use the staging bucket or prod?")
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "question"
        sender.assert_not_called()
        s = self._state(item_id)
        assert s["state"] == "needs_you"
        assert "staging bucket" in s["stop_reason"]

    def test_done_marker_echo_does_not_complete(self, monkeypatch):
        item_id, _, sender = self._setup(
            monkeypatch, tail="I will end with WORK_DONE when finished. Proceeding with step 2.")
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "continued"
        assert self._state(item_id)["state"] == "agent_working"

    def test_delayed_event_cannot_resurrect_done(self):
        item_id = _mkitem("resurrect guard")
        work_store.add_run(item_id, f"sid-res-{item_id}", f"work-{item_id}", "/tmp")
        work_store.apply_action(item_id, "done")
        work_store.record_event(f"sid-res-{item_id}", "UserPromptSubmit", {})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "done"

    def test_done_marker_completes_item(self, monkeypatch):
        item_id, run_id, sender = self._setup(monkeypatch, tail="All files written.\nWORK_DONE")
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "done"
        sender.assert_not_called()
        assert self._state(item_id)["state"] == "done"
        run = db.query_one("SELECT status FROM work_runs WHERE id = ?", (run_id,))
        assert run["status"] == "finished"

    def test_cap_blocks_continue(self, monkeypatch):
        item_id, _, sender = self._setup(monkeypatch)
        db.execute("UPDATE work_items SET continues_used = continue_cap WHERE id = ?", (item_id,))
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "capped"
        sender.assert_not_called()
        assert self._state(item_id)["state"] == "needs_you"

    def test_disabled_blocks_continue(self, monkeypatch):
        item_id, _, sender = self._setup(monkeypatch)
        db.execute("UPDATE work_items SET autocontinue = 0 WHERE id = ?", (item_id,))
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "disabled"
        sender.assert_not_called()

    def test_dead_session_marks_failed(self, monkeypatch):
        item_id, _, _ = self._setup(monkeypatch, send_ok=False)
        out = work_store.maybe_autocontinue(f"sid-auto-{item_id}", "/tmp/t.jsonl")
        assert out == "session_gone"
        assert self._state(item_id)["state"] == "failed_stale"


class TestStaleSweep:
    def _mkstale(self, tmp_path, tail_text="Working on step 3.", minutes=40):
        import json as _json
        item_id = _mkitem("stale sweep item")
        run_id = work_store.add_run(item_id, f"sid-sweep-{item_id}", f"work-{item_id}", "/tmp")
        transcript = tmp_path / f"t-{item_id}.jsonl"
        transcript.write_text(_json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": tail_text}]}}
        ) + "\n")
        db.execute("UPDATE work_runs SET transcript_path = ? WHERE id = ?",
                   (str(transcript), run_id))
        old = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        db.execute("UPDATE work_items SET updated_at = ? WHERE id = ?", (old, item_id))
        return item_id, run_id, transcript

    def _age_transcript(self, transcript, minutes=40):
        import os
        past = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).timestamp()
        os.utime(transcript, (past, past))

    def test_live_transcript_refreshes_updated_at(self, tmp_path, monkeypatch):
        item_id, _, _ = self._mkstale(tmp_path)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": (_ for _ in ()).throw(AssertionError))
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "refreshed"} in actions
        item = db.query_one("SELECT state, updated_at FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(minutes=work_store.STALE_AFTER_MINUTES)).isoformat()
        assert item["updated_at"] > cutoff

    def test_dead_session_marked_failed(self, tmp_path, monkeypatch):
        item_id, run_id, transcript = self._mkstale(tmp_path)
        self._age_transcript(transcript)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "failed"} in actions
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"
        assert "without a Stop event" in item["stop_reason"]
        run = db.query_one("SELECT status FROM work_runs WHERE id = ?", (run_id,))
        assert run["status"] == "stopped"
        kinds = {e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))}
        assert "stale_failed" in kinds

    def test_live_idle_session_gets_synthesized_stop_and_continues(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        item_id, _, transcript = self._mkstale(tmp_path)
        self._age_transcript(transcript)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "stop_synthesized:continued"} in actions
        sender.assert_called_once()
        item = db.query_one("SELECT state, continues_used FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        assert item["continues_used"] == 1
        kinds = {e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))}
        assert {"Stop", "auto_continued"} <= kinds

    def test_live_idle_session_with_question_prompts_operator(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        item_id, _, transcript = self._mkstale(
            tmp_path, tail_text="Should I use the staging bucket or prod?")
        self._age_transcript(transcript)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "stop_synthesized:question"} in actions
        sender.assert_not_called()
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "staging bucket" in item["stop_reason"]

    def test_fresh_item_untouched(self):
        item_id = _mkitem("fresh item")
        work_store.add_run(item_id, f"sid-fresh-{item_id}", f"work-{item_id}", "/tmp")
        assert work_store.sweep_stale_items() == []
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "agent_working"

    def _mkpending(self, tmp_path, minutes):
        import json as _json
        item_id, run_id, transcript = self._mkstale(tmp_path, minutes=minutes)
        transcript.write_text("\n".join(_json.dumps(x) for x in [
            {"type": "assistant", "message": {"content": [
                {"type": "text", "text": "Running the long build."},
                {"type": "tool_use", "id": "toolu_1", "name": "Bash"}]}},
        ]) + "\n")
        self._age_transcript(transcript, minutes=minutes)
        return item_id, run_id, transcript

    def test_pending_tool_blocks_synthesized_stop(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        item_id, _, _ = self._mkpending(tmp_path, minutes=40)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "busy_tool"} in actions
        sender.assert_not_called()
        item = db.query_one("SELECT state, updated_at FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        cutoff = (datetime.now(timezone.utc)
                  - timedelta(minutes=work_store.STALE_AFTER_MINUTES)).isoformat()
        assert item["updated_at"] > cutoff

    def test_pending_tool_past_stuck_window_prompts_operator(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        item_id, _, _ = self._mkpending(
            tmp_path, minutes=work_store.STUCK_AFTER_MINUTES + 10)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "stuck_tool"} in actions
        sender.assert_not_called()
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "has not returned" in item["stop_reason"]
        kinds = {e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))}
        assert "stuck_tool" in kinds

    def test_answered_tool_call_is_not_pending(self, tmp_path):
        import json as _json
        p = tmp_path / "answered.jsonl"
        p.write_text("\n".join(_json.dumps(x) for x in [
            {"type": "assistant", "message": {"content": [
                {"type": "tool_use", "id": "toolu_1", "name": "Bash"}]}},
            {"type": "user", "message": {"content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]}},
            {"type": "assistant", "message": {"content": [
                {"type": "text", "text": "Build finished."}]}},
        ]) + "\n")
        assert work_store.pending_tool_calls(str(p)) is False

    def test_unanswered_tool_call_is_pending(self, tmp_path):
        import json as _json
        p = tmp_path / "pending.jsonl"
        p.write_text("\n".join(_json.dumps(x) for x in [
            {"type": "assistant", "message": {"content": [
                {"type": "tool_use", "id": "toolu_1", "name": "Bash"}]}},
            {"type": "user", "message": {"content": [
                {"type": "tool_result", "tool_use_id": "toolu_1", "content": "ok"}]}},
            {"type": "assistant", "message": {"content": [
                {"type": "tool_use", "id": "toolu_2", "name": "Bash"}]}},
        ]) + "\n")
        assert work_store.pending_tool_calls(str(p)) is True

    def test_pending_tool_missing_transcript_is_false(self):
        assert work_store.pending_tool_calls("/nonexistent/x.jsonl") is False


class TestReply:
    def test_reply_resumes(self, monkeypatch):
        from unittest.mock import MagicMock
        item_id = _mkitem("reply item")
        work_store.add_run(item_id, f"sid-reply-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-reply-{item_id}", "Stop", {})
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        out = work_store.reply(item_id, "use the staging bucket")
        assert out == {"id": item_id, "action": "reply"}
        sender.assert_called_once_with(f"work-{item_id}", "use the staging bucket")
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"

    def test_reply_dead_session_errors(self, monkeypatch):
        item_id = _mkitem("reply dead")
        work_store.add_run(item_id, f"sid-rd-{item_id}", f"work-{item_id}", "/tmp")
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        out = work_store.reply(item_id, "hello")
        assert "no live Claude" in out["error"]

    def test_reply_on_done_item_refused(self, monkeypatch):
        item_id = _mkitem("reply done")
        work_store.add_run(item_id, f"sid-rdn-{item_id}", f"work-{item_id}", "/tmp")
        work_store.apply_action(item_id, "done")
        out = work_store.reply(item_id, "hello")
        assert "reopen" in out["error"]


class TestSnoozedQuestion:
    def _snoozed_with_question(self, until):
        item_id = _mkitem("snoozed with question")
        work_store.add_run(item_id, f"sid-sq-{item_id}", f"work-{item_id}", "/tmp")
        db.execute("UPDATE work_items SET pending_question = ? WHERE id = ?",
                   ('{"questions": [{"question": "Push it?"}]}', item_id))
        work_store.apply_action(item_id, "snooze", until=until)
        return item_id

    def test_detail_remaps_expired_snooze_to_needs_you(self):
        now = datetime.now(timezone.utc)
        item_id = self._snoozed_with_question((now - timedelta(hours=1)).isoformat())
        detail = work_store.item_detail(item_id)
        assert detail["item"]["state"] == "needs_you"
        assert detail["item"]["pending_question"]

    def test_detail_keeps_future_snooze_waiting(self):
        now = datetime.now(timezone.utc)
        item_id = self._snoozed_with_question((now + timedelta(hours=1)).isoformat())
        detail = work_store.item_detail(item_id)
        assert detail["item"]["state"] == "waiting_external"
        assert detail["item"]["pending_question"]

    def test_reply_clears_snooze_and_question(self, monkeypatch):
        now = datetime.now(timezone.utc)
        item_id = self._snoozed_with_question((now + timedelta(hours=1)).isoformat())
        monkeypatch.setattr(work_store, "tmux_send", lambda k, t: True)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        out = work_store.reply(item_id, "push it")
        assert out == {"id": item_id, "action": "reply"}
        item = db.query_one(
            "SELECT state, pending_question, snoozed_until FROM work_items WHERE id = ?",
            (item_id,))
        assert item["state"] == "agent_working"
        assert item["pending_question"] == ""
        assert item["snoozed_until"] is None

    def test_prompt_submit_clears_snooze(self):
        now = datetime.now(timezone.utc)
        item_id = self._snoozed_with_question((now + timedelta(hours=1)).isoformat())
        work_store.record_event(f"sid-sq-{item_id}", "UserPromptSubmit", {})
        item = db.query_one(
            "SELECT state, pending_question, snoozed_until FROM work_items WHERE id = ?",
            (item_id,))
        assert item["state"] == "agent_working"
        assert item["pending_question"] == ""
        assert item["snoozed_until"] is None


class TestTranscriptTail:
    def test_last_assistant_text(self, tmp_path):
        import json as _json
        p = tmp_path / "t.jsonl"
        lines = [
            {"type": "user", "message": {"content": "hi"}},
            {"type": "assistant", "message": {"content": [{"type": "text", "text": "first"}]}},
            {"type": "assistant", "message": {"content": [
                {"type": "tool_use", "name": "Bash"},
                {"type": "text", "text": "final answer"}]}},
        ]
        p.write_text("\n".join(_json.dumps(x) for x in lines))
        assert work_store.last_assistant_text(str(p)) == "final answer"

    def test_missing_file_empty(self):
        assert work_store.last_assistant_text("/nonexistent/x.jsonl") == ""

    def test_timeline_keeps_image_only_prompt(self, tmp_path):
        import json as _json
        image_bytes = b"jpeg bytes"
        p = tmp_path / "image-only.jsonl"
        p.write_text(_json.dumps({
            "type": "user", "timestamp": "T1", "message": {"content": [{
                "type": "image", "source": {"type": "base64", "media_type": "image/jpeg",
                                               "data": base64.b64encode(image_bytes).decode()},
            }]},
        }))

        timeline = work_store.transcript_timeline(str(p))
        assert timeline[0]["kind"] == "prompt"
        assert timeline[0]["text"] == ""
        assert timeline[0]["images"][0]["media_type"] == "image/jpeg"
        assert work_store.transcript_image(str(p), timeline[0]["images"][0]["id"]) == \
            (image_bytes, "image/jpeg")


class TestArtifacts:
    def test_record_and_find(self, tmp_path):
        import json as _json
        item_id = _mkitem("make a report")
        work_store.add_run(item_id, f"sid-art-{item_id}", f"work-{item_id}", "/tmp")
        t = tmp_path / "t.jsonl"
        t.write_text(_json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "Done.\nARTIFACT: /tmp/report.html - quarterly report page"}]}}))
        added = work_store.record_artifacts(f"sid-art-{item_id}", str(t))
        assert added == 1
        again = work_store.record_artifacts(f"sid-art-{item_id}", str(t))
        assert again == 0
        hits = work_store.find_artifacts("quarterly")
        assert hits and hits[0]["path"] == "/tmp/report.html"
        assert hits[0]["note"] == "quarterly report page"
        assert hits[0]["objective"] == "make a report"
        assert work_store.find_artifacts("no-such-thing-xyz") == []

    def test_ignores_relative_paths(self, tmp_path):
        import json as _json
        item_id = _mkitem("bad artifact")
        work_store.add_run(item_id, f"sid-artb-{item_id}", f"work-{item_id}", "/tmp")
        t = tmp_path / "t.jsonl"
        t.write_text(_json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "ARTIFACT: relative/path.txt - nope"}]}}))
        assert work_store.record_artifacts(f"sid-artb-{item_id}", str(t)) == 0

    def test_detail_exposes_artifact_id(self, tmp_path):
        import json as _json
        item_id = _mkitem("image artifact item")
        work_store.add_run(item_id, f"sid-arti-{item_id}", f"work-{item_id}", "/tmp")
        t = tmp_path / "t.jsonl"
        t.write_text(_json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "ARTIFACT: /tmp/shot.png - screenshot"}]}}))
        assert work_store.record_artifacts(f"sid-arti-{item_id}", str(t)) == 1
        d = work_store.item_detail(item_id)
        assert d["artifacts"][0]["path"] == "/tmp/shot.png"
        assert isinstance(d["artifacts"][0]["id"], int)


class TestTodayProducer:
    def test_launch_links_work_item(self):
        from web.today import _ensure_work_item
        m = {"sid": "sid-today-1", "ticket_key": "DEV-999", "title": "Fix DEV-999 CI"}
        _ensure_work_item("aimyable", m, "loop-key-1", "/tmp")
        run = db.query_one("SELECT work_item_id FROM work_runs WHERE session_id = 'sid-today-1'")
        assert run is not None
        item = db.query_one("SELECT objective, scope, scope_ref, instance_key FROM work_items WHERE id = ?",
                            (run["work_item_id"],))
        assert item["objective"] == "Fix DEV-999 CI"
        assert item["scope"] == "ticket"
        assert item["scope_ref"] == "DEV-999"
        assert item["instance_key"] == "aimyable"
        _ensure_work_item("aimyable", m, "loop-key-1", "/tmp")
        n = db.query_one("SELECT COUNT(*) AS n FROM work_runs WHERE session_id = 'sid-today-1'")["n"]
        assert n == 1


class TestDetail:
    def test_timeline_and_done_source(self, tmp_path):
        import json as _json
        item_id = _mkitem("transparent item")
        t = tmp_path / "t.jsonl"
        lines = [
            {"type": "user", "message": {"content": "Begin the objective."}, "timestamp": "T1"},
            {"type": "assistant", "message": {"content": [
                {"type": "tool_use", "name": "Bash", "input": {"command": "git push origin branch"}}]},
             "timestamp": "T2"},
            {"type": "user", "message": {"content": [{"type": "tool_result", "content": "ok"}]},
             "toolUseResult": {"ok": True}},
            {"type": "assistant", "message": {"content": [
                {"type": "text", "text": "Pushed the branch. WORK_DONE"}]}, "timestamp": "T3"},
        ]
        t.write_text("\n".join(_json.dumps(x) for x in lines))
        work_store.add_run(item_id, f"sid-det-{item_id}", f"work-{item_id}", "/tmp")
        db.execute("UPDATE work_runs SET transcript_path = ? WHERE session_id = ?",
                   (str(t), f"sid-det-{item_id}"))
        db.execute(
            "INSERT INTO work_events(work_item_id, kind, payload, created_at) "
            "VALUES (?, 'self_reported_done', '{}', 'now')", (item_id,))
        db.execute("UPDATE work_items SET state = 'done' WHERE id = ?", (item_id,))
        d = work_store.item_detail(item_id)
        kinds = [e["kind"] for e in d["timeline"]]
        assert kinds == ["prompt", "tool", "text"]
        assert d["timeline"][1]["name"] == "Bash"
        assert "git push" in d["timeline"][1]["arg"]
        assert d["item"]["done_source"] == "agent"

    def test_unknown_item_errors(self):
        assert work_store.item_detail(999999) == {"error": "unknown work item"}

    def test_read_system_prompt_from_launch_file(self, tmp_path, monkeypatch):
        import core.terminal as terminal
        from services import work_launch
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path))
        item_id = _mkitem("prompted item")
        sid = f"sid-sp-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
        (tmp_path / f"{sid}.md").write_text("# Work item\n\n## Objective\n\nship the widget")
        runs = work_store.item_detail(item_id)["runs"]
        assert "ship the widget" in work_launch.read_system_prompt(runs)

    def test_read_system_prompt_missing_file_empty(self, tmp_path, monkeypatch):
        import core.terminal as terminal
        from services import work_launch
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path))
        item_id = _mkitem("unprompted item")
        work_store.add_run(item_id, f"sid-nosp-{item_id}", f"work-{item_id}", "/tmp")
        runs = work_store.item_detail(item_id)["runs"]
        assert work_launch.read_system_prompt(runs) == ""


class TestQuestions:
    QUESTIONS = {"questions": [{
        "question": "Which bucket should the release use?",
        "header": "Bucket",
        "options": [{"label": "staging", "description": "safe"},
                    {"label": "prod", "description": "live"}],
        "multiSelect": False,
    }]}

    def _mkrun(self, name):
        item_id = _mkitem(name)
        sid = f"sid-q-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
        return item_id, sid

    def test_record_question_sets_pending(self):
        import json as _json
        item_id, sid = self._mkrun("question item")
        ok = work_store.record_question(sid, self.QUESTIONS)
        assert ok is True
        item = db.query_one(
            "SELECT state, stop_reason, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "Which bucket" in item["stop_reason"]
        parsed = _json.loads(item["pending_question"])
        assert parsed["questions"][0]["options"][1]["label"] == "prod"
        kinds = [e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))]
        assert "question_asked" in kinds

    def test_record_question_unknown_session(self):
        assert work_store.record_question("sid-q-nope", self.QUESTIONS) is False

    def test_record_question_empty_input(self):
        item_id, sid = self._mkrun("question empty")
        assert work_store.record_question(sid, {}) is False
        assert work_store.record_question(sid, {"questions": [{"question": " "}]}) is False
        item = db.query_one("SELECT pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["pending_question"] == ""

    def test_record_question_done_item_refused(self):
        item_id, sid = self._mkrun("question done")
        work_store.apply_action(item_id, "done")
        assert work_store.record_question(sid, self.QUESTIONS) is False

    def test_pending_question_blocks_autocontinue(self, monkeypatch):
        from unittest.mock import MagicMock
        item_id, sid = self._mkrun("question blocks auto")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.record_event(sid, "Stop", {})
        monkeypatch.setattr(work_store, "last_assistant_text",
                            lambda p: "I am blocked on the bucket decision. Waiting.")
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        out = work_store.maybe_autocontinue(sid, "/tmp/t.jsonl")
        assert out == "question"
        sender.assert_not_called()

    def test_reply_clears_pending_question(self, monkeypatch):
        from unittest.mock import MagicMock
        item_id, sid = self._mkrun("question reply")
        work_store.record_question(sid, self.QUESTIONS)
        monkeypatch.setattr(work_store, "tmux_send", MagicMock(return_value=True))
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        work_store.reply(item_id, "Bucket: staging")
        item = db.query_one(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        assert item["pending_question"] == ""

    def test_done_clears_pending_question(self):
        item_id, sid = self._mkrun("question done clears")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.apply_action(item_id, "done")
        item = db.query_one("SELECT pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["pending_question"] == ""

    def test_hook_denies_ask_user_question(self):
        import json as _json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        item_id, sid = self._mkrun("question hook")
        payload = _json.dumps({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "AskUserQuestion",
                               "tool_input": self.QUESTIONS})
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0, r.stderr
        out = _json.loads(r.stdout)
        assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
        item = db.query_one(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "Which bucket" in item["pending_question"]

    def test_hook_ignores_other_tools(self):
        import json as _json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        item_id, sid = self._mkrun("question other tool")
        payload = _json.dumps({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Bash", "tool_input": {"command": "ls"}})
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0
        assert r.stdout.strip() == ""
        item = db.query_one("SELECT pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["pending_question"] == ""

    def test_hook_foreign_session_no_deny(self):
        import json as _json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        payload = _json.dumps({"session_id": "sid-not-work", "hook_event_name": "PreToolUse",
                               "tool_name": "AskUserQuestion",
                               "tool_input": self.QUESTIONS})
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0
        assert r.stdout.strip() == ""

    def test_stale_done_marker_does_not_override_question(self, monkeypatch):
        from unittest.mock import MagicMock
        item_id, sid = self._mkrun("question beats stale done")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.record_event(sid, "Stop", {})
        monkeypatch.setattr(work_store, "last_assistant_text",
                            lambda p: "All PRs summarized.\nWORK_DONE")
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        out = work_store.maybe_autocontinue(sid, "/tmp/t.jsonl")
        assert out == "question"
        sender.assert_not_called()
        item = db.query_one(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "Which bucket" in item["pending_question"]
        kinds = [e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))]
        assert "self_reported_done" not in kinds

    def test_prompt_submit_clears_pending_question(self):
        item_id, sid = self._mkrun("prompt clears question")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.record_event(sid, "UserPromptSubmit", {})
        item = db.query_one(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        assert item["pending_question"] == ""

    WAKEUP = ("<task-notification>\n<task-id>b2ms6zphy</task-id>\n"
              "<summary>Monitor event: \"E2E on main\"</summary>\n</task-notification>")

    def test_wakeup_prompt_keeps_pending_question(self):
        item_id, sid = self._mkrun("wakeup keeps question")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.record_event(sid, "UserPromptSubmit", {"prompt": self.WAKEUP})
        item = db.query_one(
            "SELECT state, stop_reason, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "Which bucket" in item["pending_question"]
        assert "Which bucket" in item["stop_reason"]

    def test_operator_prompt_still_clears_pending_question(self):
        item_id, sid = self._mkrun("operator prompt clears question")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.record_event(sid, "UserPromptSubmit", {"prompt": "Bucket: staging"})
        item = db.query_one(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        assert item["pending_question"] == ""

    def test_wakeup_prompt_leaves_no_question_alone(self):
        item_id, sid = self._mkrun("wakeup without question")
        work_store.record_event(sid, "Stop", {})
        work_store.record_event(sid, "UserPromptSubmit", {"prompt": self.WAKEUP})
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"

    def test_wakeup_prompt_does_not_autocontinue_past_question(self, monkeypatch):
        from unittest.mock import MagicMock
        item_id, sid = self._mkrun("wakeup blocks autocontinue")
        work_store.record_question(sid, self.QUESTIONS)
        work_store.record_event(sid, "Stop", {})
        work_store.record_event(sid, "UserPromptSubmit", {"prompt": self.WAKEUP})
        work_store.record_event(sid, "Stop", {})
        monkeypatch.setattr(work_store, "last_assistant_text", lambda p: "Read the task output.")
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        assert work_store.maybe_autocontinue(sid, "/tmp/t.jsonl") == "question"
        sender.assert_not_called()

    def test_hook_forwards_prompt_and_keeps_question(self):
        import json as _json
        import subprocess
        import core.db as _db
        dbfile = str(_db._DB_PATH)
        item_id, sid = self._mkrun("hook forwards prompt")
        work_store.record_question(sid, self.QUESTIONS)
        payload = _json.dumps({"session_id": sid, "hook_event_name": "UserPromptSubmit",
                               "prompt": self.WAKEUP})
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**__import__("os").environ, "FRSHTY_DB": dbfile},
        )
        assert r.returncode == 0, r.stderr
        item = db.query_one(
            "SELECT state, pending_question FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "Which bucket" in item["pending_question"]


class TestFollowup:
    def _client(self):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient
        from web.work import router
        a = FastAPI()
        a.include_router(router)
        return TestClient(a)

    def _done_parent(self, objective="parent job"):
        item_id = work_store.create_item(objective)
        work_store.add_run(item_id, f"sid-fup-{item_id}", f"work-{item_id}", "/tmp")
        now = work_store._now()
        db.execute(
            "UPDATE work_items SET state = 'done', summary = 'shipped the widget, PR #9' WHERE id = ?",
            (item_id,))
        db.execute(
            "INSERT INTO work_artifacts(work_item_id, work_run_id, path, note, created_at) "
            "VALUES (?, NULL, '/tmp/report.html', 'the report', ?)", (item_id, now))
        return item_id

    def _patched(self, tmp_path):
        from unittest.mock import MagicMock, patch
        reg = MagicMock()
        reg.config = {"workspace": {"root": tmp_path}}
        instances = MagicMock()
        instances.get.return_value = reg
        return (
            patch("services.work_launch.runtime.instances", return_value=instances),
            patch("services.work_launch.terminal.launch_claude"),
            patch("services.work_launch.terminal.session_healthy",
                  return_value={"alive": True, "agent_running": True}),
        )

    def test_followup_carries_parent_context(self, tmp_path):
        from services import work_launch
        parent = self._done_parent("upgrade normalize_ace")
        p_inst, p_launch, p_health = self._patched(tmp_path)
        with p_inst, p_launch as mock_launch, p_health:
            out = work_launch.launch_followup(parent, "open the follow-up PR")
        assert "error" not in out, out
        ctx = mock_launch.call_args.args[3]
        assert "Previous work item" in ctx
        assert f"work item {parent}: upgrade normalize_ace" in ctx
        assert "shipped the widget, PR #9" in ctx
        assert "/tmp/report.html - the report" in ctx
        child = db.query_one("SELECT source_item_id FROM work_items WHERE id = ?",
                             (out["item_id"],))
        assert child["source_item_id"] == parent

    def test_followup_requires_done_parent(self, tmp_path):
        from services import work_launch
        parent = work_store.create_item("still running")
        p_inst, p_launch, p_health = self._patched(tmp_path)
        with p_inst, p_launch, p_health:
            out = work_launch.launch_followup(parent, "too early")
        assert "not done" in out["error"]
        out = work_launch.launch_followup(999999, "no parent")
        assert "unknown source" in out["error"]

    def test_launch_rejects_unknown_source(self, tmp_path):
        from services import work_launch
        p_inst, p_launch, p_health = self._patched(tmp_path)
        with p_inst, p_launch, p_health:
            out = work_launch.launch("orphan", source_item_id=999999)
        assert "unknown source" in out["error"]

    def test_followup_endpoint(self, tmp_path):
        parent = self._done_parent("endpoint parent")
        p_inst, p_launch, p_health = self._patched(tmp_path)
        with p_inst, p_launch, p_health:
            client = self._client()
            r = client.post(f"/api/work/items/{parent}/followup",
                            json={"text": "next step"})
            assert r.status_code == 200, r.text
            child_id = r.json()["item_id"]
            bad = client.post(f"/api/work/items/{child_id}/followup",
                              json={"text": "child is not done"})
        assert bad.status_code == 400
        child = db.query_one("SELECT source_item_id, objective FROM work_items WHERE id = ?",
                             (child_id,))
        assert child["source_item_id"] == parent
        assert child["objective"] == "next step"

    def test_detail_exposes_lineage(self, tmp_path):
        from services import work_launch
        parent = self._done_parent("lineage parent")
        p_inst, p_launch, p_health = self._patched(tmp_path)
        with p_inst, p_launch, p_health:
            out = work_launch.launch_followup(parent, "lineage child")
        child_id = out["item_id"]
        parent_detail = work_store.item_detail(parent)
        assert parent_detail["source_item"] is None
        assert [c["id"] for c in parent_detail["followup_children"]] == [child_id]
        child_detail = work_store.item_detail(child_id)
        assert child_detail["source_item"]["id"] == parent
        assert child_detail["source_item"]["objective"] == "lineage parent"
        assert child_detail["followup_children"] == []


class TestBackgroundWait:
    def _transcript(self, tmp_path, entries):
        import json as _json
        p = tmp_path / "bg.jsonl"
        p.write_text("\n".join(_json.dumps(e) for e in entries) + "\n")
        return str(p)

    def _bash_start(self, task_id):
        return {"type": "user", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1",
             "content": f"Command running in background with ID: {task_id}. Output is being "
                        "written to: /tmp/x.output. You will be notified when it completes."}]}}

    def _monitor_start(self, task_id, persistent=True):
        mode = "persistent — runs until TaskStop or session end" if persistent else "timeout 600000ms"
        return {"type": "user", "message": {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t2",
             "content": f"Monitor started (task {task_id}, {mode}). You will be notified on each event."}]}}

    def _notification(self, task_id, status, shape="queue"):
        text = (f"<task-notification>\n<task-id>{task_id}</task-id>\n"
                f"<status>{status}</status>\n<summary>done</summary>\n</task-notification>")
        if shape == "queue":
            return {"type": "queue-operation", "operation": "enqueue", "content": text}
        if shape == "attachment":
            return {"type": "attachment", "attachment": {"type": "queued_command", "prompt": text}}
        return {"type": "user", "message": {"role": "user", "content": text}}

    def _task_stop(self, task_id):
        return {"type": "assistant", "message": {"content": [
            {"type": "tool_use", "name": "TaskStop", "input": {"task_id": task_id}}]}}

    def _checkpoint(self, text):
        return {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}

    def test_completed_notification_ends_task(self, tmp_path):
        for shape in ("queue", "attachment", "user"):
            path = self._transcript(tmp_path, [
                self._bash_start("b111"), self._notification("b111", "completed", shape)])
            assert work_store.pending_background_tasks(path) == set()

    def test_unfinished_tasks_are_pending(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._bash_start("b111"), self._monitor_start("b222"),
            self._notification("b111", "completed")])
        assert work_store.pending_background_tasks(path) == {"b222"}

    def test_running_status_is_not_terminal(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("b333"), self._notification("b333", "running")])
        assert work_store.pending_background_tasks(path) == {"b333"}

    def test_task_stop_ends_task(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("b444"), self._task_stop("b444")])
        assert work_store.pending_background_tasks(path) == set()

    def test_sidechain_entries_ignored(self, tmp_path):
        start = self._bash_start("b555")
        start["isSidechain"] = True
        path = self._transcript(tmp_path, [start])
        assert work_store.pending_background_tasks(path) == set()

    def test_stop_with_pending_bg_waits_external(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("b666"),
            self._checkpoint("Checkpoint: waiting on CI. The monitor fires when it finishes.")])
        item_id = _mkitem("bg wait item")
        work_store.add_run(item_id, f"sid-bg-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bg-{item_id}", "Stop", {
            "transcript_path": path,
            "last_assistant_message": "Checkpoint: waiting on CI."})
        item = db.query_one(
            "SELECT state, stop_reason, snoozed_until FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "waiting_external"
        assert item["stop_reason"].startswith("Waiting on a background task")
        assert item["snoozed_until"]
        out = work_store.maybe_autocontinue(f"sid-bg-{item_id}", path)
        assert out == "not_applicable"

    def test_stop_without_pending_bg_needs_you(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._bash_start("b777"), self._notification("b777", "completed"),
            self._checkpoint("All background work finished.")])
        item_id = _mkitem("bg done item")
        work_store.add_run(item_id, f"sid-bgd-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bgd-{item_id}", "Stop", {"transcript_path": path})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"

    def test_pending_question_outranks_bg_wait(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("b888"), self._checkpoint("Blocked on the question above.")])
        item_id = _mkitem("bg question item")
        work_store.add_run(item_id, f"sid-bgq-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_question(f"sid-bgq-{item_id}",
                                   {"questions": [{"question": "Staging or prod?"}]})
        work_store.record_event(f"sid-bgq-{item_id}", "Stop", {"transcript_path": path})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"

    def test_question_tail_outranks_bg_wait(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("b999"),
            self._checkpoint("The monitor is running. Should I also restart the worker?")])
        item_id = _mkitem("bg tail question item")
        work_store.add_run(item_id, f"sid-bgt-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bgt-{item_id}", "Stop", {"transcript_path": path})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"

    def test_work_done_tail_outranks_bg_wait(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("baaa"),
            self._checkpoint("All finished.\nWORK_DONE")])
        item_id = _mkitem("bg done marker item")
        work_store.add_run(item_id, f"sid-bgw-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bgw-{item_id}", "Stop", {"transcript_path": path})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"
        out = work_store.maybe_autocontinue(f"sid-bgw-{item_id}", path)
        assert out == "done"
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "done"

    def test_operator_ask_tail_outranks_bg_wait(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("bddd"),
            self._checkpoint("Restarted. Send another code when ready — the prompt "
                             "appears in about 100 seconds and waits 15 minutes.")])
        item_id = _mkitem("bg 2fa item")
        work_store.add_run(item_id, f"sid-bg2fa-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bg2fa-{item_id}", "Stop", {"transcript_path": path})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"

    def test_operator_ask_notification_outranks_bg_wait(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("beee"),
            self._checkpoint("Send me a fresh 6-digit code and I will write it in.")])
        item_id = _mkitem("bg 2fa notif item")
        work_store.add_run(item_id, f"sid-bg2fn-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bg2fn-{item_id}", "Notification", {
            "transcript_path": path, "message": "Claude is waiting for your input"})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"

    def test_idle_notification_with_pending_bg_waits_external(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("bbbb"), self._checkpoint("Waiting on the monitor.")])
        item_id = _mkitem("bg idle notif item")
        work_store.add_run(item_id, f"sid-bgn-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bgn-{item_id}", "Notification", {
            "transcript_path": path, "message": "Claude is waiting for your input"})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "waiting_external"

    def test_permission_notification_stays_needs_you(self, tmp_path):
        path = self._transcript(tmp_path, [
            self._monitor_start("bccc"), self._checkpoint("Working through the steps.")])
        item_id = _mkitem("bg perm notif item")
        work_store.add_run(item_id, f"sid-bgp-{item_id}", f"work-{item_id}", "/tmp")
        work_store.record_event(f"sid-bgp-{item_id}", "Notification", {
            "transcript_path": path, "message": "Claude needs your permission to use Bash"})
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"


class TestSuspendResume:
    def _patch_sessions(self, monkeypatch, sessions, killed):
        import core.terminal as terminal
        monkeypatch.setattr(terminal, "list_sessions", lambda: sessions)
        monkeypatch.setattr(terminal, "kill_terminal", lambda key: killed.append(key))

    def test_suspend_kills_idle_done_session(self, monkeypatch):
        from services import work_launch
        item_id = _mkitem("suspend done item")
        work_store.apply_action(item_id, "done")
        killed = []
        self._patch_sessions(monkeypatch,
                             [{"name": f"term-work-{item_id}", "activity": 1000}], killed)
        out = work_launch.suspend_idle_done_sessions(now=1000 + work_launch.SUSPEND_IDLE_SECONDS)
        assert out == [item_id]
        assert killed == [f"work-{item_id}"]

    def test_suspend_skips_recent_activity(self, monkeypatch):
        from services import work_launch
        item_id = _mkitem("suspend recent item")
        work_store.apply_action(item_id, "done")
        killed = []
        self._patch_sessions(monkeypatch,
                             [{"name": f"term-work-{item_id}", "activity": 1000}], killed)
        out = work_launch.suspend_idle_done_sessions(now=1000 + work_launch.SUSPEND_IDLE_SECONDS - 1)
        assert out == []
        assert killed == []

    def test_suspend_skips_non_done_item(self, monkeypatch):
        from services import work_launch
        item_id = _mkitem("suspend live item")
        killed = []
        self._patch_sessions(monkeypatch,
                             [{"name": f"term-work-{item_id}", "activity": 0}], killed)
        assert work_launch.suspend_idle_done_sessions(now=10 ** 9) == []
        assert killed == []

    def test_suspend_kills_session_without_item_row(self, monkeypatch):
        from services import work_launch
        killed = []
        self._patch_sessions(monkeypatch,
                             [{"name": "term-work-99999999", "activity": 0}], killed)
        assert work_launch.suspend_idle_done_sessions(now=10 ** 9) == [99999999]
        assert killed == ["work-99999999"]

    def test_suspend_ignores_ticket_sessions(self, monkeypatch):
        from services import work_launch
        killed = []
        self._patch_sessions(monkeypatch,
                             [{"name": "term-DEV-604", "activity": 0},
                              {"name": "term-work-7-discuss", "activity": 0}], killed)
        assert work_launch.suspend_idle_done_sessions(now=10 ** 9) == []
        assert killed == []

    def test_resume_relaunches_claude_with_resume(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        import core.terminal as terminal
        from services import work_launch
        item_id = _mkitem("resume item")
        sid = f"sid-resume-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(tmp_path))
        config = {"workspace": {"root": tmp_path}}
        monkeypatch.setattr(work_launch, "personal_config", lambda: config)
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": False, "agent_running": False})
        launcher = MagicMock()
        monkeypatch.setattr(terminal, "launch_agent", launcher)
        assert work_launch.resume_session(item_id) is True
        launcher.assert_called_once_with(f"work-{item_id}", str(tmp_path), sid, "", False,
                                         config=config, agent="claude", agent_session_id="")

    def test_resume_unknown_item_is_false(self, monkeypatch):
        from services import work_launch
        monkeypatch.setattr(work_launch, "personal_config", lambda: {"workspace": {"root": "/tmp"}})
        assert work_launch.resume_session(99999999) is False

    def test_resume_falls_back_to_workspace_root_cwd(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        import core.terminal as terminal
        from services import work_launch
        item_id = _mkitem("resume gone cwd item")
        sid = f"sid-resume2-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(tmp_path / "deleted"))
        config = {"workspace": {"root": tmp_path}}
        monkeypatch.setattr(work_launch, "personal_config", lambda: config)
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": False, "agent_running": False})
        launcher = MagicMock()
        monkeypatch.setattr(terminal, "launch_agent", launcher)
        assert work_launch.resume_session(item_id) is True
        assert launcher.call_args[0][1] == str(tmp_path)

    def test_resume_relaunches_when_pane_exists_without_agent(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        import core.terminal as terminal
        from services import work_launch
        item_id = _mkitem("resume empty pane item")
        sid = f"sid-resume-empty-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", str(tmp_path))
        config = {"workspace": {"root": tmp_path}}
        monkeypatch.setattr(work_launch, "personal_config", lambda: config)
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": False})
        launcher = MagicMock()
        monkeypatch.setattr(terminal, "launch_agent", launcher)
        assert work_launch.resume_session(item_id) is True
        launcher.assert_called_once_with(f"work-{item_id}", str(tmp_path), sid, "", False,
                                         config=config, agent="claude", agent_session_id="")

    def test_resume_noop_while_agent_running(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        import core.terminal as terminal
        from services import work_launch
        item_id = _mkitem("resume alive item")
        work_store.add_run(item_id, f"sid-resume3-{item_id}", f"work-{item_id}", str(tmp_path))
        monkeypatch.setattr(work_launch, "personal_config",
                            lambda: {"workspace": {"root": tmp_path}})
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": True})
        launcher = MagicMock()
        monkeypatch.setattr(terminal, "launch_agent", launcher)
        assert work_launch.resume_session(item_id) is True
        launcher.assert_not_called()


def _panel(question, body_lines, done=True):
    hints = "  ←/→ to switch · c to copy · f to fork · Esc to close" if done \
        else "  ←/→ to switch · x to clear history · Esc to close"
    return ["● earlier output", "", f"  /btw {question}", ""] + \
        [f"    {ln}" for ln in body_lines] + ["", hints, "", "❯ "]


class _FakePane:
    def __init__(self, captures):
        self.captures = list(captures)
        self.keys: list[str] = []

    def capture(self, session):
        return self.captures.pop(0) if len(self.captures) > 1 else self.captures[0]

    def run(self, *args):
        import subprocess
        if args[0] == "send-keys":
            self.keys.append(args[-1])
        return subprocess.CompletedProcess(args, 0, "", "")


def _install_pane(monkeypatch, pane):
    monkeypatch.setattr(work_store, "_capture_pane", pane.capture)
    monkeypatch.setattr(work_store, "_tmux_run", pane.run)
    monkeypatch.setattr(work_store.time, "sleep", lambda s: None)


class TestBtwOverlay:
    def test_reads_question_and_body_when_done(self):
        panel = work_store.btw_overlay(_panel("what is 17 times 3?", ["51"]))
        assert panel["done"] is True
        assert panel["asked"] == "what is 17 times 3?"
        assert work_store._btw_answer_text(panel["body"]) == "51"

    def test_answering_panel_is_not_done(self):
        panel = work_store.btw_overlay(
            _panel("what is 17 times 3?", ["· Answering…"], done=False))
        assert panel["done"] is False

    def test_no_panel_returns_none(self):
        assert work_store.btw_overlay(["● just output", "❯ "]) is None

    def test_answer_text_keeps_relative_indent(self):
        panel = work_store.btw_overlay(_panel("q", ["def f():", "    return 1"]))
        assert work_store._btw_answer_text(panel["body"]) == "def f():\n    return 1"

    def test_merge_window_appends_only_new_lines(self):
        body = ["a", "b", "c"]
        assert work_store._merge_window(body, ["b", "c", "d"]) is True
        assert body == ["a", "b", "c", "d"]
        assert work_store._merge_window(body, ["b", "c", "d"]) is False
        assert body == ["a", "b", "c", "d"]


class TestAskBtw:
    def test_reads_answer_and_closes_panel(self, monkeypatch):
        idle = ["● just output", "❯ "]
        answering = _panel("capital of France?", ["· Answering…"], done=False)
        done = _panel("capital of France?", ["Paris"])
        pane = _FakePane([idle, answering, done])
        _install_pane(monkeypatch, pane)
        out = work_store.ask_btw("work-1", "capital of France?")
        assert out == {"answer": "Paris"}
        assert "/btw capital of France?" in pane.keys
        assert pane.keys.count("Escape") == 1

    def test_stitches_scrolled_lines(self, monkeypatch):
        idle = ["● just output", "❯ "]
        first = _panel("primes?", ["2", "3", "5"])
        second = _panel("primes?", ["3", "5", "7"])
        pane = _FakePane([idle, first, second])
        _install_pane(monkeypatch, pane)
        out = work_store.ask_btw("work-1", "primes?")
        assert out["answer"] == "2\n3\n5\n7"

    def test_answer_to_another_question_is_refused(self, monkeypatch):
        idle = ["● just output", "❯ "]
        stale = _panel("something else entirely", ["nope"])
        pane = _FakePane([idle, stale])
        _install_pane(monkeypatch, pane)
        out = work_store.ask_btw("work-1", "capital of France?")
        assert "different /btw question" in out["error"]

    def test_timeout_without_panel(self, monkeypatch):
        pane = _FakePane([["● just output", "❯ "]])
        _install_pane(monkeypatch, pane)
        out = work_store.ask_btw("work-1", "capital of France?", timeout=0.2)
        assert "no /btw answer" in out["error"]
        assert "Escape" not in pane.keys

    def test_timeout_with_open_panel_closes_it(self, monkeypatch):
        idle = ["● just output", "❯ "]
        answering = _panel("capital of France?", ["· Answering…"], done=False)
        pane = _FakePane([idle, answering])
        _install_pane(monkeypatch, pane)
        out = work_store.ask_btw("work-1", "capital of France?", timeout=0.2)
        assert "no /btw answer" in out["error"]
        assert "Escape" in pane.keys

    def test_answer_to_another_question_closes_panel(self, monkeypatch):
        idle = ["● just output", "❯ "]
        stale = _panel("something else entirely", ["nope"])
        pane = _FakePane([idle, stale])
        _install_pane(monkeypatch, pane)
        work_store.ask_btw("work-1", "capital of France?")
        assert "Escape" in pane.keys


class TestPaneHandover:
    def test_close_btw_panel_is_a_noop_without_a_panel(self, monkeypatch):
        pane = _FakePane([["● just output", "❯ "]])
        _install_pane(monkeypatch, pane)
        assert work_store.close_btw_panel("term-work-1") is True
        assert pane.keys == []

    def test_close_btw_panel_closes_an_open_one(self, monkeypatch):
        open_panel = _panel("capital of France?", ["Paris"])
        idle = ["● just output", "❯ "]
        pane = _FakePane([open_panel, idle])
        _install_pane(monkeypatch, pane)
        assert work_store.close_btw_panel("term-work-1") is True
        assert pane.keys == ["Escape"]

    def test_close_btw_panel_reports_a_stuck_panel(self, monkeypatch):
        pane = _FakePane([_panel("capital of France?", ["Paris"])])
        _install_pane(monkeypatch, pane)
        assert work_store.close_btw_panel("term-work-1") is False

    def test_send_refuses_while_a_panel_swallows_keys(self, monkeypatch):
        import subprocess
        from unittest.mock import MagicMock
        runner = MagicMock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        monkeypatch.setattr(work_store.subprocess, "run", runner)
        monkeypatch.setattr(work_store, "close_btw_panel", lambda session: False)
        assert work_store.tmux_send("work-1", "use the staging bucket") is False
        assert not any("send-keys" in str(c) for c in runner.call_args_list)

    def test_send_proceeds_once_the_panel_is_closed(self, monkeypatch):
        import subprocess
        from unittest.mock import MagicMock
        runner = MagicMock(return_value=subprocess.CompletedProcess([], 0, "", ""))
        monkeypatch.setattr(work_store.subprocess, "run", runner)
        monkeypatch.setattr(work_store.time, "sleep", lambda s: None)
        monkeypatch.setattr(work_store, "close_btw_panel", lambda session: True)
        assert work_store.tmux_send("work-1", "use the staging bucket") is True
        assert any("use the staging bucket" in str(c) for c in runner.call_args_list)


class TestSideQuestion:
    def test_records_exchange(self, monkeypatch):
        item_id = _mkitem("side question item")
        work_store.add_run(item_id, f"sid-btw-{item_id}", f"work-{item_id}", "/tmp")
        db.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        monkeypatch.setattr(work_store, "ask_btw", lambda key, q: {"answer": "Paris"})
        out = work_store.side_question(item_id, "  capital of\n France? ")
        assert out["answer"] == "Paris"
        assert out["question"] == "capital of France?"
        row = db.query_one(
            "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = 'btw'", (item_id,))
        assert '"answer": "Paris"' in row["payload"]
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"

    def test_dead_session_errors(self, monkeypatch):
        item_id = _mkitem("side question dead")
        work_store.add_run(item_id, f"sid-btwd-{item_id}", f"work-{item_id}", "/tmp")
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        out = work_store.side_question(item_id, "hello?")
        assert "no live Claude" in out["error"]

    def test_empty_question_errors(self):
        item_id = _mkitem("side question empty")
        out = work_store.side_question(item_id, "   ")
        assert out["error"] == "empty question"

    def test_failed_ask_records_nothing(self, monkeypatch):
        item_id = _mkitem("side question failed")
        work_store.add_run(item_id, f"sid-btwf-{item_id}", f"work-{item_id}", "/tmp")
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        monkeypatch.setattr(work_store, "ask_btw", lambda key, q: {"error": "tmux session gone"})
        out = work_store.side_question(item_id, "hello?")
        assert out["error"] == "tmux session gone"
        assert db.query_one(
            "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = 'btw'",
            (item_id,)) is None


class TestIdleNotificationContinues:
    """An API error ends the turn with a Notification, not a Stop.

    Claude Code fires no Stop hook when the turn dies on an API error. It
    fires the idle Notification instead, so the autocontinue decision has to
    hang off both events or the item sits in needs_you forever.
    """

    def _transcript(self, tmp_path, item_id, text):
        import json as _json
        path = tmp_path / f"notif-{item_id}.jsonl"
        path.write_text(_json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": text}]}}
        ) + "\n")
        return path

    def test_idle_notification_is_an_idle_stop(self):
        assert work_store.is_idle_stop("Stop", {}) is True
        assert work_store.is_idle_stop(
            "Notification", {"message": "Claude is waiting for your input"}) is True

    def test_permission_notification_is_not_an_idle_stop(self):
        assert work_store.is_idle_stop(
            "Notification", {"message": "Claude needs your permission to use Bash"}) is False

    def test_operator_ask_without_a_question_mark_blocks(self):
        for tail in ("Restarted. Send another code when ready — the prompt appears soon.",
                     "Ready. Send me a fresh 6-digit code and I will write it in.",
                     "I am blocked. Paste the admin password and I will continue.",
                     "Waiting for your approval before the deploy.",
                     "Let me know which tenant to use."):
            assert work_store._blocked_on_operator(tail) is True, tail

    def test_progress_tail_is_not_an_operator_ask(self):
        for tail in ("Checkpoint: waiting on CI. The monitor fires when it finishes.",
                     "All background work finished.",
                     "Checkpoint: tests pass. Sent the branch to origin.",
                     "Reply posted to the PR comment.",
                     "Mac working tree already matches the target commit."):
            assert work_store._blocked_on_operator(tail) is False, tail

    def test_api_error_tail_is_not_read_as_a_question(self):
        tail = ("API Error: 529 Overloaded. This is a server-side issue, usually "
                "temporary — try again in a moment. If it persists, check "
                "https://status.claude.com.")
        assert work_store._looks_like_question(tail) is False

    def test_hook_continues_on_idle_notification(self, tmp_path):
        import json
        import os
        import subprocess
        import core.db as _db
        item_id = _mkitem("api error item")
        work_store.add_run(item_id, "sid-notif-1", "work-notif-1", "/tmp")
        transcript = self._transcript(tmp_path, item_id, "API Error: 529 Overloaded.")
        payload = json.dumps({
            "session_id": "sid-notif-1", "hook_event_name": "Notification",
            "message": "Claude is waiting for your input",
            "transcript_path": str(transcript),
        })
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**os.environ, "FRSHTY_DB": str(_db._DB_PATH)},
        )
        assert r.returncode == 0, r.stderr
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"
        assert item["stop_reason"] == "tmux session gone"

    def test_hook_leaves_permission_notification_to_the_operator(self, tmp_path):
        import json
        import os
        import subprocess
        import core.db as _db
        item_id = _mkitem("permission prompt item")
        work_store.add_run(item_id, "sid-notif-2", "work-notif-2", "/tmp")
        transcript = self._transcript(tmp_path, item_id, "Running the migration.")
        payload = json.dumps({
            "session_id": "sid-notif-2", "hook_event_name": "Notification",
            "message": "Claude needs your permission to use Bash",
            "transcript_path": str(transcript),
        })
        r = subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=payload, capture_output=True, text=True, timeout=15,
            env={**os.environ, "FRSHTY_DB": str(_db._DB_PATH)},
        )
        assert r.returncode == 0, r.stderr
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        kinds = {e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))}
        assert "auto_continued" not in kinds


class TestMissedAutocontinueSweep:
    def _stuck(self, tmp_path, tail_text="API Error: 529 Overloaded.", minutes=40,
               kind="Notification"):
        import json as _json
        item_id = _mkitem("missed autocontinue item")
        run_id = work_store.add_run(item_id, f"sid-miss-{item_id}", f"work-{item_id}", "/tmp")
        transcript = tmp_path / f"miss-{item_id}.jsonl"
        transcript.write_text(_json.dumps(
            {"type": "assistant", "message": {"content": [{"type": "text", "text": tail_text}]}}
        ) + "\n")
        db.execute("UPDATE work_runs SET transcript_path = ? WHERE id = ?",
                   (str(transcript), run_id))
        db.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, ?, '{}', ?)",
            (item_id, run_id, kind, work_store._now()))
        old = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        db.execute("UPDATE work_items SET state = 'needs_you', updated_at = ? WHERE id = ?",
                   (old, item_id))
        return item_id, run_id, transcript

    def _cutoff(self):
        return (datetime.now(timezone.utc)
                - timedelta(minutes=work_store.STALE_AFTER_MINUTES)).isoformat()

    def test_missed_decision_is_run_late(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        item_id, _, _ = self._stuck(tmp_path)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        actions = work_store.retry_missed_autocontinues(self._cutoff())
        assert {"id": item_id, "action": "autocontinue_retry:continued"} in actions
        sender.assert_called_once()
        item = db.query_one("SELECT state, continues_used FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        assert item["continues_used"] == 1

    def test_sweep_runs_the_retry_pass(self, tmp_path, monkeypatch):
        from unittest.mock import MagicMock
        item_id, _, _ = self._stuck(tmp_path)
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        monkeypatch.setattr(work_store, "tmux_send", MagicMock(return_value=True))
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "autocontinue_retry:continued"} in actions

    def _assert_skipped(self, item_id, monkeypatch, live=True):
        """The retry pass must not touch this item.

        Other tests leave their own eligible items in the session database,
        so the assertion is scoped to this item rather than to the whole
        action list.
        """
        from unittest.mock import MagicMock
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": live)
        monkeypatch.setattr(work_store, "tmux_send", MagicMock(return_value=True))
        actions = work_store.retry_missed_autocontinues(self._cutoff())
        assert [a for a in actions if a["id"] == item_id] == []
        item = db.query_one(
            "SELECT state, continues_used FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        return item

    def test_decision_already_recorded_is_skipped(self, tmp_path, monkeypatch):
        item_id, run_id, _ = self._stuck(tmp_path)
        db.execute(
            "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
            "VALUES (?, ?, 'question_detected', '{}', ?)",
            (item_id, run_id, work_store._now()))
        assert self._assert_skipped(item_id, monkeypatch)["continues_used"] == 0

    def test_dead_session_is_left_alone(self, tmp_path, monkeypatch):
        item_id, _, _ = self._stuck(tmp_path)
        self._assert_skipped(item_id, monkeypatch, live=False)

    def test_pending_question_is_left_for_the_operator(self, tmp_path, monkeypatch):
        item_id, _, _ = self._stuck(tmp_path)
        db.execute("UPDATE work_items SET pending_question = 'staging or prod?' WHERE id = ?",
                   (item_id,))
        self._assert_skipped(item_id, monkeypatch)

    def test_recent_item_is_left_for_the_hook(self, tmp_path, monkeypatch):
        item_id, _, _ = self._stuck(tmp_path, minutes=1)
        self._assert_skipped(item_id, monkeypatch)

    def test_capped_item_is_left_alone(self, tmp_path, monkeypatch):
        item_id, _, _ = self._stuck(tmp_path)
        db.execute("UPDATE work_items SET continues_used = continue_cap WHERE id = ?", (item_id,))
        self._assert_skipped(item_id, monkeypatch)
