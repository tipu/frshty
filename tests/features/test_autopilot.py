import os
import re
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

import core.db as db
import core.runtime as runtime
import core.scheduler as scheduler
import core.state as state
import core.tasks.tickets as pipeline
import features.autopilot as autopilot
from core.consensus_scope import SCOPE_DIRECTIVE
from services import work_store

NOW = datetime(2026, 11, 17, 18, 0, tzinfo=timezone.utc)
KEY = "inst"
CONFIG = {"job": {"key": KEY}, "features": {}, "workspace": {"root": "/nowhere"},
          "autopilot": {"enabled": True}}


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = KEY
    yield


def _ask(key, kind, fingerprint="1"):
    return autopilot._ask(key, kind, f"title {key}", fingerprint)


class _Launcher:
    """Stands in for work_launch.launch: puts a real item on the board."""

    def __init__(self, error=""):
        self.calls = []
        self.error = error

    def __call__(self, objective, **kwargs):
        self.calls.append({"objective": objective, **kwargs})
        if self.error:
            return {"error": self.error}
        return {"item_id": work_store.create_item(objective, instance_key=KEY)}


def _sweep(asks, launcher):
    with patch.object(autopilot, "collect", return_value=(asks, [])), \
            patch.object(autopilot.work_launch, "launch", launcher), \
            patch.object(autopilot.work_launch, "project_entries", return_value=[]):
        return autopilot.sweep(CONFIG, KEY, now=NOW)


def _close(item_id):
    db.execute("UPDATE work_items SET state='done' WHERE id=?", (item_id,))


class TestCadence:
    def test_each_sweep_lands_inside_the_interval_give_or_take_the_jitter(self):
        draws = {scheduler._advance_recurring("every_30m_jitter_7m", NOW, NOW) for _ in range(50)}
        assert all(NOW + timedelta(minutes=23) <= d <= NOW + timedelta(minutes=37) for d in draws)
        assert len(draws) > 40

    def test_the_jitter_never_reaches_a_full_interval(self):
        draws = [scheduler._advance_recurring("every_5m_jitter_9m", NOW, NOW) for _ in range(50)]
        assert all(d >= NOW + timedelta(minutes=1) for d in draws)

    def test_the_config_names_the_cadence(self):
        assert autopilot.cadence(CONFIG) == "every_30m_jitter_7m"
        assert autopilot.cadence({"autopilot": {"interval_minutes": 10, "jitter_minutes": 3}}) \
            == "every_10m_jitter_3m"

    def test_seeding_schedules_the_sweep_only_when_autopilot_is_on(self):
        runtime._seed_recurring_schedules([{"job": {"key": KEY}, "features": {},
                                            "autopilot": {"enabled": True}}])
        row = next(r for r in scheduler.list_all(KEY) if r["key"] == "autopilot_sweep")
        assert row["cadence"] == "every_30m_jitter_7m"
        assert row["task"] == "autopilot_sweep"
        runtime._seed_recurring_schedules([{"job": {"key": KEY}, "features": {}}])
        assert not [r for r in scheduler.list_all(KEY) if r["key"] == "autopilot_sweep"]


class TestSweep:
    def test_changed_asks_start_one_triage_task_and_one_task_per_ticket(self):
        launcher = _Launcher()
        out = _sweep([_ask("slack:C1:1.0", "slack"), _ask("pr:heatreally#5", "own_pr"),
                      _ask("linear:ENG-1", "ticket")], launcher)
        assert len(out["started"]) == 2
        objectives = [c["objective"] for c in launcher.calls]
        assert objectives[0].startswith("Autopilot: develop ENG-1")
        assert objectives[1].startswith("Autopilot sweep: 2 changed")
        runs = autopilot.runs(KEY)
        assert {r["kind"] for r in runs} == {"ticket", "triage"}
        triage = next(r for r in runs if r["kind"] == "triage")
        assert f"autopilot_note.py {triage['id']} " in launcher.calls[1]["brief"]

    def test_unchanged_asks_start_nothing_and_record_no_run(self):
        asks = [_ask("slack:C1:1.0", "slack")]
        first = _Launcher()
        _sweep(asks, first)
        _close(autopilot.runs(KEY)[0]["work_item_id"])
        second = _Launcher()
        out = _sweep(asks, second)
        assert second.calls == [] and out["started"] == []
        assert len(autopilot.runs(KEY)) == 1

    def test_a_new_triage_task_waits_until_the_open_one_closes(self):
        _sweep([_ask("slack:C1:1.0", "slack")], _Launcher())
        moved = [_ask("slack:C1:1.0", "slack", fingerprint="2")]
        held = _Launcher()
        _sweep(moved, held)
        assert held.calls == []
        _close(autopilot.runs(KEY)[0]["work_item_id"])
        resumed = _Launcher()
        _sweep(moved, resumed)
        assert len(resumed.calls) == 1

    def test_ticket_tasks_respect_the_cap_and_never_double_up_on_a_ticket(self):
        _sweep([_ask("linear:ENG-1", "ticket"), _ask("linear:ENG-2", "ticket")], _Launcher())
        assert [r["ask_key"] for r in autopilot.runs(KEY)] == ["linear:ENG-1"]
        again = _Launcher()
        _sweep([_ask("linear:ENG-1", "ticket", fingerprint="2"),
                _ask("linear:ENG-2", "ticket")], again)
        assert again.calls == []

    def test_a_failed_launch_keeps_the_error_and_retries_the_asks_next_sweep(self):
        asks = [_ask("slack:C1:1.0", "slack")]
        out = _sweep(asks, _Launcher(error="no tmux"))
        assert out["started"][0]["error"] == "no tmux"
        assert autopilot.runs(KEY)[0]["error"] == "no tmux"
        retry = _Launcher()
        _sweep(asks, retry)
        assert len(retry.calls) == 1

    def test_a_sweep_that_is_off_reads_nothing(self):
        with patch.object(autopilot, "collect") as collect:
            out = autopilot.sweep({"autopilot": {"enabled": False}}, KEY, now=NOW)
        collect.assert_not_called()
        assert out == {"skipped": "autopilot is off"}

    def test_a_source_that_fails_does_not_stop_the_others(self):
        with patch.object(autopilot, "slack_asks", side_effect=RuntimeError("down")), \
                patch.object(autopilot, "pr_asks", return_value=[_ask("pr:heatreally#5", "own_pr")]), \
                patch.object(autopilot, "ticket_asks", return_value=[]):
            asks, errors = autopilot.collect(CONFIG, KEY, NOW)
        assert [a["key"] for a in asks] == ["pr:heatreally#5"]
        assert errors == ["slack: RuntimeError: down"]


class TestPlaybook:
    def test_every_stage_runs_in_pipeline_order(self):
        text = autopilot.ticket_playbook()
        heads = re.findall(r"^## Stage \d+: (\S+)", text, re.MULTILINE)
        assert heads == ["new", "planning", "reviewing", "testing", "testing", "proving",
                         "pr_ready", "in_review"]

    def test_each_stage_carries_the_pipelines_own_prompt(self):
        text = autopilot.ticket_playbook()
        for prompt in (pipeline._TRI_REVIEW_PROMPT, pipeline._WRITE_TESTS_PROMPT,
                       pipeline._PROVE_PROMPT_TEMPLATE, SCOPE_DIRECTIVE,
                       pipeline._SCOPE_FIX_PROMPT, pipeline._PR_DESCRIPTIONS_SESSION_PROMPT):
            assert autopilot._fill(prompt) in text

    def test_no_pipeline_placeholder_is_left_unfilled(self):
        text = autopilot.ticket_playbook()
        assert re.findall(r"\{([a-z_]+)\}", text) == []
        assert "{{" not in text

    def test_the_ticket_resumes_from_the_stage_its_docs_record(self, tmp_path):
        docs = tmp_path / "tickets" / "ENG-1-thing" / "docs"
        docs.mkdir(parents=True)
        (docs / "autopilot-state.md").write_text("STATE: testing\nrunning suites\n")
        config = {"workspace": {"root": str(tmp_path), "tickets_dir": "tickets"}}
        assert autopilot._ticket_stage(config, "ENG-1") == "testing"
        assert autopilot._ticket_stage(config, "ENG-2") == ""


class TestNotes:
    SCRIPT = Path(autopilot.NOTE_SCRIPT)

    def _run(self, fresh_db, *args):
        env = {**os.environ, "FRSHTY_DB": str(fresh_db)}
        return subprocess.run([sys.executable, str(self.SCRIPT), *args],
                              capture_output=True, text=True, env=env)

    def test_the_script_appends_a_note_to_the_run(self, fresh_db):
        _sweep([_ask("slack:C1:1.0", "slack")], _Launcher())
        run_id = autopilot.runs(KEY)[0]["id"]
        result = self._run(fresh_db, str(run_id), "drafted reply to thread")
        assert result.returncode == 0, result.stderr
        assert [n["text"] for n in autopilot.runs(KEY)[0]["notes"]] == ["drafted reply to thread"]

    def test_the_script_falls_back_to_the_state_root_database(self, fresh_db, tmp_path):
        root = tmp_path / "root"
        root.mkdir()
        (root / "frshty.db").write_bytes(Path(fresh_db).read_bytes())
        env = {k: v for k, v in os.environ.items() if k != "FRSHTY_DB"}
        result = subprocess.run([sys.executable, str(self.SCRIPT), "999", "x"],
                                capture_output=True, text=True,
                                env={**env, "FRSHTY_ROOT": str(root)})
        assert "no autopilot run 999" in result.stderr

    def test_the_script_refuses_a_run_that_does_not_exist(self, fresh_db):
        result = self._run(fresh_db, "999", "anything")
        assert result.returncode == 1
        assert db.query_all("SELECT * FROM autopilot_notes") == []


def test_the_api_lists_runs_with_their_notes():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    import web.autopilot as web_autopilot
    _sweep([_ask("slack:C1:1.0", "slack")], _Launcher())
    run_id = autopilot.runs(KEY)[0]["id"]
    db.execute("INSERT INTO autopilot_notes(run_id, ts, text) VALUES (?, ?, ?)",
               (run_id, NOW.isoformat(), "reviewed heatreally#5"))
    app = FastAPI()
    app.include_router(web_autopilot.router)
    with patch.dict(web_autopilot._config, CONFIG, clear=True):
        body = TestClient(app).get("/api/autopilot").json()
    assert body["enabled"] is True
    assert body["runs"][0]["notes"][0]["text"] == "reviewed heatreally#5"
    assert body["runs"][0]["asks"][0]["key"] == "slack:C1:1.0"


class TestTicketAsks:
    def _asks(self, tmp_path, issues):
        config = {"workspace": {"root": str(tmp_path), "tickets_dir": "tickets"}}
        system = type("S", (), {"fetch_tickets": lambda self: issues})()
        with patch.object(autopilot, "make_ticket_system", return_value=system):
            return autopilot.ticket_asks(config)

    def test_a_ticket_the_pipeline_left_in_review_is_not_developed_again(self, tmp_path):
        state.save_ticket("ENG-7", {"status": "in_review"})
        state.save_ticket("ENG-8", {"status": "tests_failed"})
        asks = self._asks(tmp_path, [{"key": "ENG-7", "summary": "a"},
                                     {"key": "ENG-8", "summary": "b"},
                                     {"key": "ENG-9", "summary": "c"}])
        assert [a["key"] for a in asks] == ["linear:ENG-8", "linear:ENG-9"]
        assert "Recorded stage: tests_failed" in asks[0]["detail"]

    def test_the_autopilot_stage_wins_over_the_pipeline_status(self, tmp_path):
        state.save_ticket("ENG-7", {"status": "tests_failed"})
        docs = tmp_path / "tickets" / "ENG-7-x" / "docs"
        docs.mkdir(parents=True)
        (docs / "autopilot-state.md").write_text("STATE: in_review\n")
        assert self._asks(tmp_path, [{"key": "ENG-7", "summary": "a"}]) == []


def test_both_briefs_carry_the_safety_rules():
    ask = _ask("linear:ENG-1", "ticket")
    for brief in (autopilot.triage_brief(3, [_ask("pr:heatreally#5", "own_pr")]),
                  autopilot.ticket_brief(3, ask)):
        for rule in autopilot.SAFETY_RULES:
            assert rule in brief
        assert f"autopilot_note.py 3 " in brief


def test_tickets_stay_with_the_pipeline_when_it_is_on():
    with patch.object(autopilot, "make_ticket_system") as make:
        assert autopilot.ticket_asks({"features": {"tickets": True}}) == []
    make.assert_not_called()


def test_handed_out_slack_threads_do_not_hold_back_newer_changed_ones():
    for i in range(autopilot.MAX_SLACK_ASKS + 3):
        db.execute(
            "INSERT INTO slack_conversations(instance_key, workspace, thread_ts, channel_id,"
            " first_ts, last_ts, message_count, involves_operator, created_at, updated_at, revision)"
            " VALUES (?, 'w', ?, ?, ?, ?, 1, 1, ?, ?, 1)",
            (KEY, f"{i}.0", f"C{i}", f"{i}.0", f"{i}.0", NOW.isoformat(),
             (NOW - timedelta(minutes=i)).isoformat()))
    with patch.object(autopilot.slack_conversations, "_names", return_value={}), \
            patch.object(autopilot.slack_conversations, "_operator_id", return_value="U1"), \
            patch.object(autopilot.slack_conversations, "_transcript", return_value=("hi", [], False, False)), \
            patch.object(autopilot.slack_conversations, "_channel_label", return_value="#c"):
        first = autopilot.slack_asks({}, KEY, NOW)
        assert len(first) == autopilot.MAX_SLACK_ASKS
        _sweep(first, _Launcher())
        rest = autopilot.slack_asks({}, KEY, NOW)
        assert [a["key"] for a in rest] == [f"slack:C{i}:{i}.0" for i in range(15, 18)]
        db.execute("UPDATE slack_conversations SET revision = 2 WHERE channel_id = 'C0'")
        edited = autopilot.slack_asks({}, KEY, NOW)
    assert "slack:C0:0.0" in [a["key"] for a in edited]


def test_an_item_whose_agent_died_does_not_hold_the_next_triage():
    _sweep([_ask("slack:C1:1.0", "slack")], _Launcher())
    db.execute("UPDATE work_items SET state='failed_stale' WHERE id=?",
               (autopilot.runs(KEY)[0]["work_item_id"],))
    retry = _Launcher()
    _sweep([_ask("slack:C1:1.0", "slack", fingerprint="2")], retry)
    assert len(retry.calls) == 1


@pytest.mark.parametrize("module", ["core.worker", "web.tickets", "core.runtime", "core.tasks",
                                    "features.autopilot", "web.autopilot"])
def test_each_module_imports_on_its_own(module):
    root = Path(autopilot.__file__).resolve().parent.parent
    result = subprocess.run([sys.executable, "-c", f"import {module}"], cwd=root,
                            capture_output=True, text=True)
    assert result.returncode == 0, result.stderr[-500:]
