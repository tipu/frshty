"""The live check: the agent names a command, the board runs it a day later.

A fix that looked done and did not hold a day later reopened 25 frshty-board
tasks by hand. These tests pin that the board records the command, runs it
once after the delay, records the result, and reopens the task on failure."""
import sys
from datetime import datetime, timedelta, timezone

import pytest

import core.db as db
from core.tasks.registry import TaskContext, run_task
from services import live_check, work_store


@pytest.fixture
def config(tmp_path):
    (tmp_path / "frshty").mkdir()
    return {"job": {"key": "frshty"},
            "workspace": {"root": tmp_path, "repos": ["frshty"]}}


def _finished_item(command: str, state: str = "needs_ack") -> int:
    item_id = work_store.create_item("fix the gate")
    sid = f"sid-live-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    assert work_store.record_live_check(sid, "", texts=[f"deployed\nLIVE_CHECK: {command}"]) == command
    db.execute("UPDATE work_items SET state = ? WHERE id = ?", (state, item_id))
    return item_id


def _item(item_id: int) -> dict:
    return db.query_one("SELECT * FROM work_items WHERE id = ?", (item_id,))


def _later() -> datetime:
    return datetime.now(timezone.utc) + timedelta(hours=live_check.LIVE_CHECK_DELAY_HOURS, minutes=1)


def _py(code: str) -> str:
    return f"{sys.executable} -c '{code}'"


def test_a_passing_check_is_recorded_and_leaves_the_task_closed(config):
    item_id = _finished_item(_py("print(\"still holds\")"))
    results = live_check.run_due(config, now=_later())
    assert [r["passed"] for r in results if r["id"] == item_id] == [True]
    row = _item(item_id)
    assert row["state"] == "needs_ack"
    assert row["live_check_exit"] == 0
    assert "still holds" in row["live_check_output"]
    assert row["live_check_ran_at"]


def test_a_failing_check_reopens_the_task_with_the_command_code_and_output(config):
    command = _py("import sys; print(\"gate is open\"); sys.exit(3)")
    item_id = _finished_item(command)
    live_check.run_due(config, now=_later())
    row = _item(item_id)
    assert row["state"] == "needs_you"
    assert command in row["stop_reason"]
    assert "exit code 3" in row["stop_reason"]
    assert "gate is open" in row["stop_reason"]
    assert db.query_one("SELECT 1 AS x FROM work_events WHERE work_item_id = ? "
                        "AND kind = 'operator_reopen'", (item_id,))


def test_a_check_runs_once(config, tmp_path):
    marker = tmp_path / "runs.txt"
    item_id = _finished_item(_py(f"open(\"{marker}\", \"a\").write(\"x\")"))
    live_check.run_due(config, now=_later())
    live_check.run_due(config, now=_later() + timedelta(hours=2))
    assert marker.read_text() == "x"
    assert len(db.query_all("SELECT id FROM work_events WHERE work_item_id = ? "
                            "AND kind = 'live_check'", (item_id,))) == 1


def test_a_check_younger_than_the_delay_is_skipped(config):
    item_id = _finished_item(_py("pass"))
    early = datetime.now(timezone.utc) + timedelta(hours=live_check.LIVE_CHECK_DELAY_HOURS - 1)
    assert item_id not in [r["id"] for r in live_check.run_due(config, now=early)]
    assert _item(item_id)["live_check_ran_at"] is None


def test_an_open_task_is_not_checked(config):
    item_id = _finished_item(_py("pass"), state="agent_working")
    assert item_id not in [r["id"] for r in live_check.run_due(config, now=_later())]


def test_the_command_runs_without_a_shell(config, tmp_path):
    marker = tmp_path / "injected"
    item_id = _finished_item(f"{_py('pass')} ; touch {marker}")
    live_check.run_due(config, now=_later())
    assert _item(item_id)["live_check_ran_at"]
    assert not marker.exists()


def test_a_check_that_hangs_is_a_failure(config, monkeypatch):
    monkeypatch.setattr(live_check, "LIVE_CHECK_TIMEOUT_SECONDS", 1)
    item_id = _finished_item(_py("import time; time.sleep(30)"))
    live_check.run_due(config, now=_later())
    row = _item(item_id)
    assert row["state"] == "needs_you"
    assert "timed out" in row["stop_reason"]


def test_a_new_command_after_a_failure_is_checked_again(config):
    item_id = _finished_item(_py("import sys; sys.exit(1)"))
    live_check.run_due(config, now=_later())
    sid = f"sid-live-{item_id}"
    assert work_store.record_live_check(sid, "", texts=[f"LIVE_CHECK: {_py('pass')}"])
    db.execute("UPDATE work_items SET state = 'needs_ack' WHERE id = ?", (item_id,))
    live_check.run_due(config, now=_later())
    assert _item(item_id)["live_check_exit"] == 0


@pytest.mark.parametrize("cfg, expected", [
    ({"job": {"key": "frshty"}}, True),
    ({"job": {"key": "personal"}}, False),
    ({"job": {"key": "personal"}, "live_check": {"enabled": True}}, True),
    ({"job": {"key": "frshty"}, "live_check": {"enabled": False}}, False),
])
def test_only_the_frshty_board_runs_checks_by_default(cfg, expected):
    assert live_check.enabled(cfg) is expected


def test_the_task_skips_an_instance_where_it_is_off(config):
    item_id = _finished_item(_py("pass"))
    config["live_check"] = {"enabled": False}
    ctx = TaskContext(instance_key="frshty", ticket_key=None, task="work_live_check",
                      payload={}, job_id=0, triggering_event_id=None, config=config,
                      registry=None, now=_later())
    result = run_task(ctx)
    assert result.status == "ok"
    assert _item(item_id)["live_check_ran_at"] is None


@pytest.mark.parametrize("contexts, runs", [("", True), ("frshty", True), ("frshty,slack_int", True),
                                            ("stepioxcorp", False)])
def test_a_check_runs_only_for_a_task_of_this_board(config, tmp_path, contexts, runs):
    marker = tmp_path / "ran.txt"
    item_id = _finished_item(_py(f"open(\"{marker}\", \"a\").write(\"x\")"))
    db.execute("UPDATE work_items SET contexts = ? WHERE id = ?", (contexts, item_id))
    live_check.run_due(config, now=_later())
    assert marker.exists() is runs
    assert _item(item_id)["state"] == "needs_ack"


def test_checks_of_other_projects_do_not_hold_back_this_board_s_checks(config, tmp_path):
    marker = tmp_path / "own.txt"
    for _ in range(live_check.LIVE_CHECKS_PER_RUN + 1):
        other = _finished_item(_py("print(1)"))
        db.execute("UPDATE work_items SET contexts = 'stepioxcorp' WHERE id = ?", (other,))
    own = _finished_item(_py(f"open(\"{marker}\", \"a\").write(\"x\")"))
    live_check.run_due(config, now=_later())
    assert marker.exists()
    assert _item(own)["live_check_ran_at"]
    assert db.query_one("SELECT COUNT(*) AS n FROM work_items WHERE contexts = 'stepioxcorp' "
                        "AND live_check_ran_at IS NOT NULL")["n"] == 0
