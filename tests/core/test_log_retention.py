"""Job events and the nightly log retention job."""
import json
import time
from datetime import datetime, timedelta, timezone

import core.db as db
import core.log as log
import core.queue as q
import core.runtime as runtime
import core.scheduler as scheduler
import core.worker as worker_mod
from core.tasks.registry import TaskResult, task, _REGISTRY

NOW = datetime(2026, 10, 12, 12, 0, tzinfo=timezone.utc)


def _event(ev_id: str, age_days: float, noise: bool) -> None:
    db.execute(
        "INSERT INTO log_events(id, instance_key, job, event, summary, links, meta, ts) "
        "VALUES(?,?,?,?,?,?,?,?)",
        (ev_id, "rt", "rt", "e", "s", "{}", json.dumps({"category": "noise"} if noise else {}),
         (NOW - timedelta(days=age_days)).isoformat()))


def _invocation(inv_id: str, age_days: float) -> None:
    db.execute(
        "INSERT INTO claude_invocations(id, instance_key, function_name, model, prompt, "
        "prompt_length, started_at, status, output, output_length, cost_usd) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (inv_id, "rt", "f", "m", "the prompt", 10, (NOW - timedelta(days=age_days)).isoformat(),
         "ok", "the output", 10, 1.5))


def _ids() -> set[str]:
    return {r["id"] for r in db.query_all("SELECT id FROM log_events WHERE instance_key='rt'")}


def test_prune_applies_each_retention_window(fresh_db):
    _event("noise_8d", 8, True)
    _event("noise_6d", 6, True)
    _event("plain_59d", 59, False)
    _event("plain_61d", 61, False)
    _invocation("inv_31d", 31)
    _invocation("inv_29d", 29)

    result = log.prune(NOW)

    assert _ids() == {"noise_6d", "plain_59d"}
    old = db.query_one("SELECT * FROM claude_invocations WHERE id='inv_31d'")
    assert (old["prompt"], old["output"]) == ("", "")
    assert (old["prompt_length"], old["output_length"], old["cost_usd"]) == (10, 10, 1.5)
    recent = db.query_one("SELECT * FROM claude_invocations WHERE id='inv_29d'")
    assert (recent["prompt"], recent["output"]) == ("the prompt", "the output")
    assert result == {"noise_deleted": 1, "expired_deleted": 1, "invocations_blanked": 1}
    assert log.prune(NOW) == {"noise_deleted": 0, "expired_deleted": 0, "invocations_blanked": 0}


def test_prune_drains_a_backlog_larger_than_one_batch(fresh_db, monkeypatch):
    monkeypatch.setattr(log, "PRUNE_BATCH", 2)
    for i in range(5):
        _event(f"n{i}", 10, True)
        _invocation(f"i{i}", 40)

    result = log.prune(NOW)

    assert _ids() == set()
    assert result["noise_deleted"] == 5 and result["invocations_blanked"] == 5


def test_seed_schedules_log_retention_for_every_instance(fresh_db):
    runtime._seed_recurring_schedules([{"job": {"key": "rt"}, "features": {}}])

    row = next(r for r in scheduler.list_all("rt") if r["key"] == "log_retention")
    assert (row["task"], row["cadence"]) == ("log_retention", "daily_3_local")
    run_at = scheduler.run_at("rt", "log_retention")
    assert run_at is not None and run_at > datetime.now(timezone.utc)


class _FakeRegistry:
    instance_key = "t"
    config: dict = {}
    base_url = ""


def _run(task_name: str) -> None:
    job_id = q.enqueue_job("t", task_name)
    deadline = time.time() + 5
    while time.time() < deadline:
        row = db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))
        if row and row["status"] not in ("queued", "running"):
            return
        time.sleep(0.05)
    raise AssertionError(f"job {job_id} did not finish")


def _events(name: str) -> list[dict]:
    return db.query_all("SELECT summary FROM log_events WHERE event=?", (name,))


def test_worker_writes_job_events_only_for_a_job_that_is_not_ok(fresh_db):
    @task("rt_ok")
    def rt_ok(ctx):
        return TaskResult("ok")

    @task("rt_fail")
    def rt_fail(ctx):
        return TaskResult("failed", "boom")

    pool = worker_mod.WorkerPool({"t": _FakeRegistry()}, size=1, poll_interval=0.05)
    pool.start()
    try:
        _run("rt_ok")
        _run("rt_ok")
        _run("rt_fail")
    finally:
        pool.stop()
        _REGISTRY.pop("rt_ok", None)
        _REGISTRY.pop("rt_fail", None)

    assert _events("job_started") == []
    finished = _events("job_finished")
    assert len(finished) == 1 and "rt_fail" in finished[0]["summary"]
    assert "status=failed reason=boom" in finished[0]["summary"]
    alive = _events("worker_alive")
    assert len(alive) == 1 and "rt_ok" in alive[0]["summary"]
