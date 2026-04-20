"""Smoke test for the event-driven worker pool.

Runs in-process with a temp SQLite db. Registers a fake task, enqueues a job,
spins up one worker, waits for completion.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.queue as q  # noqa: E402
import core.worker as worker_mod  # noqa: E402
import core.event_bus as bus  # noqa: E402
import core.tasks  # noqa: F401,E402   ensure routes register
from core.tasks.registry import TaskContext, TaskResult, task, _REGISTRY  # noqa: E402


def test_end_to_end(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")

    @task("echo")
    def echo(ctx: TaskContext) -> TaskResult:
        return TaskResult("ok", artifacts={"echo": ctx.payload})

    class FakeRegistry:
        def __init__(self):
            self.instance_key = "t"
            self.config = {}
            self.base_url = ""

    registries = {"t": FakeRegistry()}
    pool = worker_mod.WorkerPool(registries, size=1, poll_interval=0.1)
    pool.start()
    try:
        job_id = q.enqueue_job("t", "echo", payload={"x": 1})
        deadline = time.time() + 5
        row = None
        while time.time() < deadline:
            row = db.query_one("SELECT status, response FROM jobs WHERE id=?", (job_id,))
            if row and row["status"] != "queued" and row["status"] != "running":
                break
            time.sleep(0.1)
        assert row, "job row missing"
        assert row["status"] == "ok", f"expected ok, got {row['status']} response={row['response']}"
        import json
        resp = json.loads(row["response"])
        assert resp["artifacts"]["echo"]["x"] == 1, resp
    finally:
        pool.stop()
        _REGISTRY.pop("echo", None)


def test_precondition_skip(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")

    @task("needs_true", preconditions=[lambda ctx: (False, "always fails")])
    def needs_true(ctx):
        return TaskResult("ok")

    class FakeRegistry:
        def __init__(self):
            self.instance_key = "t"
            self.config = {}
            self.base_url = ""

    registries = {"t": FakeRegistry()}
    pool = worker_mod.WorkerPool(registries, size=1, poll_interval=0.1)
    pool.start()
    try:
        job_id = q.enqueue_job("t", "needs_true")
        deadline = time.time() + 5
        row = None
        while time.time() < deadline:
            row = db.query_one("SELECT status, response FROM jobs WHERE id=?", (job_id,))
            if row and row["status"] != "queued" and row["status"] != "running":
                break
            time.sleep(0.1)
        assert row and row["status"] == "skipped", f"got {row}"
    finally:
        pool.stop()
        _REGISTRY.pop("needs_true", None)


def _wait_job(job_id: int, timeout: float = 5.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = db.query_one("SELECT status, response FROM jobs WHERE id=?", (job_id,))
        if row and row["status"] not in ("queued", "running"):
            return row
        time.sleep(0.1)
    raise AssertionError(f"job {job_id} did not finish within {timeout}s")


def test_set_state_roundtrip(tmp_path):
    """ui_set_state event routes to set_state task, which mutates state.save('tickets', ...)."""
    import core.state as state
    db.init(tmp_path / "t.db", ROOT / "migrations")
    state.init("t")
    state.save("tickets", {"T-1": {"status": "pr_failed", "slug": "slug-1", "branch": "feat/t-1"}})

    class FakeRegistry:
        def __init__(self):
            self.instance_key = "t"
            self.config = {}
            self.base_url = ""

    registries = {"t": FakeRegistry()}
    pool = worker_mod.WorkerPool(registries, size=1, poll_interval=0.1)
    dispatcher = bus.Dispatcher(registries, poll_interval=0.1)
    pool.start()
    dispatcher.start()
    try:
        ev_id = q.emit_event(
            source="ui", kind="ui_set_state",
            payload={"target": "pr_ready", "ticket_key": "T-1"},
            instance_key="t",
        )
        assert ev_id
        deadline = time.time() + 5
        tickets = {}
        while time.time() < deadline:
            tickets = state.load("tickets")
            if tickets.get("T-1", {}).get("status") == "pr_ready":
                break
            time.sleep(0.1)
        assert tickets.get("T-1", {}).get("status") == "pr_ready", f"got {tickets}"

        ev_row = db.query_one("SELECT dispatched_at, dispatch_reason FROM events WHERE id=?", (ev_id,))
        assert ev_row and ev_row["dispatched_at"] is not None, ev_row
        assert ev_row["dispatch_reason"] == "routed", ev_row
    finally:
        dispatcher.stop()
        pool.stop()


def test_auto_pr_precondition_reads_per_ticket(tmp_path):
    """auto_pr_true precondition reads per-ticket auto_pr; missing inherits config."""
    import core.state as state
    db.init(tmp_path / "t.db", ROOT / "migrations")
    state.init("t")
    state.save("tickets", {
        "T-A": {"status": "pr_ready"},
        "T-B": {"status": "pr_ready", "auto_pr": True},
        "T-C": {"status": "pr_ready", "auto_pr": False},
    })
    from core.tasks import preconditions as pc

    def ctx_for(key: str, cfg_auto_pr: bool):
        class Ctx: pass
        c = Ctx()
        c.instance_key = "t"
        c.ticket_key = key
        c.config = {"pr": {"auto_pr": cfg_auto_pr}}
        return c

    ok_a_true, _ = pc.auto_pr_true(ctx_for("T-A", True))
    ok_a_false, _ = pc.auto_pr_true(ctx_for("T-A", False))
    ok_b, _ = pc.auto_pr_true(ctx_for("T-B", False))
    ok_c, _ = pc.auto_pr_true(ctx_for("T-C", True))

    assert ok_a_true is True, "missing auto_pr should inherit config=True"
    assert ok_a_false is False, "missing auto_pr should inherit config=False"
    assert ok_b is True, "per-ticket auto_pr=True should win even if config=False"
    assert ok_c is False, "per-ticket auto_pr=False should win even if config=True"


if __name__ == "__main__":
    import tempfile
    tests = [test_end_to_end, test_precondition_skip,
             test_set_state_roundtrip, test_auto_pr_precondition_reads_per_ticket]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
