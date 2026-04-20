"""Smoke test for the event-driven worker pool.

Runs in-process with a temp SQLite db. Registers a fake task, enqueues a job,
spins up one worker, waits for completion.
"""
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.queue as q  # noqa: E402
import core.worker as worker_mod  # noqa: E402
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


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_end_to_end(tmp)
        print("test_end_to_end: PASS")
    with tempfile.TemporaryDirectory() as d:
        tmp = Path(d)
        test_precondition_skip(tmp)
        print("test_precondition_skip: PASS")
