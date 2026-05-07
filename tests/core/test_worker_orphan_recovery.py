"""Tests for WorkerPool orphan recovery: detached `claude -p` survives a
frshty restart, and on the next start the worker pool reconciles 'running'
jobs without blindly resetting them to 'queued' (which used to corrupt
worktrees by replaying claude over partial commits).
"""
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.job_logs as job_logs  # noqa: E402
import core.queue as q  # noqa: E402
import core.state as state  # noqa: E402
import core.tasks.tickets  # noqa: E402,F401  registers ticket-pipeline tasks
from core.worker import WorkerPool  # noqa: E402


def _seed_running_job(instance_key: str, task_name: str, ticket_key: str | None) -> int:
    started = datetime.now(timezone.utc).isoformat()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO jobs(instance_key, ticket_key, task, status, enqueued_at, started_at)"
            " VALUES (?, ?, ?, 'running', ?, ?)",
            (instance_key, ticket_key, task_name, started, started),
        )
        return cur.lastrowid or 0


def _seed_ticket(instance_key: str, ticket_key: str, slug: str, status: str) -> None:
    state.use(instance_key)
    state.save_ticket(ticket_key, {
        "instance_key": instance_key,
        "ticket_key": ticket_key,
        "slug": slug,
        "status": status,
    })


def _make_pool(tmp_path: Path, instance_key: str) -> WorkerPool:
    state_dir = tmp_path / instance_key
    state_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "_state_dir": state_dir,
        "workspace": {
            "root": tmp_path / "workspace",
            "tickets_dir": "tickets",
        },
    }
    reg = types.SimpleNamespace(config=config, base_url="")
    return WorkerPool(registries={instance_key: reg})


def test_orphan_with_passing_postconditions_marks_done(fresh_db, tmp_path):
    state.init(tmp_path / "state")

    instance = "inst1"
    ticket_key = "DEV-1"
    slug = "dev1-test"

    ticket_dir = tmp_path / "workspace" / "tickets" / slug
    (ticket_dir / "docs").mkdir(parents=True)
    (ticket_dir / "docs" / "change-manifest.md").write_text("ok")

    _seed_ticket(instance, ticket_key, slug, "planning")
    job_id = _seed_running_job(instance, "start_planning", ticket_key)

    pool = _make_pool(tmp_path, instance)
    pool._reconcile_orphans()

    row = db.query_one("SELECT status, response FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "ok"
    assert "orphan recovered" in (row["response"] or "")

    state.use(instance)
    t = state.load_ticket(ticket_key)
    assert t and t["status"] == "reviewing"


def test_orphan_with_failing_postconditions_marks_failed_not_requeued(fresh_db, tmp_path):
    """The bug we're fixing: under sweep_stale(0), this job would be reset
    to 'queued' and re-run, replaying /ctp on a worktree the killed claude
    already partially mutated. After the fix, an orphan with unmet
    postconditions is marked failed — surface, don't replay."""
    state.init(tmp_path / "state")

    instance = "inst1"
    ticket_key = "DEV-2"
    slug = "dev2-test"

    ticket_dir = tmp_path / "workspace" / "tickets" / slug
    (ticket_dir / "docs").mkdir(parents=True)

    _seed_ticket(instance, ticket_key, slug, "planning")
    job_id = _seed_running_job(instance, "start_planning", ticket_key)

    pool = _make_pool(tmp_path, instance)
    pool._reconcile_orphans()

    row = db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "failed", \
        "missing change-manifest.md must mark orphan failed, never requeue"


def test_orphan_no_postconditions_marks_failed(fresh_db, tmp_path):
    """fix_reported_bug has no postconditions defined — we cannot prove
    completion, so it must be marked failed rather than left running or
    silently replayed."""
    state.init(tmp_path / "state")

    instance = "inst1"
    ticket_key = "DEV-3"
    slug = "dev3-test"

    ticket_dir = tmp_path / "workspace" / "tickets" / slug
    ticket_dir.mkdir(parents=True)

    _seed_ticket(instance, ticket_key, slug, "in_review")
    job_id = _seed_running_job(instance, "fix_reported_bug", ticket_key)

    pool = _make_pool(tmp_path, instance)
    pool._reconcile_orphans()

    row = db.query_one("SELECT status, response FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "failed"
    assert "no postconditions" in (row["response"] or "")


def test_live_job_is_skipped_by_orphan_poll(fresh_db, tmp_path):
    """The poll loop must NOT touch jobs being watched by an in-process
    worker thread, even if their pid is on disk and postconditions pass."""
    state.init(tmp_path / "state")

    instance = "inst1"
    ticket_key = "DEV-4"
    slug = "dev4-test"

    ticket_dir = tmp_path / "workspace" / "tickets" / slug
    (ticket_dir / "docs").mkdir(parents=True)
    (ticket_dir / "docs" / "change-manifest.md").write_text("ok")

    _seed_ticket(instance, ticket_key, slug, "planning")
    job_id = _seed_running_job(instance, "start_planning", ticket_key)

    pool = _make_pool(tmp_path, instance)
    pool._live_jobs.add(job_id)

    jobs = q.running_jobs()
    for job in jobs:
        if job["id"] in pool._live_jobs:
            continue
        pool._finalize_orphan(job)

    row = db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "running", \
        "live job must remain running; only the worker thread finalizes it"


def test_pid_file_paths_round_trip():
    p = job_logs.job_pid_path("inst", 42)
    assert p.name == "42.pid"
    assert p.parent.name == "jobs"
    assert p.parent.parent.name == "inst"
