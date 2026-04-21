"""Regression: sweep_stale must catch 'running' jobs whose started_at is
ISO-8601 with a T separator. The naive SQL comparison against datetime()'s
space-separated return value fails lexicographically because T (0x54) > space
(0x20), so started_at < threshold never evaluates true.
"""
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.queue as q  # noqa: E402


def _seed_running_job(instance_key: str, task_name: str, started_at_iso: str) -> int:
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO jobs(instance_key, task, status, enqueued_at, started_at)"
            " VALUES (?, ?, 'running', ?, ?)",
            (instance_key, task_name, started_at_iso, started_at_iso),
        )
        return cur.lastrowid or 0


def test_sweep_stale_resets_old_running_job(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")
    old = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    job_id = _seed_running_job("inst", "start_planning", old)

    reset = q.sweep_stale(max_age_seconds=3600)

    assert reset == 1, "a 2h-old running job should be reset by sweep_stale(max_age=1h)"
    row = db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "queued"


def test_sweep_stale_zero_age_resets_all_running_jobs(tmp_path):
    """Startup behavior: WorkerPool.start() calls sweep_stale(max_age_seconds=0)
    to reset every ghost 'running' row from a prior process. Must reset even
    seconds-old rows since no worker can actually be executing at startup."""
    db.init(tmp_path / "t.db", ROOT / "migrations")
    started = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    job_id = _seed_running_job("inst", "start_planning", started)

    reset = q.sweep_stale(max_age_seconds=0)

    assert reset == 1, "sweep_stale(max_age=0) at startup must reset any 'running' job regardless of age"
    row = db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "queued"


def test_sweep_stale_leaves_fresh_running_jobs_alone(tmp_path):
    """A job that just started should NOT be reset by a non-zero threshold."""
    db.init(tmp_path / "t.db", ROOT / "migrations")
    fresh = datetime.now(timezone.utc).isoformat()
    job_id = _seed_running_job("inst", "start_planning", fresh)

    reset = q.sweep_stale(max_age_seconds=3600)

    assert reset == 0
    row = db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))
    assert row and row["status"] == "running"
