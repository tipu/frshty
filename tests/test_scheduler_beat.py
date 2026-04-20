"""Covers the beat thread + scheduler table round-trip."""
import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_recurring_fires_and_advances(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.scheduler as scheduler

    db.init(tmp_path / "t.db", ROOT / "migrations")

    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    scheduler.upsert_recurring("t", "billing_check", "billing_check",
                                cadence="weekly", next_run_at=past)

    fired = scheduler.fire_due_recurring()
    assert len(fired) == 1, fired
    assert fired[0]["task"] == "billing_check"
    assert fired[0]["instance_key"] == "t"

    job_row = db.query_one("SELECT instance_key, task, status FROM jobs WHERE task='billing_check'")
    assert job_row and job_row["instance_key"] == "t", job_row

    sched_row = db.query_one(
        "SELECT run_at, data FROM scheduler WHERE instance_key='t' AND key='billing_check'"
    )
    assert sched_row is not None
    new_run_at = datetime.fromisoformat(sched_row["run_at"])
    assert new_run_at > datetime.now(timezone.utc), "next_run_at should be in the future"
    data = json.loads(sched_row["data"])
    assert data["last_run_at"], "last_run_at stamped"


def test_recurring_skip_ahead_on_missed_windows(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.scheduler as scheduler

    db.init(tmp_path / "t.db", ROOT / "migrations")

    very_old = datetime.now(timezone.utc) - timedelta(days=60)
    scheduler.upsert_recurring("t", "billing_check", "billing_check",
                                cadence="weekly", next_run_at=very_old)

    scheduler.fire_due_recurring()

    job_count = db.query_one("SELECT COUNT(*) AS n FROM jobs WHERE task='billing_check'")
    assert job_count is not None and job_count["n"] == 1, "skip-ahead: exactly one fire, not one per missed week"

    sched_row = db.query_one(
        "SELECT run_at FROM scheduler WHERE instance_key='t' AND key='billing_check'"
    )
    assert sched_row is not None
    new_run_at = datetime.fromisoformat(sched_row["run_at"])
    assert new_run_at > datetime.now(timezone.utc), "next_run_at moved past now"


def test_oneshot_coexists_with_recurring(tmp_path):
    """Both kinds live in the same scheduler table and are listable together."""
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.state as state
    import core.scheduler as scheduler

    db.init(tmp_path / "t.db", ROOT / "migrations")
    state.init("t")

    future = datetime.now(timezone.utc) + timedelta(hours=5)
    scheduler.schedule("TICKET-1", "create_pr", future, meta={"slug": "x"})

    next_week = datetime.now(timezone.utc) + timedelta(days=7)
    scheduler.upsert_recurring("t", "billing_check", "billing_check",
                                cadence="weekly", next_run_at=next_week)

    rows = scheduler.list_all("t")
    kinds = {r.get("kind", "oneshot") for r in rows}
    assert kinds == {"oneshot", "recurring"}, kinds
    tasks = {r.get("task") for r in rows if r.get("kind") == "recurring"}
    assert "billing_check" in tasks
    actions = {r.get("action") for r in rows if r.get("kind", "oneshot") == "oneshot"}
    assert "create_pr" in actions


if __name__ == "__main__":
    tests = [test_recurring_fires_and_advances,
             test_recurring_skip_ahead_on_missed_windows,
             test_oneshot_coexists_with_recurring]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
