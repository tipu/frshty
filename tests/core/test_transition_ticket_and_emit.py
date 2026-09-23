"""transition_ticket_and_emit commits the ticket write, the job completion and
the next-stage events in one SQLite transaction. A failure in any part leaves
none of them written."""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.queue as q  # noqa: E402
import core.state as state  # noqa: E402
from core.worker import transition_ticket_and_emit  # noqa: E402


def _running_job(instance_key: str, ticket_key: str) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO jobs(instance_key, ticket_key, task, status, enqueued_at, started_at)"
            " VALUES (?, ?, 'start_planning', 'running', ?, ?)",
            (instance_key, ticket_key, now, now),
        )
        return cur.lastrowid or 0


def _advance_events(ticket_key: str) -> list[dict]:
    rows = db.query_all("SELECT instance_key, source, payload FROM events"
                        " WHERE kind='ticket_advance'", ())
    return [r for r in rows if json.loads(r["payload"]).get("ticket_key") == ticket_key]


def _job_status(job_id: int) -> str:
    return db.query_one("SELECT status FROM jobs WHERE id=?", (job_id,))["status"]


@pytest.fixture()
def inst(fresh_db, tmp_path):
    state.init(tmp_path / "inst1")
    return "inst1"


def test_commits_ticket_job_and_event_together(inst):
    state.save_ticket("T-1", {"status": "planning", "slug": "t1"})
    job_id = _running_job(inst, "T-1")

    saved = transition_ticket_and_emit("T-1", inst, target="reviewing", job_id=job_id,
                                       job_response={"reason": "done"},
                                       next_events=[{"kind": "pm_findings_written",
                                                     "payload": {"ticket_key": "T-1"}}])

    assert saved["status"] == "reviewing"
    assert state.load_ticket("T-1")["status"] == "reviewing"
    assert _job_status(job_id) == "ok"
    events = _advance_events("T-1")
    assert len(events) == 1 and events[0]["instance_key"] == inst
    assert db.query_one("SELECT COUNT(*) AS n FROM events WHERE kind='pm_findings_written'",
                        ())["n"] == 1


def test_event_failure_rolls_back_ticket_and_job(inst, monkeypatch):
    state.save_ticket("T-2", {"status": "planning", "slug": "t2"})
    job_id = _running_job(inst, "T-2")

    def boom(*a, **kw):
        raise RuntimeError("disk full")
    monkeypatch.setattr(q, "insert_event", boom)

    with pytest.raises(RuntimeError):
        transition_ticket_and_emit("T-2", inst, target="reviewing", job_id=job_id)

    assert state.load_ticket("T-2")["status"] == "planning"
    assert _job_status(job_id) == "running"


def test_event_failure_rolls_back_job_without_ticket_write(inst, monkeypatch):
    job_id = _running_job(inst, "T-3")

    def boom(*a, **kw):
        raise RuntimeError("disk full")
    monkeypatch.setattr(q, "insert_event", boom)

    with pytest.raises(RuntimeError):
        transition_ticket_and_emit("T-3", inst, job_id=job_id)

    assert _job_status(job_id) == "running"


def test_illegal_transition_writes_nothing(inst):
    state.save_ticket("T-4", {"status": "new", "slug": "t4"})
    job_id = _running_job(inst, "T-4")

    with pytest.raises(state.TicketStateError):
        transition_ticket_and_emit("T-4", inst, target="in_review", job_id=job_id)

    assert state.load_ticket("T-4")["status"] == "new"
    assert _job_status(job_id) == "running"
    assert _advance_events("T-4") == []


def test_mutate_writes_ticket_and_event(inst):
    state.save_ticket("T-5", {"status": "new", "slug": "t5"})

    saved = transition_ticket_and_emit("T-5", inst, mutate=lambda cur: {**cur, "branch": "b"},
                                       source="ui")

    assert saved["branch"] == "b"
    events = _advance_events("T-5")
    assert len(events) == 1 and events[0]["source"] == "ui"


def test_no_instance_key_writes_ticket_without_event(inst):
    state.save_ticket("T-6", {"status": "planning", "slug": "t6"})

    transition_ticket_and_emit("T-6", None, target="reviewing")

    assert state.load_ticket("T-6")["status"] == "reviewing"
    assert _advance_events("T-6") == []
