"""Regression test: a ticket's events must be retrievable even when buried
behind a firehose of unrelated/noise events (the ticket detail page bug)."""

import pytest

import core.db as db
import core.log as log
import core.state as state


@pytest.fixture(autouse=True)
def _clear_events():
    try:
        db.execute("DELETE FROM log_events")
    except Exception:
        pass
    yield


def test_ticket_event_survives_noise_firehose(tmp_log):
    state._instance_key_cv.set("test")

    log.emit("ticket_pr_comment_fixed",
             'FRG-186-support: Fixed "worth typing?" — changed app/main.py (abc1234)',
             meta={"ticket": "FRG-186", "commit": "abc1234"})

    for i in range(300):
        log.emit("job_finished", f"advance_ticket ticket=DSC-94 job_id={i}",
                 meta={"category": "noise"})

    global_recent = log.get_events(limit=200)
    assert not any(e["event"] == "ticket_pr_comment_fixed" for e in global_recent), \
        "precondition: the fix event is buried beyond the global 200-event window"

    history = log.get_events_for_ticket("FRG-186", limit=200)
    assert any(e["event"] == "ticket_pr_comment_fixed" for e in history)
    assert all(e["meta"].get("category") != "noise" for e in history)


def test_excludes_noise_and_other_tickets(tmp_log):
    state._instance_key_cv.set("test")
    log.emit("ticket_pr_comment_fixed", "FRG-186-support: Fixed", meta={"ticket": "FRG-186"})
    log.emit("ticket_pr_comment_fixed", "DSC-94-other: Fixed", meta={"ticket": "DSC-94"})
    log.emit("job_started", "advance_ticket ticket=FRG-186", meta={"category": "noise"})

    history = log.get_events_for_ticket("FRG-186", limit=200)

    assert len(history) == 1
    assert history[0]["meta"]["ticket"] == "FRG-186"
