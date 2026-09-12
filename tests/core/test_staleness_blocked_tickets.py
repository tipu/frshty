"""Tests for staleness.blocked_tickets — tickets parked at status='blocked',
which no automation retries."""

import pytest

import core.db as db
import core.state as state
import manager.staleness as staleness


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    state._instance_key_cv.set("test")
    yield


def _seed(key, status="planning"):
    state.save_ticket(key, {
        "status": status,
        "slug": key.lower(),
        "summary": f"summary for {key}",
        "discovered_at": "2026-09-01T00:00:00Z",
        "url": f"http://tickets/{key}",
    })


def _park(key, reason="task run_tests failed; releasing repo gate"):
    _seed(key)
    state.transition_ticket(key, "blocked", reason=reason)


def _transition_row(key, ts, reason, new_status="blocked", rejected=0):
    db.execute(
        "INSERT INTO ticket_transitions"
        "(instance_key, ticket_key, prior_status, new_status, rejected,"
        " rejection_reason, actor, reason, co_field_diff, ts)"
        " VALUES ('test', ?, 'planning', ?, ?, '', '', ?, '{}', ?)",
        (key, new_status, rejected, reason, ts),
    )


def test_a_blocked_ticket_is_surfaced_with_the_reason_it_was_blocked():
    _park("DEV-728", reason="task run_tests failed; releasing repo gate")

    out = staleness.blocked_tickets("test")

    assert len(out) == 1
    row = out[0]
    assert row["ticket_key"] == "DEV-728"
    assert row["summary"] == "summary for DEV-728"
    assert row["blocked_reason"] == "task run_tests failed; releasing repo gate"
    assert row["blocked_at"]
    assert row["url"] == "http://tickets/DEV-728"


def test_a_ticket_in_another_status_is_not_surfaced():
    _seed("DEV-729")

    assert staleness.blocked_tickets("test") == []


def test_an_obsolete_blocked_ticket_is_not_surfaced():
    _park("DEV-730")
    db.execute("UPDATE tickets SET obsolete_at='2026-09-02T00:00:00Z'"
               " WHERE ticket_key='DEV-730'")

    assert staleness.blocked_tickets("test") == []


def test_another_instance_does_not_see_the_block():
    _park("DEV-731")

    assert staleness.blocked_tickets("other") == []


def test_the_latest_block_is_the_one_reported():
    """A ticket restarted and blocked again reports the block it is in now."""
    _seed("DEV-732", status="blocked")
    db.execute("DELETE FROM ticket_transitions WHERE ticket_key='DEV-732'")
    _transition_row("DEV-732", "2026-09-08T09:00:00+00:00", "first block")
    _transition_row("DEV-732", "2026-09-10T09:00:00+00:00", "second block")

    out = staleness.blocked_tickets("test")

    assert out[0]["blocked_at"] == "2026-09-10T09:00:00+00:00"
    assert out[0]["blocked_reason"] == "second block"


def test_a_rejected_transition_is_not_read_as_a_block():
    """A rejected transition never happened, so it must not overwrite the
    block the ticket is actually in."""
    _seed("DEV-733", status="blocked")
    db.execute("DELETE FROM ticket_transitions WHERE ticket_key='DEV-733'")
    _transition_row("DEV-733", "2026-09-09T09:00:00+00:00", "real block")
    _transition_row("DEV-733", "2026-09-10T09:00:00+00:00", "illegal move",
                    rejected=1)

    out = staleness.blocked_tickets("test")

    assert out[0]["blocked_at"] == "2026-09-09T09:00:00+00:00"
    assert out[0]["blocked_reason"] == "real block"


def test_a_block_with_no_transition_row_is_still_surfaced():
    """The history is evidence, not the test. A ticket whose transition row is
    missing is still parked at blocked, so hiding it would be the silence this
    bucket exists to break."""
    _seed("DEV-734", status="blocked")
    db.execute("DELETE FROM ticket_transitions WHERE ticket_key='DEV-734'")

    out = staleness.blocked_tickets("test")

    assert [r["ticket_key"] for r in out] == ["DEV-734"]
    assert out[0]["blocked_at"] == ""
    assert out[0]["blocked_reason"] == ""


def test_the_bucket_is_registered_in_the_aggregate():
    _park("DEV-735")

    loops = staleness.aggregate_all("test", config={}, thresholds={})

    assert [r["ticket_key"] for r in loops["blocked_tickets"]] == ["DEV-735"]
