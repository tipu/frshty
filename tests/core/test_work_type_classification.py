"""Tests for ticket work-type classification (code/research/unknown), the
research status path, and the needs_classification surfacing bucket."""

import json
from unittest.mock import patch

import pytest

import core.db as db
import core.state as state
import core.ticket_status as tstatus
import features.tickets as tix
import manager.staleness as staleness


def _set_instance(key="test"):
    state._default_instance_key = key
    state._instance_key_cv.set(key)


class TestResearchTransitions:
    def test_new_to_researching(self):
        assert tstatus.transition("new", "researching") == "researching"

    def test_researching_to_done(self):
        assert tstatus.transition("researching", "done") == "done"

    def test_researching_to_new_retry(self):
        assert tstatus.transition("researching", "new") == "new"

    def test_researching_cannot_enter_code_pipeline(self):
        with pytest.raises(ValueError):
            tstatus.transition("researching", "reviewing")


class TestEnsureWorkType:
    def test_prd_is_always_code(self):
        ts = {"source": "prd", "status": "new"}
        with patch.object(tix.state, "save_ticket"):
            assert tix._ensure_work_type({}, {"key": "P-1"}, ts) == "code"
        assert ts["work_type"] == "code"

    def test_idempotent_no_reclassify(self):
        ts = {"work_type": "research"}
        with patch.object(tix, "run_haiku") as m:
            assert tix._ensure_work_type({}, {"key": "X-1"}, ts) == "research"
            m.assert_not_called()

    def test_classifies_research(self):
        ts = {"status": "new"}
        with patch.object(tix.state, "save_ticket"), \
             patch.object(tix, "run_haiku", return_value="research"):
            r = tix._ensure_work_type({}, {"key": "R-1", "summary": "Spike: evaluate X"}, ts)
        assert r == "research" and ts["work_type"] == "research"

    def test_garbage_output_falls_back_to_unknown(self):
        ts = {"status": "new"}
        with patch.object(tix.state, "save_ticket"), \
             patch.object(tix, "run_haiku", return_value="hmm not sure"):
            r = tix._ensure_work_type({}, {"key": "U-1", "summary": "???"}, ts)
        assert r == "unknown"


def _insert_ticket(key, status, data):
    db.execute(
        "INSERT INTO tickets(instance_key, ticket_key, status, data, updated_at)"
        " VALUES(?,?,?,?,?)",
        ("test", key, status, json.dumps(data), "2026-01-01T00:00:00Z"),
    )


class TestNeedsClassificationBucket:
    @pytest.fixture(autouse=True)
    def _clear(self):
        try:
            db.execute("DELETE FROM tickets")
        except Exception:
            pass
        yield

    def test_surfaces_only_unknown_new_tickets(self):
        _set_instance()
        _insert_ticket("U-1", "new", {"work_type": "unknown", "summary": "spike?", "discovered_at": "2026-01-01"})
        _insert_ticket("C-1", "new", {"work_type": "code", "summary": "fix bug", "discovered_at": "2026-01-01"})
        _insert_ticket("R-1", "researching", {"work_type": "research", "summary": "spike", "discovered_at": "2026-01-01"})

        out = staleness.needs_classification("test")

        assert [t["ticket_key"] for t in out] == ["U-1"]
        assert out[0]["summary"] == "spike?"
