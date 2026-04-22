import json
import threading
from datetime import datetime, timezone

import pytest

import core.db as db
import core.state as state
from core.state import TicketStateError


class TestInit:
    def test_creates_directory(self, tmp_path):
        target = tmp_path / "new_state"
        state.init(target)
        assert target.is_dir()


class TestLoadSave:
    def test_load_missing_returns_empty(self, tmp_state):
        assert state.load("nonexistent") == {}

    def test_save_and_load(self, tmp_state):
        state.save("test", {"key": "value"})
        assert state.load("test") == {"key": "value"}

    def test_save_overwrites(self, tmp_state):
        state.save("test", {"a": 1})
        state.save("test", {"b": 2})
        result = state.load("test")
        assert result == {"b": 2}
        assert "a" not in result

    def test_save_persists_to_kv_table(self, tmp_state):
        state.save("mod", {"x": 1})
        row = db.query_one(
            "SELECT data FROM kv WHERE instance_key=? AND key=?",
            (tmp_state.name, "mod"),
        )
        assert row is not None, f"expected kv row for instance={tmp_state.name} key=mod"
        assert json.loads(row["data"]) == {"x": 1}

    def test_save_preserves_on_write_failure(self, tmp_state):
        state.save("safe", {"original": True})
        loaded = state.load("safe")
        assert loaded == {"original": True}


class TestTicketsShim:
    def test_save_tickets_writes_per_row(self, tmp_state):
        state.save("tickets", {"A": {"status": "new", "slug": "a"}})
        row = db.query_one(
            "SELECT status, slug FROM tickets WHERE instance_key=? AND ticket_key=?",
            (tmp_state.name, "A"),
        )
        assert row is not None, "expected row A in tickets table"
        assert row["status"] == "new"
        assert row["slug"] == "a"
        kv_row = db.query_one(
            "SELECT data FROM kv WHERE instance_key=? AND key='tickets'",
            (tmp_state.name,),
        )
        assert kv_row is None, "tickets must not round-trip through kv table"

    def test_shim_deletes_absent_keys(self, tmp_state):
        state.save("tickets", {
            "A": {"status": "new", "slug": "a"},
            "B": {"status": "new", "slug": "b"},
        })
        state.save("tickets", {"A": {"status": "planning", "slug": "a"}})
        loaded = state.load("tickets")
        assert set(loaded) == {"A"}
        assert loaded["A"]["status"] == "planning"

    def test_shim_upserts_present(self, tmp_state):
        state.save("tickets", {"A": {"status": "new", "slug": "a"}})
        state.save("tickets", {"A": {"status": "planning", "slug": "a"}})
        loaded = state.load("tickets")
        assert loaded["A"]["status"] == "planning"


class TestLazyMigration:
    def test_kv_tickets_blob_migrates_to_rows(self, tmp_state):
        legacy = {"LEG-1": {"status": "planning", "slug": "leg-1-s"}}
        now = datetime.now(timezone.utc).isoformat()
        db.execute(
            "INSERT INTO kv(instance_key, key, data, updated_at) VALUES (?, 'tickets', ?, ?)",
            (tmp_state.name, json.dumps(legacy), now),
        )
        state._TICKETS_MIGRATED.discard(tmp_state.name)

        first = state.load("tickets")
        assert "LEG-1" in first
        assert first["LEG-1"]["status"] == "planning"

        rows = db.query_all(
            "SELECT ticket_key FROM tickets WHERE instance_key=?",
            (tmp_state.name,),
        )
        assert {r["ticket_key"] for r in rows} == {"LEG-1"}

        second = state.load("tickets")
        assert second == first

        rows_after = db.query_all(
            "SELECT ticket_key FROM tickets WHERE instance_key=?",
            (tmp_state.name,),
        )
        assert len(rows_after) == 1, "migration must not duplicate rows on re-read"


class TestTransitionTicket:
    def test_happy_path_persists(self, tmp_state):
        state.save_ticket("T-1", {"status": "new", "slug": "t-1"})
        result = state.transition_ticket("T-1", "planning")
        assert result["status"] == "planning"
        reloaded = state.load_ticket("T-1")
        assert reloaded["status"] == "planning"
        assert reloaded["slug"] == "t-1"

    def test_illegal_raises(self, tmp_state):
        state.save_ticket("T-1", {"status": "pr_failed", "slug": "t-1"})
        with pytest.raises(TicketStateError):
            state.transition_ticket("T-1", "pr_created")
        assert state.load_ticket("T-1")["status"] == "pr_failed"

    def test_missing_raises(self, tmp_state):
        with pytest.raises(TicketStateError):
            state.transition_ticket("NOPE", "planning")

    def test_self_transition_is_noop(self, tmp_state):
        state.save_ticket("T-1", {"status": "reviewing", "slug": "t-1"})
        result = state.transition_ticket("T-1", "reviewing")
        assert result["status"] == "reviewing"

    def test_merged_requires_external_status(self, tmp_state):
        state.save_ticket("T-1", {"status": "pr_created", "slug": "t-1"})
        with pytest.raises(TicketStateError, match="merged_external_status"):
            state.transition_ticket("T-1", "merged")
        state.transition_ticket("T-1", "merged", merged_external_status="Released")
        assert state.load_ticket("T-1")["merged_external_status"] == "Released"


class TestSaveTicketInvariants:
    def test_save_ticket_merged_without_external_status_raises(self, tmp_state):
        with pytest.raises(TicketStateError, match="merged_external_status"):
            state.save_ticket("T-1", {"status": "merged", "slug": "t-1"})

    def test_save_ticket_merged_with_external_status_ok(self, tmp_state):
        state.save_ticket("T-1", {
            "status": "merged",
            "slug": "t-1",
            "merged_external_status": "Done",
        })
        assert state.load_ticket("T-1")["merged_external_status"] == "Done"


class TestConcurrency:
    def test_concurrent_writes_no_corruption(self, tmp_state):
        def writer(key, value):
            for _ in range(50):
                d = state.load("shared")
                d[key] = value
                state.save("shared", d)

        t1 = threading.Thread(target=writer, args=("a", 1))
        t2 = threading.Thread(target=writer, args=("b", 2))
        t1.start(); t2.start()
        t1.join(); t2.join()

        final = state.load("shared")
        assert isinstance(final, dict)
        assert "a" in final or "b" in final
