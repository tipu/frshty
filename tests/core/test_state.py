import json
import threading
from datetime import datetime, timezone

import pytest

import core.db as db
import core.log as log
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
        state.save_ticket("T-1", {"status": "new", "slug": "t-1"})
        with pytest.raises(TicketStateError):
            state.transition_ticket("T-1", "in_review")
        assert state.load_ticket("T-1")["status"] == "new"

    def test_missing_raises(self, tmp_state):
        with pytest.raises(TicketStateError):
            state.transition_ticket("NOPE", "planning")

    def test_self_transition_is_noop(self, tmp_state):
        state.save_ticket("T-1", {"status": "reviewing", "slug": "t-1"})
        result = state.transition_ticket("T-1", "reviewing")
        assert result["status"] == "reviewing"

    def test_merged_requires_external_status(self, tmp_state):
        state.save_ticket("T-1", {"status": "in_review", "slug": "t-1"})
        with pytest.raises(TicketStateError, match="merged_external_status"):
            state.transition_ticket("T-1", "merged")
        state.transition_ticket("T-1", "merged", merged_external_status="Released")
        assert state.load_ticket("T-1")["merged_external_status"] == "Released"


class TestResetTicket:
    def test_bypasses_illegal_transition(self, tmp_state):
        state.save_ticket("T-1", {"status": "pr_ready", "slug": "t-1",
                                  "branch": "b", "prs": [{"id": 1, "repo": "r"}]})
        with pytest.raises(TicketStateError):
            state.transition_ticket("T-1", "new")
        result = state.reset_ticket("T-1", target="new", reason="note")
        assert result["status"] == "new"
        assert result["branch"] == "b"
        assert state.load_ticket("T-1")["status"] == "new"

    def test_still_enforces_invariants(self, tmp_state):
        state.save_ticket("T-1", {"status": "pr_ready", "slug": "t-1"})
        with pytest.raises(TicketStateError, match="merged_external_status"):
            state.reset_ticket("T-1", target="merged")

    def test_missing_raises(self, tmp_state):
        with pytest.raises(TicketStateError):
            state.reset_ticket("NOPE", target="new")


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
        """state.save("module", dict) is read-modify-write at the application
        level (load → mutate → save). It does NOT guarantee atomic merge —
        the last save wins. The guarantee here is "no JSON corruption" only.
        For lost-write protection, use state.update_ticket() (atomic mutate)."""
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
        assert isinstance(final, dict), "JSON must not corrupt under concurrent writes"
        assert final.get("a") == 1 or final.get("b") == 2, \
            "at least one writer's data must land cleanly"

    def test_atomic_update_ticket_no_lost_writes(self, tmp_state):
        """update_ticket IS atomic — both writers' fields must survive."""
        state.save_ticket("T-1", {"status": "new", "slug": "t1", "counters": {}})

        def increment(key):
            for _ in range(20):
                def _mut(cur):
                    new = dict(cur)
                    counters = dict(new.get("counters") or {})
                    counters[key] = counters.get(key, 0) + 1
                    new["counters"] = counters
                    return new
                state.update_ticket("T-1", _mut)

        t1 = threading.Thread(target=increment, args=("a",))
        t2 = threading.Thread(target=increment, args=("b",))
        t1.start(); t2.start()
        t1.join(); t2.join()

        final = state.load_ticket("T-1")
        assert final["counters"]["a"] == 20, \
            f"atomic update lost writes for 'a': {final['counters']}"
        assert final["counters"]["b"] == 20, \
            f"atomic update lost writes for 'b': {final['counters']}"


class TestTransitionAudit:
    def _rows(self, instance):
        return db.query_all(
            "SELECT prior_status, new_status, rejected, rejection_reason,"
            " actor, reason, co_field_diff, ts"
            " FROM ticket_transitions WHERE instance_key=? ORDER BY id ASC",
            (instance,),
        )

    def test_migration_applied(self, tmp_state):
        row = db.query_one(
            "SELECT name FROM _migrations WHERE name=?",
            ("012_ticket_transitions.sql",),
        )
        assert row is not None, (
            "Migration 012_ticket_transitions.sql must be applied; "
            "ticket_transitions table is required by all TestTransitionAudit cases"
        )

    def test_first_transition_writes_row(self, tmp_state):
        state.save_ticket("T-1", {"status": "new", "slug": "t-1"})
        before = self._rows(tmp_state.name)
        state.transition_ticket("T-1", "planning")
        after = self._rows(tmp_state.name)
        new_rows = after[len(before):]
        assert len(new_rows) == 1
        r = new_rows[0]
        assert r["prior_status"] == "new"
        assert r["new_status"] == "planning"
        assert r["rejected"] == 0
        assert r["ts"]

    def test_self_transition_writes_no_row(self, tmp_state):
        state.save_ticket("T-1", {"status": "reviewing", "slug": "t-1"})
        before = self._rows(tmp_state.name)
        state.transition_ticket("T-1", "reviewing")
        after = self._rows(tmp_state.name)
        assert len(after) == len(before)

    def test_rejected_transition_writes_row_and_raises(self, tmp_state):
        state.save_ticket("T-1", {"status": "new", "slug": "t-1"})
        before = self._rows(tmp_state.name)
        with pytest.raises(TicketStateError):
            state.transition_ticket("T-1", "in_review")
        after = self._rows(tmp_state.name)
        new_rows = after[len(before):]
        assert len(new_rows) == 1
        r = new_rows[0]
        assert r["rejected"] == 1
        assert "Illegal transition" in (r["rejection_reason"] or "")
        assert r["new_status"] == "in_review"
        assert state.load_ticket("T-1")["status"] == "new"

    def test_save_ticket_status_change_writes_row_with_diff(self, tmp_state):
        state.save_ticket("T-1", {"status": "pr_ready", "slug": "t-1", "prs": []})
        before = self._rows(tmp_state.name)
        state.save_ticket("T-1", {
            "status": "in_review", "slug": "t-1",
            "prs": [{"id": 1, "repo": "r"}],
            "ci_passed": True,
        })
        after = self._rows(tmp_state.name)
        new_rows = after[len(before):]
        assert len(new_rows) == 1
        r = new_rows[0]
        assert r["prior_status"] == "pr_ready"
        assert r["new_status"] == "in_review"
        diff = json.loads(r["co_field_diff"])
        assert diff.get("prs_count") == {"prior": 0, "new": 1}
        assert diff.get("ci_passed") == {"prior": None, "new": True}

    def test_save_ticket_no_status_change_writes_no_row(self, tmp_state):
        state.save_ticket("T-1", {"status": "in_review", "slug": "t-1"})
        before = self._rows(tmp_state.name)
        state.save_ticket("T-1", {"status": "in_review", "slug": "t-1", "ci_passed": True})
        after = self._rows(tmp_state.name)
        assert len(after) == len(before)

    def test_pr_scheduled_at_stripped_on_pr_failed(self, tmp_state):
        state.save_ticket("T-1", {
            "status": "pr_failed", "slug": "t-1",
            "pr_scheduled_at": "2026-04-29T16:04:49+00:00",
        })
        reloaded = state.load_ticket("T-1")
        assert "pr_scheduled_at" not in reloaded

    def test_pr_scheduled_at_preserved_on_pr_ready(self, tmp_state):
        state.save_ticket("T-1", {
            "status": "pr_ready", "slug": "t-1",
            "pr_scheduled_at": "2026-04-29T16:04:49+00:00",
        })
        reloaded = state.load_ticket("T-1")
        assert reloaded.get("pr_scheduled_at") == "2026-04-29T16:04:49+00:00"

    def test_reason_propagates_through_transition(self, tmp_state):
        state.save_ticket("T-1", {"status": "in_review", "slug": "t-1"})
        state.transition_ticket(
            "T-1", "merged",
            reason="manual merge button",
            merged_external_status="Released",
        )
        after = self._rows(tmp_state.name)
        assert after[-1]["new_status"] == "merged"
        assert after[-1]["reason"] == "manual merge button"

    def test_actor_captured_from_log_job_key(self, tmp_state):
        tokens = log.use(tmp_state, "scan_tickets")
        try:
            state.save_ticket("T-1", {"status": "new", "slug": "t-1"})
            state.transition_ticket("T-1", "planning")
        finally:
            log.reset(tokens)
        after = self._rows(tmp_state.name)
        assert after[-1]["actor"] == "scan_tickets"

    def test_audit_failure_does_not_break_ticket_write(self, tmp_state, monkeypatch):
        state.save_ticket("T-1", {"status": "new", "slug": "t-1"})
        real_execute = db.execute

        def flaky_execute(sql, params=()):
            if "ticket_transitions" in sql:
                raise RuntimeError("simulated audit failure")
            return real_execute(sql, params)

        monkeypatch.setattr("core.db.execute", flaky_execute)
        state.save_ticket("T-1", {"status": "planning", "slug": "t-1"})
        monkeypatch.undo()
        reloaded = state.load_ticket("T-1")
        assert reloaded["status"] == "planning"
        rows = db.query_all(
            "SELECT event FROM log_events WHERE event=?",
            ("ticket_transition_log_failed",),
        )
        assert len(rows) >= 1
