"""Tests for SQLite-based supervisor state (supervisor_actions, supervisor_escalations tables)."""
import time
import pytest
import core.db as db


class TestSupervisorStateSQLite:
    """supervisor._load_state() and _save_state() use SQLite tables."""

    def test_load_state_returns_empty_dict_when_tables_empty(self, tmp_log):
        """_load_state() returns {"actions": {}, "escalations": {}} when no data."""
        from supervisor import _load_state
        state = _load_state()
        assert state == {"actions": {}, "escalations": {}}

    def test_load_state_reconstructs_actions(self, tmp_log):
        """_load_state() reconstructs actions dict from supervisor_actions table."""
        db.execute(
            "INSERT INTO supervisor_actions(key, count, ts) VALUES(?,?,?)",
            ("instance1:ticket1:autofix", 3, 1234567890.0)
        )
        from supervisor import _load_state
        state = _load_state()
        assert state["actions"]["instance1:ticket1:autofix"] == {"count": 3, "ts": 1234567890.0}

    def test_load_state_reconstructs_escalations(self, tmp_log):
        """_load_state() reconstructs escalations dict from supervisor_escalations table."""
        db.execute(
            "INSERT INTO supervisor_escalations(key, ts) VALUES(?,?)",
            ("instance1:ticket1:error", 1234567890.0)
        )
        from supervisor import _load_state
        state = _load_state()
        assert state["escalations"]["instance1:ticket1:error"] == 1234567890.0

    def test_save_state_clears_and_rewrites_actions(self, tmp_log):
        """_save_state() DELETEs and INSERTs, replacing old data."""
        db.execute(
            "INSERT INTO supervisor_actions(key, count, ts) VALUES(?,?,?)",
            ("old_key", 1, 1111111111.0)
        )
        from supervisor import _save_state
        new_state = {
            "actions": {
                "new_key": {"count": 5, "ts": 2222222222.0}
            },
            "escalations": {}
        }
        _save_state(new_state)

        rows = db.query_all("SELECT key FROM supervisor_actions WHERE key=?", ("old_key",))
        assert len(rows) == 0

        rows = db.query_all("SELECT key, count FROM supervisor_actions WHERE key=?", ("new_key",))
        assert len(rows) == 1
        assert rows[0]["count"] == 5

    def test_save_state_clears_and_rewrites_escalations(self, tmp_log):
        """_save_state() DELETEs and INSERTs escalations."""
        db.execute(
            "INSERT INTO supervisor_escalations(key, ts) VALUES(?,?)",
            ("old_esc", 1111111111.0)
        )
        from supervisor import _save_state
        new_state = {
            "actions": {},
            "escalations": {
                "new_esc": 2222222222.0
            }
        }
        _save_state(new_state)

        rows = db.query_all("SELECT key FROM supervisor_escalations WHERE key=?", ("old_esc",))
        assert len(rows) == 0

        rows = db.query_all("SELECT key, ts FROM supervisor_escalations WHERE key=?", ("new_esc",))
        assert len(rows) == 1
        assert rows[0]["ts"] == 2222222222.0

    def test_save_and_load_roundtrip(self, tmp_log):
        """Save and then load preserves all state."""
        from supervisor import _save_state, _load_state
        original_state = {
            "actions": {
                "i1:t1:fix": {"count": 2, "ts": 1234567890.0},
                "i1:t2:fix": {"count": 1, "ts": 1234567891.0}
            },
            "escalations": {
                "i1:t1:error": 9876543210.0,
                "i2:t3:error": 9876543211.0
            }
        }
        _save_state(original_state)
        loaded_state = _load_state()
        assert loaded_state == original_state

    def test_load_state_handles_missing_tables_gracefully(self, tmp_log):
        """_load_state() catches exceptions and returns empty dict."""
        from supervisor import _load_state
        try:
            db.execute("DROP TABLE supervisor_actions")
            db.execute("DROP TABLE supervisor_escalations")
        except:
            pass
        state = _load_state()
        assert state == {"actions": {}, "escalations": {}}

    def test_save_state_uses_transaction(self, tmp_log):
        """_save_state() uses a transaction for atomicity."""
        from supervisor import _save_state
        state = {
            "actions": {
                "key1": {"count": 1, "ts": 1000.0},
                "key2": {"count": 2, "ts": 2000.0}
            },
            "escalations": {
                "esc1": 3000.0
            }
        }
        _save_state(state)

        actions = db.query_all("SELECT COUNT(*) as c FROM supervisor_actions")
        escalations = db.query_all("SELECT COUNT(*) as c FROM supervisor_escalations")
        assert actions[0]["c"] == 2
        assert escalations[0]["c"] == 1


class TestSupervisorStateAcidProperties:
    """Verify ACID properties of supervisor state storage."""

    def test_multiple_saves_dont_corrupt_state(self, tmp_log):
        """Multiple saves don't leave partial data."""
        from supervisor import _save_state, _load_state
        for i in range(3):
            state = {
                "actions": {
                    f"key{i}": {"count": i, "ts": float(i)}
                },
                "escalations": {}
            }
            _save_state(state)

        final = _load_state()
        assert len(final["actions"]) == 1
        assert "key2" in final["actions"]

    def test_empty_state_can_be_saved(self, tmp_log):
        """Saving empty state clears tables."""
        from supervisor import _save_state, _load_state
        db.execute("INSERT INTO supervisor_actions(key, count, ts) VALUES(?,?,?)", ("key", 1, 1.0))
        _save_state({"actions": {}, "escalations": {}})

        loaded = _load_state()
        assert loaded == {"actions": {}, "escalations": {}}

        actions = db.query_all("SELECT COUNT(*) as c FROM supervisor_actions")
        assert actions[0]["c"] == 0
