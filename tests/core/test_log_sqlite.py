"""Tests for SQLite-based log storage (log_events, log_read_state tables)."""
import json
import pytest
import core.log as log
import core.db as db
import core.state as state


@pytest.fixture(autouse=True)
def _clean_log_state():
    """Clean up and isolate log state for each test."""
    # Set a fresh instance key for each test
    key = f"test_instance_{id(pytest)}"
    state._instance_key_cv.set(key)
    yield
    # Clean up: reset contextvars
    try:
        state._instance_key_cv.reset(state._instance_key_cv.set(None))
    except:
        pass


class TestEmitToSQLite:
    """log.emit() writes to SQLite instead of JSONL files."""

    def test_emit_stores_in_log_events_table(self, tmp_log):
        """emit() inserts a row in log_events."""
        key = state._instance_key_cv.get()
        log.emit("ticket_found", "DEV-123 assigned")
        rows = db.query_all("SELECT * FROM log_events WHERE instance_key=? AND event=?", (key, "ticket_found"))
        assert len(rows) == 1
        assert rows[0]["summary"] == "DEV-123 assigned"
        assert rows[0]["job"] == "test"

    def test_emit_stores_instance_key(self, tmp_log):
        """instance_key is captured from contextvar."""
        key = "test_key_unique"
        state._instance_key_cv.set(key)
        log.emit("test_event", "msg")
        rows = db.query_all("SELECT instance_key FROM log_events WHERE instance_key=? AND event=?", (key, "test_event"))
        assert len(rows) >= 1
        assert rows[0]["instance_key"] == key

    def test_emit_stores_links_as_json(self, tmp_log):
        """links dict is stored as JSON in the database."""
        key = state._instance_key_cv.get()
        log.emit("evt", "msg", links={"pr": "https://github.com/owner/repo/pull/123"})
        rows = db.query_all("SELECT links FROM log_events WHERE instance_key=? AND event=?", (key, "evt"))
        assert len(rows) >= 1
        links = json.loads(rows[0]["links"])
        assert links["pr"] == "https://github.com/owner/repo/pull/123"

    def test_emit_stores_meta_as_json(self, tmp_log):
        """meta dict is stored as JSON in the database."""
        key = state._instance_key_cv.get()
        log.emit("evt", "msg", meta={"retry_count": 3, "status": "failed"})
        rows = db.query_all("SELECT meta FROM log_events WHERE instance_key=? AND event=?", (key, "evt"))
        assert len(rows) >= 1
        meta = json.loads(rows[0]["meta"])
        assert meta["retry_count"] == 3
        assert meta["status"] == "failed"

    def test_emit_filters_empty_links(self, tmp_log):
        """Empty link values are filtered out before storing."""
        key = state._instance_key_cv.get()
        record = log.emit("evt", "msg", links={"good": "http://x", "empty": "", "none": None})
        assert record["links"] == {"good": "http://x"}
        rows = db.query_all("SELECT links FROM log_events WHERE instance_key=? AND event=?", (key, "evt"))
        links = json.loads(rows[0]["links"])
        assert "empty" not in links
        assert "none" not in links

    def test_emit_generates_12char_id(self, tmp_log):
        """id is a 12-character hex string."""
        key = state._instance_key_cv.get()
        record = log.emit("evt", "msg")
        assert len(record["id"]) == 12
        assert all(c in "0123456789abcdef" for c in record["id"])
        rows = db.query_all("SELECT id FROM log_events WHERE instance_key=? AND id=?", (key, record["id"]))
        assert len(rows) >= 1
        assert rows[0]["id"] == record["id"]


class TestGetEventsFromSQLite:
    """log.get_events() queries log_events and log_read_state tables."""

    def test_get_events_returns_empty_when_no_logs(self, tmp_log):
        """get_events() returns [] when table is empty for this instance."""
        key = f"empty_instance_{id(pytest)}"
        state._instance_key_cv.set(key)
        assert log.get_events() == []

    def test_get_events_returns_events_reversed(self, tmp_log):
        """get_events() returns events newest-first."""
        key = state._instance_key_cv.get()
        log.emit("first", "1")
        log.emit("second", "2")
        events = log.get_events()
        assert len(events) >= 2
        # Filter to just our events
        our_events = [e for e in events if e["job"] == "test"]
        assert len(our_events) >= 2
        assert our_events[0]["event"] == "second"
        assert our_events[1]["event"] == "first"

    def test_get_events_respects_limit(self, tmp_log):
        """get_events(limit=N) returns at most N events."""
        key = state._instance_key_cv.get()
        for i in range(10):
            log.emit("evt", f"msg_{i}")
        assert len(log.get_events(limit=5)) == 5

    def test_get_events_filters_by_instance_key(self, tmp_log):
        """get_events() only returns events for current instance_key."""
        key_a = f"instance_a_{id(pytest)}"
        key_b = f"instance_b_{id(pytest)}"

        state._instance_key_cv.set(key_a)
        log.emit("event_a", "from A")

        state._instance_key_cv.set(key_b)
        log.emit("event_b", "from B")

        state._instance_key_cv.set(key_a)
        events = log.get_events()
        assert len(events) >= 1
        assert any(e["event"] == "event_a" for e in events)
        assert not any(e["event"] == "event_b" for e in events)

    def test_get_events_filters_by_job_key(self, tmp_log):
        """get_events() only returns events for current job."""
        key = state._instance_key_cv.get()
        log.emit("job_a_event", "msg")

        log_tokens = log.use(tmp_log, "job_b")
        log.emit("job_b_event", "msg")
        log.reset(log_tokens)

        events = log.get_events()
        assert any(e["event"] == "job_a_event" for e in events)
        assert not any(e["event"] == "job_b_event" for e in events)

    def test_get_events_after_filter(self, tmp_log):
        """get_events(after=ts) only returns events newer than ts."""
        key = state._instance_key_cv.get()
        r1 = log.emit("first", "1")
        log.emit("second", "2")
        events = log.get_events(after=r1["ts"])
        assert len(events) >= 1
        assert all(e["ts"] > r1["ts"] for e in events)

    def test_get_events_parses_links_and_meta_from_json(self, tmp_log):
        """get_events() deserializes JSON links and meta."""
        key = state._instance_key_cv.get()
        log.emit("evt", "msg", links={"url": "http://example.com"}, meta={"key": "value"})
        events = log.get_events()
        evt = next((e for e in events if e["event"] == "evt"), None)
        assert evt is not None
        assert isinstance(evt["links"], dict)
        assert evt["links"]["url"] == "http://example.com"
        assert isinstance(evt["meta"], dict)
        assert evt["meta"]["key"] == "value"


class TestDismissSQLite:
    """log.dismiss() and log.dismiss_ids() manage log_read_state table."""

    def test_dismiss_marks_event_as_read(self, tmp_log):
        """dismiss(id) inserts the id into log_read_state."""
        key = state._instance_key_cv.get()
        record = log.emit("evt", "msg")
        log.dismiss(record["id"])

        rows = db.query_all(
            "SELECT event_id FROM log_read_state WHERE instance_key=? AND event_id=?",
            (key, record["id"])
        )
        assert len(rows) == 1

    def test_dismiss_ids_batch(self, tmp_log):
        """dismiss_ids() marks multiple ids as read atomically."""
        key = state._instance_key_cv.get()
        r1 = log.emit("evt1", "msg")
        r2 = log.emit("evt2", "msg")

        count = log.dismiss_ids({r1["id"], r2["id"]})
        assert count == 2

        rows = db.query_all(
            "SELECT COUNT(*) as c FROM log_read_state WHERE instance_key=? AND event_id IN (?,?)",
            (key, r1["id"], r2["id"])
        )
        assert rows[0]["c"] == 2

    def test_get_events_marks_read_events(self, tmp_log):
        """get_events() includes a 'read' boolean reflecting log_read_state."""
        key = state._instance_key_cv.get()
        r1 = log.emit("evt1", "msg")
        r2 = log.emit("evt2", "msg")
        log.dismiss(r1["id"])

        events = log.get_events()
        evt1 = next((e for e in events if e["id"] == r1["id"]), None)
        evt2 = next((e for e in events if e["id"] == r2["id"]), None)
        assert evt1 is not None
        assert evt2 is not None
        assert evt1["read"] is True
        assert evt2["read"] is False

    def test_dismiss_all_marks_all_events_as_read(self, tmp_log):
        """dismiss_all() marks all events in the current job as read."""
        key = state._instance_key_cv.get()
        r1 = log.emit("evt0", "msg_0")
        r2 = log.emit("evt1", "msg_1")
        r3 = log.emit("evt2", "msg_2")
        r4 = log.emit("evt3", "msg_3")
        r5 = log.emit("evt4", "msg_4")

        log.dismiss_all()

        rows = db.query_all(
            "SELECT COUNT(*) as c FROM log_read_state WHERE instance_key=?",
            (key,)
        )
        assert rows[0]["c"] == 5

    def test_dismiss_ids_idempotent(self, tmp_log):
        """Dismissing the same id twice returns 0 on second call (INSERT OR IGNORE)."""
        key = state._instance_key_cv.get()
        record = log.emit("evt", "msg")

        count1 = log.dismiss_ids({record["id"]})
        count2 = log.dismiss_ids({record["id"]})

        assert count1 == 1
        assert count2 == 0


class TestUnreadOnlyFilter:
    """get_events(unread_only=True) filters out read events."""

    def test_unread_only_excludes_dismissed(self, tmp_log):
        """unread_only=True filters out events in log_read_state."""
        key = state._instance_key_cv.get()
        r1 = log.emit("unread", "msg")
        r2 = log.emit("to_dismiss", "msg")
        log.dismiss(r2["id"])

        events = log.get_events(unread_only=True)
        assert any(e["id"] == r1["id"] for e in events)
        assert not any(e["id"] == r2["id"] for e in events)


class TestSupervisorSQLite:
    """supervisor_actions and supervisor_escalations tables."""

    def test_supervisor_actions_table_exists(self, tmp_log):
        """supervisor_actions table is created by migration."""
        rows = db.query_all("SELECT name FROM sqlite_master WHERE type='table' AND name='supervisor_actions'")
        assert len(rows) == 1

    def test_supervisor_escalations_table_exists(self, tmp_log):
        """supervisor_escalations table is created by migration."""
        rows = db.query_all("SELECT name FROM sqlite_master WHERE type='table' AND name='supervisor_escalations'")
        assert len(rows) == 1

    def test_supervisor_actions_insert(self, tmp_log):
        """Actions can be inserted and retrieved."""
        db.execute(
            "INSERT INTO supervisor_actions(key, count, ts) VALUES(?,?,?)",
            ("instance1:ticket1:autofix", 2, 1234567890.0)
        )
        rows = db.query_all("SELECT key, count, ts FROM supervisor_actions WHERE key=?", ("instance1:ticket1:autofix",))
        assert len(rows) == 1
        assert rows[0]["key"] == "instance1:ticket1:autofix"
        assert rows[0]["count"] == 2

    def test_supervisor_escalations_insert(self, tmp_log):
        """Escalations can be inserted and retrieved."""
        db.execute(
            "INSERT INTO supervisor_escalations(key, ts) VALUES(?,?)",
            ("instance1:ticket1:error_event", 1234567890.0)
        )
        rows = db.query_all("SELECT key, ts FROM supervisor_escalations WHERE key=?", ("instance1:ticket1:error_event",))
        assert len(rows) == 1
        assert rows[0]["key"] == "instance1:ticket1:error_event"


class TestMigrationSchema:
    """Verify migration 002 schema is correct."""

    def test_log_events_schema(self, tmp_log):
        """log_events has correct columns."""
        rows = db.query_all("PRAGMA table_info(log_events)")
        columns = {r["name"] for r in rows}
        expected = {"id", "instance_key", "job", "event", "summary", "links", "meta", "ts"}
        assert expected.issubset(columns)

    def test_log_read_state_schema(self, tmp_log):
        """log_read_state has correct columns."""
        rows = db.query_all("PRAGMA table_info(log_read_state)")
        columns = {r["name"] for r in rows}
        expected = {"instance_key", "event_id"}
        assert expected.issubset(columns)

    def test_log_events_primary_key(self, tmp_log):
        """log_events has composite primary key (instance_key, id)."""
        rows = db.query_all("PRAGMA table_info(log_events)")
        pk_cols = {r["name"] for r in rows if r["pk"] > 0}
        assert "instance_key" in pk_cols
        assert "id" in pk_cols

    def test_log_read_state_primary_key(self, tmp_log):
        """log_read_state has composite primary key (instance_key, event_id)."""
        rows = db.query_all("PRAGMA table_info(log_read_state)")
        pk_cols = {r["name"] for r in rows if r["pk"] > 0}
        assert "instance_key" in pk_cols
        assert "event_id" in pk_cols

    def test_log_events_index_exists(self, tmp_log):
        """log_events has idx_log_events_job_ts index."""
        rows = db.query_all("PRAGMA index_list(log_events)")
        index_names = {r["name"] for r in rows}
        assert "idx_log_events_job_ts" in index_names
