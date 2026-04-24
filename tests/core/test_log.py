import json

import core.log as log


class TestEmit:
    def test_returns_record(self, tmp_log):
        record = log.emit("test_event", "hello")
        assert record["event"] == "test_event"
        assert record["summary"] == "hello"
        assert record["job"] == "test"
        assert len(record["id"]) == 12

    def test_filters_empty_links(self, tmp_log):
        record = log.emit("evt", "msg", links={"good": "http://x", "empty": "", "none": None})
        assert record["links"] == {"good": "http://x"}

    def test_none_links_become_empty(self, tmp_log):
        record = log.emit("evt", "msg", links=None)
        assert record["links"] == {}


class TestGetEvents:
    def test_empty_log(self, tmp_log):
        assert log.get_events() == []

    def test_returns_events_reversed(self, tmp_log):
        log.emit("first", "1")
        log.emit("second", "2")
        events = log.get_events()
        assert events[0]["event"] == "second"
        assert events[1]["event"] == "first"

    def test_limit(self, tmp_log):
        for i in range(5):
            log.emit("evt", str(i))
        assert len(log.get_events(limit=3)) == 3

    def test_after_filter(self, tmp_log):
        r1 = log.emit("first", "1")
        log.emit("second", "2")
        events = log.get_events(after=r1["ts"])
        assert len(events) == 1
        assert events[0]["event"] == "second"

    def test_unread_only(self, tmp_log):
        r1 = log.emit("first", "1")
        log.emit("second", "2")
        log.dismiss(r1["id"])
        events = log.get_events(unread_only=True)
        assert len(events) == 1
        assert events[0]["event"] == "second"


class TestDismiss:
    def test_marks_as_read(self, tmp_log):
        r = log.emit("evt", "msg")
        log.dismiss(r["id"])
        events = log.get_events()
        assert events[0]["read"] is True


class TestDismissAll:
    def test_marks_all_read(self, tmp_log):
        log.emit("a", "1")
        log.emit("b", "2")
        log.dismiss_all()
        assert log.get_events(unread_only=True) == []

    def test_truncates_long_log(self, tmp_log):
        for i in range(log.MAX_LOG_LINES + 50):
            log.emit("evt", str(i))
        log.dismiss_all()
        events = log.get_events(limit=log.MAX_LOG_LINES + 100)
        assert len(events) <= log.MAX_LOG_LINES
