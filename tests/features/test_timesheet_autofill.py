from datetime import date, datetime

import features.timesheet as tsmod


WED = date(2026, 5, 20)  # a Wednesday, within auto-fill weekday range
NOW = datetime(2026, 5, 20, 19, 0)  # inside default fill_window [18, 20]
CONFIG = {"timesheet": {"auto_fill": True, "fill_window": [18, 20], "fill_target": 8}}


def _patch_clock_and_state(monkeypatch):
    monkeypatch.setattr(tsmod.tz, "today_local", lambda: WED)
    monkeypatch.setattr(tsmod.tz, "now_local", lambda: NOW)
    saved = {}
    monkeypatch.setattr(tsmod.state, "load", lambda k: {})
    monkeypatch.setattr(tsmod.state, "save", lambda k, v: saved.__setitem__(k, v))
    return saved


def _capture_logwork(monkeypatch):
    calls = []
    monkeypatch.setattr(tsmod, "log_work",
                        lambda c, t, d, ts: (calls.append((t, d, ts)), {"ok": True})[1])
    return calls


def _capture_events(monkeypatch):
    events = []
    monkeypatch.setattr(tsmod.log, "emit",
                        lambda ev, msg="", **k: events.append(ev))
    return events


def test_autofill_excludes_terminal_ticket(monkeypatch):
    _patch_clock_and_state(monkeypatch)
    calls = _capture_logwork(monkeypatch)
    events = _capture_events(monkeypatch)
    data = {
        "userAccountId": "me",
        "tickets": [{"key": "DEV-444", "summary": "Handle 503", "status": "Done",
                     "assignee_id": "me", "hoursEstimated": 24, "hoursSpentTotal": 22,
                     "in_progress_at": "2026-04-01"}],
        "gitCommits": {"2026-05-19": [{"branch": "DEV-444-handle-503", "message": "fix"}]},
        "worklogs": {}, "recurring": {}, "prReviews": {},
        "claudeSessions": {}, "dailySummaries": {},
    }
    monkeypatch.setattr(tsmod, "build_timesheet", lambda *a, **k: data)
    tsmod._auto_fill(CONFIG)
    assert calls == [], "terminal DEV-444 must never be auto-logged"
    assert "auto_fill_skipped" in events


def test_autofill_logs_in_flight_ticket(monkeypatch):
    _patch_clock_and_state(monkeypatch)
    calls = _capture_logwork(monkeypatch)
    data = {
        "userAccountId": "me",
        "tickets": [{"key": "DEV-1", "summary": "Active work", "status": "In Progress",
                     "assignee_id": "me", "hoursEstimated": 16, "hoursSpentTotal": 0,
                     "in_progress_at": "2026-05-15"}],
        "gitCommits": {"2026-05-19": [{"branch": "DEV-1-active", "message": "wip"}]},
        "worklogs": {}, "recurring": {}, "prReviews": {},
        "claudeSessions": {}, "dailySummaries": {},
    }
    monkeypatch.setattr(tsmod, "build_timesheet", lambda *a, **k: data)
    tsmod._auto_fill(CONFIG)
    assert len(calls) == 1, "in-flight ticket should be logged"
    assert calls[0][0] == "DEV-1"
    assert calls[0][1] == "2026-05-20"
    assert calls[0][2] == "8.0h"


def test_autofill_skips_when_assignee_not_me(monkeypatch):
    _patch_clock_and_state(monkeypatch)
    calls = _capture_logwork(monkeypatch)
    events = _capture_events(monkeypatch)
    data = {
        "userAccountId": "me",
        "tickets": [{"key": "DEV-9", "summary": "Someone else", "status": "In Progress",
                     "assignee_id": "other", "hoursEstimated": 16, "hoursSpentTotal": 0,
                     "in_progress_at": "2026-05-15"}],
        "gitCommits": {"2026-05-19": [{"branch": "DEV-9-x", "message": "wip"}]},
        "worklogs": {}, "recurring": {}, "prReviews": {},
        "claudeSessions": {}, "dailySummaries": {},
    }
    monkeypatch.setattr(tsmod, "build_timesheet", lambda *a, **k: data)
    tsmod._auto_fill(CONFIG)
    assert calls == [], "tickets not assigned to the user must not be logged"
    assert "auto_fill_skipped" in events


def test_autofill_early_returns_when_day_full(monkeypatch):
    saved = _patch_clock_and_state(monkeypatch)
    calls = _capture_logwork(monkeypatch)
    data = {
        "userAccountId": "me",
        "tickets": [{"key": "DEV-1", "summary": "x", "status": "In Progress",
                     "assignee_id": "me", "hoursEstimated": 16, "hoursSpentTotal": 0,
                     "in_progress_at": "2026-05-15"}],
        "gitCommits": {"2026-05-19": [{"branch": "DEV-1-x", "message": "wip"}]},
        "worklogs": {"2026-05-20": [{"ticket": "DEV-2", "hours": 8}]},
        "recurring": {}, "prReviews": {}, "claudeSessions": {}, "dailySummaries": {},
    }
    monkeypatch.setattr(tsmod, "build_timesheet", lambda *a, **k: data)
    tsmod._auto_fill(CONFIG)
    assert calls == [], "already at target → nothing logged"
    assert saved.get("timesheet_fill", {}).get("2026-05-20", {}).get("filled") is True
