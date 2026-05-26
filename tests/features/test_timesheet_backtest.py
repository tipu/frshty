import features.timesheet as tsmod


CONFIG = {"timesheet": {"fill_target": 8}}


def test_backtest_reports_agreement_and_excludes_terminal(monkeypatch):
    # Mon 2026-05-18 and Tue 2026-05-19 are weekdays.
    data = {
        "userAccountId": "me",
        "tickets": [
            {"key": "DEV-1", "summary": "Active", "status": "In Progress",
             "assignee_id": "me", "hoursEstimated": 40, "hoursSpentTotal": 0,
             "in_progress_at": "2026-05-10"},
            {"key": "DEV-444", "summary": "Done thing", "status": "Done",
             "assignee_id": "me", "hoursEstimated": 24, "hoursSpentTotal": 22,
             "in_progress_at": "2026-04-01"},
        ],
        "gitCommits": {
            "2026-05-18": [{"branch": "DEV-1-active", "message": "wip"}],
            "2026-05-19": [{"branch": "DEV-1-active", "message": "more"}],
            "2026-05-20": [{"branch": "DEV-1-active", "message": "push"}],
        },
        "worklogs": {
            "2026-05-18": [{"ticket": "DEV-1", "hours": 8}],
            "2026-05-19": [{"ticket": "DEV-444", "hours": 8}],
        },
        "recurring": {}, "prReviews": {}, "claudeSessions": {}, "dailySummaries": {},
    }
    monkeypatch.setattr(tsmod, "build_timesheet", lambda *a, **k: data)
    report = tsmod.backtest_timesheet_selection(CONFIG, "2026-05-18", "2026-05-20")

    assert report["weekdays"] == 3, "Mon-Wed are 3 weekdays"
    assert report["compared_days"] >= 1
    predicted_tickets = {a["ticket"] for a in report["allocations"]}
    assert "DEV-444" not in predicted_tickets, "terminal ticket never predicted"
    assert "DEV-1" in predicted_tickets
    assert 0.0 <= report["agreement"] <= 1.0


def test_backtest_invalid_range():
    assert tsmod.backtest_timesheet_selection(CONFIG, "2026-05-20", "2026-05-18").get("error")
    assert tsmod.backtest_timesheet_selection(CONFIG, "not-a-date", "2026-05-18").get("error")
