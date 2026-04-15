from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import core.state as state
from actions import schedule_pr


class TestHandle:
    def test_with_delay_hours(self, tmp_state):
        payload = {"ticket_key": "T-1", "slug": "s", "branch": "b"}
        trigger = {"delay_hours": [1, 2], "quiet_hours": [23, 7], "timezone": "US/Pacific"}
        config = {}

        with patch("actions.schedule_pr.scheduler.compute_delay_time", return_value=datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)) as mock_delay, \
             patch("actions.schedule_pr.scheduler.schedule") as mock_sched:
            schedule_pr.handle(payload, trigger, config)
        mock_delay.assert_called_once()
        mock_sched.assert_called_once()
        ts = state.load("tickets")
        assert "pr_scheduled_at" in ts.get("T-1", {})

    def test_with_estimate(self, tmp_state):
        payload = {"ticket_key": "T-2", "estimate_seconds": 28800, "discovered_at": "2026-04-15T10:00:00+00:00", "slug": "s", "branch": "b"}
        trigger = {"jitter_hours": 3, "work_hours": [9, 17]}
        config = {}

        with patch("actions.schedule_pr.scheduler.compute_target_time", return_value=datetime(2026, 4, 16, 14, 0, tzinfo=timezone.utc)) as mock_target, \
             patch("actions.schedule_pr.scheduler.schedule") as mock_sched:
            schedule_pr.handle(payload, trigger, config)
        mock_target.assert_called_once()
        mock_sched.assert_called_once()

    def test_missing_estimate_skips(self, tmp_state):
        payload = {"ticket_key": "T-3"}
        trigger = {}
        config = {}

        with patch("actions.schedule_pr.log") as mock_log:
            schedule_pr.handle(payload, trigger, config)
        mock_log.emit.assert_called_once()
        assert "skip" in mock_log.emit.call_args[1].get("meta", {}).get("ticket", "") or \
               "skip" in mock_log.emit.call_args[0][1].lower() or \
               mock_log.emit.call_args[0][0] == "schedule_pr_skipped"
