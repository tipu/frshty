import random
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

import pytest

import core.state as state
import core.scheduler as scheduler


class TestSchedule:
    def test_saves_to_state(self, tmp_state):
        log_mock = MagicMock()
        with patch("core.scheduler.log", log_mock):
            run_at = datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)
            scheduler.schedule("PROJ-1", "create_pr", run_at, meta={"slug": "s"})

        pending = state.load("scheduler")
        assert "PROJ-1" in pending
        assert pending["PROJ-1"]["action"] == "create_pr"
        assert pending["PROJ-1"]["meta"] == {"slug": "s"}


class TestCheckDue:
    def test_executes_due_task(self, tmp_state):
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        state.save("scheduler", {
            "T-1": {"action": "create_pr", "run_at": past.isoformat(), "meta": {}},
        })
        with patch("core.scheduler._execute") as mock_exec, \
             patch("core.scheduler.log"):
            scheduler.check_due({})
            mock_exec.assert_called_once_with("create_pr", "T-1", {}, {})

        assert state.load("scheduler") == {}

    def test_skips_future_task(self, tmp_state):
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        state.save("scheduler", {
            "T-1": {"action": "create_pr", "run_at": future.isoformat(), "meta": {}},
        })
        with patch("core.scheduler._execute") as mock_exec, \
             patch("core.scheduler.log"):
            scheduler.check_due({})
            mock_exec.assert_not_called()

        assert "T-1" in state.load("scheduler")

    def test_empty_scheduler(self, tmp_state):
        scheduler.check_due({})


class TestComputeTargetTime:
    def test_half_day_minimum(self):
        start = datetime(2026, 4, 15, 9, 0, tzinfo=timezone.utc)  # Wednesday
        random.seed(0)
        result = scheduler.compute_target_time(start, 100, jitter_hours=0)
        assert result.weekday() < 5
        assert result >= start

    def test_one_biz_day(self):
        start = datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc)  # Monday
        random.seed(42)
        result = scheduler.compute_target_time(start, 28800, jitter_hours=0)
        assert result.weekday() < 5

    def test_skips_weekends(self):
        start = datetime(2026, 4, 17, 9, 0, tzinfo=timezone.utc)  # Friday
        random.seed(0)
        result = scheduler.compute_target_time(start, 28800 * 2, jitter_hours=0)
        assert result.weekday() < 5

    def test_clamps_to_work_hours(self):
        start = datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc)
        random.seed(0)
        result = scheduler.compute_target_time(start, 28800, jitter_hours=0, work_hours=[9, 17])
        assert 9 <= result.hour < 17

    def test_result_not_on_weekend(self):
        start = datetime(2026, 4, 14, 9, 0, tzinfo=timezone.utc)
        for seed in range(20):
            random.seed(seed)
            result = scheduler.compute_target_time(start, 28800, jitter_hours=3)
            assert result.weekday() < 5, f"seed {seed} produced weekend: {result}"


class TestComputeDelayTime:
    def test_basic_delay(self):
        start = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
        random.seed(0)
        result = scheduler.compute_delay_time(start, [1, 2])
        assert result > start
        assert result.tzinfo is not None

    def test_quiet_hours_shift(self):
        start = datetime(2026, 4, 15, 20, 0, tzinfo=timezone.utc)
        random.seed(0)
        result = scheduler.compute_delay_time(start, [4, 5], quiet_hours=[23, 7], tz_name="US/Pacific")
        tz = ZoneInfo("US/Pacific")
        local = result.astimezone(tz)
        assert local.hour >= 7 or local.hour < 23

    def test_returns_utc(self):
        start = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
        random.seed(0)
        result = scheduler.compute_delay_time(start, [1, 2])
        assert result.tzinfo == timezone.utc
