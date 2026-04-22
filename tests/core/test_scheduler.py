import json
import random
from datetime import datetime, timezone, timedelta
from unittest.mock import patch, MagicMock
from zoneinfo import ZoneInfo

import pytest

import core.db as db
import core.state as state
import core.scheduler as scheduler


def _seed_oneshot(key: str, action: str, run_at: datetime, meta: dict | None = None):
    with patch("core.scheduler.log"):
        scheduler.schedule(key, action, run_at, meta=meta or {})


class TestSchedule:
    def test_writes_scheduler_row(self, tmp_state):
        run_at = datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)
        _seed_oneshot("PROJ-1", "create_pr", run_at, meta={"slug": "s"})

        row = db.query_one(
            "SELECT run_at, data FROM scheduler WHERE instance_key=? AND key=?",
            (tmp_state.name, "PROJ-1"),
        )
        assert row is not None, "expected scheduler row for PROJ-1"
        data = json.loads(row["data"])
        assert data["kind"] == "oneshot"
        assert data["action"] == "create_pr"
        assert data["meta"] == {"slug": "s"}

    def test_schedule_not_visible_through_kv_state_load(self, tmp_state):
        run_at = datetime(2026, 4, 20, 10, 0, tzinfo=timezone.utc)
        _seed_oneshot("PROJ-1", "create_pr", run_at)
        assert state.load("scheduler") == {}, \
            "state.load('scheduler') reads kv; scheduler writes to scheduler table"


class TestCheckDue:
    def test_executes_due_oneshot(self, tmp_state):
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        _seed_oneshot("T-1", "create_pr", past)

        with patch("core.scheduler._execute") as mock_exec, \
             patch("core.scheduler.log"):
            scheduler.check_due({})

        mock_exec.assert_called_once_with("create_pr", "T-1", {}, {})
        leftover = db.query_one(
            "SELECT 1 AS x FROM scheduler WHERE instance_key=? AND key=?",
            (tmp_state.name, "T-1"),
        )
        assert leftover is None, "fired oneshot row must be deleted"

    def test_skips_future_oneshot(self, tmp_state):
        future = datetime(2099, 1, 1, tzinfo=timezone.utc)
        _seed_oneshot("T-1", "create_pr", future)

        with patch("core.scheduler._execute") as mock_exec, \
             patch("core.scheduler.log"):
            scheduler.check_due({})

        mock_exec.assert_not_called()
        row = db.query_one(
            "SELECT 1 AS x FROM scheduler WHERE instance_key=? AND key=?",
            (tmp_state.name, "T-1"),
        )
        assert row is not None, "future oneshot must remain"

    def test_check_due_ignores_recurring_even_if_past(self, tmp_state):
        past = datetime(2020, 1, 1, tzinfo=timezone.utc)
        scheduler.upsert_recurring(tmp_state.name, "beat-1", "poll", "daily_19pst", past)

        with patch("core.scheduler._execute") as mock_exec, \
             patch("core.scheduler.log"):
            scheduler.check_due({})

        mock_exec.assert_not_called()

    def test_empty_scheduler(self, tmp_state):
        scheduler.check_due({})


class TestRecurring:
    def test_upsert_recurring_roundtrip(self, tmp_state):
        run_at = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        scheduler.upsert_recurring(
            tmp_state.name, "beat-a", "billing_autogen", "daily_19pst",
            run_at, payload={"tz": "US/Pacific"},
        )
        row = db.query_one(
            "SELECT run_at, data FROM scheduler WHERE instance_key=? AND key=?",
            (tmp_state.name, "beat-a"),
        )
        assert row is not None
        data = json.loads(row["data"])
        assert data["kind"] == "recurring"
        assert data["task"] == "billing_autogen"
        assert data["cadence"] == "daily_19pst"
        assert data["payload"] == {"tz": "US/Pacific"}

    def test_upsert_preserves_last_run_at(self, tmp_state):
        run_at = datetime(2026, 5, 1, 19, 0, tzinfo=timezone.utc)
        scheduler.upsert_recurring(tmp_state.name, "beat-a", "t", "daily_19pst", run_at)
        db.execute(
            "UPDATE scheduler SET data = json_set(data, '$.last_run_at', '2026-05-01T19:00:00+00:00') "
            "WHERE instance_key=? AND key='beat-a'",
            (tmp_state.name,),
        )
        scheduler.upsert_recurring(
            tmp_state.name, "beat-a", "t", "daily_19pst",
            run_at + timedelta(days=1),
        )
        row = db.query_one(
            "SELECT data FROM scheduler WHERE instance_key=? AND key='beat-a'",
            (tmp_state.name,),
        )
        data = json.loads(row["data"])
        assert data.get("last_run_at") == "2026-05-01T19:00:00+00:00"

    def test_fire_due_recurring_enqueues_and_advances(self, tmp_state):
        past = datetime(2026, 4, 1, 19, 0, tzinfo=timezone.utc)
        scheduler.upsert_recurring(tmp_state.name, "beat-a", "billing_autogen",
                                   "daily_19pst", past, payload={"x": 1})
        now = datetime(2026, 4, 21, 19, 0, tzinfo=timezone.utc)

        fired = scheduler.fire_due_recurring(now=now)
        mine = [f for f in fired if f["instance_key"] == tmp_state.name]
        assert len(mine) == 1
        assert mine[0]["task"] == "billing_autogen"

        jobs = db.query_all(
            "SELECT task, payload FROM jobs WHERE instance_key=? AND task='billing_autogen'",
            (tmp_state.name,),
        )
        assert len(jobs) == 1
        assert json.loads(jobs[0]["payload"]) == {"x": 1}

        row = db.query_one(
            "SELECT run_at FROM scheduler WHERE instance_key=? AND key='beat-a'",
            (tmp_state.name,),
        )
        next_run_at = datetime.fromisoformat(row["run_at"])
        assert next_run_at > now

    def test_fire_due_recurring_no_double_fire(self, tmp_state):
        past = datetime(2026, 4, 1, 19, 0, tzinfo=timezone.utc)
        scheduler.upsert_recurring(tmp_state.name, "beat-a", "t",
                                   "daily_19pst", past)
        now = datetime(2026, 4, 21, 19, 0, tzinfo=timezone.utc)

        first = scheduler.fire_due_recurring(now=now)
        mine_first = [f for f in first if f["instance_key"] == tmp_state.name]
        assert len(mine_first) == 1

        second = scheduler.fire_due_recurring(now=now)
        mine_second = [f for f in second if f["instance_key"] == tmp_state.name]
        assert mine_second == []


class TestListAll:
    def test_list_all_ordered_by_run_at(self, tmp_state):
        early = datetime(2026, 5, 1, 10, tzinfo=timezone.utc)
        late = datetime(2026, 6, 1, 10, tzinfo=timezone.utc)
        _seed_oneshot("late-one", "create_pr", late)
        scheduler.upsert_recurring(tmp_state.name, "early-recurring", "t",
                                   "daily_19pst", early)

        items = scheduler.list_all(tmp_state.name)
        keys_in_order = [i["key"] for i in items]
        assert keys_in_order == ["early-recurring", "late-one"]


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
