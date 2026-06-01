from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from core import runtime


PT = ZoneInfo("America/Los_Angeles")


class TestInQuietHours:
    def test_overnight_window_includes_late_evening(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 22, 30, tzinfo=PT), (20, 7)) is True

    def test_overnight_window_includes_early_morning(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 3, 0, tzinfo=PT), (20, 7)) is True

    def test_overnight_window_excludes_midday(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 12, 0, tzinfo=PT), (20, 7)) is False

    def test_overnight_window_boundary_start_is_quiet(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 20, 0, tzinfo=PT), (20, 7)) is True

    def test_overnight_window_boundary_end_is_active(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 7, 0, tzinfo=PT), (20, 7)) is False

    def test_non_wrapping_window(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 1, 0, tzinfo=PT), (1, 5)) is True
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 5, 0, tzinfo=PT), (1, 5)) is False
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 10, 0, tzinfo=PT), (1, 5)) is False

    def test_equal_start_end_disables(self):
        assert runtime._in_quiet_hours(datetime(2026, 5, 21, 12, 0, tzinfo=PT), (3, 3)) is False


class TestQuietConfigParsing:
    def test_default_quiet_hours_when_unset(self):
        assert runtime._quiet_hours_for({"job": {}}) == (20, 7)

    def test_empty_list_disables(self):
        assert runtime._quiet_hours_for({"job": {"quiet_hours": []}}) is None

    def test_explicit_override(self):
        assert runtime._quiet_hours_for({"job": {"quiet_hours": [22, 6]}}) == (22, 6)

    def test_default_cadence(self):
        assert runtime._quiet_cadence_for({"job": {}}) == 1800

    def test_explicit_cadence(self):
        assert runtime._quiet_cadence_for({"job": {"quiet_cadence": 600}}) == 600

    def test_no_job_section_uses_defaults(self):
        assert runtime._quiet_hours_for({}) == (20, 7)
        assert runtime._quiet_cadence_for({}) == 1800


class TestShouldEmitCron:
    def test_emits_during_active_hours_regardless_of_last_emit(self):
        # Active hours: emit once the per-workspace tick_interval has elapsed
        # (default 360s). Not "always" anymore — the interval governs cadence.
        cfg = {"job": {"quiet_hours": [20, 7]}}
        active = datetime(2026, 5, 21, 12, 0, tzinfo=PT)
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=400.0, now_local=active) is True
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=100.0, now_local=active) is False

    def test_active_hours_respects_custom_tick_interval(self):
        # A workspace with tick_interval=30 emits every ~30s in active hours.
        cfg = {"job": {"quiet_hours": [20, 7], "tick_interval": 30}}
        active = datetime(2026, 5, 21, 12, 0, tzinfo=PT)
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=31.0, now_local=active) is True
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=20.0, now_local=active) is False

    def test_emits_in_quiet_hours_when_cadence_elapsed(self):
        cfg = {"job": {"quiet_hours": [20, 7], "quiet_cadence": 1800}}
        quiet = datetime(2026, 5, 21, 23, 0, tzinfo=PT)
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=2000.0, now_local=quiet) is True

    def test_skips_in_quiet_hours_within_cadence(self):
        cfg = {"job": {"quiet_hours": [20, 7], "quiet_cadence": 1800}}
        quiet = datetime(2026, 5, 21, 23, 0, tzinfo=PT)
        assert runtime._should_emit_cron(cfg, last_emit_ts=1000.0, now_ts=2000.0, now_local=quiet) is False

    def test_emits_when_quiet_hours_disabled_via_empty_list(self):
        # quiet_hours=[] disables the night throttle, so the active-hours
        # tick_interval (default 360s) governs even at 23:00.
        cfg = {"job": {"quiet_hours": []}}
        quiet = datetime(2026, 5, 21, 23, 0, tzinfo=PT)
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=400.0, now_local=quiet) is True
        assert runtime._should_emit_cron(cfg, last_emit_ts=0.0, now_ts=100.0, now_local=quiet) is False
