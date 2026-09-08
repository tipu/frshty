"""The Slack capture watch in core.runtime.

A Slack request used to wait for two poll cycles that had nothing to do with
it: one before the message reached the conversation index, one after its
settle window ran out and before a scan noticed. At the default six-minute
cron cadence that is up to twelve minutes on top of the window itself. The
watch removes both by reading the capture file's own size and modification
time and raising slack_tick at the two moments a scan is worth running.
"""
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import patch
from zoneinfo import ZoneInfo

from core import runtime
from core.tasks import routes


PT = ZoneInfo("America/Los_Angeles")
ACTIVE = datetime(2026, 5, 21, 12, 0, tzinfo=PT)
QUIET = datetime(2026, 5, 21, 23, 0, tzinfo=PT)
SETTLE = 300


def _config(slack=True, messages_dir="/nowhere", **job):
    return {"features": {"slack": slack}, "job": job,
            "slack": {"messages_dir": messages_dir}}


class TestSlackPulse:
    def test_a_mark_never_seen_before_scans_and_opens_a_window(self):
        pulse, windows = runtime._slack_pulse((10, 1), None, [], SETTLE, 1000.0)
        assert pulse is True
        assert windows == [1000.0 + SETTLE + runtime._SLACK_SETTLE_MARGIN]

    def test_a_capture_that_grew_scans_and_opens_another_window(self):
        pulse, windows = runtime._slack_pulse((20, 2), (10, 1), [1200.0],
                                              SETTLE, 1100.0)
        assert pulse is True, "the new message belongs in the index now"
        assert windows == [1200.0, 1100.0 + SETTLE + runtime._SLACK_SETTLE_MARGIN]

    def test_a_later_write_does_not_postpone_an_open_window(self):
        """The defect this list exists for. A request lands at 1000 and stops
        moving. A bot posts at 1285, in a conversation of its own. With one
        window per instance the request's deadline moved from 1305 to 1590 and
        the request waited for the cron backstop instead."""
        _, windows = runtime._slack_pulse((10, 1), None, [], SETTLE, 1000.0)
        _, windows = runtime._slack_pulse((20, 2), (10, 1), windows, SETTLE, 1285.0)
        pulse, windows = runtime._slack_pulse((20, 2), (20, 2), windows,
                                              SETTLE, 1305.0)
        assert pulse is True, "the request's own window has run out"
        assert windows == [1590.0], "and the bot's window is still open"

    def test_a_rotated_capture_counts_as_growth(self):
        """slack_int rotates filtered.jsonl and starts a smaller file. The
        mark drops rather than rises, and a watch that only looked for growth
        would then never scan again."""
        pulse, _ = runtime._slack_pulse((4, 9), (900, 8), [], SETTLE, 1000.0)
        assert pulse is True

    def test_an_unchanged_capture_inside_the_window_does_not_scan(self):
        pulse, windows = runtime._slack_pulse((10, 1), (10, 1), [1200.0],
                                              SETTLE, 1100.0)
        assert pulse is False
        assert windows == [1200.0]

    def test_an_unchanged_capture_scans_once_when_a_window_runs_out(self):
        pulse, windows = runtime._slack_pulse((10, 1), (10, 1), [1200.0],
                                              SETTLE, 1200.0)
        assert pulse is True, "the conversation is judgeable from now on"
        assert windows == [], "and the window is spent"

    def test_windows_that_ran_out_together_cost_one_scan(self):
        pulse, windows = runtime._slack_pulse((10, 1), (10, 1),
                                              [1100.0, 1150.0, 1900.0],
                                              SETTLE, 1200.0)
        assert pulse is True
        assert windows == [1900.0]

    def test_no_open_window_does_not_scan_again(self):
        pulse, windows = runtime._slack_pulse((10, 1), (10, 1), [], SETTLE, 9999.0)
        assert pulse is False
        assert windows == []

    def test_the_list_of_open_windows_bounds_itself(self):
        """A wake appends at most one deadline and a deadline lives at most one
        settle window, so a capture written to on every wake holds one entry
        per wake of that window and no more. Nothing is dropped to keep the
        list short: with a long settle window the newest deadline is the one
        the request needs, and a cap would drop exactly that one."""
        windows: list[float] = []
        seen = None
        for step in range(200):
            now = 1000.0 + step * runtime._MIN_TICK_FLOOR
            mark = (step, step)
            _, windows = runtime._slack_pulse(mark, seen, windows, SETTLE, now)
            seen = mark
        limit = (SETTLE + runtime._SLACK_SETTLE_MARGIN) / runtime._MIN_TICK_FLOOR + 1
        assert len(windows) <= limit, len(windows)

    def test_the_newest_window_is_never_dropped(self):
        """The request is in the newest message. Its window is the one nothing
        recreates once the workspace goes quiet, so a crowded list may not
        drop it to stay short."""
        crowded = [9000.0 + n for n in range(200)]
        _, windows = runtime._slack_pulse((10, 1), (9, 1), crowded, SETTLE, 1000.0)
        assert 1000.0 + SETTLE + runtime._SLACK_SETTLE_MARGIN in windows
        assert len(windows) == len(crowded) + 1


class TestWatchedCapture:
    def test_a_filtered_capture_is_watched(self, tmp_path):
        (tmp_path / "filtered.jsonl").write_text("{}\n")
        config = _config(messages_dir=str(tmp_path))
        assert runtime._watched_capture(config) == str(tmp_path / "filtered.jsonl")

    def test_a_raw_capture_is_not_watched(self, tmp_path):
        """slack_int's raw log takes pings, typing and presence too, so its
        size and time move whether or not anybody said anything. A watch on it
        would scan on every wake and would never let the settle window run
        out."""
        (tmp_path / "messages.jsonl").write_text("{}\n")
        config = _config(messages_dir=str(tmp_path))
        assert runtime._watched_capture(config) == ""

    def test_no_capture_configured_is_not_watched(self):
        assert runtime._watched_capture({"slack": {}}) == ""


class TestCaptureMark:
    def test_a_missing_capture_marks_as_zero(self, tmp_path):
        assert runtime._capture_mark(str(tmp_path / "gone.jsonl")) == (0, 0)

    def test_the_mark_moves_when_the_capture_grows(self, tmp_path):
        capture = tmp_path / "filtered.jsonl"
        capture.write_text("{}\n")
        before = runtime._capture_mark(str(capture))
        capture.write_text("{}\n{}\n")
        after = runtime._capture_mark(str(capture))
        assert before != (0, 0)
        assert after != before


class TestTickerSleep:
    def _sleep(self, key, config):
        instances = SimpleNamespace(
            keys=lambda: [key],
            get=lambda k: SimpleNamespace(config=config))
        with patch.object(runtime, "_instances", instances):
            return runtime._ticker_sleep_seconds()

    def test_a_watched_instance_pulls_the_sleep_down(self, tmp_path):
        (tmp_path / "filtered.jsonl").write_text("{}\n")
        assert self._sleep("watched", _config(messages_dir=str(tmp_path))) == \
            runtime._SLACK_WATCH_INTERVAL

    def test_an_instance_without_slack_keeps_its_tick_interval(self, tmp_path):
        """The oracle for the test above: same capture, feature off, and the
        sleep stays at the cron cadence."""
        (tmp_path / "filtered.jsonl").write_text("{}\n")
        assert self._sleep("plain", _config(slack=False, messages_dir=str(tmp_path))) == \
            runtime._DEFAULT_TICK_INTERVAL

    def test_an_instance_with_only_a_raw_capture_keeps_its_tick_interval(self, tmp_path):
        (tmp_path / "messages.jsonl").write_text("{}\n")
        assert self._sleep("raw", _config(messages_dir=str(tmp_path))) == \
            runtime._DEFAULT_TICK_INTERVAL


class TestEmitSlackTick:
    def _emit(self, config, marks, pending, now_ts, now_local,
              capture="filtered.jsonl"):
        with patch.object(runtime.q, "emit_event") as emit, \
             patch.object(runtime, "_capture_mark", return_value=(7, 7)), \
             patch.object(runtime, "_slack_conversations",
                          return_value=SimpleNamespace(
                              settle_seconds=lambda cfg: SETTLE,
                              capture_path=lambda cfg: capture)):
            runtime._emit_slack_tick("alpha", config, marks, pending, now_ts,
                                     now_local)
        return emit

    def test_a_new_capture_raises_slack_tick(self):
        marks, pending = {}, {}
        emit = self._emit(_config(), marks, pending, 1000.0, ACTIVE)
        assert emit.call_count == 1
        assert emit.call_args.kwargs["kind"] == "slack_tick"
        assert emit.call_args.kwargs["instance_key"] == "alpha"
        assert marks["alpha"] == (7, 7)
        assert pending["alpha"] == [1000.0 + SETTLE + runtime._SLACK_SETTLE_MARGIN]

    def test_an_unchanged_capture_raises_nothing(self):
        marks, pending = {"alpha": (7, 7)}, {"alpha": [9999.0]}
        emit = self._emit(_config(), marks, pending, 1000.0, ACTIVE)
        assert emit.call_count == 0

    def test_the_window_running_out_raises_slack_tick_and_clears(self):
        marks, pending = {"alpha": (7, 7)}, {"alpha": [1000.0]}
        emit = self._emit(_config(), marks, pending, 1000.0, ACTIVE)
        assert emit.call_count == 1
        assert "alpha" not in pending

    def test_quiet_hours_raise_nothing(self):
        """At night the cron cadence drops to quiet_cadence on purpose. A fast
        path that ignored that would open tasks at 3am that frshty did not
        open before."""
        emit = self._emit(_config(quiet_hours=[20, 7]), {}, {}, 1000.0, QUIET)
        assert emit.call_count == 0

    def test_an_instance_without_slack_raises_nothing(self):
        emit = self._emit(_config(slack=False), {}, {}, 1000.0, ACTIVE)
        assert emit.call_count == 0

    def test_an_instance_with_only_a_raw_capture_raises_nothing(self):
        emit = self._emit(_config(), {}, {}, 1000.0, ACTIVE,
                          capture="messages.jsonl")
        assert emit.call_count == 0


class TestTickerLoop:
    """One pass of the ticker thread raises both events, so the watch is
    actually wired to the thread and not only to its helper."""

    class _StopAfterOnePass:
        def __init__(self):
            self.passes = 0

        def is_set(self):
            self.passes += 1
            return self.passes > 1

        def wait(self, _seconds):
            return True

    def test_one_pass_raises_cron_tick_and_slack_tick(self, tmp_path):
        (tmp_path / "filtered.jsonl").write_text("{}\n")
        config = _config(messages_dir=str(tmp_path), quiet_hours=[])
        instances = SimpleNamespace(
            keys=lambda: ["alpha"],
            get=lambda k: SimpleNamespace(config=config))
        with patch.object(runtime, "_instances", instances), \
             patch.object(runtime, "_cron_stop", self._StopAfterOnePass()), \
             patch.object(runtime, "_slack_conversations",
                          return_value=SimpleNamespace(
                              settle_seconds=lambda cfg: SETTLE,
                              capture_path=lambda cfg: str(tmp_path / "filtered.jsonl"))), \
             patch.object(runtime.q, "emit_event") as emit:
            runtime._cron_ticker(interval=240)

        kinds = [c.kwargs["kind"] for c in emit.call_args_list]
        assert kinds == ["cron_tick", "slack_tick"], kinds


class TestSlackTickRoute:
    def _registries(self, slack=True):
        return {"alpha": SimpleNamespace(config=_config(slack=slack))}

    def test_slack_tick_enqueues_the_conversation_scan(self):
        jobs = routes._slack_routes({"instance_key": "alpha"}, self._registries())
        assert jobs == [{"instance_key": "alpha", "task": "slack_conversation_scan"}]

    def test_slack_tick_for_an_instance_without_slack_enqueues_nothing(self):
        jobs = routes._slack_routes({"instance_key": "alpha"},
                                    self._registries(slack=False))
        assert jobs == []

    def test_slack_tick_for_an_unknown_instance_enqueues_nothing(self):
        assert routes._slack_routes({"instance_key": "ghost"}, self._registries()) == []

    def test_a_slack_tick_event_reaches_the_job_queue(self, fresh_db):
        """The route above is only reachable if the dispatcher knows the kind.
        This drives the real dispatcher over a real event row."""
        import core.db as db
        import core.queue as q
        from core.event_bus import Dispatcher
        import core.tasks  # noqa: F401

        q.emit_event(source="slack_watch", kind="slack_tick", payload={},
                     instance_key="alpha")
        Dispatcher(self._registries())._drain()

        rows = db.query_all("SELECT task FROM jobs WHERE instance_key = 'alpha'")
        assert [r["task"] for r in rows] == ["slack_conversation_scan"]
