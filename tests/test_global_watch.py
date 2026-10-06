from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import core.discovery as discovery
import core.scheduler as scheduler
import core.state as state
from services import global_watch

NOW = datetime(2026, 10, 6, 18, 0, tzinfo=timezone.utc)
SINCE = (NOW - timedelta(minutes=15)).isoformat()
STALE = (NOW - timedelta(hours=2)).isoformat()
RECENT = (NOW - timedelta(minutes=5)).isoformat()
OLD = (NOW - timedelta(minutes=60)).isoformat()


def _ev(key, ev_id, event="job_started", ts=RECENT):
    return {"id": ev_id, "event": event, "ts": ts, "instance_key": key}


def test_discovery_reads_peers_when_only_own_config_is_mounted(tmp_path, monkeypatch):
    (tmp_path / "personal.toml").write_text('[job]\nkey = "personal"\nport = 7110\n')
    (tmp_path / "peers.toml").write_text(
        '[[peers]]\nkey = "frshty"\nbase_url = "http://127.0.0.1:7131/"\n'
        '[[peers]]\nkey = "personal"\nbase_url = "http://127.0.0.1:7135"\n'
        '[[peers]]\nkey = "aimyable"\nbase_url = "http://127.0.0.1:7132"\n')
    monkeypatch.setattr(discovery, "CONFIG_DIR", tmp_path)

    found = {i["key"]: i["base_url"] for i in discovery.discover_instances()}

    assert found == {"personal": "http://localhost:7110",
                     "frshty": "http://127.0.0.1:7131",
                     "aimyable": "http://127.0.0.1:7132"}


def test_discovery_without_peers_finds_only_own_config(tmp_path, monkeypatch):
    (tmp_path / "personal.toml").write_text('[job]\nkey = "personal"\nport = 7110\n')
    monkeypatch.setattr(discovery, "CONFIG_DIR", tmp_path)

    assert [i["key"] for i in discovery.discover_instances()] == ["personal"]


def test_every_minutes_cadence_advances_past_now():
    prev = NOW - timedelta(minutes=50)

    nxt = scheduler._advance_recurring("every_15m", prev, NOW)

    assert nxt == NOW + timedelta(minutes=10)


def test_every_minutes_cadence_steps_once_when_on_time():
    prev = NOW - timedelta(seconds=30)

    assert scheduler._advance_recurring("every_15m", prev, NOW) == prev + timedelta(minutes=15)


def test_evaluate_healthy_feed_has_no_findings():
    events = [_ev("personal", "a"), _ev("frshty", "b")]

    assert global_watch.evaluate(events, {}, ["frshty", "personal"], SINCE, STALE) == []


def test_evaluate_flags_a_feed_that_shows_only_one_instance():
    events = [_ev("personal", "a")]

    findings = global_watch.evaluate(events, {}, ["aimyable", "frshty", "personal"], SINCE, STALE)

    assert [(f["kind"], f["instance"]) for f in findings] == [
        ("silent", "aimyable"), ("silent", "frshty")]


def test_evaluate_flags_unreachable_relabelled_and_error_events():
    events = [_ev("aimyable", "x"), _ev("bh", "x"),
              _ev("frshty", "y", event="ticket_check_error"),
              _ev("frshty", "z", event="pr_base_sync_failed", ts=OLD)]

    findings = global_watch.evaluate(events, {"quill": "timed out"},
                                     ["aimyable", "bh", "frshty", "quill"], SINCE, STALE)

    assert [(f["kind"], f["instance"]) for f in findings] == [
        ("unreachable", "quill"), ("relabelled", "bh"), ("error_events", "frshty")]
    assert findings[2]["detail"] == "ticket_check_error x1"


def _config():
    return {"job": {"key": "personal"}, "_base_url": "https://personal.frshty.localhost",
            "global_watch": {"enabled": True}}


def _response(events, errors=None):
    return events, errors or {}


def test_run_alerts_once_per_finding_set_and_reports_recovery(tmp_path):
    state.init(tmp_path)
    instances = [{"key": "personal", "base_url": "http://127.0.0.1:7135"},
                 {"key": "frshty", "base_url": "http://127.0.0.1:7131"}]
    broken = _response([_ev("personal", "a")])
    healthy = _response([_ev("personal", "a"), _ev("frshty", "b")])
    with patch.object(global_watch, "discover_instances", return_value=instances), \
         patch.object(global_watch, "read_feed", side_effect=[broken, broken, healthy]) as feed, \
         patch.object(global_watch.log, "emit") as emit:
        first = global_watch.run(_config(), now=NOW)
        global_watch.run(_config(), now=NOW + timedelta(minutes=15))
        last = global_watch.run(_config(), now=NOW + timedelta(minutes=30))

    assert feed.call_args_list[0].args == (2, NOW)
    assert [f["kind"] for f in first["findings"]] == ["silent"]
    assert last["findings"] == []
    assert [c.args[0] for c in emit.call_args_list] == ["global_watch_alert", "global_watch_ok"]


def test_read_feed_keeps_every_instance_past_the_merged_cut():
    busy = [_ev("atropos", str(i)) for i in range(global_watch.FETCH_LIMIT - 1)]
    busy.append(_ev("atropos", "old", ts=OLD))
    quiet = [_ev("personal", "p", event="preflight_fail")]

    async def _remote(**_kwargs):
        return busy, {}

    with patch.object(global_watch, "_fetch_local_global_events", return_value=quiet), \
         patch.object(global_watch, "_fetch_remote_global_events", side_effect=_remote):
        events, errors = global_watch.read_feed(2, NOW)

    assert len(events) == global_watch.FETCH_LIMIT + 1
    findings = global_watch.evaluate(events, errors, ["atropos", "personal"], SINCE, STALE)
    assert [(f["kind"], f["instance"]) for f in findings] == [("error_events", "personal")]


def test_run_reads_back_to_the_last_run_when_it_is_older_than_the_stale_window(tmp_path):
    state.init(tmp_path)
    state.save("global_watch", {"last_run_at": (NOW - timedelta(minutes=180)).isoformat(),
                                "fingerprint": []})
    events = [_ev("personal", "a"),
              _ev("personal", "b", event="ticket_check_error",
                  ts=(NOW - timedelta(minutes=150)).isoformat())]
    with patch.object(global_watch, "discover_instances",
                      return_value=[{"key": "personal", "base_url": "x"}]), \
         patch.object(global_watch, "read_feed", return_value=(events, {})) as feed, \
         patch.object(global_watch.log, "emit"):
        out = global_watch.run(_config(), now=NOW)

    assert feed.call_args.args == (3, NOW)
    assert [(f["kind"], f["detail"]) for f in out["findings"]] == [
        ("error_events", "ticket_check_error x1")]


def test_evaluate_flags_an_instance_whose_page_starts_after_the_last_run():
    events = [_ev("aimyable", str(i)) for i in range(global_watch.FETCH_LIMIT)]

    findings = global_watch.evaluate(events, {}, ["aimyable"], SINCE, STALE)

    assert [(f["kind"], f["instance"]) for f in findings] == [("truncated", "aimyable")]


def test_evaluate_accepts_a_full_page_that_reaches_back_to_the_last_run():
    events = [_ev("aimyable", str(i)) for i in range(global_watch.FETCH_LIMIT - 1)]
    events.append(_ev("aimyable", "old", ts=OLD))

    assert global_watch.evaluate(events, {}, ["aimyable"], SINCE, STALE) == []
