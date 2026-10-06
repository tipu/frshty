"""Check that the /global feed still covers every instance.

The feed merges this instance's events with the events of every instance that
core/discovery.py finds. Nothing errors when discovery finds nobody: the page
just shows one project. A container that mounts only its own config did
exactly that. This check calls the fetchers the page's handler calls, in this
process, and reports what is wrong with their answer. It reads them before the
handler's merged cut, so one busy instance cannot push another out of view.

A finding is one of:

  unreachable   the feed carries an error for an instance
  silent        a discovered instance has no event inside the stale window
  relabelled    two instances return the same event ids, so one host answered
                for the other
  error_events  an instance logged error or failure events since the last run
  truncated     an instance returned a full page that starts after the last
                run, so its older events, errors included, went unread

An alert event goes to the feed when the set of findings changes, and a
recovery event when it clears, so a fault that persists does not repeat every
run."""
import asyncio
import math
from datetime import datetime, timedelta, timezone

import core.log as log
import core.state as state
from core.discovery import discover_instances
from web.observability import _fetch_local_global_events, _fetch_remote_global_events

DEFAULT_INTERVAL_MINUTES = 15
DEFAULT_STALE_HOURS = 2
FETCH_LIMIT = 5000
ERROR_MARKERS = ("error", "fail", "crash")
_STATE_MODULE = "global_watch"


def settings(config: dict) -> dict:
    return (config or {}).get("global_watch") or {}


def _is_error_event(name: str) -> bool:
    return any(marker in name for marker in ERROR_MARKERS)


def read_feed(window_hours: int, now: datetime) -> tuple[list[dict], dict]:
    after = (now - timedelta(hours=window_hours)).isoformat()
    local = _fetch_local_global_events(limit=FETCH_LIMIT, unread_only=False, after=after)
    remote, errors = asyncio.run(_fetch_remote_global_events(
        limit=FETCH_LIMIT, unread_only=False, since_hours=window_hours))
    return local + remote, errors


def evaluate(events: list[dict], errors: dict, expected: list[str],
             since: str, stale_since: str) -> list[dict]:
    findings: list[dict] = []
    for key, err in sorted(errors.items()):
        findings.append({"kind": "unreachable", "instance": key, "detail": err})
    by_instance: dict[str, list[dict]] = {}
    for ev in events:
        by_instance.setdefault(ev.get("instance_key") or "", []).append(ev)
    for key in sorted(expected):
        fresh = [ev for ev in by_instance.get(key, []) if (ev.get("ts") or "") > stale_since]
        if key not in errors and not fresh:
            findings.append({"kind": "silent", "instance": key,
                             "detail": "no event in the stale window"})
    id_sets = {key: frozenset(str(ev.get("id")) for ev in rows)
               for key, rows in by_instance.items() if rows}
    keys = sorted(id_sets)
    for i, a in enumerate(keys):
        for b in keys[i + 1:]:
            if id_sets[a] == id_sets[b]:
                findings.append({"kind": "relabelled", "instance": b,
                                 "detail": f"returns the same {len(id_sets[a])} event ids as {a}"})
    for key in sorted(by_instance):
        rows = by_instance[key]
        oldest = min((ev.get("ts") or "") for ev in rows)
        if len(rows) >= FETCH_LIMIT and oldest > since:
            findings.append({"kind": "truncated", "instance": key,
                             "detail": f"{len(rows)} events since {oldest}; events since {since} went unread"})
        counts: dict[str, int] = {}
        for ev in by_instance[key]:
            name = ev.get("event") or ""
            if (ev.get("ts") or "") > since and _is_error_event(name):
                counts[name] = counts.get(name, 0) + 1
        for name, n in sorted(counts.items()):
            findings.append({"kind": "error_events", "instance": key,
                             "detail": f"{name} x{n}", "event": name})
    return findings


def _fingerprint(findings: list[dict]) -> list[str]:
    return sorted(f"{f['kind']}:{f['instance']}:{f.get('event', '')}" for f in findings)


def run(config: dict, now: datetime | None = None) -> dict:
    cfg = settings(config)
    now = now or datetime.now(timezone.utc)
    stale_hours = int(cfg.get("stale_hours", DEFAULT_STALE_HOURS))
    interval = int(cfg.get("interval_minutes", DEFAULT_INTERVAL_MINUTES))
    prior = state.load(_STATE_MODULE) or {}
    since = prior.get("last_run_at") or (now - timedelta(minutes=interval)).isoformat()
    stale_since = (now - timedelta(hours=stale_hours)).isoformat()
    gap_hours = math.ceil((now - datetime.fromisoformat(since)).total_seconds() / 3600)
    window_hours = max(stale_hours, gap_hours)

    instances = discover_instances()
    expected = sorted({inst["key"] for inst in instances} | {config["job"]["key"]})
    try:
        events, errors = read_feed(window_hours, now)
    except Exception as e:
        log.emit("global_watch_error",
                 f"global feed unreadable: {type(e).__name__}: {e}",
                 links={"global": "/global"})
        raise

    findings = evaluate(events, errors, expected, since, stale_since)
    fingerprint = _fingerprint(findings)
    if findings and fingerprint != prior.get("fingerprint"):
        lines = [f"{f['instance']} {f['kind']}: {f['detail']}" for f in findings]
        log.emit("global_watch_alert",
                 f"global feed: {len(findings)} finding(s) across {len(expected)} instance(s): "
                 + "; ".join(lines),
                 links={"global": "/global"},
                 meta={"findings": findings, "expected": expected})
    elif not findings and prior.get("fingerprint"):
        log.emit("global_watch_ok",
                 f"global feed covers all {len(expected)} instance(s) again",
                 links={"global": "/global"}, meta={"expected": expected})

    state.save(_STATE_MODULE, {"last_run_at": now.isoformat(),
                               "fingerprint": fingerprint,
                               "findings": findings,
                               "expected": expected})
    return {"findings": findings, "expected": expected,
            "instances_seen": sorted({ev.get("instance_key") for ev in events})}
