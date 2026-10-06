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
run.

After the rules, a cheap codex model reads a digest of each instance's events
since the last run, plus the rule findings, and judges whether the instances
work as expected. The rules cannot see a fault whose events look ordinary: a
ticket that loops, a job that repeats with no progress, a scan that stopped
producing results. The model can. Its verdict goes to the feed on the same
change-only terms. The model runs in an empty directory with a read-only
sandbox, so it reads only the digest it is given."""
import asyncio
import math
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import core.log as log
import core.state as state
from core.llm import extract_json, run_external_model
from core.discovery import discover_instances
from web.observability import _fetch_local_global_events, _fetch_remote_global_events

DEFAULT_INTERVAL_MINUTES = 15
DEFAULT_STALE_HOURS = 2
FETCH_LIMIT = 5000
ERROR_MARKERS = ("error", "fail", "crash")
_STATE_MODULE = "global_watch"
DEFAULT_AGENT_MODEL = "gpt-6-luna"
DEFAULT_AGENT_EFFORT = "low"
AGENT_TIMEOUT = 300
DIGEST_TOP_EVENTS = 12
DIGEST_RECENT_EVENTS = 15
DIGEST_SUMMARY_CHARS = 160
AGENT_PROMPT = """You watch a fleet of frshty instances. frshty is an automation
service: each instance polls tickets, pull requests, Slack and other sources,
runs jobs and coding agents, and logs every step as an event in its feed.

Below is a digest of every instance's feed since the last check, {since}, up
to now, {now}. Each instance lists its event count, its most frequent event
names and its latest non-noise events. Rule findings come first; the rules
already alert on them.

Judge whether the instances work as expected. Report a problem only when the
digest shows it: an instance whose work stopped, a ticket or job that repeats
without progress, a failure that recurs, an instance that only logs noise
while it has work, events that contradict each other. Ordinary churn, one
failure that a retry cleared, and quiet periods are not problems. Do not
repeat a rule finding unless you can add its likely cause.

Reply with one JSON object and nothing else:
{{"status": "ok" | "problem", "problems": [{{"instance": "<key>", "what": "<one sentence>", "evidence": "<event names or summaries from the digest>", "likely_cause": "<one sentence>"}}]}}

DIGEST
{digest}
"""


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


def build_digest(events: list[dict], expected: list[str], findings: list[dict],
                 since: str) -> str:
    by_instance: dict[str, list[dict]] = {key: [] for key in expected}
    for ev in events:
        if (ev.get("ts") or "") > since:
            by_instance.setdefault(ev.get("instance_key") or "", []).append(ev)
    parts = ["RULE FINDINGS"]
    parts += [f"- {f['instance']} {f['kind']}: {f['detail']}" for f in findings] or ["- none"]
    for key in sorted(by_instance):
        rows = sorted(by_instance[key], key=lambda ev: ev.get("ts") or "")
        noise = [ev for ev in rows if (ev.get("meta") or {}).get("category") == "noise"]
        counts: dict[str, int] = {}
        for ev in rows:
            name = ev.get("event") or ""
            counts[name] = counts.get(name, 0) + 1
        top = sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[:DIGEST_TOP_EVENTS]
        recent = [ev for ev in rows if (ev.get("meta") or {}).get("category") != "noise"]
        parts.append(f"\nINSTANCE {key}: {len(rows)} events, {len(noise)} noise")
        parts.append("top: " + (", ".join(f"{name} x{n}" for name, n in top) or "none"))
        for ev in recent[-DIGEST_RECENT_EVENTS:]:
            summary = " ".join(str(ev.get("summary") or "").split())[:DIGEST_SUMMARY_CHARS]
            parts.append(f"{(ev.get('ts') or '')[11:19]} {ev.get('event')}: {summary}")
    return "\n".join(parts)


def ask_agent(config: dict, digest: str, since: str, now: datetime) -> dict:
    """The model's verdict, or {"status": "failed", "reason": ...}.

    codex only: a failed call is reported, never retried on another vendor."""
    cfg = settings(config)
    model = str(cfg.get("agent_model") or DEFAULT_AGENT_MODEL)
    effort = str(cfg.get("agent_effort") or DEFAULT_AGENT_EFFORT)
    prompt = AGENT_PROMPT.format(since=since, now=now.isoformat(), digest=digest)
    with tempfile.TemporaryDirectory(prefix="global-watch-") as tmp:
        last = Path(tmp) / "last.txt"
        cmd = ["codex", "exec", "--skip-git-repo-check", "--sandbox", "read-only",
               "--ephemeral", "-m", model, "-c", f"model_reasoning_effort={effort}",
               "-o", str(last), "-"]
        text, code = run_external_model(cmd, fn_name="global_watch_agent", model=f"codex:{model}",
                                        prompt=prompt, cwd=Path(tmp), timeout=AGENT_TIMEOUT,
                                        last_message_file=last, stdin_text=prompt)
    if code != 0 or not text:
        return {"status": "failed", "reason": f"codex exit={code}"}
    verdict = extract_json(text)
    if not isinstance(verdict, dict) or verdict.get("status") not in ("ok", "problem"):
        return {"status": "failed", "reason": f"unparseable verdict: {text.strip()[:200]}"}
    raw_problems = verdict.get("problems") or []
    if not isinstance(raw_problems, list):
        return {"status": "failed", "reason": f"problems is not a list: {text.strip()[:200]}"}
    problems = [p for p in raw_problems if isinstance(p, dict)]
    if verdict["status"] == "problem" and not problems:
        return {"status": "failed", "reason": "problem verdict names no problem"}
    return {"status": verdict["status"], "problems": problems if verdict["status"] == "problem" else []}


def _report_agent(verdict: dict, prior: dict, expected: list[str]) -> list[str]:
    if verdict["status"] == "failed":
        if prior.get("agent_status") != "failed":
            log.emit("global_watch_agent_failed",
                     f"global feed agent gave no verdict: {verdict['reason']}",
                     links={"global": "/global"}, meta={"reason": verdict["reason"]})
        return prior.get("agent_fingerprint") or []
    problems = verdict["problems"]
    fingerprint = sorted({str(p.get("instance") or "") for p in problems})
    if problems and fingerprint != prior.get("agent_fingerprint"):
        lines = [f"{p.get('instance')}: {p.get('what')} (cause: {p.get('likely_cause')})"
                 for p in problems]
        log.emit("global_watch_agent_alert",
                 f"global feed agent: {len(problems)} problem(s): " + "; ".join(lines),
                 links={"global": "/global"},
                 meta={"problems": problems, "expected": expected})
    elif not problems and prior.get("agent_fingerprint"):
        log.emit("global_watch_agent_ok",
                 f"global feed agent sees all {len(expected)} instance(s) working again",
                 links={"global": "/global"}, meta={"expected": expected})
    return fingerprint


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

    verdict = {"status": "off", "problems": []}
    agent_fingerprint = prior.get("agent_fingerprint") or []
    if cfg.get("agent", True):
        digest = build_digest(events, expected, findings, since)
        verdict = ask_agent(config, digest, since, now)
        agent_fingerprint = _report_agent(verdict, prior, expected)

    state.save(_STATE_MODULE, {"last_run_at": now.isoformat(),
                               "fingerprint": fingerprint,
                               "findings": findings,
                               "expected": expected,
                               "agent_status": verdict["status"],
                               "agent_fingerprint": agent_fingerprint,
                               "agent_problems": verdict.get("problems") or []})
    return {"findings": findings, "expected": expected, "agent": verdict,
            "instances_seen": sorted({ev.get("instance_key") for ev in events})}
