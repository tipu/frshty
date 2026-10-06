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
sandbox, so it reads only the digest it is given.

The model names one task for each problem it reports, and the run puts that
task on the board as a proposal for the operator to approve. The task key the
model gives is the proposal key, so a fault that persists is proposed once:
an open, running, declined or canceled proposal holds its key. The model sees
the tasks that already hold a key and reuses the key for the same fault. The
run opens nothing while max_open_tasks of its proposals wait on the board."""
import asyncio
import math
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import core.db as db
import core.log as log
import core.state as state
from core.llm import extract_json, run_external_model
from core.discovery import discover_instances
from services import work_launch, work_store
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
DEFAULT_MAX_OPEN_TASKS = 5
TASK_KEY_PREFIX = "global_watch:"
TASK_KEY_CHARS = 60
TASK_PROJECT = "frshty"
KNOWN_TASKS_LIMIT = 30
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
failure that a retry cleared, and quiet periods are not problems. Report a
rule finding as a problem only when it needs work, and add its likely cause.

For each problem, name one task that an agent can do to fix the problem or to
find its cause: "task" is one imperative sentence, and "task_key" is a short
kebab-case name of the fault. One fault on several instances is one problem
with one task. KNOWN TASKS lists the tasks that already exist for a fault.
When a problem is the fault of a known task, give that task's key.

Reply with one JSON object and nothing else:
{{"status": "ok" | "problem", "problems": [{{"instance": "<key>", "what": "<one sentence>", "evidence": "<event names or summaries from the digest>", "likely_cause": "<one sentence>", "task_key": "<kebab-case>", "task": "<one imperative sentence>"}}]}}

KNOWN TASKS
{known_tasks}

DIGEST
{digest}
"""
TASK_BRIEF = """

## Why this task exists

The global_watch check on {host} read the /global feed of every instance from
{since} to {now}. A model judged that an instance does not work as expected,
and the check proposed this task.

- instance: {instance}
- problem: {what}
- evidence: {evidence}
- likely cause: {likely_cause}

The likely cause is the model's guess from an event digest. Confirm the
problem in the instance's events and state before you change anything.
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


def known_tasks() -> list[dict]:
    """The global_watch proposals that hold their key, newest first."""
    return db.query_all(
        "SELECT id, proposal_key, objective, state FROM work_items "
        f"WHERE proposal_key LIKE ? AND state NOT IN {work_store.FINISHED_STATES_SQL} "
        "AND COALESCE(stop_reason, '') != ? ORDER BY id DESC LIMIT ?",
        (TASK_KEY_PREFIX + "%", work_store.SUPERSEDED_REASON, KNOWN_TASKS_LIMIT))


def _format_known(tasks: list[dict]) -> str:
    lines = [f"- {t['proposal_key'][len(TASK_KEY_PREFIX):]} ({t['state']}): {t['objective']}"
             for t in tasks]
    return "\n".join(lines) or "- none"


def _task_key(raw) -> str:
    slug = "-".join("".join(ch if ch.isalnum() else " " for ch in str(raw or "").lower()).split())
    return slug[:TASK_KEY_CHARS].strip("-")


def ask_agent(config: dict, digest: str, since: str, now: datetime,
              known: list[dict] | None = None) -> dict:
    """The model's verdict, or {"status": "failed", "reason": ...}.

    codex only: a failed call is reported, never retried on another vendor."""
    cfg = settings(config)
    model = str(cfg.get("agent_model") or DEFAULT_AGENT_MODEL)
    effort = str(cfg.get("agent_effort") or DEFAULT_AGENT_EFFORT)
    prompt = AGENT_PROMPT.format(since=since, now=now.isoformat(), digest=digest,
                                 known_tasks=_format_known(known or []))
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


def propose_tasks(config: dict, problems: list[dict], since: str, now: datetime,
                  prior_capped: list[str]) -> tuple[list[dict], list[str]]:
    """Put one proposal on the board for each problem whose task no proposal
    holds yet, until max_open_tasks of this check's proposals wait there.

    Returns the proposals and the task keys the cap held back. The cap goes
    to the feed only when that set of keys changes."""
    cfg = settings(config)
    if not cfg.get("propose_tasks", True):
        return [], []
    limit = int(cfg.get("max_open_tasks", DEFAULT_MAX_OPEN_TASKS))
    known = known_tasks()
    held = {t["proposal_key"] for t in known}
    waiting = sum(1 for t in known if t["state"] == work_store.PROPOSED_STATE)
    entry = next((e for e in work_launch.project_entries() if e["key"] == TASK_PROJECT), None)
    host = config["job"]["key"]
    proposed: list[dict] = []
    capped: list[str] = []
    for problem in problems:
        objective = " ".join(str(problem.get("task") or "").split())
        slug = _task_key(problem.get("task_key"))
        if not objective or not slug or TASK_KEY_PREFIX + slug in held:
            continue
        if waiting >= limit:
            capped.append(slug)
            continue
        instance = str(problem.get("instance") or "")
        brief = TASK_BRIEF.format(host=host, since=since, now=now.isoformat(), instance=instance,
                                  what=problem.get("what"), evidence=problem.get("evidence"),
                                  likely_cause=problem.get("likely_cause"))
        try:
            item_id = work_store.create_proposal(
                f"[{instance}] {objective}", note=f"Proposed by global_watch on {host}",
                instance_key=work_store.BOARD_INSTANCE_KEY,
                contexts=TASK_PROJECT if entry else "", cwd=entry["root"] if entry else "",
                brief=brief, proposal_key=TASK_KEY_PREFIX + slug)
        except work_store.ProposalKeyHeld:
            continue
        except Exception as e:
            log.emit("global_watch_task_failed",
                     f"global feed agent could not propose a task for {instance}: "
                     f"{type(e).__name__}: {e}",
                     links={"global": "/global"}, meta={"problem": problem})
            continue
        waiting += 1
        held.add(TASK_KEY_PREFIX + slug)
        log.emit("global_watch_task_proposed",
                 f"global feed agent proposed task {item_id} for {instance}: {objective}",
                 links={"detail": f"/tasks/{item_id}", "global": "/global"},
                 meta={"work_item_id": item_id, "task_key": slug, "problem": problem})
        proposed.append({"work_item_id": item_id, "task_key": slug, "instance": instance})
    capped = sorted(set(capped))
    if capped and capped != sorted(prior_capped):
        log.emit("global_watch_task_capped",
                 f"global feed agent held back {len(capped)} task(s): {', '.join(capped)}; "
                 f"{waiting} of its proposals already wait on the board",
                 links={"global": "/global"}, meta={"task_keys": capped, "limit": limit})
    return proposed, capped


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
    proposed: list[dict] = []
    capped: list[str] = prior.get("capped_tasks") or []
    if cfg.get("agent", True):
        digest = build_digest(events, expected, findings, since)
        verdict = ask_agent(config, digest, since, now, known_tasks())
        agent_fingerprint = _report_agent(verdict, prior, expected)
        proposed, capped = propose_tasks(config, verdict.get("problems") or [], since, now,
                                         prior.get("capped_tasks") or [])

    state.save(_STATE_MODULE, {"last_run_at": now.isoformat(),
                               "fingerprint": fingerprint,
                               "findings": findings,
                               "expected": expected,
                               "agent_status": verdict["status"],
                               "agent_fingerprint": agent_fingerprint,
                               "agent_problems": verdict.get("problems") or [],
                               "proposed_tasks": proposed,
                               "capped_tasks": capped})
    return {"findings": findings, "expected": expected, "agent": verdict,
            "proposed_tasks": proposed,
            "instances_seen": sorted({ev.get("instance_key") for ev in events})}
