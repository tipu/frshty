"""Open a task for an attention bucket that nobody has answered.

manager/staleness.py already detects every condition the operator used to
notice by hand: a ticket that stopped moving, a review comment frshty failed
to fix, a pipeline that gave up. It renders those on /today and then waits for
a click. Around forty work items existed only because the operator read that
page, or noticed the silence himself, and typed the question in.

This module turns the detection into an action. A bucket entry that stays
present past its escalation window, that no open work item already covers,
and that the operator has not snoozed on /today, opens its own doctor task
tagged with the project.

Escalation is not "the bucket fired". Most buckets wait on a person: a
reviewer who has not looked yet, a ticket the operator has not picked up, an
approval only he can give. Opening a task on the first sighting would fire at
ordinary waiting. So only the buckets whose contents mean an automation
stopped without saying so are watched, and each one names how long the same
entity must stay present before the silence counts as a fault.

Persistence is measured by this module, not read off the bucket rows. Several
of these buckets carry no usable age at all, and the ones that do carry the
ticket's discovery time rather than the time it entered the state that is
stuck. A fingerprint first seen at one scan and still present many scans later
is the honest signal, and it clears itself: when the condition disappears the
row is dropped and the clock restarts.

The buckets deliberately left out of RULES all wait on a person, so a task
opened against one of them would report waiting rather than a fault.
needs_classification, pickup_new, ready_to_submit, merge_ready,
pending_approvals_stuck and billcom_invoice_due wait on the operator.
stale_own_prs and peer_pr_reviews wait on a reviewer. regressions_recent and
timesheet_underfilled are reports, not stalls. pr_comments_needs_reply looks
like a fault and is not one: features/tickets.py:2473 assigns needs_reply
precisely when a comment is a question rather than a change request, so the
human reply is the designed next step, not a step that failed.
"""
import os
import re
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
from features.platforms import make_platform
from manager import staleness
from services import ticket_doctor, work_launch, work_store

FRSHTY_TAG = "frshty"
DEFAULT_SCAN_INTERVAL_MINUTES = 30
DEFAULT_MAX_OPENS_PER_SCAN = 2
DEFAULT_MAX_OPENS_PER_DAY = 6
FAILED_RETRY_HOURS = 1
MAX_DETAIL_CHARS = 600
_STATE_MODULE = "watchdog"
_LIVE_JOB_STATES = ("queued", "running")
LIVE_JOB_MAX_AGE_HOURS = 6
_FINISHED_ITEM_STATES = ("needs_ack", "done")
_FENCE_BUCKET = "migration"
_FAILED_RUN_STATUS = "launch_failed"
_NON_COVERING_JOBS = frozenset({"advance_ticket"})


@dataclass(frozen=True)
class Rule:
    """One watched bucket.

    escalate_after_hours is how long the same entity must stay in the bucket
    before frshty opens a task. cooldown_hours is how long it then waits
    before opening another task for that same entity, so a condition the first
    task could not clear does not spawn one task per scan.
    """
    bucket: str
    escalate_after_hours: int
    cooldown_hours: int
    fault: str


RULES = (
    Rule("blocked_pr_comments", 6, 48,
         "frshty tried to auto-fix a review comment on this PR and failed at "
         "least twice. The retry loop is not converging and nothing else "
         "reports it."),
    Rule("pr_failed_tickets", 6, 48,
         "the ticket pipeline parked this ticket at pr_failed. It retries "
         "nothing further on its own."),
    Rule("in_review_no_ci", 48, 72,
         "this ticket has sat in review for two days and CI has still not "
         "gone green. A PR whose checks are simply running clears this bucket "
         "long before the window."),
    Rule("stale_unattended", 24, 72,
         "this ticket has stayed in planning or reviewing past the staleness "
         "threshold and has not advanced since."),
)


@dataclass(frozen=True)
class Entry:
    """One bucket row reduced to what the watchdog needs.

    entity_id is the fingerprint: the thing a task would be opened about. It
    is coarser than the bucket row on purpose — three stuck comments on one PR
    are one investigation, not three. snooze_id is the identifier /today uses
    for the same row, which is sometimes finer than the fingerprint.
    """
    entity_id: str
    ticket_key: str
    snooze_id: str
    detail: str


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _settings(config: dict) -> dict:
    return ((config or {}).get("manager") or {}).get("watchdog") or {}


def _thresholds(config: dict) -> dict:
    return ((config or {}).get("manager") or {}).get("thresholds") or {}


def enabled(config: dict) -> bool:
    return bool(_settings(config).get("enabled", True))


def _entries(bucket: str, instance_key: str, config: dict) -> list[Entry]:
    if bucket == "blocked_pr_comments":
        return [
            Entry(f"{r['repo']}/{r['pr_id']}", "",
                  f"{r['repo']}/{r['pr_id']}/{r['comment_id']}",
                  f"comment {r['comment_id']} failed {r['attempts']} times: {r['reason']}")
            for r in staleness.blocked_pr_comments(instance_key)
            if r.get("repo") and r.get("pr_id") and r.get("url")
        ]
    if bucket == "pr_failed_tickets":
        return [
            Entry(r["ticket_key"], r["ticket_key"], r["ticket_key"],
                  f"pr_failed_reason={r.get('pr_failed_reason') or 'unrecorded'},"
                  f" ci_fix_attempts={r.get('ci_fix_attempts', 0)}")
            for r in staleness.pr_failed_tickets(instance_key)
        ]
    if bucket == "in_review_no_ci":
        return [
            Entry(r["ticket_key"], r["ticket_key"], r["ticket_key"],
                  f"ci_fix_attempts={r.get('ci_fix_attempts', 0)},"
                  f" discovered_at={r.get('discovered_at') or 'unrecorded'}")
            for r in staleness.in_review_no_ci(instance_key)
        ]
    if bucket == "stale_unattended":
        return [
            Entry(r["ticket_key"], r["ticket_key"], r["ticket_key"],
                  f"status={r.get('status')},"
                  f" discovered_at={r.get('discovered_at') or 'unrecorded'}")
            for r in staleness.stale_unattended_tickets(instance_key, _thresholds(config).get(
                "stale_ticket_hours", 72))
        ]
    raise ValueError(f"no entity mapping for bucket: {bucket}")


def _snoozed(instance_key: str, bucket: str) -> set[str]:
    rows = db.query_all(
        "SELECT entity_id FROM today_snoozes"
        " WHERE instance_key=? AND loop_type=?"
        " AND (snooze_until IS NULL OR datetime(snooze_until) > datetime('now'))",
        (instance_key, bucket),
    )
    return {r["entity_id"] for r in rows}


def _group(entries: list[Entry], snoozed: set[str]) -> dict[str, Entry]:
    """One entry per fingerprint, with the details of its rows joined.

    A row the operator snoozed on /today is dropped before grouping, so a
    fingerprint whose every row is snoozed is absent from this scan and its
    escalation clock restarts."""
    grouped: dict[str, Entry] = {}
    details: dict[str, list[str]] = {}
    for e in entries:
        if e.snooze_id in snoozed:
            continue
        grouped.setdefault(e.entity_id, e)
        details.setdefault(e.entity_id, []).append(e.detail)
    return {
        entity_id: Entry(e.entity_id, e.ticket_key, e.snooze_id,
                         "; ".join(details[entity_id])[:MAX_DETAIL_CHARS])
        for entity_id, e in grouped.items()
    }


_OBSERVATION_COLUMNS = ("SELECT id, first_seen_at, last_seen_at, opened_at, work_item_id,"
                        " cleared_at FROM watchdog_observations"
                        " WHERE instance_key=? AND bucket=? AND entity_id=?")


def _observe(instance_key: str, bucket: str, entry: Entry, now: datetime) -> dict:
    """Record that this fingerprint is present, and return its row.

    first_seen_at is the escalation clock, so it does not move while the
    condition holds. A row that _forget marked cleared is a condition that went
    away and came back, and the clock starts again from this sighting: the
    window means the fault persisted that long, not that it once did."""
    stamp = _iso(now)
    row = db.query_one(_OBSERVATION_COLUMNS, (instance_key, bucket, entry.entity_id))
    if row and row["cleared_at"]:
        db.execute(
            "UPDATE watchdog_observations SET first_seen_at=?, last_seen_at=?,"
            " ticket_key=?, cleared_at=NULL WHERE id=?",
            (stamp, stamp, entry.ticket_key, row["id"]),
        )
    else:
        db.execute(
            "INSERT INTO watchdog_observations"
            "(instance_key, bucket, entity_id, ticket_key, first_seen_at, last_seen_at)"
            " VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(instance_key, bucket, entity_id) DO UPDATE SET"
            " last_seen_at=excluded.last_seen_at, ticket_key=excluded.ticket_key",
            (instance_key, bucket, entry.entity_id, entry.ticket_key, stamp, stamp),
        )
    return db.query_one(_OBSERVATION_COLUMNS, (instance_key, bucket, entry.entity_id))


def _forget(instance_key: str, bucket: str, present: set[str], now: datetime) -> None:
    """Drop rows for conditions that cleared, so the clock restarts if they
    come back. A row still inside its cooldown is kept: a condition that
    flaps must not shed the cooldown by disappearing for one scan."""
    rows = db.query_all(
        "SELECT id, entity_id, opened_at, work_item_id, cleared_at"
        " FROM watchdog_observations WHERE instance_key=? AND bucket=?",
        (instance_key, bucket),
    )
    for r in rows:
        if r["entity_id"] in present:
            continue
        opened = _parse(r["opened_at"])
        if opened is not None and now - opened < _wait(r, _rule(bucket)):
            db.execute("UPDATE watchdog_observations SET cleared_at=COALESCE(cleared_at, ?)"
                       " WHERE id=?", (_iso(now), r["id"]))
            continue
        db.execute("DELETE FROM watchdog_observations WHERE id=?", (r["id"],))


def _rule(bucket: str) -> Rule:
    for rule in RULES:
        if rule.bucket == bucket:
            return rule
    raise ValueError(f"no rule for bucket: {bucket}")


def _wait(observation: dict, rule: Rule) -> timedelta:
    """How long to wait after the last open attempt before trying again.

    A launch that failed to reach a working agent is retried within the hour.
    A launch that produced a working task waits the rule's full cooldown."""
    if observation["work_item_id"] is None:
        return timedelta(hours=FAILED_RETRY_HOURS)
    return timedelta(hours=rule.cooldown_hours)


def _due(observation: dict, rule: Rule, now: datetime) -> bool:
    first_seen = _parse(observation["first_seen_at"])
    if first_seen is None or now - first_seen < timedelta(hours=rule.escalate_after_hours):
        return False
    opened = _parse(observation["opened_at"])
    if opened is not None and now - opened < _wait(observation, rule):
        return False
    return True


def _needles(entry: Entry) -> list[re.Pattern]:
    """The identifiers a task objective would use for this entity.

    Each is anchored against a trailing digit. Plain substring matching reads
    DEV-635 as a mention of DEV-63 and silences the watchdog on the wrong
    ticket; features/tickets.py:2538 guards its own key match the same way."""
    if entry.ticket_key:
        stems = [entry.ticket_key]
    else:
        repo, _, pr_id = entry.entity_id.partition("/")
        stems = [f"{repo}/{pr_id}", f"{repo}#{pr_id}"]
    return [re.compile(rf"{re.escape(s)}(?![0-9])", re.IGNORECASE) for s in stems]


def _ticket_key_is_unique(ticket_key: str) -> bool:
    row = db.query_one(
        "SELECT COUNT(DISTINCT instance_key) AS n FROM tickets WHERE ticket_key=?",
        (ticket_key,),
    )
    return bool(row) and int(row["n"]) <= 1


def covered_by_open_task(entry: Entry, instance_key: str) -> int | None:
    """The id of an open work item that already names this entity, if any.

    A task the operator typed himself counts. So does a doctor run he started
    from the ticket page: its objective carries the ticket key. A finished item
    does not: its answer has already been delivered, so a condition that
    outlived it is a new question. Neither does an item whose latest run is
    launch_failed, which is what work_launch leaves behind when tmux does not
    start (services/work_launch.py:268) — no agent ever read that objective, so
    it covers nothing and would otherwise block every retry forever. The test
    is the run, not the item state: failed_stale also means an agent that
    started and died, and that work exists and is resumable.

    A proposal covers, even though it has no run at all. It is a task frshty
    already opened about this entity that is waiting for the operator to
    approve or decline it. Opening a second task beside it would put the same
    entity in front of him twice, and approving both would run two agents on
    it. Unlike a launch_failed item it is visible on the board and asks for a
    decision, so it does not silence the watchdog indefinitely.

    A task tagged for another project does not count: one project's work must
    not silence another's. The frshty label is not a project for this purpose —
    it marks a task about the tool, which is what a doctor task is — so an item
    carrying only that label counts, as does one with no contexts at all, which
    is what a task typed into the plain compose box looks like. An unscoped
    item covers a ticket key only when exactly one instance knows that key:
    the database models a ticket as (instance_key, ticket_key), so a prefix
    two upstream projects share would otherwise let one silence the other. It
    never covers a PR, because a bare api#12 repeats across every project with
    a repo of that name."""
    unscoped_covers = bool(entry.ticket_key) and _ticket_key_is_unique(entry.ticket_key)
    placeholders = ", ".join("?" for _ in _FINISHED_ITEM_STATES)
    rows = db.query_all(
        "SELECT w.id, w.objective, w.contexts, w.state,"
        " (SELECT status FROM work_runs WHERE work_item_id = w.id"
        "  ORDER BY id DESC LIMIT 1) AS last_run_status"
        " FROM work_items w"
        f" WHERE w.archived_at IS NULL AND w.state NOT IN ({placeholders})",
        tuple(_FINISHED_ITEM_STATES),
    )
    needles = _needles(entry)
    for r in rows:
        if (r["state"] != work_store.PROPOSED_STATE
                and r["last_run_status"] in (None, _FAILED_RUN_STATUS)):
            continue
        projects = [c for c in (r["contexts"] or "").split(",")
                    if c and c != FRSHTY_TAG]
        if projects:
            if instance_key not in projects:
                continue
        elif not unscoped_covers:
            continue
        objective = r["objective"] or ""
        if any(n.search(objective) for n in needles):
            return int(r["id"])
    return None


def covered_by_live_job(entry: Entry, instance_key: str, now: datetime) -> int | None:
    """The id of a pipeline job for this entity's ticket that is still working.

    A bucket stays populated for as long as the job that would empty it is
    running, so a doctor opened now would diagnose work in progress. A job that
    has been queued or running for longer than LIVE_JOB_MAX_AGE_HOURS is not
    working, it is wedged, and a wedged job is exactly what the watchdog exists
    to report — so it does not count as cover. Neither does advance_ticket: it
    is enqueued on every cron tick and returns early when anything else is
    running, so its presence would silence the watchdog forever while proving
    no progress. Every other ticket-carrying task changes something."""
    if not entry.ticket_key:
        return None
    floor = now - timedelta(hours=LIVE_JOB_MAX_AGE_HOURS)
    for job in q.jobs_for_ticket(instance_key, entry.ticket_key):
        if job["status"] not in _LIVE_JOB_STATES:
            continue
        if job["task"] in _NON_COVERING_JOBS:
            continue
        started = _parse(job["started_at"] or job["enqueued_at"])
        if started is not None and started < floor:
            continue
        return int(job["id"])
    return None


_PR_COMMENT_MAP = """
### How frshty answers a PR review comment

- `features/own_prs.py` `check()` polls the operator's own PRs, registers each
  new review comment, and enqueues `fix_pr_comment` / `fix_pr_comments`.
- `core/tasks/polls.py` holds those task bodies.
- `core/comments.py` owns the `comment_state` table: one row per comment, with
  `state`, `error_count` and `last_error`. A row that is not `processed` and
  carries repeated errors is what put this PR in the bucket.

### How to work

Confirm every claim against the live system before you make it. There is no
psql and no `sqlite3` binary on this host; the database is SQLite, so query it
with Python, for example
`python3 -c "import sqlite3,sys; print(sqlite3.connect(sys.argv[1]).execute('select ...').fetchall())" <db path>`.

Find the exact reason the comment is not being answered. Name the file and
line that makes the decision. Then propose the smallest remedy. Do not push
code and do not post anything to the PR until the operator approves the remedy
with AskUserQuestion. Reading anything is unrestricted.

Write the diagnosis as a self-contained HTML report artifact and print its
ARTIFACT line.
"""


def _pr_brief(instance_key: str, entry: Entry, rule: Rule) -> str:
    repo, _, pr_id = entry.entity_id.partition("/")
    seen = (state.load("own_prs") or {}).get(entry.entity_id)
    seen = seen if isinstance(seen, dict) else {}
    header = [
        f"- pr: {repo}#{pr_id}",
        f"- instance: {instance_key}",
        f"- title: {seen.get('title', '')}",
        f"- url: {seen.get('url', '')}",
        f"- bucket: {rule.bucket}",
        f"- frshty database: {db.path()}",
        f"- frshty source: {ticket_doctor.FRSHTY_ROOT}",
    ]
    return ("\n\n## PR snapshot\n\n" + "\n".join(header)
            + f"\n\n### Stuck comments\n\n{entry.detail}\n"
            + _PR_COMMENT_MAP)


def _description(instance_key: str, rule: Rule, entry: Entry, observation: dict,
                 now: datetime) -> str:
    first_seen = _parse(observation["first_seen_at"])
    held = ""
    if first_seen is not None:
        hours = int((now - first_seen).total_seconds() // 3600)
        held = f" It has been there for {hours} hours."
    return (
        f"frshty opened this itself. {entry.entity_id} has been in the "
        f"{rule.bucket} attention bucket on {instance_key} since "
        f"{observation['first_seen_at']}.{held} No open task covers it. "
        f"Evidence: {entry.detail}. The fault this bucket reports is that "
        f"{rule.fault} Find why frshty stopped, name the file and line that "
        f"decides it, and propose the smallest remedy."
    )


def _pr_is_live(config: dict, repo: str, pr_id: str) -> bool:
    """Whether the platform still reports this PR as open.

    comment_state rows outlive the PR that produced them, and the own_prs cache
    cannot be trusted to prune the last one: own_prs.check returns before its
    prune loop when the fetch comes back empty (features/own_prs.py:59), and an
    empty fetch is also what a total API failure looks like. So the state is
    read from the platform, once, and only for an entity that already cleared
    its escalation window. A call that fails answers nothing, and a task about
    a live PR is the cheaper mistake, so an unknown state counts as live. It
    runs as a guard before the launch budget is spent, not as a launch that
    fails: a dead PR that spent an attempt would keep spending one every hour
    for as long as its comment rows survive."""
    try:
        info = make_platform(config).get_pr_info(repo, int(pr_id)) or {}
    except Exception as e:
        log.emit("watchdog_pr_state_unknown",
                 f"could not read the state of {repo}#{pr_id}:"
                 f" {type(e).__name__}: {e}",
                 meta={"repo": repo, "pr_id": pr_id})
        return True
    return (info.get("state") or "OPEN").upper() == "OPEN"


def _open_task(config: dict, instance_key: str, rule: Rule, entry: Entry,
               observation: dict, now: datetime) -> dict:
    description = _description(instance_key, rule, entry, observation, now)
    if entry.ticket_key:
        return ticket_doctor.launch(config, entry.ticket_key, description)
    repo, _, pr_id = entry.entity_id.partition("/")
    contexts = [c for c in (instance_key, FRSHTY_TAG) if c]
    cwd = ticket_doctor.FRSHTY_ROOT if os.path.isdir(ticket_doctor.FRSHTY_ROOT) else ""
    objective = f"Doctor PR {repo}#{pr_id} ({instance_key}). {description}"
    return work_launch.launch(objective, cwd=cwd, contexts=contexts,
                              brief=_pr_brief(instance_key, entry, rule))


def _opened_today(instance_key: str, now: datetime) -> int:
    """Launches this watchdog attempted for this instance in the last 24 hours.

    The per-scan budget alone does not bound the cost of a backlog: on the
    first run after a quiet week every stuck entity clears its window at once,
    and two tasks per scan would keep launching agents for hours. This is the
    ceiling on that. It counts rows in watchdog_launches, not observations: an
    observation carries only its most recent attempt, so three retries against
    one entity would have counted once. A failed attempt counts too — it costs
    the same, and a work layer that is down is not fixed by trying harder."""
    row = db.query_one(
        "SELECT COUNT(*) AS n FROM watchdog_launches"
        " WHERE instance_key=? AND bucket != ?"
        " AND datetime(created_at) > datetime(?)",
        (instance_key, _FENCE_BUCKET, _iso(now - timedelta(hours=24))),
    )
    return int(row["n"]) if row else 0


def _fenced(instance_key: str, now: datetime) -> bool:
    """Whether the upgrade fence still holds for this instance.

    The launch ledger replaced a count kept on the observation rows, which held
    only an entity's latest attempt, so the attempts made before the upgrade
    cannot be reconstructed. migrations/029 writes one fence row per instance
    frshty knows, and the first day after the upgrade spends no budget at all
    rather than guess how much of it is already gone. The fence is a marker,
    not a row count, so it holds whatever max_opens_per_day is set to."""
    row = db.query_one(
        "SELECT 1 AS present FROM watchdog_launches"
        " WHERE instance_key=? AND bucket=?"
        " AND datetime(created_at) > datetime(?) LIMIT 1",
        (instance_key, _FENCE_BUCKET, _iso(now - timedelta(hours=24))),
    )
    return bool(row)


def _record_attempt(instance_key: str, bucket: str, entity_id: str,
                    now: datetime) -> int:
    """Write the attempt before the launch, so an attempt that raises or is
    killed still spends the budget it cost."""
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO watchdog_launches"
            "(instance_key, bucket, entity_id, error, created_at)"
            " VALUES (?, ?, ?, 'launching', ?)",
            (instance_key, bucket, entity_id, _iso(now)),
        )
        return cur.lastrowid


def _record_outcome(launch_id: int, item_id: int | None, error: str) -> None:
    db.execute("UPDATE watchdog_launches SET work_item_id=?, error=? WHERE id=?",
               (item_id, error, launch_id))


def _record_open(observation_id: int, item_id: int | None, now: datetime) -> None:
    db.execute(
        "UPDATE watchdog_observations SET opened_at=?, work_item_id=? WHERE id=?",
        (_iso(now), item_id, observation_id),
    )


def scan(config: dict, instance_key: str = "", now: datetime | None = None,
         max_opens: int | None = None) -> list[dict]:
    """Walk every watched bucket once and open the tasks that are due.

    Returns one row per task opened. Scanning is always safe to repeat: the
    escalation window, the cooldown and the open-task check each stop a second
    task for the same entity.

    An expected launch failure is a returned error and does not stop the scan.
    An exception out of a launch is a defect, so every entity is still handled
    and the exception is then re-raised: the job records it with a traceback
    rather than reporting a scan that opened nothing as a success. The attempt
    ledger is written first, so the retry that failure invites is bounded."""
    instance_key = instance_key or state.active_instance_key()
    now = now or _now()
    if max_opens is None:
        max_opens = int(_settings(config).get("max_opens_per_scan",
                                              DEFAULT_MAX_OPENS_PER_SCAN))
    max_per_day = int(_settings(config).get("max_opens_per_day",
                                            DEFAULT_MAX_OPENS_PER_DAY))
    if _fenced(instance_key, now):
        budget = 0
    else:
        budget = min(max_opens, max(0, max_per_day - _opened_today(instance_key, now)))
    attempts = 0
    raised: Exception | None = None
    opened: list[dict] = []
    for rule in RULES:
        try:
            entries = _entries(rule.bucket, instance_key, config)
        except Exception as e:
            log.emit("watchdog_bucket_failed",
                     f"[{instance_key}] {rule.bucket}: {type(e).__name__}: {e}")
            continue
        grouped = _group(entries, _snoozed(instance_key, rule.bucket))
        for entity_id, entry in sorted(grouped.items()):
            observation = _observe(instance_key, rule.bucket, entry, now)
            if attempts >= budget:
                continue
            if not _due(observation, rule, now):
                continue
            covering = covered_by_open_task(entry, instance_key)
            if covering is not None:
                log.emit("watchdog_already_covered",
                         f"[{instance_key}] {rule.bucket} {entity_id} is covered by"
                         f" open work item {covering}",
                         meta={"bucket": rule.bucket, "entity_id": entity_id,
                               "work_item_id": covering})
                continue
            if not entry.ticket_key:
                repo, _, pr_id = entry.entity_id.partition("/")
                if not _pr_is_live(config, repo, pr_id):
                    log.emit("watchdog_pr_closed",
                             f"[{instance_key}] {rule.bucket} {entity_id} is no"
                             " longer open; dropping it",
                             meta={"bucket": rule.bucket, "entity_id": entity_id})
                    db.execute("DELETE FROM watchdog_observations WHERE id=?",
                               (observation["id"],))
                    continue
            live_job = covered_by_live_job(entry, instance_key, now)
            if live_job is not None:
                log.emit("watchdog_pipeline_working",
                         f"[{instance_key}] {rule.bucket} {entity_id} has job"
                         f" {live_job} still working",
                         meta={"bucket": rule.bucket, "entity_id": entity_id,
                               "job_id": live_job})
                continue
            attempts += 1
            launch_id = _record_attempt(instance_key, rule.bucket, entity_id, now)
            try:
                result = _open_task(config, instance_key, rule, entry, observation, now)
            except Exception as e:
                result = {"error": f"{type(e).__name__}: {e}"}
                raised = e
                log.emit("watchdog_launch_raised",
                         f"[{instance_key}] {rule.bucket} {entity_id} raised"
                         f" {type(e).__name__}: {e}",
                         meta={"bucket": rule.bucket, "entity_id": entity_id,
                               "traceback": traceback.format_exc()})
            item_id = result.get("item_id") if "error" not in result else None
            _record_outcome(launch_id, item_id, result.get("error") or "")
            _record_open(observation["id"], item_id, now)
            if item_id is None:
                log.emit("watchdog_open_failed",
                         f"[{instance_key}] {rule.bucket} {entity_id}:"
                         f" {result.get('error')}",
                         meta={"bucket": rule.bucket, "entity_id": entity_id,
                               "failed_work_item_id": result.get("item_id")})
                continue
            log.emit("watchdog_opened",
                     f"[{instance_key}] {rule.bucket} {entity_id} opened work item"
                     f" {item_id}",
                     links={"detail": f"/tasks/{item_id}"},
                     meta={"bucket": rule.bucket, "entity_id": entity_id,
                           "work_item_id": item_id,
                           "ticket": entry.ticket_key or None})
            opened.append({"bucket": rule.bucket, "entity_id": entity_id,
                           "ticket_key": entry.ticket_key, "work_item_id": item_id,
                           "action": result.get("action", "launch")})
        _forget(instance_key, rule.bucket, set(grouped), now)
    if raised is not None:
        raise raised
    return opened


def due_for_scan(config: dict, now: datetime | None = None) -> bool:
    now = now or _now()
    interval = int(_settings(config).get("scan_interval_minutes",
                                         DEFAULT_SCAN_INTERVAL_MINUTES))
    last = _parse((state.load(_STATE_MODULE) or {}).get("last_scan_at"))
    return last is None or now - last >= timedelta(minutes=interval)


def run(config: dict, instance_key: str = "", now: datetime | None = None) -> list[dict]:
    """The scheduled entry point: scan when enabled and when the interval is
    up, and remember when the scan happened."""
    if not enabled(config):
        return []
    now = now or _now()
    if not due_for_scan(config, now):
        return []
    opened = scan(config, instance_key=instance_key, now=now)
    blob = state.load(_STATE_MODULE) or {}
    blob["last_scan_at"] = _iso(now)
    state.save(_STATE_MODULE, blob)
    return opened
