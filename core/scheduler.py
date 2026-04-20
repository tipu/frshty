"""Scheduler backed by the sqlite `scheduler` table.

Two kinds of rows live side-by-side in the same table, keyed by
(instance_key, key):

  oneshot  legacy deferred actions (e.g. a scheduled PR for a ticket at a
           future instant). Row is deleted once it fires.
           data = {"kind":"oneshot", "action":"<name>", "meta":{...},
                   "scheduled_at":"<iso>"}.

  recurring durable periodic schedules owned by the beat thread. Row stays;
           next_run_at is advanced after each firing. data = {"kind":"recurring",
           "task":"<name>", "cadence":"weekly|monthly|daily_19pst",
           "payload":{...}, "last_run_at":"<iso>"}.

Both kinds surface together on /scheduled sorted by run_at.
"""
from __future__ import annotations
import random
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import Callable

import core.db as db
import core.log as log
import core.state as state


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def schedule(key: str, action: str, run_at: datetime, meta: dict | None = None) -> None:
    """Legacy oneshot API preserved. Writes a row with kind=oneshot."""
    instance_key = state.active_instance_key()
    data = {
        "kind": "oneshot",
        "action": action,
        "scheduled_at": _now_iso(),
        "meta": meta or {},
    }
    db.execute(
        "INSERT INTO scheduler(instance_key, key, run_at, data) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(instance_key, key) DO UPDATE SET run_at=excluded.run_at, data=excluded.data",
        (instance_key, key, run_at.isoformat(), db.dump_json(data)),
    )
    log.emit("scheduled", f"Scheduled {action} for {key} at {run_at.strftime('%a %b %d %I:%M%p')}",
             meta={"ticket": key, "action": action, "run_at": run_at.isoformat()})


def upsert_recurring(instance_key: str, key: str, task: str, cadence: str,
                      next_run_at: datetime, payload: dict | None = None) -> None:
    import json
    data = {
        "kind": "recurring",
        "task": task,
        "cadence": cadence,
        "payload": payload or {},
    }
    existing = db.query_one(
        "SELECT data FROM scheduler WHERE instance_key=? AND key=?",
        (instance_key, key),
    )
    if existing:
        try:
            prev = json.loads(existing["data"] or "{}")
            if prev.get("last_run_at"):
                data["last_run_at"] = prev["last_run_at"]
        except json.JSONDecodeError:
            pass
    db.execute(
        "INSERT INTO scheduler(instance_key, key, run_at, data) VALUES (?, ?, ?, ?) "
        "ON CONFLICT(instance_key, key) DO UPDATE SET run_at=excluded.run_at, data=excluded.data",
        (instance_key, key, next_run_at.isoformat(), db.dump_json(data)),
    )


def list_all(instance_key: str | None = None) -> list[dict]:
    if instance_key is None:
        rows = db.query_all("SELECT instance_key, key, run_at, data FROM scheduler ORDER BY run_at")
    else:
        rows = db.query_all(
            "SELECT instance_key, key, run_at, data FROM scheduler WHERE instance_key=? ORDER BY run_at",
            (instance_key,),
        )
    out = []
    for r in rows:
        data = db.load_json(r, "data")
        out.append({"instance_key": r["instance_key"], "key": r["key"], "run_at": r["run_at"], **data})
    return out


def delete(instance_key: str, key: str) -> None:
    db.execute("DELETE FROM scheduler WHERE instance_key=? AND key=?", (instance_key, key))


def check_due(config: dict) -> None:
    """Legacy oneshot firing path (scheduler_check task). Recurring rows are
    owned by the beat thread elsewhere and ignored here."""
    instance_key = state.active_instance_key()
    now = datetime.now(timezone.utc)
    rows = db.query_all(
        "SELECT key, run_at, data FROM scheduler WHERE instance_key=? AND run_at IS NOT NULL",
        (instance_key,),
    )
    for r in rows:
        data = db.load_json(r, "data")
        if data.get("kind") not in (None, "oneshot"):
            continue
        try:
            run_at = datetime.fromisoformat(r["run_at"])
        except (TypeError, ValueError):
            continue
        if run_at > now:
            continue
        action = data.get("action", "")
        log.emit("schedule_fired", f"Executing scheduled {action} for {r['key']}",
                 meta={"ticket": r["key"], "action": action, "scheduled_for": r["run_at"]})
        try:
            _execute(action, r["key"], data.get("meta", {}), config)
        except Exception as e:
            log.emit("schedule_exec_failed", f"{action} for {r['key']}: {type(e).__name__}: {e}")
        delete(instance_key, r["key"])


def fire_due_recurring(enqueue_fn: Callable[[str, str, dict, str | None], int] | None = None,
                       now: datetime | None = None) -> list[dict]:
    """Beat entrypoint. Claims every recurring row whose run_at has passed,
    enqueues the task, advances next_run_at, all inside one BEGIN IMMEDIATE
    transaction to avoid skipped/duplicated windows across a crash."""
    import json
    now = now or datetime.now(timezone.utc)
    fired: list[dict] = []

    with db.tx() as conn:
        due_rows = conn.execute(
            "SELECT instance_key, key, run_at, data FROM scheduler"
            " WHERE run_at IS NOT NULL AND run_at <= ?",
            (now.isoformat(),),
        ).fetchall()

        for r in due_rows:
            try:
                data = json.loads(r["data"] or "{}")
            except json.JSONDecodeError:
                continue
            if data.get("kind") != "recurring":
                continue
            try:
                prev_run_at = datetime.fromisoformat(r["run_at"])
            except (TypeError, ValueError):
                continue
            task = data.get("task")
            if not task:
                continue
            payload = data.get("payload", {})
            conn.execute(
                "INSERT INTO jobs(instance_key, task, payload, enqueued_at) VALUES (?, ?, ?, ?)",
                (r["instance_key"], task, db.dump_json(payload), now.isoformat()),
            )
            next_run_at = _advance_recurring(data.get("cadence", ""), prev_run_at, now)
            data["last_run_at"] = now.isoformat()
            conn.execute(
                "UPDATE scheduler SET run_at=?, data=? WHERE instance_key=? AND key=?",
                (next_run_at.isoformat(), db.dump_json(data), r["instance_key"], r["key"]),
            )
            fired.append({"instance_key": r["instance_key"], "key": r["key"],
                          "task": task, "next_run_at": next_run_at.isoformat()})
    return fired


def _advance_recurring(cadence: str, prev_run_at: datetime, now: datetime) -> datetime:
    """Compute next_run_at from the scheduled time, not now(), then skip
    forward past `now` if we're multiple windows behind (no catch-up, to
    avoid log spam and duplicate bill.com calls if the process was down)."""
    try:
        from features.billing import _next_fire as billing_next_fire, FIRE_TZ
    except Exception:
        billing_next_fire = None
        FIRE_TZ = ZoneInfo("America/Los_Angeles")

    if cadence in ("weekly", "monthly") and billing_next_fire is not None:
        tz_prev = prev_run_at.astimezone(FIRE_TZ) if prev_run_at.tzinfo else prev_run_at.replace(tzinfo=timezone.utc).astimezone(FIRE_TZ)
        candidate = billing_next_fire(tz_prev, cadence)
        if candidate is None:
            return prev_run_at + (timedelta(weeks=1) if cadence == "weekly" else timedelta(days=30))
        guard = 0
        while candidate <= now and guard < 520:
            nxt = billing_next_fire(candidate, cadence)
            if nxt is None or nxt <= candidate:
                break
            candidate = nxt
            guard += 1
        return candidate

    if cadence == "daily_19pst":
        tz = ZoneInfo("America/Los_Angeles")
        local = prev_run_at.astimezone(tz) if prev_run_at.tzinfo else prev_run_at.replace(tzinfo=timezone.utc).astimezone(tz)
        candidate = local.replace(hour=19, minute=0, second=0, microsecond=0) + timedelta(days=1)
        while candidate.astimezone(timezone.utc) <= now:
            candidate = candidate + timedelta(days=1)
        return candidate

    return prev_run_at + timedelta(hours=1)


def _execute(action: str, key: str, meta: dict, config: dict):
    if action == "create_pr":
        _execute_create_pr(key, meta, config)


def _execute_create_pr(key: str, _meta: dict, config: dict):
    from features.tickets import _create_pr
    from features.ticket_systems import make_ticket_system

    ticket_state = state.load("tickets")
    ts = ticket_state.get(key)
    if not ts or ts.get("status") != "pr_ready":
        return

    ticket_system = make_ticket_system(config)
    tickets_list = ticket_system.fetch_tickets() if ticket_system else []
    ticket = next((t for t in tickets_list if t["key"] == key), None)
    if not ticket:
        return

    base_url = config.get("_base_url", "")
    updated = _create_pr(config, ticket, ts, base_url)
    if updated:
        updated.pop("pr_scheduled_at", None)
        ticket_state[key] = updated
        state.save("tickets", ticket_state)


def compute_target_time(start: datetime, estimate_seconds: int, jitter_hours: int = 3,
                         work_hours: list[int] | None = None) -> datetime:
    if not work_hours:
        work_hours = [9, 17]

    biz_days = max(estimate_seconds / 28800, 0.5)
    full_days = int(biz_days)
    remaining_hours = (biz_days - full_days) * 8

    current = start
    days_added = 0
    while days_added < full_days:
        current += timedelta(days=1)
        if current.weekday() < 5:
            days_added += 1

    current += timedelta(hours=remaining_hours)
    while current.weekday() >= 5:
        current += timedelta(days=1)

    jitter = random.uniform(-jitter_hours, jitter_hours)
    current += timedelta(hours=jitter)

    if current.hour < work_hours[0]:
        current = current.replace(hour=work_hours[0], minute=random.randint(0, 59))
    elif current.hour >= work_hours[1]:
        current = current.replace(hour=work_hours[1] - 1, minute=random.randint(0, 59))

    while current.weekday() >= 5:
        current += timedelta(days=1)

    return current


def compute_delay_time(start: datetime, delay_hours: list[int], quiet_hours: list[int] | None = None,
                        tz_name: str = "US/Pacific") -> datetime:
    if not quiet_hours:
        quiet_hours = [23, 7]

    delay = random.uniform(delay_hours[0], delay_hours[1])
    target = start + timedelta(hours=delay)

    tz = ZoneInfo(tz_name)
    local = target.astimezone(tz)

    quiet_start, quiet_end = quiet_hours
    if local.hour >= quiet_start or local.hour < quiet_end:
        next_day = local.date()
        if local.hour >= quiet_start:
            next_day += timedelta(days=1)
        local = local.replace(year=next_day.year, month=next_day.month, day=next_day.day,
                              hour=quiet_end, minute=random.randint(0, 59))

    return local.astimezone(timezone.utc)
