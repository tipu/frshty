import random
from datetime import datetime, timezone, timedelta

import core.log as log
import core.state as state


def schedule(key: str, action: str, run_at: datetime, meta: dict | None = None):
    pending = state.load("scheduler")
    pending[key] = {
        "action": action,
        "scheduled_at": datetime.now(timezone.utc).isoformat(),
        "run_at": run_at.isoformat(),
        "meta": meta or {},
    }
    state.save("scheduler", pending)
    log.emit("scheduled", f"Scheduled {action} for {key} at {run_at.strftime('%a %b %d %I:%M%p')}",
        meta={"ticket": key, "action": action, "run_at": run_at.isoformat()})


def check_due(config: dict):
    pending = state.load("scheduler")
    if not pending:
        return

    now = datetime.now(timezone.utc)
    executed = []

    for key, item in pending.items():
        run_at = datetime.fromisoformat(item["run_at"])
        if now >= run_at:
            log.emit("schedule_fired", f"Executing scheduled {item['action']} for {key}",
                meta={"ticket": key, "action": item["action"], "scheduled_for": item["run_at"]})
            _execute(item["action"], key, item.get("meta", {}), config)
            executed.append(key)

    if executed:
        for key in executed:
            del pending[key]
        state.save("scheduler", pending)


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
    tickets = ticket_system.fetch_tickets() if ticket_system else []
    ticket = next((t for t in tickets if t["key"] == key), None)
    if not ticket:
        return

    base_url = config["_base_url"]
    ts = _create_pr(config, ticket, ts, base_url)
    ts.pop("pr_scheduled_at", None)
    ticket_state[key] = ts
    state.save("tickets", ticket_state)


def compute_target_time(start: datetime, estimate_seconds: int, jitter_hours: int = 3, work_hours: list[int] | None = None) -> datetime:
    if not work_hours:
        work_hours = [9, 17]

    biz_days = max(estimate_seconds / 28800, 0.5)  # 8h workday, minimum half day
    full_days = int(biz_days)
    remaining_hours = (biz_days - full_days) * 8

    current = start
    days_added = 0
    while days_added < full_days:
        current += timedelta(days=1)
        if current.weekday() < 5:  # skip weekends
            days_added += 1

    current += timedelta(hours=remaining_hours)

    # skip weekends if we landed on one
    while current.weekday() >= 5:
        current += timedelta(days=1)

    jitter = random.uniform(-jitter_hours, jitter_hours)
    current += timedelta(hours=jitter)

    # clamp to work hours
    if current.hour < work_hours[0]:
        current = current.replace(hour=work_hours[0], minute=random.randint(0, 59))
    elif current.hour >= work_hours[1]:
        current = current.replace(hour=work_hours[1] - 1, minute=random.randint(0, 59))

    # if jitter pushed us to a weekend, move to Monday
    while current.weekday() >= 5:
        current += timedelta(days=1)

    return current
