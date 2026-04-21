from datetime import datetime, timezone

import core.log as log
import core.scheduler as scheduler
import core.state as state


def handle(payload: dict, trigger: dict, config: dict):
    key = payload["ticket_key"]

    delay_hours = trigger.get("delay_hours")
    if delay_hours:
        quiet_hours = trigger.get("quiet_hours", [23, 7])
        tz_name = trigger.get("timezone", "US/Pacific")
        run_at = scheduler.compute_delay_time(datetime.now(timezone.utc), delay_hours, quiet_hours, tz_name)
    else:
        estimate_seconds = payload.get("estimate_seconds", 0)
        discovered_at_str = payload.get("discovered_at")

        if not estimate_seconds or not discovered_at_str:
            log.emit("schedule_pr_skipped", f"Missing estimation data for {key}, skipping schedule",
                meta={"ticket": key})
            return

        discovered_at = datetime.fromisoformat(discovered_at_str)
        jitter_hours = trigger.get("jitter_hours", 3)
        work_hours = trigger.get("work_hours", [9, 17])
        run_at = scheduler.compute_target_time(discovered_at, estimate_seconds, jitter_hours, work_hours)

    scheduler.schedule(key, "create_pr", run_at, meta={
        "slug": payload.get("slug", ""),
        "branch": payload.get("branch", ""),
    })

    def _mark(current: dict) -> dict:
        new = dict(current or {})
        new["pr_scheduled_at"] = run_at.isoformat()
        return new
    state.update_ticket(key, _mark)
