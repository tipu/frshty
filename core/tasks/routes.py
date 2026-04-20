"""Event routing rules consumed by core.event_bus.Dispatcher."""
from core.event_bus import register, kind_is


def _cron_routes(event: dict, registries: dict) -> list[dict]:
    jobs = []
    target_instance = event.get("instance_key")
    target_keys = [target_instance] if target_instance else list(registries.keys())
    for instance_key in target_keys:
        reg = registries.get(instance_key)
        if not reg:
            continue
        features = reg.config.get("features", {})
        if features.get("tickets"):
            jobs.append({"instance_key": instance_key, "task": "scan_tickets"})
        if features.get("review_prs"):
            jobs.append({"instance_key": instance_key, "task": "poll_own_prs"})
            jobs.append({"instance_key": instance_key, "task": "poll_reviewer"})
        if features.get("timesheet"):
            jobs.append({"instance_key": instance_key, "task": "timesheet_check"})
        if features.get("slack"):
            jobs.append({"instance_key": instance_key, "task": "slack_scan"})
        if features.get("billing"):
            jobs.append({"instance_key": instance_key, "task": "billing_check"})
        jobs.append({"instance_key": instance_key, "task": "scheduler_check"})
    return jobs


def _ui_retry(event: dict, registries: dict) -> list[dict]:
    p = event.get("payload", {})
    return [{"instance_key": event["instance_key"], "task": p["task"],
             "payload": p.get("payload", {}), "ticket_key": p.get("ticket_key")}]


def _ui_set_state(event: dict, registries: dict) -> list[dict]:
    p = event.get("payload", {})
    return [{"instance_key": event["instance_key"], "task": "set_state",
             "payload": {"target": p["target"]}, "ticket_key": p.get("ticket_key")}]


def _ui_notes(event: dict, registries: dict) -> list[dict]:
    p = event.get("payload", {})
    return [{"instance_key": event["instance_key"], "task": "apply_note_reset",
             "payload": {"note": p.get("note", "")},
             "ticket_key": p.get("ticket_key")}]


register(kind_is("cron_tick"), _cron_routes)
register(kind_is("ui_retry"), _ui_retry)
register(kind_is("ui_set_state"), _ui_set_state)
register(kind_is("ui_notes"), _ui_notes)
