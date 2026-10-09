"""The work an instance holds that is not finished, split by who acts next.

global_watch gives this to its model with each instance's event digest. A
digest of routine poll jobs means an idle instance when nothing waits on an
agent, and a stuck one when something does."""
import core.db as db
from services import work_store

AGENT_TICKET_STATUSES = ("new", "researching", "planning", "reviewing", "testing",
                         "tests_failed", "proving", "validation")
CLOSED_TICKET_STATUSES = ("done", "epic", "ignored")
AGENT_WORK_STATES = ("agent_working",)
OPEN_JOB_STATUSES = ("queued", "running")


def _placeholders(values: tuple) -> str:
    return ", ".join("?" for _ in values)


def snapshot(instance_key: str) -> dict:
    """{"agent": {...}, "person": {...}}: each maps "tickets" and
    "work_items" to {state: count}; "agent" also maps "jobs" to the queued
    and running jobs."""
    tickets = db.query_all(
        "SELECT status, COUNT(*) AS n FROM tickets WHERE instance_key = ? AND obsolete_at IS NULL "
        f"AND status NOT IN ({_placeholders(CLOSED_TICKET_STATUSES)}) GROUP BY status",
        (instance_key, *CLOSED_TICKET_STATUSES))
    items = db.query_all(
        "SELECT state, COUNT(*) AS n FROM work_items WHERE instance_key = ? "
        f"AND state NOT IN ({_placeholders(work_store.CLOSED_STATES)}) GROUP BY state",
        (instance_key, *work_store.CLOSED_STATES))
    jobs = db.query_all(
        "SELECT status, COUNT(*) AS n FROM jobs WHERE instance_key = ? "
        f"AND status IN ({_placeholders(OPEN_JOB_STATUSES)}) GROUP BY status",
        (instance_key, *OPEN_JOB_STATUSES))
    out = {"agent": {"tickets": {}, "work_items": {}, "jobs": {}},
           "person": {"tickets": {}, "work_items": {}}}
    for row in tickets:
        side = "agent" if row["status"] in AGENT_TICKET_STATUSES else "person"
        out[side]["tickets"][row["status"]] = row["n"]
    for row in items:
        side = "agent" if row["state"] in AGENT_WORK_STATES else "person"
        out[side]["work_items"][row["state"]] = row["n"]
    for row in jobs:
        out["agent"]["jobs"][row["status"]] = row["n"]
    return out
