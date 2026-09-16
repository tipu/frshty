"""The day's declared intent, and the loop that keeps asking about it.

frshty reacts to external facts. A ticket is assigned, a bucket goes stale, a
task finishes, a Slack thread asks for work. What the operator decided to do
today is recorded nowhere. The manager digest comes closest and is a dead end:
it is written once a morning, read once, and replaced the next morning. Nothing
hangs off it, so a line the operator ignores disappears in silence.

A standup is the durable version of that decision. One row per working day
holds the action items the operator committed to, each one a goal rather than a
task. A goal may need no task, one task or six, and some of them frshty can
never run. That is why an action item is its own row and not a work item:
manager.watchdog.covered_by_open_task matches an entity against the objective
of every open work item, so an action item reading "Unblock DEV-635" stored as
a work item would silence the watchdog on DEV-635 for the whole day, which is
exactly the ticket the operator wants watched.

The nudge loop is the point. While an action item is open, nothing is running
against it and the operator has not said "not now", frshty comes back to it. It
starts at the cheapest grade — a proposal on the board, which is a card with an
Approve button — and escalates to a question only when the proposal was
declined or ignored. Eight gates must all pass before any of it fires, and a
daily budget bounds the whole day. A loop that fires wrong is worse than no
loop, because it is switched off after one bad day.

Nothing here decides anything about the ticket pipeline. It proposes work, asks
questions and records what the operator answered.
"""
import json
import re
from datetime import datetime, time, timedelta, timezone

import core.db as db
import core.log as log
from core import tz
from manager import watchdog
from services import work_launch, work_store

DRAFT, OPEN, CLOSED = "draft", "open", "closed"
ITEM_STATES = ("open", "awaiting_check", "done", "parked", "dropped")
CARRYING_STATES = ("open", "awaiting_check", "parked")
LIVE_TASK_STATES = ("proposed", "agent_working", "needs_you", "waiting_external")
RUNNING_TASK_STATES = ("agent_working", "needs_you")
BLOCKING_TASK_STATES = ("agent_working", "needs_you", "waiting_external")
ORIGINS = ("operator", "carry", "watchdog", "board", "proposal")
MAX_TEXT = 500
MAX_DRAFT_ITEMS = 12

DEFAULTS = {
    "enabled": False,
    "open_at": "09:00",
    "close_at": "18:30",
    "days": ["mon", "tue", "wed", "thu", "fri"],
    "nudge_after_hours": 3.0,
    "nudge_backoff": 2.0,
    "max_nudges_per_day": 6,
    "quiet_hours": [19, 8],
    "carry_limit": 3,
    "tick_interval_minutes": 30,
    "shadow": False,
}

_WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
_TICKET_RE = re.compile(r"\b([A-Z][A-Z0-9]+-[0-9]+)\b")


class StandupError(Exception):
    """A request the standup refuses. The caller turns it into a 4xx."""


def settings(config: dict | None) -> dict:
    merged = dict(DEFAULTS)
    merged.update((config or {}).get("standup") or {})
    return merged


def enabled(config: dict | None) -> bool:
    return bool(settings(config)["enabled"])


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _hhmm(value: str, fallback: time) -> time:
    try:
        hour, _, minute = str(value).partition(":")
        return time(int(hour), int(minute or 0))
    except ValueError:
        return fallback


def _clean(text: str) -> str:
    return " ".join((text or "").split())[:MAX_TEXT]


def _labels(contexts) -> str:
    if isinstance(contexts, str):
        parts = contexts.split(",")
    else:
        parts = list(contexts or [])
    return ",".join(sorted({p.strip() for p in parts if isinstance(p, str) and p.strip()}))


def working_day(day: str, config: dict | None) -> bool:
    """Whether the standup runs on this calendar day.

    A standup that nags on Saturday is switched off on Sunday."""
    names = [str(d).strip().lower()[:3] for d in settings(config)["days"]]
    try:
        weekday = datetime.strptime(day, "%Y-%m-%d").date().weekday()
    except ValueError:
        return False
    return _WEEKDAYS[weekday] in names


def quiet(now_local: datetime, config: dict | None) -> bool:
    """Whether the clock is inside the hours the loop stays silent.

    quiet_hours is [from, to] in local hours and wraps midnight: [19, 8] is
    quiet from 19:00 until 08:00."""
    hours = settings(config)["quiet_hours"]
    try:
        start, end = int(hours[0]), int(hours[1])
    except (TypeError, ValueError, IndexError):
        return False
    hour = now_local.hour
    if start == end:
        return False
    if start < end:
        return start <= hour < end
    return hour >= start or hour < end


# --- the day ---------------------------------------------------------------


def today_key() -> str:
    return tz.today_local().isoformat()


def _standup_row(day: str) -> dict | None:
    return db.query_one("SELECT * FROM standups WHERE day = ?", (day,))


def _previous_row(day: str) -> dict | None:
    return db.query_one(
        "SELECT * FROM standups WHERE day < ? ORDER BY day DESC LIMIT 1", (day,))


def ensure_day(config: dict | None = None, day: str | None = None) -> int:
    """The id of a day's standup, created as a draft the first time it is asked
    for.

    The draft is written once. A redraft is an explicit operator action, so a
    second call never rewrites action items the operator has already edited."""
    day = day or today_key()
    row = _standup_row(day)
    if row:
        return int(row["id"])
    now = _now()
    with db.tx() as c:
        existing = c.execute("SELECT id FROM standups WHERE day = ?", (day,)).fetchone()
        if existing:
            return int(existing["id"])
        cur = c.execute(
            "INSERT INTO standups(day, state, created_at) VALUES (?, ?, ?)",
            (day, DRAFT, now))
        standup_id = int(cur.lastrowid)
    _draft_items(standup_id, day, config)
    return standup_id


def redraft(standup_id: int, config: dict | None = None) -> dict:
    """Throw the drafted lines away and draft them again.

    Only the lines frshty wrote are replaced. A line the operator typed is his,
    and so is any line a task has been started against."""
    row = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
    if not row:
        raise StandupError(f"unknown standup: {standup_id}")
    if row["state"] == CLOSED:
        raise StandupError("this day is closed")
    drafted = db.query_all(
        "SELECT i.id FROM standup_items i WHERE i.standup_id = ? AND i.origin != 'operator'"
        " AND NOT EXISTS (SELECT 1 FROM work_items w WHERE w.standup_item_id = i.id)"
        " AND NOT EXISTS (SELECT 1 FROM standup_events e WHERE e.standup_item_id = i.id)",
        (standup_id,))
    with db.tx() as c:
        for r in drafted:
            c.execute("DELETE FROM standup_items WHERE id = ?", (r["id"],))
    _draft_items(standup_id, row["day"], config)
    return day_view(row["day"])


def _draft_items(standup_id: int, day: str, config: dict | None) -> None:
    """Write the day's candidate action items from what the board already knows.

    Every candidate comes from a row, never from a model reading transcripts.
    Carried lines come first because they are the operator's own commitments
    from yesterday, then the faults the watchdog is holding, then the tasks
    that are waiting on him."""
    taken = {r["text"].lower() for r in db.query_all(
        "SELECT text FROM standup_items WHERE standup_id = ?", (standup_id,))}
    position = _next_position(standup_id)
    for candidate in _candidates(day, config):
        if len(taken) >= MAX_DRAFT_ITEMS:
            return
        text = _clean(candidate["text"])
        if not text or text.lower() in taken:
            continue
        taken.add(text.lower())
        _insert_item(standup_id, text, candidate.get("contexts", ""),
                     candidate.get("origin", "board"), candidate.get("origin_ref", ""),
                     position, candidate.get("carried_from"), candidate.get("carry_count", 0),
                     candidate.get("state", "open"))
        position += 1


def _candidates(day: str, config: dict | None) -> list[dict]:
    return _carried(day) + _from_watchdog() + _from_board()


def _carried(day: str) -> list[dict]:
    """The lines of the previous standup that were never finished.

    A dropped or checked line is finished and does not come back. A parked one
    does: it is still the operator's intent, it is simply waiting on somebody,
    and it arrives parked so no nudge fires on it."""
    previous = _previous_row(day)
    if not previous:
        return []
    placeholders = ", ".join("?" for _ in CARRYING_STATES)
    rows = db.query_all(
        f"SELECT * FROM standup_items WHERE standup_id = ? AND state IN ({placeholders})"
        " ORDER BY position, id",
        (previous["id"], *CARRYING_STATES))
    return [{"text": r["text"], "contexts": r["contexts"], "origin": "carry",
             "origin_ref": f"day {previous['day']}", "carried_from": int(r["id"]),
             "carry_count": int(r["carry_count"]) + 1,
             "state": r["state"]}
            for r in rows]


def _from_watchdog() -> list[dict]:
    """A fault the watchdog is still holding, as one line of intent."""
    rows = db.query_all(
        "SELECT instance_key, bucket, entity_id, ticket_key, first_seen_at"
        " FROM watchdog_observations WHERE cleared_at IS NULL"
        " ORDER BY first_seen_at LIMIT 20")
    out = []
    for r in rows:
        subject = r["ticket_key"] or r["entity_id"]
        out.append({"text": f"Unblock {subject} — it has sat in {r['bucket']} since "
                            f"{(r['first_seen_at'] or '')[:10]}",
                    "contexts": r["instance_key"] or "",
                    "origin": "watchdog",
                    "origin_ref": f"{r['bucket']}:{r['entity_id']}"})
    return out


def _from_board() -> list[dict]:
    """A task that is waiting on the operator, as one line of intent."""
    rows = db.query_all(
        "SELECT id, objective, contexts, state FROM work_items"
        " WHERE archived_at IS NULL AND state IN ('needs_you', 'needs_ack', 'proposed')"
        " ORDER BY updated_at DESC LIMIT 20")
    verbs = {"needs_you": "Answer", "needs_ack": "Acknowledge", "proposed": "Decide on"}
    return [{"text": f"{verbs[r['state']]} task #{r['id']} — {r['objective']}",
             "contexts": r["contexts"] or "",
             "origin": "proposal" if r["state"] == "proposed" else "board",
             "origin_ref": f"work:{r['id']}"}
            for r in rows]


def yesterday(day: str) -> str:
    """What moved before this day, built from events rather than from a model.

    The window is the previous calendar day in local time through the moment
    this runs, so a standup opened late still reports the night's work."""
    try:
        start = datetime.strptime(day, "%Y-%m-%d").date() - timedelta(days=1)
    except ValueError:
        return ""
    since = _iso(datetime.combine(start, time(0, 0), tz.local_tz()).astimezone(timezone.utc))
    lines = []
    done = db.query_all(
        "SELECT DISTINCT w.id, w.objective FROM work_items w"
        " JOIN work_events e ON e.work_item_id = w.id"
        " WHERE e.kind IN ('operator_done', 'operator_ack', 'self_reported_done')"
        "   AND e.created_at >= ? ORDER BY w.id", (since,))
    if done:
        lines.append(f"{len(done)} tasks completed — "
                     + ", ".join(f"#{r['id']}" for r in done[:8]))
    moved = db.query_all(
        "SELECT ticket_key, new_status FROM ticket_transitions"
        " WHERE ts >= ? AND rejected = 0 ORDER BY ts DESC LIMIT 8", (since,))
    for r in moved:
        lines.append(f"{r['ticket_key']} reached {r['new_status']}")
    waiting = db.query_one(
        "SELECT COUNT(*) AS n FROM work_items"
        " WHERE archived_at IS NULL AND state IN ('needs_you', 'needs_ack')")
    if waiting and int(waiting["n"]):
        lines.append(f"{int(waiting['n'])} tasks are still waiting on you")
    return "\n".join(lines)


def open_day(standup_id: int) -> dict:
    """Start the day. The nudge loop reads the opening time as its clock.

    "Yesterday" is frozen here. It is the record of what the operator saw when
    he committed to the list, and recomputing it later would rewrite that."""
    row = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
    if not row:
        raise StandupError(f"unknown standup: {standup_id}")
    if row["state"] == CLOSED:
        raise StandupError("this day is closed")
    if row["state"] == OPEN:
        return day_view(row["day"])
    now = _now()
    db.execute(
        "UPDATE standups SET state = ?, opened_at = ?, yesterday_md = ?"
        " WHERE id = ? AND state = ?",
        (OPEN, now, yesterday(row["day"]), standup_id, DRAFT))
    return day_view(row["day"])


def close_day(standup_id: int, config: dict | None = None) -> dict:
    """Ask carry, park or drop on everything still open, then freeze the day.

    An item that carries silently for a fortnight is how a list dies, so the
    close is where carry-over is decided out loud. The question is written on
    every open item; the day itself is closed either way, and an unanswered
    item still carries, because losing the operator's intent because he did not
    answer a question at 18:30 would be worse than carrying it."""
    row = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
    if not row:
        raise StandupError(f"unknown standup: {standup_id}")
    if row["state"] == CLOSED:
        return day_view(row["day"])
    limit = int(settings(config)["carry_limit"])
    now = _now()
    items = db.query_all(
        "SELECT * FROM standup_items WHERE standup_id = ? AND state IN ('open', 'awaiting_check')"
        " ORDER BY position, id", (standup_id,))
    for item in items:
        _ask(int(item["id"]), _close_question(item, limit), "close", now)
    db.execute("UPDATE standups SET state = ?, closed_at = ? WHERE id = ?",
               (CLOSED, now, standup_id))
    return day_view(row["day"])


def _close_question(item: dict, carry_limit: int) -> dict:
    carried = int(item["carry_count"])
    if carried >= carry_limit:
        prompt = (f"This has carried {carried} days and is still open. "
                  f"Carrying it again is a decision, not a default.")
    else:
        prompt = "This is still open at the end of the day. What happens to it?"
    return {"prompt": prompt, "grade": "close", "options": [
        {"key": "carry", "label": "Carry to tomorrow",
         "detail": "it stays the intent and tomorrow's draft starts with it"},
        {"key": "park", "label": "Park it",
         "detail": "it is waiting on somebody else; no nudge fires on it"},
        {"key": "drop", "label": "Drop it",
         "detail": "take it off the list and do not carry it"},
    ]}


def day_view(day: str | None = None, config: dict | None = None) -> dict:
    """One day, with every action item and the tasks linked to each of them."""
    day = day or today_key()
    row = _standup_row(day)
    if not row:
        return {"day": day, "state": "", "items": [], "yesterday": "",
                "nudges_today": 0, "exists": False}
    items = db.query_all(
        "SELECT * FROM standup_items WHERE standup_id = ? ORDER BY position, id",
        (int(row["id"]),))
    tasks = _linked_tasks([int(i["id"]) for i in items])
    for item in items:
        item["tasks"] = tasks.get(int(item["id"]), [])
        item["question"] = _question(item)
        item["live_task"] = next(
            (t for t in item["tasks"] if t["state"] in LIVE_TASK_STATES), None)
        item.pop("pending_question", None)
    return {"day": day, "id": int(row["id"]), "state": row["state"],
            "yesterday": row["yesterday_md"] or "", "opened_at": row["opened_at"],
            "closed_at": row["closed_at"], "items": items, "exists": True,
            "nudges_today": _nudges_today(int(row["id"])),
            "budget": int(settings(config)["max_nudges_per_day"]),
            "shadow": bool(settings(config)["shadow"])}


def _linked_tasks(item_ids: list[int]) -> dict[int, list[dict]]:
    if not item_ids:
        return {}
    placeholders = ", ".join("?" for _ in item_ids)
    rows = db.query_all(
        "SELECT id, standup_item_id, objective, state, updated_at, archived_at"
        f" FROM work_items WHERE standup_item_id IN ({placeholders})"
        " ORDER BY id", tuple(item_ids))
    out: dict[int, list[dict]] = {}
    for r in rows:
        out.setdefault(int(r["standup_item_id"]), []).append(r)
    return out


def _question(item: dict) -> dict | None:
    raw = item.get("pending_question") or ""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return None


# --- action items ----------------------------------------------------------


def _next_position(standup_id: int) -> int:
    row = db.query_one(
        "SELECT COALESCE(MAX(position), -1) AS p FROM standup_items WHERE standup_id = ?",
        (standup_id,))
    return int(row["p"]) + 1 if row else 0


def _insert_item(standup_id: int, text: str, contexts: str, origin: str,
                 origin_ref: str, position: int, carried_from: int | None = None,
                 carry_count: int = 0, state: str = "open") -> int:
    now = _now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO standup_items(standup_id, text, contexts, state, origin,"
            " origin_ref, carried_from, carry_count, position, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (standup_id, text, _labels(contexts), state, origin, origin_ref,
             carried_from, carry_count, position, now, now))
        return int(cur.lastrowid)


def add_item(standup_id: int, text: str, contexts="", origin: str = "operator",
             origin_ref: str = "") -> dict:
    text = _clean(text)
    if not text:
        raise StandupError("empty action item")
    row = db.query_one("SELECT state FROM standups WHERE id = ?", (standup_id,))
    if not row:
        raise StandupError(f"unknown standup: {standup_id}")
    if row["state"] == CLOSED:
        raise StandupError("this day is closed")
    item_id = _insert_item(standup_id, text, contexts, origin, origin_ref,
                           _next_position(standup_id))
    return item(item_id)


def item(item_id: int) -> dict:
    row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item_id,))
    if not row:
        raise StandupError(f"unknown action item: {item_id}")
    row["tasks"] = _linked_tasks([item_id]).get(item_id, [])
    row["question"] = _question(row)
    row["live_task"] = next(
        (t for t in row["tasks"] if t["state"] in LIVE_TASK_STATES), None)
    row.pop("pending_question", None)
    return row


def _require_open_day(item_id: int) -> dict:
    row = db.query_one(
        "SELECT i.*, s.state AS day_state FROM standup_items i"
        " JOIN standups s ON s.id = i.standup_id WHERE i.id = ?", (item_id,))
    if not row:
        raise StandupError(f"unknown action item: {item_id}")
    if row["day_state"] == CLOSED:
        raise StandupError("this day is closed")
    return row


def update_item(item_id: int, text: str | None = None, contexts=None) -> dict:
    _require_open_day(item_id)
    sets, params = [], []
    if text is not None:
        cleaned = _clean(text)
        if not cleaned:
            raise StandupError("empty action item")
        sets.append("text = ?")
        params.append(cleaned)
    if contexts is not None:
        sets.append("contexts = ?")
        params.append(_labels(contexts))
    if not sets:
        return item(item_id)
    sets.append("updated_at = ?")
    params.extend([_now(), item_id])
    db.execute(f"UPDATE standup_items SET {', '.join(sets)} WHERE id = ?", tuple(params))
    return item(item_id)


def reorder(standup_id: int, ordered_ids: list[int]) -> dict:
    now = _now()
    with db.tx() as c:
        for position, item_id in enumerate(ordered_ids):
            c.execute("UPDATE standup_items SET position = ?, updated_at = ?"
                      " WHERE id = ? AND standup_id = ?",
                      (position, now, int(item_id), standup_id))
    row = db.query_one("SELECT day FROM standups WHERE id = ?", (standup_id,))
    return day_view(row["day"] if row else None)


def set_item_state(item_id: int, state: str) -> dict:
    """Move an action item by hand.

    An item the operator checks off is done. It is never checked off by a task
    completing: a completed task is evidence, and the item moves to
    awaiting_check so the nudge can change from "nothing is happening" to
    "a task says this is done — is it?"."""
    if state not in ITEM_STATES:
        raise StandupError(f"unknown action item state: {state}")
    _require_open_day(item_id)
    now = _now()
    db.execute(
        "UPDATE standup_items SET state = ?, pending_question = '', snoozed_until = NULL,"
        " updated_at = ? WHERE id = ?", (state, now, item_id))
    record_event(item_id, f"operator_{state}", {})
    return item(item_id)


def snooze(item_id: int, until: str) -> dict:
    """Hold the loop off one item. An explicit "not now" must hold."""
    if not _parse(until):
        raise StandupError("snooze needs an ISO timestamp")
    _require_open_day(item_id)
    db.execute("UPDATE standup_items SET snoozed_until = ?, pending_question = '',"
               " updated_at = ? WHERE id = ?", (until, _now(), item_id))
    record_event(item_id, "snoozed", {"until": until})
    return item(item_id)


def record_event(item_id: int, kind: str, payload: dict | None = None) -> None:
    """Write what frshty did about an action item, and what came back.

    Every nudge and every outcome is written here, so "what fraction of nudges
    were followed by an approval, a check-off, a snooze or an ad hoc request"
    is one query rather than an impression."""
    db.execute(
        "INSERT INTO standup_events(standup_item_id, kind, payload, created_at)"
        " VALUES (?, ?, ?, ?)", (item_id, kind, db.dump_json(payload or {}), _now()))


def events(item_id: int, limit: int = 50) -> list[dict]:
    return db.query_all(
        "SELECT kind, payload, created_at FROM standup_events"
        " WHERE standup_item_id = ? ORDER BY id DESC LIMIT ?", (item_id, limit))


# --- starting work against an action item ----------------------------------


def compose(item_id: int, text: str, agent: str = "claude") -> dict:
    """The operator's ad hoc request about one action item.

    An action item is a handle for starting work. When a task linked to it has
    a live agent, the request goes into that run as a side question, which is
    the "btw" path and does not disturb what the run is doing. When nothing is
    running, the request launches a task and links it, so the next nudge sees
    that something is happening."""
    row = _require_open_day(item_id)
    text = _clean(text)
    if not text:
        raise StandupError("empty request")
    live = _running_task(item_id)
    if live:
        out = work_store.side_question(int(live["id"]), text)
        if "error" not in out:
            record_event(item_id, "adhoc", {"work_item_id": int(live["id"]),
                                            "question": text})
        return out
    contexts = [c for c in (row["contexts"] or "").split(",") if c]
    out = work_launch.launch(text, contexts=contexts, agent=agent)
    if "error" not in out and out.get("item_id"):
        link_task(item_id, int(out["item_id"]))
        record_event(item_id, "launched", {"work_item_id": int(out["item_id"]),
                                           "objective": text})
    return out


def start_task(item_id: int, agent: str = "claude") -> dict:
    """Start a task whose objective is the action item itself."""
    row = _require_open_day(item_id)
    return compose(item_id, row["text"], agent=agent)


def link_task(item_id: int, work_item_id: int) -> None:
    db.execute("UPDATE work_items SET standup_item_id = ? WHERE id = ?",
               (item_id, work_item_id))


def _running_task(item_id: int) -> dict | None:
    """A task of this action item that has an agent behind it.

    A proposal is not one. It has no run at all, so a side question sent to it
    would find no session, and an ad hoc request must launch instead."""
    placeholders = ", ".join("?" for _ in RUNNING_TASK_STATES)
    return db.query_one(
        "SELECT id, state FROM work_items WHERE standup_item_id = ? AND archived_at IS NULL"
        f" AND state IN ({placeholders}) ORDER BY id DESC LIMIT 1",
        (item_id, *RUNNING_TASK_STATES))


def _blocking_task(item_id: int, config: dict | None, now: datetime) -> dict | None:
    """A task of this action item that makes a nudge wrong right now.

    A running, waiting or snoozed task blocks for as long as it holds: that is
    the operator's own condition, do not nag while something is running against
    it. A proposal blocks only while it is fresh. A card nobody approved after
    the backoff window is not work in progress, it is a grade-1 nudge that did
    not land, and the loop has to be able to escalate past it or an ignored
    card silences the item for the rest of the day."""
    placeholders = ", ".join("?" for _ in BLOCKING_TASK_STATES)
    row = db.query_one(
        "SELECT id, state FROM work_items WHERE standup_item_id = ? AND archived_at IS NULL"
        f" AND state IN ({placeholders}) ORDER BY id DESC LIMIT 1",
        (item_id, *BLOCKING_TASK_STATES))
    if row:
        return row
    cutoff = _iso(now - timedelta(hours=_backoff_hours({"nudge_count": 1}, config)))
    return db.query_one(
        "SELECT id, state FROM work_items WHERE standup_item_id = ? AND archived_at IS NULL"
        " AND state = ? AND created_at > ? ORDER BY id DESC LIMIT 1",
        (item_id, work_store.PROPOSED_STATE, cutoff))


# --- the nudge loop --------------------------------------------------------


def _nudges_today(standup_id: int) -> int:
    row = db.query_one(
        "SELECT COUNT(*) AS n FROM standup_events e"
        " JOIN standup_items i ON i.id = e.standup_item_id"
        " WHERE i.standup_id = ? AND e.kind = 'nudged'", (standup_id,))
    return int(row["n"]) if row else 0


def _last_activity(item: dict, standup: dict) -> datetime | None:
    """When something last happened on this action item.

    The idle clock starts at whichever is latest: the day opening, the last
    time a task linked to the item stopped, and the last thing the operator or
    frshty did about it. Measuring from the day opening alone would nudge an
    item the operator worked on all morning."""
    stamps = [_parse(standup.get("opened_at")), _parse(item.get("created_at"))]
    row = db.query_one(
        "SELECT MAX(updated_at) AS t FROM work_items WHERE standup_item_id = ?",
        (int(item["id"]),))
    if row:
        stamps.append(_parse(row["t"]))
    row = db.query_one(
        "SELECT MAX(created_at) AS t FROM standup_events WHERE standup_item_id = ?"
        " AND kind NOT IN ('nudged', 'asked')", (int(item["id"]),))
    if row:
        stamps.append(_parse(row["t"]))
    present = [s for s in stamps if s]
    return max(present) if present else None


def _backoff_hours(item: dict, config: dict | None) -> float:
    cfg = settings(config)
    base = float(cfg["nudge_after_hours"])
    factor = float(cfg["nudge_backoff"])
    count = int(item["nudge_count"])
    if count <= 0:
        return base
    return base * (factor ** count)


def _covering_task(item: dict) -> int | None:
    """An open task that already names the entity this action item is about.

    The watchdog and the debrief propose work too. Three sources must not stack
    three cards for one ticket. Only a line that names a ticket can be checked
    this way; a line like "call the accountant" names no entity, and gate 2
    already covers whether a task is running against it."""
    match = _TICKET_RE.search(item["text"] or "")
    if not match:
        return None
    key = match.group(1)
    entry = watchdog.Entry(key, key, key, "")
    for instance_key in [c for c in (item["contexts"] or "").split(",") if c] or [""]:
        covered = watchdog.covered_by_open_task(entry, instance_key)
        if covered is None:
            continue
        own = db.query_one("SELECT standup_item_id FROM work_items WHERE id = ?",
                           (covered,))
        if own and own["standup_item_id"] == int(item["id"]):
            continue
        return covered
    return None


def gate(item: dict, standup: dict, config: dict | None,
         now: datetime | None = None) -> str:
    """Why this action item must not be nudged, or "" when it may be.

    Every gate is a reason a nudge would be wrong rather than merely early, and
    the string it returns is written to the event log, so a loop that never
    fires says which gate held it."""
    now = now or datetime.now(timezone.utc)
    if item["state"] != "open":
        return f"the item is {item['state']}"
    blocking = _blocking_task(int(item["id"]), config, now)
    if blocking:
        return f"task #{int(blocking['id'])} is already {blocking['state']} against it"
    snoozed = _parse(item["snoozed_until"])
    if snoozed and snoozed > now:
        return f"snoozed until {item['snoozed_until']}"
    if item.get("pending_question"):
        return "a question is already waiting on it"
    last = _last_activity(item, standup)
    idle_after = float(settings(config)["nudge_after_hours"])
    if last and now - last < timedelta(hours=idle_after):
        return f"it has been idle less than {idle_after}h"
    nudged = _parse(item["last_nudge_at"])
    backoff = _backoff_hours(item, config)
    if nudged and now - nudged < timedelta(hours=backoff):
        return f"the last nudge was less than {backoff}h ago"
    now_local = now.astimezone(tz.local_tz())
    if not working_day(now_local.date().isoformat(), config):
        return "it is not a working day"
    if quiet(now_local, config):
        return "it is inside quiet hours"
    covered = _covering_task(item)
    if covered is not None:
        return f"task #{covered} already covers it"
    return ""


def tick(config: dict | None = None, now: datetime | None = None) -> dict:
    """Run the gates over today's action items and fire at most one nudge.

    One nudge per tick, on the item at the top of the list. Six open items must
    not produce six cards at once, and the operator ordered the list himself,
    so the top one is the one he said matters.

    In shadow mode every gate still runs and every decision is written to the
    event log, but nothing is put in front of the operator. That is how the
    thresholds are checked before anybody has to live with them."""
    now = now or datetime.now(timezone.utc)
    cfg = settings(config)
    day = now.astimezone(tz.local_tz()).date().isoformat()
    standup = _standup_row(day)
    if not standup or standup["state"] != OPEN:
        return {"fired": None, "skipped": "no open standup for today"}
    if not _claim_tick(int(standup["id"]), now, float(cfg["tick_interval_minutes"])):
        return {"fired": None, "skipped": "ticked too recently"}
    budget = int(cfg["max_nudges_per_day"])
    spent = _nudges_today(int(standup["id"]))
    if spent >= budget:
        return {"fired": None, "skipped": f"the daily budget of {budget} nudges is spent"}
    items = db.query_all(
        "SELECT * FROM standup_items WHERE standup_id = ? ORDER BY position, id",
        (int(standup["id"]),))
    held = {}
    for item_row in items:
        reason = gate(item_row, dict(standup), config, now)
        if reason:
            held[int(item_row["id"])] = reason
            continue
        fired = _nudge(item_row, config, now)
        return {"fired": fired, "held": held, "shadow": bool(cfg["shadow"])}
    return {"fired": None, "held": held, "skipped": "every item is gated"}


def _claim_tick(standup_id: int, now: datetime, interval_minutes: float) -> bool:
    """Take this tick, or report that another one already has it.

    Two instances can both route the tick. The claim is the write: it succeeds
    for one of them, which is also what rate-limits the loop to the configured
    interval without a second clock."""
    cutoff = _iso(now - timedelta(minutes=interval_minutes))
    with db.tx() as c:
        changed = c.execute(
            "UPDATE standups SET last_tick_at = ? WHERE id = ?"
            " AND (last_tick_at IS NULL OR last_tick_at <= ?)",
            (_iso(now), standup_id, cutoff))
        return changed.rowcount == 1


def _nudge(item: dict, config: dict | None, now: datetime) -> dict:
    """Act on one idle action item, at the cheapest grade that is left.

    Grade 1 puts a proposal on the board. It costs no attention: it is a card
    with an Approve button, and declining it is one click. Grade 2 asks a
    question, and it only happens because grade 1 was declined or ignored and
    the item is still idle."""
    item_id = int(item["id"])
    shadow = bool(settings(config)["shadow"])
    grade = 1 if int(item["nudge_count"]) == 0 else 2
    payload = {"grade": grade, "shadow": shadow, "text": item["text"]}
    if shadow:
        _count_nudge(item_id, now, payload)
        return {"item_id": item_id, "grade": grade, "shadow": True}
    if grade == 1:
        out = _propose(item, now)
    else:
        out = _ask(item_id, _idle_question(item), "ask", _iso(now))
    payload.update(out)
    _count_nudge(item_id, now, payload)
    return {"item_id": item_id, "grade": grade, **out}


def _count_nudge(item_id: int, now: datetime, payload: dict) -> None:
    db.execute(
        "UPDATE standup_items SET nudge_count = nudge_count + 1, last_nudge_at = ?,"
        " updated_at = ? WHERE id = ?", (_iso(now), _iso(now), item_id))
    record_event(item_id, "nudged", payload)


def _propose(item: dict, now: datetime) -> dict:
    """Put a task for this action item on the board, waiting for approval.

    The proposal and the link are written in one transaction. A proposal the
    board could not link would be a card the loop cannot see, and the next tick
    would propose it again."""
    item_id = int(item["id"])
    objective = item["text"]
    note = ("Nothing has run against this action item since the standup opened. "
            "frshty put this task up rather than ask a question.")
    try:
        with db.tx() as c:
            work_item_id = work_store.create_proposal(
                objective, note=note, instance_key="personal",
                contexts=item["contexts"] or "", conn=c, now=_iso(now))
            c.execute("UPDATE work_items SET standup_item_id = ? WHERE id = ?",
                      (item_id, work_item_id))
    except Exception as e:
        log.emit("standup_nudge_failed",
                 f"action item {item_id}: could not propose a task: "
                 f"{type(e).__name__}: {e}",
                 meta={"standup_item_id": item_id,
                       "error": f"{type(e).__name__}: {e}"})
        return {"error": f"{type(e).__name__}: {e}"}
    return {"work_item_id": int(work_item_id), "action": "proposed"}


def _idle_question(item: dict) -> dict:
    declined = db.query_one(
        "SELECT id FROM work_items WHERE standup_item_id = ? AND state = 'done'"
        " AND stop_reason = ? ORDER BY id DESC LIMIT 1",
        (int(item["id"]), work_store.DECLINED_REASON))
    if declined:
        prompt = (f"You declined task #{int(declined['id'])} for this and nothing has "
                  f"happened since. What is the real state?")
    else:
        prompt = "Nothing has happened on this since the day opened. What is the real state?"
    return {"prompt": prompt, "grade": "ask", "options": [
        {"key": "start", "label": "Start a task on it now",
         "detail": "launch an agent with this line as its objective"},
        {"key": "mine", "label": "I am doing this myself",
         "detail": "stop nudging today; it stays open until you check it off"},
        {"key": "park", "label": "It is waiting on somebody else",
         "detail": "park it; no nudge fires until you reopen it"},
        {"key": "drop", "label": "Drop it",
         "detail": "take it off the list and do not carry it"},
    ]}


def _ask(item_id: int, question: dict, kind: str, now: str) -> dict:
    db.execute("UPDATE standup_items SET pending_question = ?, updated_at = ?"
               " WHERE id = ?", (db.dump_json(question), now, item_id))
    record_event(item_id, "asked", {"grade": question.get("grade", kind)})
    return {"action": "asked"}


def answer(item_id: int, option: str, config: dict | None = None) -> dict:
    """Apply the operator's answer to a nudge.

    Every option maps to a transition the system actually makes. An option that
    only dismissed the question would teach the operator that the question is
    theatre, and he would stop reading them."""
    row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item_id,))
    if not row:
        raise StandupError(f"unknown action item: {item_id}")
    question = _question(row)
    if not question:
        raise StandupError("no question is waiting on this action item")
    keys = {o["key"] for o in question.get("options", [])}
    if option not in keys:
        raise StandupError(f"unknown option: {option}")
    db.execute("UPDATE standup_items SET pending_question = '', updated_at = ?"
               " WHERE id = ?", (_now(), item_id))
    record_event(item_id, "answered", {"option": option,
                                       "grade": question.get("grade", "")})
    if option == "start":
        return {"item": item(item_id), "launch": start_task(item_id)}
    if option == "mine":
        return {"item": _hold_until_close(item_id, config)}
    if option == "park":
        db.execute("UPDATE standup_items SET state = 'parked', updated_at = ?"
                   " WHERE id = ?", (_now(), item_id))
    elif option == "drop":
        db.execute("UPDATE standup_items SET state = 'dropped', updated_at = ?"
                   " WHERE id = ?", (_now(), item_id))
    elif option == "carry":
        db.execute("UPDATE standup_items SET state = 'open', updated_at = ?"
                   " WHERE id = ?", (_now(), item_id))
    return {"item": item(item_id)}


def _hold_until_close(item_id: int, config: dict | None) -> dict:
    """Stop nudging an item today without closing it.

    "I am doing this myself" is not "done". The item stays open, so it is still
    on the list and still carries, and the loop stops asking until tomorrow."""
    close_at = _hhmm(settings(config)["close_at"], time(18, 30))
    local = tz.now_local()
    until = datetime.combine(local.date(), close_at, tz.local_tz())
    if until <= local:
        until = until + timedelta(days=1)
    stamp = _iso(until.astimezone(timezone.utc))
    db.execute("UPDATE standup_items SET snoozed_until = ?, updated_at = ?"
               " WHERE id = ?", (stamp, _now(), item_id))
    record_event(item_id, "snoozed", {"until": stamp, "by": "mine"})
    return item(item_id)


def sweep_completed_tasks() -> list[int]:
    """Move an action item whose task finished to awaiting_check.

    A completed task is evidence, not proof. The item does not check itself
    off; it moves so the next nudge asks "a task says this is done — is it?",
    which is a cheaper question than "nothing is happening" and keeps the
    check-off honest."""
    rows = db.query_all(
        "SELECT DISTINCT i.id FROM standup_items i"
        " JOIN work_items w ON w.standup_item_id = i.id"
        " WHERE i.state = 'open' AND w.state IN ('needs_ack', 'done')"
        "   AND w.stop_reason != ?"
        "   AND NOT EXISTS (SELECT 1 FROM work_items o WHERE o.standup_item_id = i.id"
        f"                  AND o.state IN ({', '.join('?' for _ in LIVE_TASK_STATES)}))",
        (work_store.DECLINED_REASON, *LIVE_TASK_STATES))
    moved = []
    for r in rows:
        item_id = int(r["id"])
        db.execute("UPDATE standup_items SET state = 'awaiting_check', updated_at = ?"
                   " WHERE id = ? AND state = 'open'", (_now(), item_id))
        record_event(item_id, "awaiting_check", {})
        moved.append(item_id)
    return moved
