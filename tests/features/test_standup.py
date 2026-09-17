"""The standup holds the day's intent and nudges what nobody touched.

Every test here holds the loop to one of two properties. A nudge fires only
when all eight gates pass, and every gate can be shown to block on its own.
The second half of the file holds the day itself: a draft is written once, a
carry never loses a line, and an action item is never a work item.
"""
from datetime import date, datetime, time, timedelta, timezone
from unittest.mock import patch

import pytest

import core.db as db
from core import tz
from services import standup, work_store


def _cfg(**overrides) -> dict:
    settings = {"enabled": True, "nudge_after_hours": 1.0, "nudge_backoff": 2.0,
                "max_nudges_per_day": 6, "quiet_hours": [0, 0], "carry_limit": 3,
                "tick_interval_minutes": 0, "shadow": False,
                "days": ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]}
    settings.update(overrides)
    return {"standup": settings}


def _iso(dt: datetime) -> str:
    return dt.isoformat()


def _wipe():
    """Take the day and every task it opened out of the session database.

    A proposal a nudge wrote stays open forever, and manager.watchdog's
    coverage check reads every open work item by objective, so a leaked card
    would gate the next test's action item on a ticket key it shares."""
    db.execute("DELETE FROM work_events WHERE work_item_id IN"
               " (SELECT id FROM work_items WHERE standup_item_id IS NOT NULL)")
    db.execute("DELETE FROM work_items WHERE standup_item_id IS NOT NULL")
    db.execute("DELETE FROM standup_events")
    db.execute("DELETE FROM standup_items")
    db.execute("DELETE FROM standups")


@pytest.fixture
def clean():
    _wipe()
    yield
    _wipe()


def _day(offset: int = 0) -> str:
    """A local calendar day, relative to today.

    The loop reads the day off the local clock, so a test that hard-codes a
    date passes only while that date happens to be today in the machine's
    timezone. It went red the first time UTC rolled past midnight."""
    return (date.fromisoformat(standup.today_key()) + timedelta(days=offset)).isoformat()


def _at(day: str, hour: int, minute: int = 0) -> datetime:
    """A moment on a local calendar day, as the tick reads the clock."""
    return datetime.combine(date.fromisoformat(day), time(hour, minute),
                            tz.local_tz()).astimezone(timezone.utc)


def _open_day(day=None, config=None):
    standup_id = standup.ensure_day(config or _cfg(), day or _day())
    standup.open_day(standup_id)
    return standup_id


_keys = iter(range(1000, 9999))


def _item(standup_id, text=None, contexts="") -> dict:
    """One action item, named for a ticket key no other test uses."""
    return standup.add_item(standup_id, text or f"Unblock SUD-{next(_keys)}",
                            contexts=contexts)


def _age(item_id: int, hours: float) -> None:
    """Make an action item look idle by moving everything that touched it back."""
    stamp = _iso(datetime.now(timezone.utc) - timedelta(hours=hours))
    db.execute("UPDATE standup_items SET created_at = ?, updated_at = ? WHERE id = ?",
               (stamp, stamp, item_id))
    db.execute("UPDATE standups SET opened_at = ? WHERE id ="
               " (SELECT standup_id FROM standup_items WHERE id = ?)", (stamp, item_id))


class TestTheDay:
    def test_a_day_is_drafted_once(self, clean):
        first = standup.ensure_day(_cfg(), _day())
        assert standup.ensure_day(_cfg(), _day()) == first

    def test_a_drafted_day_starts_as_a_draft_and_nudges_nothing(self, clean):
        standup.ensure_day(_cfg(), _day())
        assert standup.day_view(_day())["state"] == standup.DRAFT
        out = standup.tick(_cfg(), _at(_day(), 12))
        assert out["fired"] is None
        assert "no open standup" in out["skipped"]

    def test_opening_freezes_yesterday(self, clean):
        standup_id = standup.ensure_day(_cfg(), _day())
        standup.open_day(standup_id)
        row = db.query_one("SELECT state, opened_at FROM standups WHERE id = ?", (standup_id,))
        assert row["state"] == standup.OPEN
        assert row["opened_at"]

    def test_an_action_item_is_not_a_work_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id, "Unblock DEV-9001")
        rows = db.query_all("SELECT id FROM work_items WHERE objective LIKE '%DEV-9001%'")
        assert rows == []
        assert standup.item(item["id"])["text"] == "Unblock DEV-9001"

    def test_an_unfinished_line_carries_and_a_finished_one_does_not(self, clean):
        first = _open_day(_day())
        kept = _item(first, "Carry me")
        gone = _item(first, "Finish me")
        standup.set_item_state(gone["id"], "done")
        second = standup.ensure_day(_cfg(), _day(1))
        texts = [i["text"] for i in standup.day_view(_day(1))["items"]]
        assert "Carry me" in texts
        assert "Finish me" not in texts
        carried = db.query_one(
            "SELECT carried_from, carry_count FROM standup_items"
            " WHERE standup_id = ? AND text = 'Carry me'", (second,))
        assert carried["carried_from"] == kept["id"]
        assert carried["carry_count"] == 1

    def test_a_parked_line_carries_parked_so_no_nudge_fires_on_it(self, clean):
        first = _open_day(_day())
        item = _item(first, "Waiting on Erik")
        standup.set_item_state(item["id"], "parked")
        standup.ensure_day(_cfg(), _day(1))
        carried = next(i for i in standup.day_view(_day(1))["items"]
                       if i["text"] == "Waiting on Erik")
        assert carried["state"] == "parked"
        day = db.query_one("SELECT * FROM standups WHERE day = ?", (_day(1),))
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (carried["id"],))
        assert standup.gate(row, day, _cfg()) == "the item is parked"

    def test_a_redraft_keeps_the_lines_the_operator_typed(self, clean):
        standup_id = _open_day()
        mine = _item(standup_id, "My own line")
        drafted = standup._insert_item(standup_id, "Drafted line", "", "board", "", 9)
        standup.redraft(standup_id, _cfg())
        texts = [i["text"] for i in standup.day_view(_day())["items"]]
        assert "My own line" in texts
        assert db.query_one("SELECT id FROM standup_items WHERE id = ?", (drafted,)) is None
        assert db.query_one("SELECT id FROM standup_items WHERE id = ?", (mine["id"],))

    def test_closing_asks_carry_park_or_drop_on_what_is_left(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        standup.close_day(standup_id, _cfg())
        question = standup.item(item["id"])["question"]
        assert question["grade"] == "close"
        assert {o["key"] for o in question["options"]} == {"carry", "park", "drop"}

    def test_a_closed_day_refuses_a_new_line(self, clean):
        standup_id = _open_day()
        standup.close_day(standup_id, _cfg())
        with pytest.raises(standup.StandupError):
            standup.add_item(standup_id, "too late")


class TestGates:
    def test_an_idle_open_item_passes_every_gate(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert standup.gate(row, day, _cfg()) == ""

    def test_a_checked_item_is_gated(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        standup.set_item_state(item["id"], "done")
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert "done" in standup.gate(row, day, _cfg())

    def test_a_running_task_gates_the_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        work_id = work_store.create_item("do the thing")
        db.execute("UPDATE work_items SET state = 'agent_working', standup_item_id = ?"
                   " WHERE id = ?", (item["id"], work_id))
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert "agent_working" in standup.gate(row, day, _cfg())

    def test_a_snooze_gates_the_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        standup.snooze(item["id"], _iso(datetime.now(timezone.utc) + timedelta(hours=2)))
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert "snoozed until" in standup.gate(row, day, _cfg())

    def test_a_fresh_item_is_gated_on_idle_time(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert "idle less than" in standup.gate(row, day, _cfg())

    def test_the_backoff_gates_a_second_nudge(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        db.execute("UPDATE standup_items SET nudge_count = 1, last_nudge_at = ? WHERE id = ?",
                   (_iso(datetime.now(timezone.utc) - timedelta(minutes=30)), item["id"]))
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert "last nudge was less than 2.0h" in standup.gate(row, day, _cfg())

    def test_a_non_working_day_gates_the_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert standup.gate(row, day, _cfg(days=[])) == "it is not a working day"

    def test_quiet_hours_gate_the_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert standup.gate(row, day, _cfg(quiet_hours=[0, 24])) == "it is inside quiet hours"

    def test_a_task_from_another_source_gates_the_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id, "Unblock DEV-4242", contexts="")
        _age(item["id"], 4)
        covering = work_store.create_proposal("Doctor DEV-4242 for the watchdog",
                                              instance_key="personal")
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert f"task #{covering} already covers it" == standup.gate(row, day, _cfg())

    def test_the_loops_own_card_does_not_gate_its_own_item(self, clean):
        standup_id = _open_day()
        item = _item(standup_id, "Unblock DEV-4343")
        _age(item["id"], 4)
        own = work_store.create_proposal("Unblock DEV-4343", instance_key="personal")
        stale = _iso(datetime.now(timezone.utc) - timedelta(hours=48))
        db.execute("UPDATE work_items SET standup_item_id = ?, created_at = ?,"
                   " updated_at = ? WHERE id = ?", (item["id"], stale, stale, own))
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert standup.gate(row, day, _cfg()) == ""


class TestNudges:
    def test_the_first_nudge_puts_a_proposal_on_the_board(self, clean):
        standup_id = _open_day()
        item = _item(standup_id, "Reply to the review comments")
        _age(item["id"], 4)
        out = standup.tick(_cfg())
        assert out["fired"]["grade"] == 1
        work_item_id = out["fired"]["work_item_id"]
        row = db.query_one("SELECT state, objective, standup_item_id FROM work_items"
                           " WHERE id = ?", (work_item_id,))
        assert row["state"] == work_store.PROPOSED_STATE
        assert row["objective"] == "Reply to the review comments"
        assert row["standup_item_id"] == item["id"]

    def test_the_second_nudge_asks_a_question_with_real_options(self, clean):
        standup_id = _open_day()
        item = _item(standup_id, "Reply to the review comments")
        _age(item["id"], 4)
        standup.tick(_cfg())
        proposal = db.query_one("SELECT id FROM work_items WHERE standup_item_id = ?",
                                (item["id"],))
        work_store.apply_action(int(proposal["id"]), "decline")
        _age(item["id"], 40)
        db.execute("UPDATE standup_items SET last_nudge_at = ? WHERE id = ?",
                   (_iso(datetime.now(timezone.utc) - timedelta(hours=40)), item["id"]))
        db.execute("UPDATE work_items SET updated_at = ? WHERE id = ?",
                   (_iso(datetime.now(timezone.utc) - timedelta(hours=40)), proposal["id"]))
        out = standup.tick(_cfg())
        assert out["fired"]["grade"] == 2
        question = standup.item(item["id"])["question"]
        assert {o["key"] for o in question["options"]} == {"start", "mine", "park", "drop"}
        assert str(int(proposal["id"])) in question["prompt"]

    def test_one_tick_fires_one_nudge(self, clean):
        standup_id = _open_day()
        for n in range(3):
            item = _item(standup_id, f"Line number {n}")
            _age(item["id"], 4)
        standup.tick(_cfg())
        assert db.query_one("SELECT COUNT(*) AS n FROM standup_events"
                            " WHERE kind = 'nudged'")["n"] == 1

    def test_the_daily_budget_stops_the_loop(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        standup.record_event(item["id"], "nudged", {})
        out = standup.tick(_cfg(max_nudges_per_day=1))
        assert out["fired"] is None
        assert "budget" in out["skipped"]

    def test_the_tick_interval_paces_the_loop(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        assert standup.tick(_cfg(tick_interval_minutes=30))["fired"]
        assert standup.tick(_cfg(tick_interval_minutes=30))["skipped"] == "ticked too recently"

    def test_shadow_mode_logs_the_nudge_and_shows_nothing(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        out = standup.tick(_cfg(shadow=True))
        assert out["fired"]["shadow"] is True
        assert db.query_all("SELECT id FROM work_items WHERE standup_item_id = ?",
                            (item["id"],)) == []
        assert standup.item(item["id"])["question"] is None
        assert db.query_one("SELECT COUNT(*) AS n FROM standup_events"
                            " WHERE kind = 'nudged'")["n"] == 1

    def test_a_held_item_names_the_gate_that_held_it(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        out = standup.tick(_cfg())
        assert out["fired"] is None
        assert "idle less than" in out["held"][item["id"]]


class TestAnswers:
    def test_park_stops_the_loop_and_keeps_the_line(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        standup.tick(_cfg())
        _age(item["id"], 40)
        db.execute("UPDATE standup_items SET last_nudge_at = ?, nudge_count = 1 WHERE id = ?",
                   (_iso(datetime.now(timezone.utc) - timedelta(hours=40)), item["id"]))
        db.execute("UPDATE work_items SET state = 'done', stop_reason = ?, updated_at = ?"
                   " WHERE standup_item_id = ?",
                   (work_store.DECLINED_REASON,
                    _iso(datetime.now(timezone.utc) - timedelta(hours=40)), item["id"]))
        standup.tick(_cfg())
        standup.answer(item["id"], "park", _cfg())
        after = standup.item(item["id"])
        assert after["state"] == "parked"
        assert after["question"] is None

    def test_an_unknown_option_is_refused(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        standup.close_day(standup_id, _cfg())
        with pytest.raises(standup.StandupError):
            standup.answer(item["id"], "definitely-not-an-option", _cfg())

    def test_carry_at_the_close_reopens_the_line(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        standup.close_day(standup_id, _cfg())
        standup.answer(item["id"], "carry", _cfg())
        assert standup.item(item["id"])["state"] == "open"

    def test_every_answer_is_recorded(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        standup.close_day(standup_id, _cfg())
        standup.answer(item["id"], "drop", _cfg())
        kinds = [e["kind"] for e in standup.events(item["id"])]
        assert "answered" in kinds
        assert "asked" in kinds


class TestCompletedTasks:
    def test_a_finished_task_moves_the_item_to_awaiting_check(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        work_id = work_store.create_item("do the thing")
        db.execute("UPDATE work_items SET state = 'needs_ack', standup_item_id = ?"
                   " WHERE id = ?", (item["id"], work_id))
        assert standup.sweep_completed_tasks() == [item["id"]]
        assert standup.item(item["id"])["state"] == "awaiting_check"

    def test_a_declined_proposal_does_not_check_the_item_off(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        work_id = work_store.create_proposal("do the thing", instance_key="personal")
        db.execute("UPDATE work_items SET standup_item_id = ? WHERE id = ?",
                   (item["id"], work_id))
        work_store.apply_action(work_id, "decline")
        assert standup.sweep_completed_tasks() == []
        assert standup.item(item["id"])["state"] == "open"


class TestRacesAndFailures:
    def test_a_failed_start_keeps_the_question_and_links_the_card(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        standup._ask(item["id"], standup._idle_question(row), "ask", _iso(
            datetime.now(timezone.utc)))
        orphan = work_store.create_item("Unblock the thing")
        db.execute("UPDATE work_items SET state = 'failed_stale' WHERE id = ?", (orphan,))
        with patch.object(standup.work_launch, "launch",
                          return_value={"error": "launch failed: tmux did not start",
                                        "item_id": orphan}):
            out = standup.answer(item["id"], "start", _cfg())
        assert "error" in out["launch"]
        assert standup.item(item["id"])["question"] is not None
        assert db.query_one("SELECT standup_item_id FROM work_items WHERE id = ?",
                            (orphan,))["standup_item_id"] == item["id"]

    def test_another_sources_task_is_found_behind_the_loops_own_card(self, clean):
        standup_id = _open_day()
        item = _item(standup_id, "Unblock DEV-5150")
        _age(item["id"], 4)
        stale = _iso(datetime.now(timezone.utc) - timedelta(hours=48))
        own = work_store.create_proposal("Unblock DEV-5150", instance_key="personal")
        db.execute("UPDATE work_items SET standup_item_id = ?, created_at = ?,"
                   " updated_at = ? WHERE id = ?", (item["id"], stale, stale, own))
        other = work_store.create_proposal("Doctor DEV-5150 for the watchdog",
                                           instance_key="personal")
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        day = db.query_one("SELECT * FROM standups WHERE id = ?", (standup_id,))
        assert standup.gate(row, day, _cfg()) == f"task #{other} already covers it"

    def test_a_nudge_that_lands_after_the_close_is_dropped(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        standup.tick(_cfg())
        proposal = db.query_one("SELECT id FROM work_items WHERE standup_item_id = ?",
                                (item["id"],))
        work_store.apply_action(int(proposal["id"]), "decline")
        _age(item["id"], 40)
        db.execute("UPDATE standup_items SET last_nudge_at = ? WHERE id = ?",
                   (_iso(datetime.now(timezone.utc) - timedelta(hours=40)), item["id"]))
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        standup.close_day(standup_id, _cfg())
        close_question = standup.item(item["id"])["question"]
        out = standup._nudge(row, _cfg(), datetime.now(timezone.utc), 6)
        assert out["error"] == "the day closed before the question was asked"
        assert standup.item(item["id"])["question"] == close_question

    def test_a_proposal_that_lands_after_the_close_is_dropped(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        row = db.query_one("SELECT * FROM standup_items WHERE id = ?", (item["id"],))
        standup.close_day(standup_id, _cfg())
        out = standup._nudge(row, _cfg(), datetime.now(timezone.utc), 6)
        assert out["error"] == "the day closed before the task was proposed"
        assert db.query_all("SELECT id FROM work_items WHERE standup_item_id = ?",
                            (item["id"],)) == []

    def test_two_ticks_cannot_both_take_the_last_budget_slot(self, clean):
        standup_id = _open_day()
        first = _item(standup_id, "The first line")
        second = _item(standup_id, "The second line")
        _age(first["id"], 4)
        _age(second["id"], 4)
        now = datetime.now(timezone.utc)
        assert standup._reserve(first["id"], standup_id, 1, now, {}) is not None
        assert standup._reserve(second["id"], standup_id, 1, now, {}) is None
        assert db.query_one("SELECT COUNT(*) AS n FROM standup_events"
                            " WHERE kind = 'nudged'")["n"] == 1

    def test_the_tick_reports_the_budget_when_the_reservation_loses(self, clean):
        standup_id = _open_day()
        item = _item(standup_id)
        _age(item["id"], 4)
        with patch.object(standup, "_reserve", return_value=None):
            out = standup.tick(_cfg(max_nudges_per_day=4))
        assert out["fired"] is None
        assert out["skipped"] == "the daily budget of 4 nudges is spent"


class TestCatchUp:
    def test_a_day_whose_open_beat_never_fired_is_opened(self, clean):
        now = _at(_day(), 12)
        out = standup.catch_up(_cfg(open_at="09:00", close_at="23:59"), now)
        assert out["opened"] == _day()
        assert standup.day_view(_day())["state"] == standup.OPEN

    def test_nothing_opens_before_the_open_hour(self, clean):
        now = _at(_day(), 7)
        assert standup.catch_up(_cfg(open_at="09:00"), now) == {}
        assert standup.day_view(_day())["exists"] is False

    def test_nothing_opens_on_a_day_off(self, clean):
        now = _at(_day(), 12)
        assert standup.catch_up(_cfg(days=["sun"], open_at="09:00"), now) == {}
        assert standup.day_view(_day())["exists"] is False

    def test_a_day_left_open_from_an_earlier_date_is_closed(self, clean):
        yesterday = _open_day(_day(-1))
        item = _item(yesterday, "Left open overnight")
        now = _at(_day(), 12)
        out = standup.catch_up(_cfg(open_at="09:00", close_at="23:59"), now)
        assert _day(-1) in out["closed"]
        assert standup.day_view(_day(-1))["state"] == standup.CLOSED
        assert standup.item(item["id"])["question"]["grade"] == "close"
        assert "Left open overnight" in [
            i["text"] for i in standup.day_view(_day())["items"]]

    def test_a_day_past_its_close_hour_is_closed(self, clean):
        _open_day(_day())
        now = _at(_day(), 20)
        out = standup.catch_up(_cfg(open_at="09:00", close_at="18:30"), now)
        assert out["closed"] == [_day()]
        assert standup.day_view(_day())["state"] == standup.CLOSED
