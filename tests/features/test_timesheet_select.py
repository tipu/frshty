from datetime import date

import pytest

from features.timesheet_select import (
    Allocation,
    Candidate,
    Demand,
    SelectionConfig,
    TicketDayActivity,
    select_allocations,
)


CFG = SelectionConfig()
D = "2026-05-20"
DAY = date.fromisoformat(D)


def _cand(ticket, **kw):
    base = dict(
        ticket=ticket,
        summary=ticket,
        status="In Progress",
        assignee_id="me",
        estimate_hours=16.0,
        logged_hours=0.0,
        in_progress_at=date(2026, 5, 15),
        last_commit_date=date(2026, 5, 22),
        activity_by_day={D: TicketDayActivity(commit_count=1)},
    )
    base.update(kw)
    return Candidate(**base)


def _demand(day=D, target=8.0, logged=0.0, recurring=0.0):
    return Demand(day=day, target_hours=target,
                  already_logged_hours=logged, recurring_pending_hours=recurring)


def test_strict_tier_selects_in_flight_ticket():
    allocs = select_allocations([D], [_cand("DEV-1")], [_demand()], CFG)
    assert allocs, "strict-eligible ticket must be selected"
    assert allocs[0].ticket == "DEV-1"
    assert allocs[0].tier == "strict"
    assert sum(a.hours for a in allocs) == 8.0


def test_relaxes_to_same_day_when_no_strict():
    # last_commit == day → fails strict (>D), passes same_day (>=D)
    c = _cand("DEV-2", last_commit_date=DAY)
    allocs = select_allocations([D], [c], [_demand()], CFG)
    assert allocs, "should relax to same_day tier"
    assert allocs[0].tier == "same_day"


def test_in_flight_tier_for_live_today_work():
    # last_commit before D (no future commit), but recent activity → in_flight
    today = "2026-05-25"
    c = _cand(
        "DEV-3",
        in_progress_at=date(2026, 5, 20),
        last_commit_date=date(2026, 5, 24),
        activity_by_day={"2026-05-24": TicketDayActivity(commit_count=2)},
    )
    allocs = select_allocations([today], [c], [_demand(day=today)], CFG)
    assert allocs, "live in-flight work must be selected"
    assert allocs[0].tier == "in_flight"


def test_terminal_ticket_never_selected_dev444():
    # DEV-444: Done, old commit, has logged hours and activity — must be excluded
    dev444 = _cand(
        "DEV-444",
        status="Done",
        estimate_hours=24.0,
        logged_hours=22.0,
        in_progress_at=date(2026, 4, 1),
        last_commit_date=date(2026, 5, 1),
        activity_by_day={D: TicketDayActivity(commit_count=5)},
    )
    allocs = select_allocations([D], [dev444], [_demand()], CFG)
    assert allocs == [], "terminal ticket must never be auto-logged"


def test_accept_the_gap_when_nothing_qualifies():
    # in_progress after the day → ineligible at every tier
    c = _cand("DEV-5", in_progress_at=date(2026, 6, 1), last_commit_date=date(2026, 6, 2))
    allocs = select_allocations([D], [c], [_demand()], CFG)
    assert allocs == [], "no eligible ticket → accept the gap (log nothing)"


def test_headroom_spill_across_tickets():
    # A: high score, only 2h headroom; B: lower score, 6h headroom; demand 8h
    a = _cand("DEV-A", estimate_hours=10.0, logged_hours=8.0,
              activity_by_day={D: TicketDayActivity(commit_count=3)})
    b = _cand("DEV-B", estimate_hours=10.0, logged_hours=4.0,
              activity_by_day={D: TicketDayActivity(commit_count=1)})
    allocs = select_allocations([D], [a, b], [_demand()], CFG)
    by_ticket = {x.ticket: x.hours for x in allocs}
    assert by_ticket.get("DEV-A") == 2.0, "A capped at its 2h headroom"
    assert by_ticket.get("DEV-B") == 6.0, "remaining spills to B"
    assert sum(by_ticket.values()) == 8.0


def test_does_not_overfill_when_headroom_short():
    # only 2h of headroom available, demand 8h → log 2h, accept 6h gap
    a = _cand("DEV-A", estimate_hours=10.0, logged_hours=8.0)
    allocs = select_allocations([D], [a], [_demand()], CFG)
    assert sum(x.hours for x in allocs) == 2.0, "never exceed headroom; rest is a gap"


def test_no_estimate_ticket_is_uncapped():
    c = _cand("DEV-NE", estimate_hours=None, logged_hours=0.0)
    allocs = select_allocations([D], [c], [_demand()], CFG)
    assert sum(x.hours for x in allocs) == 8.0


def test_already_at_target_logs_nothing():
    allocs = select_allocations([D], [_cand("DEV-1")], [_demand(logged=8.0)], CFG)
    assert allocs == []


def test_duplicate_demand_day_raises():
    with pytest.raises(ValueError):
        select_allocations([D], [_cand("DEV-1")], [_demand(), _demand()], CFG)
