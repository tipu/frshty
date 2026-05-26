from dataclasses import dataclass, field
from datetime import date


DEFAULT_TERMINAL_STATUSES = frozenset({
    "done", "closed", "resolved", "merged", "cancelled", "canceled", "released",
    "won't do", "wont do",
})
DEFAULT_IN_PROGRESS_STATUSES = frozenset({"in progress", "in review", "prioritized"})


@dataclass(frozen=True)
class TicketDayActivity:
    commit_count: int = 0
    review_minutes: int = 0
    session_count: int = 0
    summary_present: bool = False


@dataclass(frozen=True)
class Candidate:
    ticket: str
    summary: str
    status: str
    assignee_id: str
    estimate_hours: float | None
    logged_hours: float
    in_progress_at: date | None
    last_commit_date: date | None
    activity_by_day: dict[str, TicketDayActivity] = field(default_factory=dict)


@dataclass(frozen=True)
class Demand:
    day: str
    target_hours: float
    already_logged_hours: float
    recurring_pending_hours: float


@dataclass(frozen=True)
class Allocation:
    day: str
    ticket: str
    hours: float
    tier: str
    score: float


@dataclass(frozen=True)
class SelectionConfig:
    terminal_statuses: frozenset[str] = DEFAULT_TERMINAL_STATUSES
    in_progress_statuses: frozenset[str] = DEFAULT_IN_PROGRESS_STATUSES
    recency_days: int = 7
    max_chunk_hours: float = 8.0
    w_review: float = 4.0
    w_commit: float = 3.0
    w_session: float = 1.0
    w_headroom: float = 2.0
    w_recency: float = 2.0


TIERS = ("strict", "same_day", "in_flight", "relax_headroom")


def _is_terminal(status: str, cfg: SelectionConfig) -> bool:
    return (status or "").strip().lower() in cfg.terminal_statuses


def _headroom(c: Candidate) -> float | None:
    if c.estimate_hours is None:
        return None
    return c.estimate_hours - c.logged_hours


def _headroom_fraction(c: Candidate) -> float:
    if c.estimate_hours is None or c.estimate_hours <= 0:
        return 1.0
    frac = (c.estimate_hours - c.logged_hours) / c.estimate_hours
    return max(0.0, min(1.0, frac))


def _recent_activity(c: Candidate, day: date, cfg: SelectionConfig) -> bool:
    for day_iso, act in c.activity_by_day.items():
        if act.commit_count == 0 and act.review_minutes == 0 and act.session_count == 0:
            continue
        d = date.fromisoformat(day_iso)
        if 0 <= (day - d).days <= cfg.recency_days:
            return True
    return False


def _eligible(c: Candidate, day: date, tier: str, cfg: SelectionConfig) -> bool:
    has_headroom = _headroom(c) is None or _headroom(c) > 0
    if tier == "strict":
        return (c.in_progress_at is not None and c.last_commit_date is not None
                and c.in_progress_at < day and c.last_commit_date > day and has_headroom)
    if tier == "same_day":
        return (c.in_progress_at is not None and c.last_commit_date is not None
                and c.in_progress_at < day and c.last_commit_date >= day and has_headroom)
    if tier == "in_flight":
        return (c.in_progress_at is not None and c.in_progress_at <= day
                and _recent_activity(c, day, cfg) and has_headroom)
    if tier == "relax_headroom":
        return (c.in_progress_at is not None and c.in_progress_at <= day
                and _recent_activity(c, day, cfg))
    return False


def _score(c: Candidate, day: date, cfg: SelectionConfig) -> float:
    act = c.activity_by_day.get(day.isoformat(), TicketDayActivity())
    recency = 0.0
    if c.last_commit_date is not None:
        recency = 1.0 / (1.0 + abs((day - c.last_commit_date).days))
    return (cfg.w_review * (act.review_minutes / 60.0)
            + cfg.w_commit * act.commit_count
            + cfg.w_session * act.session_count
            + cfg.w_headroom * _headroom_fraction(c)
            + cfg.w_recency * recency)


def select_allocations(days: list[str], candidates: list[Candidate],
                       demands: list[Demand], cfg: SelectionConfig) -> list[Allocation]:
    seen_days: set[str] = set()
    for d in demands:
        if d.day in seen_days:
            raise ValueError(f"duplicate demand day: {d.day}")
        seen_days.add(d.day)

    remaining = {}
    for d in demands:
        if d.day not in days:
            continue
        left = d.target_hours - d.already_logged_hours - d.recurring_pending_hours
        remaining[d.day] = round(max(0.0, left), 1)

    pool = [c for c in candidates if not _is_terminal(c.status, cfg)]
    capacity: dict[str, float] = {}
    for c in pool:
        hr = _headroom(c)
        capacity[c.ticket] = float("inf") if hr is None else max(0.0, hr)

    allocations: list[Allocation] = []
    for tier in TIERS:
        for day_iso in days:
            if remaining.get(day_iso, 0.0) <= 0:
                continue
            day = date.fromisoformat(day_iso)
            while remaining[day_iso] > 0:
                eligible = [c for c in pool
                            if capacity[c.ticket] > 0 and _eligible(c, day, tier, cfg)]
                if not eligible:
                    break
                eligible.sort(key=lambda c: (-_score(c, day, cfg),
                                             -_headroom_fraction(c), c.ticket))
                chosen = eligible[0]
                cap = capacity[chosen.ticket]
                chunk = round(min(remaining[day_iso], cap, cfg.max_chunk_hours), 1)
                if chunk <= 0:
                    capacity[chosen.ticket] = 0.0
                    continue
                allocations.append(Allocation(
                    day=day_iso, ticket=chosen.ticket, hours=chunk,
                    tier=tier, score=round(_score(chosen, day, cfg), 3)))
                remaining[day_iso] = round(remaining[day_iso] - chunk, 1)
                capacity[chosen.ticket] -= chunk
    return allocations
