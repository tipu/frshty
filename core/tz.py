"""Single source of truth for the app's timezone.

Invariant: stored timestamps are always UTC. Calendar-day semantics come from
FRSHTY_TIMEZONE. Render layers convert back to that tz for display.

Usage:
    from core import tz
    today = tz.today_local()             # user's Monday, not UTC's
    now_local = tz.now_local()           # tz-aware datetime in user's zone
    now_utc = tz.now_utc()               # for storage in sqlite, events, logs
    tz.local_tz()                        # ZoneInfo for composing custom datetimes

Set FRSHTY_TIMEZONE in the environment (e.g. America/Los_Angeles). Default UTC.
"""
import os
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


def _resolve() -> ZoneInfo:
    raw = os.environ.get("FRSHTY_TIMEZONE", "").strip()
    if not raw:
        return ZoneInfo("UTC")
    try:
        return ZoneInfo(raw)
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


_TZ: ZoneInfo = _resolve()


def local_tz() -> ZoneInfo:
    return _TZ


def now_local() -> datetime:
    return datetime.now(_TZ)


def today_local() -> date:
    return datetime.now(_TZ).date()


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def refresh_from_env() -> None:
    """Test hook: re-read FRSHTY_TIMEZONE. Not used in normal operation."""
    global _TZ
    _TZ = _resolve()
