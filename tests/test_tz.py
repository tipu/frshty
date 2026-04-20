"""Covers core/tz: FRSHTY_TIMEZONE env var drives local calendar semantics.

Reproduces the 2026-04-19 18:04 PDT bug where container UTC gave date.today()
== Monday even though the user was still living in Sunday. With
FRSHTY_TIMEZONE set, today_local() returns the correct user-local date.
"""
import os
import sys
import tempfile
import time as pytime
from datetime import date, datetime, timezone
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _reload_tz():
    for mod in list(sys.modules):
        if mod == "core.tz":
            sys.modules.pop(mod, None)


def test_default_is_utc_when_env_var_unset(tmp_path):
    _reload_tz()
    with mock.patch.dict(os.environ, {}, clear=False):
        os.environ.pop("FRSHTY_TIMEZONE", None)
        import core.tz as tz
        assert tz.local_tz().key == "UTC", tz.local_tz()
        assert tz.now_utc().tzinfo is timezone.utc


def test_env_var_picks_up_iana_zone(tmp_path):
    _reload_tz()
    with mock.patch.dict(os.environ, {"FRSHTY_TIMEZONE": "America/Los_Angeles"}):
        import core.tz as tz
        assert tz.local_tz().key == "America/Los_Angeles"


def test_bogus_zone_falls_back_to_utc(tmp_path):
    _reload_tz()
    with mock.patch.dict(os.environ, {"FRSHTY_TIMEZONE": "Not/A/Zone"}):
        import core.tz as tz
        assert tz.local_tz().key == "UTC", "invalid zone should fall back to UTC, not raise"


def test_today_local_uses_configured_zone_not_system_utc(tmp_path):
    """Regression for the DEV-437 7.5h bug.

    At 2026-04-20T01:04:53Z (== 2026-04-19T18:04:53 PDT), the container's
    date.today() == Monday but the user's calendar is still Sunday. With
    FRSHTY_TIMEZONE=America/Los_Angeles, today_local() returns Sunday.
    """
    _reload_tz()
    with mock.patch.dict(os.environ, {"FRSHTY_TIMEZONE": "America/Los_Angeles"}):
        import core.tz as tz
        frozen_utc = datetime(2026, 4, 20, 1, 4, 53, tzinfo=timezone.utc)
        with mock.patch("core.tz.datetime") as m:
            m.now.side_effect = lambda t=None: frozen_utc.astimezone(t) if t else frozen_utc
            got = tz.today_local()
            assert got == date(2026, 4, 19), f"expected Sunday, got {got}"

            got_utc_side = datetime.now(timezone.utc).date()
            # Sanity: real UTC date at this instant IS Monday, so the bug is real
            assert frozen_utc.date() == date(2026, 4, 20)


def test_now_local_carries_configured_zone(tmp_path):
    _reload_tz()
    with mock.patch.dict(os.environ, {"FRSHTY_TIMEZONE": "Europe/London"}):
        import core.tz as tz
        got = tz.now_local()
        assert got.tzinfo is not None
        # London is UTC+0 or UTC+1 depending on DST; either is non-US-Pacific
        assert str(got.tzinfo) == "Europe/London", got.tzinfo


if __name__ == "__main__":
    tests = [test_default_is_utc_when_env_var_unset,
             test_env_var_picks_up_iana_zone,
             test_bogus_zone_falls_back_to_utc,
             test_today_local_uses_configured_zone_not_system_utc,
             test_now_local_carries_configured_zone]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
