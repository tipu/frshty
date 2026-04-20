"""Covers config-derived virtual rows in /api/scheduled and the 405 on virtual keys."""
import sys
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _write_config(tmp_path: Path) -> Path:
    cfg_path = tmp_path / "tiny.toml"
    state_dir = tmp_path / "tiny-state"
    state_dir.mkdir()
    (state_dir / "logs").mkdir()
    cfg_path.write_text(f"""
[job]
key = "tiny"
platform = "github"
ticket_system = "linear"
port = 18777
host = "http://tiny.localhost"

[github]
repo = "fake/tiny"

[linear]
token = "x"
assignee_email = "x@x.com"

[workspace]
root = "{tmp_path}"
repos = ["repo"]
tickets_dir = "tickets"
base_branch = "main"

[pr]
auto_pr = false

[timesheet]
recurring = [
    {{ticket = "DEV-336", days = ["mon", "wed", "fri"], time = "30m", label = "standup"}},
]
""")
    return cfg_path


def test_virtual_rows_populated_for_week(tmp_path):
    cfg_path = _write_config(tmp_path)
    sys.argv = ["frshty.py", str(cfg_path)]
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.config as cfg_mod
    import core.state as state
    import core.log as log

    db.init(tmp_path / "t.db", ROOT / "migrations")
    config = cfg_mod.load_config(str(cfg_path))
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])

    import frshty
    frshty._set_primary_config(config)

    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)

    r = client.get("/api/scheduled")
    assert r.status_code == 200, r.text
    rows = r.json()
    virtuals = [v for v in rows if v.get("type") == "recurring_virtual"]
    assert len(virtuals) >= 1, f"expected at least 1 virtual row for this week, got {rows}"

    # Each virtual row must carry source=config and mutable=false
    for v in virtuals:
        assert v.get("source") == "config", v
        assert v.get("mutable") is False, v
        assert v["key"].startswith("config:"), v
        assert v["ticket"] == "DEV-336", v
        # run_at must be UTC-normalized (end with +00:00)
        assert v["run_at"].endswith("+00:00"), v["run_at"]

    # Each weekday fires at 7 PM PDT (02:00 UTC next day)
    seen_dates = {v["run_at"].split("T")[0] for v in virtuals}
    assert len(seen_dates) == len(virtuals), "one virtual row per weekday match"


def test_virtual_row_mutation_rejected_with_405(tmp_path):
    cfg_path = _write_config(tmp_path)
    sys.argv = ["frshty.py", str(cfg_path)]
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.config as cfg_mod
    import core.state as state
    import core.log as log

    db.init(tmp_path / "t.db", ROOT / "migrations")
    config = cfg_mod.load_config(str(cfg_path))
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])

    import frshty
    frshty._set_primary_config(config)

    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)

    r = client.post("/api/scheduled/config:timesheet:DEV-336:2026-04-21/reschedule",
                     json={"run_at": "2026-04-22T19:00:00+00:00"})
    assert r.status_code == 405, r.text
    assert "read-only" in r.json().get("error", "")


if __name__ == "__main__":
    tests = [test_virtual_rows_populated_for_week,
             test_virtual_row_mutation_rejected_with_405]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
