"""Covers billing.preview_descriptions and GET /api/billing/preview."""
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _install_billing_config(tmp_path, include_daily_descriptions=False):
    cfg_path = tmp_path / "bp.toml"
    state_dir = tmp_path / "bp-state"
    state_dir.mkdir()
    (state_dir / "logs").mkdir()
    cfg_path.write_text(f"""
[job]
key = "bp"
platform = "github"
ticket_system = "linear"
port = 18800
host = "http://bp.localhost"

[github]
repo = "fake/bp"

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

[billing]
name = "Preview Client"
rate = 90
billing_freq = "weekly"
billcom_customer_id = "cust1"
invoice_prefix = "PR"
include_daily_descriptions = {"true" if include_daily_descriptions else "false"}
extras = {{ ai_tool = 20 }}
""")
    return cfg_path, state_dir


def test_preview_descriptions_matches_line_items(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod.startswith("features.") or mod in ("core", "features"):
            sys.modules.pop(mod, None)

    cfg_path, state_dir = _install_billing_config(tmp_path)

    import core.db as db
    import core.config as cfg_mod
    import core.state as state

    db.init(tmp_path / "bp.db", ROOT / "migrations")
    state.init("bp")

    config = cfg_mod.load_config(str(cfg_path))

    from features import billing
    # Seed 3 work entries Mon-Wed of first week of March (monthly extras apply for week starting day <= 7).
    # With weekly billing + extras, the first-of-month week gets the ai_tool extra added.
    tickets_state = {}  # unused; not needed for _work_days_in
    # Monkey-patch: _work_days_in reads state.load("billing_entries")
    state.save("billing_entries", {
        "2026-03-02": {"date": "2026-03-02", "type": "work", "hours": 8},
        "2026-03-03": {"date": "2026-03-03", "type": "work", "hours": 8},
        "2026-03-04": {"date": "2026-03-04", "type": "work", "hours": 4},
    })

    descs = billing.preview_descriptions(config, "2026-03-02", "2026-03-06")
    assert any("Monday" in d and "March" in d for d in descs), descs
    assert any("Tuesday" in d for d in descs), descs
    assert any("Wednesday" in d for d in descs), descs
    # Because Monday is day 2 (<=7), extras apply: "ai tool" line should be present.
    assert any("ai tool" in d for d in descs), f"expected extras line in {descs}"

    # Second-week invoice (Mar 9-13) should not have extras
    state.save("billing_entries", {
        "2026-03-09": {"date": "2026-03-09", "type": "work", "hours": 8},
        "2026-03-10": {"date": "2026-03-10", "type": "work", "hours": 8},
    })
    descs2 = billing.preview_descriptions(config, "2026-03-09", "2026-03-13")
    assert not any("ai tool" in d for d in descs2), f"extras should not apply in week 2: {descs2}"
    assert any("Monday" in d for d in descs2)


def test_preview_endpoint_returns_descriptions(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod.startswith("features.") or mod in ("core", "features"):
            sys.modules.pop(mod, None)

    cfg_path, state_dir = _install_billing_config(tmp_path)
    sys.argv = ["frshty.py", str(cfg_path)]

    import core.db as db
    import core.config as cfg_mod
    import core.state as state
    import core.log as log

    db.init(tmp_path / "bp.db", ROOT / "migrations")
    config = cfg_mod.load_config(str(cfg_path))
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])

    state.save("billing_entries", {
        "2026-03-02": {"date": "2026-03-02", "type": "work", "hours": 8},
    })

    import frshty
    frshty._set_primary_config(config)

    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)
    r = client.get("/api/billing/preview", params={"start": "2026-03-02", "end": "2026-03-06"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert "descriptions" in body
    assert isinstance(body["descriptions"], list)
    assert any("Monday" in d for d in body["descriptions"])


if __name__ == "__main__":
    tests = [test_preview_descriptions_matches_line_items, test_preview_endpoint_returns_descriptions]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
