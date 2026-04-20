"""Integration tests for the new event-driven HTTP endpoints.

Boots frshty's FastAPI app against a temp sqlite db, exercises the new
/api/tickets/<key>/set-state, /auto-pr, /retry-job, /notes, /jobs routes.
"""
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _install_single_instance_config(tmp_path):
    cfg_path = tmp_path / "tiny.toml"
    state_dir = tmp_path / "tiny-state"
    state_dir.mkdir()
    (state_dir / "logs").mkdir()
    cfg_path.write_text(f"""
[job]
key = "tiny"
platform = "github"
ticket_system = "linear"
port = 17999
host = "http://tiny.localhost"

[github]
repo = "fake/fake"

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
""")
    return cfg_path, state_dir


def test_endpoints_round_trip(tmp_path):
    cfg_path, state_dir = _install_single_instance_config(tmp_path)
    sys.argv = ["frshty.py", str(cfg_path)]

    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.runtime as rt
    import core.queue as q
    import core.event_bus as bus
    from core import tasks  # noqa: F401

    db_path = tmp_path / "frshty.db"
    migrations = ROOT / "migrations"
    db.init(db_path, migrations)

    import core.config as cfg_mod
    config = cfg_mod.load_config(str(cfg_path))

    import core.state as state
    import core.log as log
    state.init(config["_state_dir"])
    log.init(config["_state_dir"], config["job"]["key"])

    rt._started = False
    rt._instances = None
    rt._pool = None
    rt._dispatcher = None
    rt.start_events([config], db_path=db_path, migrations_dir=migrations,
                     worker_count=1, cron_interval=3600)

    tickets_json = {
        "T-1": {"status": "pr_failed", "slug": "t-1", "branch": "danial/t-1"},
    }
    (config["_state_dir"] / "tickets.json").write_text(json.dumps(tickets_json))
    now = datetime.now(timezone.utc).isoformat()
    db.execute(
        "INSERT OR REPLACE INTO tickets(instance_key, ticket_key, status, slug, branch, auto_pr, updated_at)"
        " VALUES ('tiny', 'T-1', 'pr_failed', 't-1', 'danial/t-1', 0, ?)",
        (now,),
    )

    import frshty
    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)

    r = client.post("/api/tickets/T-1/set-state", json={"target": "pr_ready"})
    assert r.status_code == 200, r.text
    assert r.json()["target"] == "pr_ready"

    deadline = time.time() + 5
    final = None
    while time.time() < deadline:
        row = db.query_one(
            "SELECT status FROM tickets WHERE instance_key='tiny' AND ticket_key='T-1'"
        )
        if row and row["status"] == "pr_ready":
            final = row
            break
        time.sleep(0.1)
    assert final and final["status"] == "pr_ready", f"set_state did not land: {final}"

    r = client.patch("/api/tickets/T-1/auto-pr", json={"auto_pr": True})
    assert r.status_code == 200, r.text
    assert r.json()["auto_pr"] is True
    row = db.query_one("SELECT auto_pr FROM tickets WHERE ticket_key='T-1'")
    assert row["auto_pr"] == 1

    r = client.patch("/api/tickets/T-1/auto-pr", json={"auto_pr": False})
    assert r.status_code == 200

    import core.db as dbb
    dbb.execute(
        "UPDATE tickets SET status='pr_created' WHERE ticket_key='T-1'"
    )
    state_data = state.load("tickets")
    state_data["T-1"]["status"] = "pr_created"
    state.save("tickets", state_data)
    r = client.patch("/api/tickets/T-1/auto-pr", json={"auto_pr": True})
    assert r.status_code == 400, r.text

    r = client.get("/api/tickets/T-1/jobs")
    assert r.status_code == 200
    rows = r.json()
    assert isinstance(rows, list)
    assert any(j["task"] == "set_state" for j in rows), "expected set_state in job history"

    rt.stop_events()


if __name__ == "__main__":
    import tempfile
    with tempfile.TemporaryDirectory() as d:
        test_endpoints_round_trip(Path(d))
        print("test_endpoints_round_trip: PASS")
