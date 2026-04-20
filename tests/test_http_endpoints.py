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


def _write_config(tmp_path: Path, key: str, port: int) -> Path:
    cfg_path = tmp_path / f"{key}.toml"
    state_dir = tmp_path / f"{key}-state"
    state_dir.mkdir()
    (state_dir / "logs").mkdir()
    cfg_path.write_text(f"""
[job]
key = "{key}"
platform = "github"
ticket_system = "linear"
port = {port}
host = "http://{key}.localhost"

[github]
repo = "fake/{key}"

[linear]
token = "x"
assignee_email = "{key}@x.com"

[workspace]
root = "{tmp_path}"
repos = ["repo"]
tickets_dir = "tickets"
base_branch = "main"

[pr]
auto_pr = false

[slack]
workspace = "ws-{key}"
""")
    return cfg_path


def test_multi_registers_all_instances(tmp_path):
    """--multi boot path: start_events with two configs registers both instances and starts one pool."""
    a = _write_config(tmp_path, "alpha", 17001)
    b = _write_config(tmp_path, "beta", 17002)

    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.runtime as rt
    import core.config as cfg_mod
    import core.queue as q
    from core import tasks  # noqa: F401

    db_path = tmp_path / "multi.db"
    migrations = ROOT / "migrations"
    db.init(db_path, migrations)

    configs = [cfg_mod.load_config(str(a)), cfg_mod.load_config(str(b))]
    rt._started = False
    rt._instances = None
    rt._pool = None
    rt._dispatcher = None
    instances = rt.start_events(configs, db_path=db_path, migrations_dir=migrations,
                                 worker_count=1, cron_interval=3600)

    assert sorted(instances.keys()) == ["alpha", "beta"], instances.keys()
    assert instances.route_slack("ws-alpha") == "alpha"
    assert instances.route_slack("ws-beta") == "beta"
    assert instances.route_slack("nope") is None

    q.emit_event(source="cron", kind="cron_tick", payload={}, instance_key="alpha")
    q.emit_event(source="cron", kind="cron_tick", payload={}, instance_key="beta")

    deadline = time.time() + 5
    got = {"alpha": False, "beta": False}
    while time.time() < deadline and not all(got.values()):
        rows = db.query_all(
            "SELECT instance_key FROM jobs WHERE status IN ('queued','running','ok','skipped','failed')"
        )
        for r in rows:
            got[r["instance_key"]] = True
        time.sleep(0.1)
    assert got["alpha"] and got["beta"], f"expected jobs for both instances, got {got}"

    rt.stop_events()


def test_multi_hostname_routing(tmp_path):
    """Two configs behind --multi; each hostname sees its own state dir."""
    a = _write_config(tmp_path, "alpha", 17101)
    b = _write_config(tmp_path, "beta", 17102)

    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.runtime as rt
    import core.config as cfg_mod
    import core.state as state
    import core.log as log
    from core import tasks  # noqa: F401

    db_path = tmp_path / "multi-host.db"
    migrations = ROOT / "migrations"
    db.init(db_path, migrations)

    configs = [cfg_mod.load_config(str(a)), cfg_mod.load_config(str(b))]
    # Redirect state dirs into tmp_path instead of ~/.frshty/<key>
    for c in configs:
        c["_state_dir"] = tmp_path / f"{c['job']['key']}-state"
        c["_state_dir"].mkdir(parents=True, exist_ok=True)
    rt._started = False
    rt._instances = None
    rt._pool = None
    rt._dispatcher = None
    rt.start_events(configs, db_path=db_path, migrations_dir=migrations,
                     worker_count=1, cron_interval=3600)

    # Simulate main() populating hostname map + primary config
    primary = configs[0]
    state.init(primary["_state_dir"])
    log.init(primary["_state_dir"], primary["job"]["key"])

    import frshty
    frshty._set_primary_config(primary)
    frshty._configs_by_host.clear()
    frshty._configs_by_host["alpha.localhost"] = configs[0]
    frshty._configs_by_host["beta.localhost"] = configs[1]

    (configs[0]["_state_dir"] / "tickets.json").write_text(json.dumps({
        "A-1": {"status": "pr_ready", "slug": "a-1", "branch": "x"},
    }))
    (configs[1]["_state_dir"] / "tickets.json").write_text(json.dumps({
        "B-1": {"status": "pr_failed", "slug": "b-1", "branch": "y"},
    }))

    from fastapi.testclient import TestClient
    client = TestClient(frshty.app)

    r_a = client.get("/api/config", headers={"host": "alpha.localhost"})
    assert r_a.status_code == 200, r_a.text
    assert r_a.json()["job"]["key"] == "alpha"

    r_b = client.get("/api/config", headers={"host": "beta.localhost"})
    assert r_b.status_code == 200, r_b.text
    assert r_b.json()["job"]["key"] == "beta"

    r_list_a = client.get("/api/tickets/list", headers={"host": "alpha.localhost"})
    assert r_list_a.status_code == 200, r_list_a.text
    assert "A-1" in r_list_a.json(), f"expected A-1 in alpha tickets, got {r_list_a.json()}"
    assert "B-1" not in r_list_a.json(), "alpha should not see beta ticket"

    r_list_b = client.get("/api/tickets/list", headers={"host": "beta.localhost"})
    assert r_list_b.status_code == 200, r_list_b.text
    assert "B-1" in r_list_b.json(), f"expected B-1 in beta tickets, got {r_list_b.json()}"
    assert "A-1" not in r_list_b.json(), "beta should not see alpha ticket"

    rt.stop_events()


def test_multi_rejects_duplicate_slack_workspace(tmp_path):
    a = _write_config(tmp_path, "inst-a", 18001)
    b_path = _write_config(tmp_path, "inst-b", 18002)
    b_content = b_path.read_text().replace('workspace = "ws-inst-b"', 'workspace = "ws-inst-a"')
    b_path.write_text(b_content)

    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.runtime as rt
    import core.config as cfg_mod

    db_path = tmp_path / "dup.db"
    migrations = ROOT / "migrations"
    db.init(db_path, migrations)

    configs = [cfg_mod.load_config(str(a)), cfg_mod.load_config(str(b_path))]
    rt._started = False
    rt._instances = None
    rt._pool = None
    rt._dispatcher = None
    raised = False
    try:
        rt.start_events(configs, db_path=db_path, migrations_dir=migrations,
                         worker_count=1, cron_interval=3600)
    except ValueError as e:
        raised = "already claimed" in str(e)
    finally:
        rt.stop_events()
    assert raised, "expected ValueError on duplicate slack workspace"


if __name__ == "__main__":
    import tempfile
    tests = [test_endpoints_round_trip, test_multi_registers_all_instances,
             test_multi_hostname_routing, test_multi_rejects_duplicate_slack_workspace]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
