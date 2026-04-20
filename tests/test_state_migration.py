"""Covers core/state.py sqlite backend and scripts/migrate_state.py."""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def test_state_save_load_roundtrip(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.db as db
    import core.state as state

    db.init(tmp_path / "t.db", ROOT / "migrations")
    state.init("alpha")

    state.save("tickets", {"T-1": {"status": "new", "slug": "t-1"}})
    assert state.load("tickets") == {"T-1": {"status": "new", "slug": "t-1"}}

    state.save("tickets", {"T-1": {"status": "planning", "slug": "t-1"},
                            "T-2": {"status": "new"}})
    loaded = state.load("tickets")
    assert loaded["T-1"]["status"] == "planning"
    assert loaded["T-2"]["status"] == "new"

    tok = state.use("beta")
    try:
        assert state.load("tickets") == {}, "fresh instance_key should be empty"
        state.save("tickets", {"B-1": {"status": "new"}})
    finally:
        state.reset(tok)

    assert "B-1" not in state.load("tickets"), "alpha should not see beta rows"
    tok = state.use("beta")
    try:
        assert state.load("tickets") == {"B-1": {"status": "new"}}
    finally:
        state.reset(tok)


def test_migrate_state_script(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    root = tmp_path / "frshty-root"
    for key, tickets in (("inst-a", {"A-1": {"status": "new"}}),
                         ("inst-b", {"B-1": {"status": "merged"}, "B-2": {"status": "pr_ready"}})):
        inst_dir = root / key
        inst_dir.mkdir(parents=True)
        (inst_dir / "tickets.json").write_text(json.dumps(tickets))
        (inst_dir / "scheduler.json").write_text(json.dumps({f"{key}-sched": {"run_at": "2026-05-01"}}))

    db_path = tmp_path / "multi.db"

    r = subprocess.run([sys.executable, str(ROOT / "scripts" / "migrate_state.py"),
                         "--root", str(root), "--db", str(db_path)],
                        capture_output=True, text=True, timeout=30)
    assert r.returncode == 0, r.stderr
    assert "Done" in r.stdout

    import core.db as db
    import core.state as state
    db.init(db_path, ROOT / "migrations")
    state.init("inst-a")
    assert state.load("tickets") == {"A-1": {"status": "new"}}
    assert state.load("scheduler") == {"inst-a-sched": {"run_at": "2026-05-01"}}

    tok = state.use("inst-b")
    try:
        loaded = state.load("tickets")
        assert loaded == {"B-1": {"status": "merged"}, "B-2": {"status": "pr_ready"}}
    finally:
        state.reset(tok)


def test_log_contextvar_isolation(tmp_path):
    for mod in list(sys.modules):
        if mod == "frshty" or mod.startswith("core.") or mod == "core":
            sys.modules.pop(mod, None)

    import core.log as log

    dir_a = tmp_path / "alpha"
    dir_b = tmp_path / "beta"
    (dir_a / "logs").mkdir(parents=True)
    (dir_b / "logs").mkdir(parents=True)

    log.init(dir_a, "alpha")
    log.emit("event1", "from default alpha")

    tokens = log.use(dir_b, "beta")
    try:
        log.emit("event2", "from contextvar beta")
    finally:
        log.reset(tokens)

    log.emit("event3", "back to default alpha")

    a_lines = (dir_a / "logs" / "alpha.jsonl").read_text().splitlines()
    b_lines = (dir_b / "logs" / "beta.jsonl").read_text().splitlines()

    assert len(a_lines) == 2, f"alpha should have 2 entries, got {len(a_lines)}"
    assert len(b_lines) == 1, f"beta should have 1 entry, got {len(b_lines)}"
    a0 = json.loads(a_lines[0])
    a1 = json.loads(a_lines[1])
    b0 = json.loads(b_lines[0])
    assert a0["event"] == "event1" and a0["job"] == "alpha"
    assert a1["event"] == "event3" and a1["job"] == "alpha"
    assert b0["event"] == "event2" and b0["job"] == "beta"


if __name__ == "__main__":
    tests = [test_state_save_load_roundtrip, test_migrate_state_script,
             test_log_contextvar_isolation]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
