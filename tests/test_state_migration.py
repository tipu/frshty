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


if __name__ == "__main__":
    tests = [test_state_save_load_roundtrip, test_migrate_state_script]
    for t in tests:
        with tempfile.TemporaryDirectory() as d:
            t(Path(d))
            print(f"{t.__name__}: PASS")
