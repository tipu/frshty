"""Covers core/state.py sqlite backend.

Uses the session-scoped isolated DB from tests/conftest.py. Each test picks a
unique instance_key (via state.use) so rows don't collide across tests.
"""
import json

import core.db as db
import core.state as state
import core.log as log


def test_state_save_load_roundtrip(tmp_state):
    state.save("tickets", {"T-1": {"status": "new", "slug": "t-1"}})
    assert state.load("tickets") == {"T-1": {"status": "new", "slug": "t-1"}}

    state.save("tickets", {"T-1": {"status": "planning", "slug": "t-1"},
                            "T-2": {"status": "new"}})
    loaded = state.load("tickets")
    assert loaded["T-1"]["status"] == "planning"
    assert loaded["T-2"]["status"] == "new"

    beta_key = f"{tmp_state.name}-beta"
    tok = state.use(beta_key)
    try:
        assert state.load("tickets") == {}, "fresh instance_key should be empty"
        state.save("tickets", {"B-1": {"status": "new"}})
    finally:
        state.reset(tok)

    assert "B-1" not in state.load("tickets"), "alpha should not see beta rows"
    tok = state.use(beta_key)
    try:
        assert state.load("tickets") == {"B-1": {"status": "new"}}
    finally:
        state.reset(tok)


def test_log_contextvar_isolation(tmp_path):
    dir_a = tmp_path / "alpha"
    dir_b = tmp_path / "beta"
    (dir_a / "logs").mkdir(parents=True)
    (dir_b / "logs").mkdir(parents=True)

    state.init(dir_a)
    log.init(dir_a, "alpha")
    log.emit("event1", "from default alpha")

    state_token = state.use(dir_b)
    log_tokens = log.use(dir_b, "beta")
    try:
        log.emit("event2", "from contextvar beta")
    finally:
        log.reset(log_tokens)
        state.reset(state_token)

    log.emit("event3", "back to default alpha")

    a_rows = db.query_all(
        "SELECT event, job FROM log_events WHERE job=? AND event IN (?,?,?) ORDER BY ts",
        ("alpha", "event1", "event2", "event3"),
    )
    b_rows = db.query_all(
        "SELECT event, job FROM log_events WHERE job=? AND event IN (?,?,?) ORDER BY ts",
        ("beta", "event1", "event2", "event3"),
    )

    assert len(a_rows) == 2, f"alpha should have 2 entries, got {len(a_rows)}"
    assert len(b_rows) == 1, f"beta should have 1 entry, got {len(b_rows)}"
    assert a_rows[0]["event"] == "event1" and a_rows[0]["job"] == "alpha"
    assert a_rows[1]["event"] == "event3" and a_rows[1]["job"] == "alpha"
    assert b_rows[0]["event"] == "event2" and b_rows[0]["job"] == "beta"


def test_per_row_ticket_api(tmp_state):
    state.save_ticket("T-1", {"status": "planning", "slug": "T-1-s", "branch": "b"})
    assert state.load_ticket("T-1") == {"status": "planning", "slug": "T-1-s", "branch": "b"}

    state.update_ticket("T-1", lambda c: {**c, "ci_fix_attempts": (c.get("ci_fix_attempts", 0) + 1)})
    state.update_ticket("T-1", lambda c: {**c, "ci_fix_attempts": (c.get("ci_fix_attempts", 0) + 1)})
    assert state.load_ticket("T-1")["ci_fix_attempts"] == 2

    state.save_ticket("T-2", {"status": "new"})
    all_t = state.load("tickets")
    assert set(all_t.keys()) == {"T-1", "T-2"}

    state.update_ticket("T-1", lambda c: None)
    assert state.load_ticket("T-1") is None
    assert "T-1" not in state.load("tickets")


def test_kv_to_rows_migration_lazy(tmp_state):
    instance = tmp_state.name
    db.execute(
        "INSERT INTO kv(instance_key, key, data, updated_at) VALUES (?, ?, ?, datetime('now'))",
        (instance, "tickets", json.dumps({
            "OLD-1": {"status": "planning", "slug": "old-1"},
            "OLD-2": {"status": "in_review", "slug": "old-2", "ci_fix_attempts": 1},
        })),
    )
    state._TICKETS_MIGRATED.discard(instance)

    loaded = state.load("tickets")
    assert set(loaded.keys()) == {"OLD-1", "OLD-2"}
    assert state.load_ticket("OLD-2")["ci_fix_attempts"] == 1

    state.save_ticket("NEW-1", {"status": "new"})
    rows = db.query_all("SELECT ticket_key FROM tickets WHERE instance_key=?", (instance,))
    assert {r["ticket_key"] for r in rows} == {"OLD-1", "OLD-2", "NEW-1"}
