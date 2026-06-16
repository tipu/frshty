"""Tests for staleness.blocked_pr_comments — surfacing PR comments frshty
tried to auto-fix but couldn't."""

import pytest

import core.db as db
import core.state as state
import manager.staleness as staleness


@pytest.fixture(autouse=True)
def _clear_tables():
    for t in ("comment_state", "kv"):
        try:
            db.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    yield


def _set_instance(key="test"):
    state._default_instance_key = key
    state._instance_key_cv.set(key)


def _insert_comment(resource_id, comment_id, state_val, error_count, last_error):
    db.execute(
        "INSERT INTO comment_state(instance_key, resource_type, resource_id,"
        " comment_id, last_checked_at, state, error_count, last_error)"
        " VALUES(?,?,?,?,?,?,?,?)",
        ("test", "pr", resource_id, comment_id, "2026-06-04T00:00:00Z",
         state_val, error_count, last_error),
    )


def test_surfaces_stuck_comment_with_pr_metadata():
    _set_instance()
    _insert_comment("saas-dashboard/147", "806275043", "new", 3, "Could not create worktree")
    state.save("own_prs", {"saas-dashboard/147": {"title": "DEV-457", "url": "http://pr/147"}})

    out = staleness.blocked_pr_comments("test")

    assert len(out) == 1
    row = out[0]
    assert row["repo"] == "saas-dashboard"
    assert row["pr_id"] == "147"
    assert row["comment_id"] == "806275043"
    assert row["attempts"] == 3
    assert row["reason"] == "Could not create worktree"
    assert row["title"] == "DEV-457"
    assert row["url"] == "http://pr/147"


def test_ignores_processed_and_below_threshold():
    _set_instance()
    _insert_comment("saas-dashboard/200", "c-processed", "processed", 5, "Could not create worktree")
    _insert_comment("saas-dashboard/201", "c-transient", "new", 1, "classification failed")

    out = staleness.blocked_pr_comments("test")

    assert out == []
