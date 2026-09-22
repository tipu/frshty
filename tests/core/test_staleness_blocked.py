"""Tests for staleness.blocked_pr_comments — surfacing PR comments still
owed an answer."""

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


def _insert_comment(resource_id, comment_id, state_val, error_count, last_error,
                    last_checked_at="2026-06-04T00:00:00Z"):
    db.execute(
        "INSERT INTO comment_state(instance_key, resource_type, resource_id,"
        " comment_id, last_checked_at, state, error_count, last_error)"
        " VALUES(?,?,?,?,?,?,?,?)",
        ("test", "pr", resource_id, comment_id, last_checked_at,
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
    assert row["reason_kind"] == "failing"
    assert row["title"] == "DEV-457"
    assert row["url"] == "http://pr/147"


def test_ignores_processed():
    _set_instance()
    _insert_comment("saas-dashboard/200", "c-processed", "processed", 5, "Could not create worktree")

    assert staleness.blocked_pr_comments("test") == []


def test_a_single_failure_is_still_owed():
    """The floor this selector used to carry was error_count >= 2. Thirteen
    comments on saas-dashboard/149 sat in 'new' with error_count=1 from
    2026-06-02, never retried and never shown. One failure is owed."""
    _set_instance()
    _insert_comment("saas-dashboard/149", "c-transient", "new", 1, "classification failed")

    out = staleness.blocked_pr_comments("test")

    assert [r["comment_id"] for r in out] == ["c-transient"]
    assert out[0]["reason_kind"] == "failing"


def test_a_row_with_no_error_is_owed_as_stalled():
    """The other half of the old floor: a recorded error. A row nothing ever
    touched has none, and was invisible for that reason alone."""
    _set_instance()
    _insert_comment("saas-dashboard/150", "c-quiet", "new", 0, None)

    out = staleness.blocked_pr_comments("test")

    assert [r["comment_id"] for r in out] == ["c-quiet"]
    assert out[0]["reason_kind"] == "stalled"
    assert out[0]["reason"] == "no attempt recorded"


def test_a_manual_comment_is_owed_and_named_manual():
    _set_instance()
    _insert_comment("saas-dashboard/151", "c-manual", "manual", 0, "open question")

    out = staleness.blocked_pr_comments("test")

    assert [r["comment_id"] for r in out] == ["c-manual"]
    assert out[0]["reason_kind"] == "manual"


def test_a_deleted_comment_leaves_the_bucket():
    """A comment gone from the platform keeps the error_count of its last live
    attempt and can never reach 'processed'. Reading only 'processed' as
    finished pins it in the bucket for good."""
    _set_instance()
    _insert_comment("saas-dashboard/300", "c-deleted", "deleted", 6, "no changes produced")

    assert staleness.blocked_pr_comments("test") == []
