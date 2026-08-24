"""Regression: the scan dispatcher holds a ticket snapshot across a long
dispatch window while worker tasks transition the ticket concurrently.
Writing the stale snapshot back reverted start_planning's gate-locked
new->planning transition, which let a sibling ticket pass the per-repo
serialization gate, branch from the same base commit, and wedge the shared
clone on a conflicting merge (the flake in
tests/integration/test_ticket_repo_serialization_e2e.py).

_save_ticket_if_unmoved drops a snapshot the stored status cannot legally
follow, and keeps every write the single-threaded dispatch flow performs."""
import core.state as state
from features.tickets import _save_ticket_if_unmoved


def test_stale_snapshot_does_not_revert_concurrent_transition(tmp_log):
    state.save_ticket("RACE-1", {"status": "new", "source": "prd"})
    snapshot = state.load_ticket("RACE-1")
    state.transition_ticket("RACE-1", "planning")

    written = _save_ticket_if_unmoved("RACE-1", snapshot, "new")

    assert written is False
    assert state.load_ticket("RACE-1")["status"] == "planning"


def test_unmoved_ticket_accepts_snapshot_write(tmp_log):
    state.save_ticket("RACE-2", {"status": "new"})
    snapshot = state.load_ticket("RACE-2")
    snapshot["summary"] = "updated by scan"

    written = _save_ticket_if_unmoved("RACE-2", snapshot, "new")

    assert written is True
    assert state.load_ticket("RACE-2")["summary"] == "updated by scan"


def test_snapshot_advancing_past_own_midsave_writes(tmp_log):
    state.save_ticket("RACE-3", {"status": "pr_ready", "slug": "s", "branch": "b"})
    snapshot = state.load_ticket("RACE-3")
    snapshot["status"] = "in_review"
    state.save_ticket("RACE-3", dict(snapshot))
    snapshot["status"] = "merged"
    snapshot["merged_external_status"] = "Done"

    written = _save_ticket_if_unmoved("RACE-3", snapshot, "pr_ready")

    assert written is True
    assert state.load_ticket("RACE-3")["status"] == "merged"


def test_snapshot_for_missing_ticket_writes(tmp_log):
    written = _save_ticket_if_unmoved("RACE-4", {"status": "new"}, "new")

    assert written is True
    assert state.load_ticket("RACE-4")["status"] == "new"


def test_status_change_by_snapshot_itself_writes(tmp_log):
    state.save_ticket("RACE-5", {"status": "done", "merged_external_status": "Done",
                                 "prs": [{"repo": "app", "id": 1}]})
    snapshot = state.load_ticket("RACE-5")
    snapshot["status"] = "in_review"

    written = _save_ticket_if_unmoved("RACE-5", snapshot, "done")

    assert written is True
    assert state.load_ticket("RACE-5")["status"] == "in_review"
