import pytest

from core.ticket_status import TicketStatus, transition, _ALLOWED


class TestTransitionValid:
    def test_new_to_planning(self):
        assert transition("new", "planning") == "planning"

    def test_planning_to_reviewing(self):
        assert transition("planning", "reviewing") == "reviewing"

    def test_reviewing_to_pr_ready(self):
        assert transition("reviewing", "pr_ready") == "pr_ready"

    def test_reviewing_to_planning(self):
        assert transition("reviewing", "planning") == "planning"

    def test_pr_ready_to_pr_created(self):
        assert transition("pr_ready", "pr_created") == "pr_created"

    def test_pr_ready_to_pr_failed(self):
        assert transition("pr_ready", "pr_failed") == "pr_failed"

    def test_pr_ready_to_merged(self):
        assert transition("pr_ready", "merged") == "merged"

    def test_pr_created_to_merged(self):
        assert transition("pr_created", "merged") == "merged"

    def test_pr_created_to_in_review(self):
        assert transition("pr_created", "in_review") == "in_review"

    def test_pr_created_to_pr_failed(self):
        assert transition("pr_created", "pr_failed") == "pr_failed"

    def test_in_review_to_merged(self):
        assert transition("in_review", "merged") == "merged"

    def test_in_review_to_pr_created(self):
        assert transition("in_review", "pr_created") == "pr_created"

    def test_in_review_self_loop(self):
        assert transition("in_review", "in_review") == "in_review"

    def test_in_review_to_pr_failed(self):
        assert transition("in_review", "pr_failed") == "pr_failed"

    def test_pr_failed_to_pr_ready(self):
        assert transition("pr_failed", "pr_ready") == "pr_ready"


class TestTransitionDone:
    @pytest.mark.parametrize("status", [s.value for s in TicketStatus])
    def test_done_reachable_from_any(self, status):
        assert transition(status, "done") == "done"


class TestTransitionIllegal:
    @pytest.mark.parametrize("current,target", [
        ("new", "merged"),
        ("new", "reviewing"),
        ("new", "pr_ready"),
        ("planning", "pr_ready"),
        ("planning", "merged"),
        ("pr_failed", "pr_created"),
        ("pr_failed", "merged"),
        ("merged", "new"),
        ("merged", "planning"),
    ])
    def test_illegal_raises(self, current, target):
        with pytest.raises(ValueError, match="Illegal transition"):
            transition(current, target)


class TestTransitionInvalidEnum:
    def test_invalid_current(self):
        with pytest.raises(ValueError):
            transition("nonexistent", "planning")

    def test_invalid_target(self):
        with pytest.raises(ValueError):
            transition("new", "nonexistent")


class TestAllowedGraph:
    def test_merged_is_terminal(self):
        assert _ALLOWED[TicketStatus.merged] == set()

    def test_done_is_terminal(self):
        assert _ALLOWED[TicketStatus.done] == set()

    def test_all_states_have_entries(self):
        for s in TicketStatus:
            assert s in _ALLOWED
