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

    def test_pr_ready_to_in_review(self):
        assert transition("pr_ready", "in_review") == "in_review"

    def test_pr_ready_to_pr_failed(self):
        assert transition("pr_ready", "pr_failed") == "pr_failed"

    def test_pr_ready_to_merged(self):
        assert transition("pr_ready", "merged") == "merged"

    def test_in_review_to_merged(self):
        assert transition("in_review", "merged") == "merged"

    def test_in_review_self_loop(self):
        assert transition("in_review", "in_review") == "in_review"

    def test_in_review_to_pr_failed(self):
        assert transition("in_review", "pr_failed") == "pr_failed"

    def test_pr_failed_to_pr_ready(self):
        assert transition("pr_failed", "pr_ready") == "pr_ready"

    def test_pr_failed_to_in_review(self):
        assert transition("pr_failed", "in_review") == "in_review"


class TestTransitionDone:
    @pytest.mark.parametrize("status", [s.value for s in TicketStatus])
    def test_done_reachable_from_any(self, status):
        assert transition(status, "done") == "done"


class TestTransitionIllegal:
    @pytest.mark.parametrize("current,target", [
        ("new", "reviewing"),
        ("new", "pr_ready"),
        ("planning", "pr_ready"),
        ("planning", "merged"),
        ("merged", "planning"),
        ("epic", "planning"),  # epics are terminal — can only reach done
        ("epic", "reviewing"),
    ])
    def test_illegal_raises(self, current, target):
        with pytest.raises(ValueError, match="Illegal transition"):
            transition(current, target)


class TestSelfTransition:
    """A transition to the status you're already at must be a no-op, not a raise.

    The task registry's on_entry_status fires every time a task is claimed,
    so e.g. start_planning on a ticket already at 'planning' (from a prior
    failed run) would otherwise raise and block every retry.
    """

    @pytest.mark.parametrize("status", [s.value for s in TicketStatus])
    def test_self_transition_is_legal(self, status):
        assert transition(status, status) == status


class TestTransitionLegalized:
    @pytest.mark.parametrize("current,target", [
        ("pr_ready", "reviewing"), # rewind to re-review before opening PR
        ("pr_failed", "merged"),   # manual match-state
        ("merged", "new"),         # requeue
        ("done", "new"),           # revive on upstream reopen
        ("done", "pr_ready"),      # revive with slug
        ("done", "in_review"),     # revive with PRs
        ("new", "merged"),         # pre-merged short-circuit (existing PRs)
        ("new", "epic"),           # issue_type=Epic detected at discovery time
    ])
    def test_now_legal(self, current, target):
        assert transition(current, target) == target


class TestTransitionInvalidEnum:
    def test_invalid_current(self):
        with pytest.raises(ValueError):
            transition("nonexistent", "planning")

    def test_invalid_target(self):
        with pytest.raises(ValueError):
            transition("new", "nonexistent")


class TestAllowedGraph:
    def test_merged_proceeds_to_validation_or_recovers(self):
        assert _ALLOWED[TicketStatus.merged] == {TicketStatus.validation, TicketStatus.new}

    def test_validation_finishes_or_recovers(self):
        assert _ALLOWED[TicketStatus.validation] == {TicketStatus.done, TicketStatus.new}

    def test_pending_approval_approve_or_reject(self):
        assert _ALLOWED[TicketStatus.pending_approval] == {TicketStatus.new, TicketStatus.done}

    def test_done_revivals(self):
        assert _ALLOWED[TicketStatus.done] == {TicketStatus.new, TicketStatus.pr_ready, TicketStatus.testing, TicketStatus.proving, TicketStatus.in_review}

    def test_all_states_have_entries(self):
        for s in TicketStatus:
            assert s in _ALLOWED


class TestTestingState:
    """New tests state inserted between reviewing and pr_ready.

    Acceptance criteria 1-2, 9 from docs/technical-plan.md.
    """

    def test_testing_status_in_enum(self):
        assert TicketStatus.testing.value == "testing"

    def test_tests_failed_status_in_enum(self):
        assert TicketStatus.tests_failed.value == "tests_failed"

    def test_reviewing_to_testing(self):
        assert transition("reviewing", "testing") == "testing"

    def test_reviewing_to_pr_ready_still_legal_for_backward_compat(self):
        """Pre-feature in-flight tickets and manual operator overrides need
        this edge preserved."""
        assert transition("reviewing", "pr_ready") == "pr_ready"

    def test_testing_to_pr_ready(self):
        assert transition("testing", "pr_ready") == "pr_ready"

    def test_testing_to_tests_failed(self):
        assert transition("testing", "tests_failed") == "tests_failed"

    def test_testing_to_reviewing_for_regress(self):
        """Manual operator path: send back to reviewing if the test plan
        revealed a real design issue."""
        assert transition("testing", "reviewing") == "reviewing"

    def test_tests_failed_to_testing_retry(self):
        assert transition("tests_failed", "testing") == "testing"

    def test_tests_failed_to_reviewing(self):
        assert transition("tests_failed", "reviewing") == "reviewing"

    def test_tests_failed_to_pr_ready_force_through(self):
        """Operator decision: ship without tests."""
        assert transition("tests_failed", "pr_ready") == "pr_ready"

    def test_tests_failed_to_done(self):
        assert transition("tests_failed", "done") == "done"

    def test_pr_ready_to_testing_manual_recycle(self):
        """Operator can send a ready ticket back through tests."""
        assert transition("pr_ready", "testing") == "testing"

    def test_done_to_testing_for_revival(self):
        """Mirrors existing `done → pr_ready` revival edge."""
        assert transition("done", "testing") == "testing"

    def test_illegal_testing_to_planning(self):
        """testing only routes to pr_ready, tests_failed, reviewing —
        never directly back to planning."""
        with pytest.raises(ValueError, match="Illegal transition"):
            transition("testing", "planning")

    def test_illegal_planning_to_testing(self):
        """planning still only flows to reviewing."""
        with pytest.raises(ValueError, match="Illegal transition"):
            transition("planning", "testing")

    def test_illegal_new_to_testing(self):
        with pytest.raises(ValueError, match="Illegal transition"):
            transition("new", "testing")


class TestProvingState:
    """New proving state inserted between testing and pr_ready when a
    PROOF.md exists at workspace.root. AC 11."""

    def test_proving_status_in_enum(self):
        assert TicketStatus.proving.value == "proving"

    def test_testing_to_proving(self):
        assert transition("testing", "proving") == "proving"

    def test_testing_to_pr_ready_still_legal_for_skip_path(self):
        """When PROOF.md is absent, enter_proving routes testing → pr_ready
        directly without entering the proving state."""
        assert transition("testing", "pr_ready") == "pr_ready"

    def test_proving_to_pr_ready(self):
        assert transition("proving", "pr_ready") == "pr_ready"

    def test_proving_to_testing_for_regress(self):
        """Operator path: send back to testing if proof reveals a real
        product issue."""
        assert transition("proving", "testing") == "testing"

    def test_proving_to_reviewing_for_regress(self):
        assert transition("proving", "reviewing") == "reviewing"

    def test_pr_ready_to_proving_manual_recycle(self):
        assert transition("pr_ready", "proving") == "proving"

    def test_done_to_proving_revival(self):
        assert transition("done", "proving") == "proving"

    def test_illegal_proving_to_planning(self):
        with pytest.raises(ValueError, match="Illegal transition"):
            transition("proving", "planning")

    def test_illegal_new_to_proving(self):
        with pytest.raises(ValueError, match="Illegal transition"):
            transition("new", "proving")

    def test_illegal_reviewing_to_proving(self):
        """Proving is only reached via testing (with a passing test run),
        never directly from reviewing."""
        with pytest.raises(ValueError, match="Illegal transition"):
            transition("reviewing", "proving")


class TestFullMatrix:
    """Parametrized over every (src, dst) pair. A transition is legal iff:
      - dst == src (self-transition), or
      - dst == done (always), or
      - dst is in _ALLOWED[src].
    This test locks the graph: adding/removing edges in core/ticket_status.py
    will require updating expectations here, making graph changes intentional.
    """

    @pytest.mark.parametrize(
        "src,dst",
        [(s.value, d.value) for s in TicketStatus for d in TicketStatus],
    )
    def test_pair(self, src, dst):
        src_enum = TicketStatus(src)
        dst_enum = TicketStatus(dst)
        expected_legal = (
            dst_enum == src_enum
            or dst_enum == TicketStatus.done
            or dst_enum in _ALLOWED.get(src_enum, set())
        )
        if expected_legal:
            assert transition(src, dst) == dst
        else:
            with pytest.raises(ValueError, match="Illegal transition"):
                transition(src, dst)
