from unittest.mock import MagicMock, patch

import features.tickets as tickets
import features.ticket_states as ticket_states
from core.consensus_scope import MAX_SCOPE_FIX_ATTEMPTS
from tests.conftest import make_ticket, make_ticket_state


class TestScopeReviewState:
    def test_disabled_when_feature_off(self, fake_config):
        assert tickets._scope_review_state(fake_config, make_ticket_state()) == "disabled"

    def test_disabled_when_no_fingerprint(self, fake_config):
        fake_config["features"]["scope_review"] = True
        with patch("core.consensus_scope.scope_fingerprint", return_value=""):
            assert tickets._scope_review_state(fake_config, make_ticket_state()) == "disabled"

    def test_pending_without_record(self, fake_config):
        fake_config["features"]["scope_review"] = True
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:abc"):
            assert tickets._scope_review_state(fake_config, make_ticket_state()) == "pending"

    def test_pending_on_fingerprint_mismatch(self, fake_config):
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(scope_review={"fingerprint": "r:old", "verdict": "pass"})
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:abc"):
            assert tickets._scope_review_state(fake_config, ts) == "pending"

    def test_pass_on_current_pass_verdict(self, fake_config):
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(scope_review={"fingerprint": "r:abc", "verdict": "pass"})
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:abc"):
            assert tickets._scope_review_state(fake_config, ts) == "pass"

    def test_fail_on_current_fail_verdict(self, fake_config):
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(scope_review={"fingerprint": "r:abc", "verdict": "fail"})
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:abc"):
            assert tickets._scope_review_state(fake_config, ts) == "fail"


class TestAnUnfinishedCorrectionHoldsTheGate:
    """A correction pass that started and did not finish leaves the branch
    carrying what the reviewers named, or work a blocked commit left loose. A
    later review of that state can pass, so the gate reads the mark instead."""

    def test_an_open_correction_reads_as_fail_over_a_recorded_pass(self, fake_config):
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(
            scope_review={"fingerprint": "r:abc", "verdict": "pass"},
            scope_fix={"attempts": 1, "incomplete": "a commit was blocked part way"})
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:abc"):
            assert tickets._scope_review_state(fake_config, ts) == "fail"

    def test_an_open_correction_holds_even_with_no_branch_diff(self, fake_config):
        """A correction can take the last of the branch diff off. Reading that
        as 'disabled' would open every gate on a ticket whose PR still carries
        the change the reviewers named."""
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(
            scope_fix={"attempts": 1,
                       "incomplete": "the removal did not reach the PR branch"})
        with patch("core.consensus_scope.scope_fingerprint", return_value=""):
            assert tickets._scope_review_state(fake_config, ts) == "fail"

    def test_a_branch_that_moved_asks_for_a_review_not_another_correction(self, fake_config):
        """The findings name file and line. While the report describes code
        that is no longer on the branch, a correction pass has nothing to act
        on and skips, so answering 'fail' here would queue one every poll."""
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(
            scope_review={"fingerprint": "r:old", "verdict": "fail"},
            scope_fix={"attempts": 1, "incomplete": "a commit was blocked part way"})
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:new"):
            assert tickets._scope_review_state(fake_config, ts) == "pending"

    def test_a_finished_correction_lets_the_verdict_stand(self, fake_config):
        fake_config["features"]["scope_review"] = True
        ts = make_ticket_state(
            scope_review={"fingerprint": "r:abc", "verdict": "pass"},
            scope_fix={"attempts": 1})
        with patch("core.consensus_scope.scope_fingerprint", return_value="r:abc"):
            assert tickets._scope_review_state(fake_config, ts) == "pass"


class TestPrReadyScopeGate:
    def _handle(self, fake_config, ts, scope):
        with patch("features.tickets._scope_review_state", return_value=scope), \
             patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets._create_pr",
                   side_effect=lambda c, t, s, b: {**s, "status": "in_review"}) as cp, \
             patch("features.ticket_states.state"):
            result, stop = ticket_states._handle_pr_ready_ticket(
                fake_config, make_ticket(), ts, "http://base", "inst", True)
        return result, stop, eq, cp

    def test_pending_enqueues_review_and_holds_pr(self, fake_config):
        ts = make_ticket_state(status="pr_ready")
        result, stop, eq, cp = self._handle(fake_config, ts, "pending")
        assert stop is True
        eq.assert_any_call("inst", "PROJ-1", "scope_review")
        cp.assert_not_called()

    def test_fail_holds_the_pr_and_queues_the_correction(self, fake_config, tmp_state):
        """A FAIL names the changes that do not serve the ticket. frshty takes
        them off the branch itself; it used to leave them for the operator."""
        ts = make_ticket_state(status="pr_ready")
        result, stop, eq, cp = self._handle(fake_config, ts, "fail")
        assert stop is True
        assert not any(c.args[2] == "scope_review" for c in eq.call_args_list)
        eq.assert_any_call("inst", "PROJ-1", "fix_scope_findings")
        cp.assert_not_called()

    def test_fail_with_the_correction_budget_spent_queues_nothing(self, fake_config, tmp_state):
        """Two corrections that did not change the verdict mean the reviewers
        and the fixer disagree. That is the operator's call, so the gate goes
        back to holding the branch."""
        ts = make_ticket_state(status="pr_ready",
                               scope_fix={"attempts": MAX_SCOPE_FIX_ATTEMPTS})
        result, stop, eq, cp = self._handle(fake_config, ts, "fail")
        assert stop is True
        assert not any(c.args[2] == "fix_scope_findings" for c in eq.call_args_list)
        cp.assert_not_called()

    def test_pass_creates_pr(self, fake_config):
        ts = make_ticket_state(status="pr_ready")
        with patch("features.tickets._repo_gate_blocked", return_value=None):
            result, stop, eq, cp = self._handle(fake_config, ts, "pass")
        cp.assert_called_once()

    def test_disabled_creates_pr(self, fake_config):
        ts = make_ticket_state(status="pr_ready")
        with patch("features.tickets._repo_gate_blocked", return_value=None):
            result, stop, eq, cp = self._handle(fake_config, ts, "disabled")
        cp.assert_called_once()


class TestTheCorrectionBudgetIsDurable:
    """The budget bounds how much LLM work one FAIL verdict can start. A poll
    cycle writes back the ticket it loaded after it enqueues, so a count kept
    only on the ticket can be erased by that stale copy. The job rows cannot."""

    def _finished_runs(self, key, count):
        import core.queue as q
        for _ in range(count):
            job_id = q.enqueue_job("inst", "fix_scope_findings", ticket_key=key)
            q.mark_done(job_id, "ok", {})

    def test_the_job_history_bounds_the_correction(self, fake_config, tmp_state):
        self._finished_runs("BUDGET-SPENT", MAX_SCOPE_FIX_ATTEMPTS)
        ts = make_ticket_state(status="pr_ready")
        with patch("features.tickets._repo_gate_blocked", return_value=None):
            assert tickets._enqueue_scope_fix("inst", "BUDGET-SPENT", ts) is None

    def test_the_mark_reaches_the_copy_the_poll_writes_back(self, fake_config, tmp_state):
        """The poll hands its own ticket dict to the enqueue and saves that
        dict afterwards. A mark written only to the row would be erased by
        that save, and the mark is what holds the branch when a pass dies."""
        import core.state as state
        ts = make_ticket_state(status="pr_ready")
        state.save_ticket("MARKED-1", dict(ts))
        with patch("features.tickets._repo_gate_blocked", return_value=None):
            assert tickets._enqueue_scope_fix("inst", "MARKED-1", ts) is not None
        assert tickets._scope_fix_incomplete(ts)
        state.save_ticket("MARKED-1", ts)
        assert tickets._scope_fix_incomplete(state.load_ticket("MARKED-1"))

    def test_a_skipped_pass_spends_nothing(self, fake_config, tmp_state):
        """A pass that skipped ran no model. Counting it would spend the budget
        on the branch moving under a queued job."""
        import core.queue as q
        for _ in range(MAX_SCOPE_FIX_ATTEMPTS):
            job_id = q.enqueue_job("inst", "fix_scope_findings", ticket_key="BUDGET-SKIP")
            q.mark_done(job_id, "skipped", {})
        ts = make_ticket_state(status="pr_ready")
        with patch("features.tickets._repo_gate_blocked", return_value=None):
            assert tickets._enqueue_scope_fix("inst", "BUDGET-SKIP", ts) is not None

    def test_a_ticket_under_the_budget_still_gets_its_correction(self, fake_config, tmp_state):
        self._finished_runs("BUDGET-LEFT", MAX_SCOPE_FIX_ATTEMPTS - 1)
        ts = make_ticket_state(status="pr_ready")
        with patch("features.tickets._repo_gate_blocked", return_value=None):
            assert tickets._enqueue_scope_fix("inst", "BUDGET-LEFT", ts) is not None


def _reconciled(config, ticket, ts, base_url, **kwargs):
    """Stand in for _check_in_review with nothing owed.

    The merge gate refuses to merge when the reconciliation keys are absent,
    so a no-op patch here would assert that the merge is blocked by the gate
    rather than by the condition under test."""
    ts[tickets.RECONCILE_READ_KEY] = True
    ts[tickets.RECONCILE_OWED_KEY] = 0
    return ts


class TestInReviewScopeGate:
    def _handle(self, fake_config, ts, scope):
        platform = MagicMock()
        platform.monitor_ci.return_value = ts
        with patch("features.tickets._scope_review_state", return_value=scope), \
             patch("features.tickets._resolve_conflicts_pending", return_value=False), \
             patch("features.tickets._build_pr_info_map", return_value={}), \
             patch("features.tickets._has_conflicting_pr", return_value=False), \
             patch("features.tickets._pr_base_moved", return_value=False), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._merge") as merge, \
             patch("features.tickets._check_in_review", side_effect=_reconciled), \
             patch("features.tickets._enqueue_stage") as eq, \
             patch("features.ticket_states.state"):
            ticket_states._handle_in_review_ticket(
                fake_config, make_ticket(), ts, "http://base", "inst", True)
        return eq, merge

    def test_pending_enqueues_review(self, fake_config):
        ts = make_ticket_state(status="in_review")
        eq, merge = self._handle(fake_config, ts, "pending")
        eq.assert_any_call("inst", "PROJ-1", "scope_review")

    def test_fail_blocks_auto_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True)
        eq, merge = self._handle(fake_config, ts, "fail")
        merge.assert_not_called()

    def test_pass_allows_auto_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True)
        eq, merge = self._handle(fake_config, ts, "pass")
        merge.assert_called_once()

    def test_disabled_allows_auto_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True)
        eq, merge = self._handle(fake_config, ts, "disabled")
        merge.assert_called_once()


class TestScheduledCreatePrScopeGate:
    def test_held_when_review_not_passed(self, fake_config, tmp_state):
        import core.scheduler as scheduler
        import core.state as state
        state.save_ticket("PROJ-1", make_ticket_state(
            status="pr_ready", pr_scheduled_at="2026-01-01T00:00:00+00:00"))
        with patch("features.tickets._scope_review_state", return_value="pending"), \
             patch("features.tickets._create_pr") as cp:
            scheduler._execute_create_pr("PROJ-1", {}, fake_config)
        cp.assert_not_called()
        assert "pr_scheduled_at" not in state.load_ticket("PROJ-1")

    def test_runs_when_review_passed(self, fake_config, tmp_state):
        import core.scheduler as scheduler
        import core.state as state
        state.save_ticket("PROJ-1", make_ticket_state(status="pr_ready"))
        system = MagicMock()
        system.fetch_tickets.return_value = [make_ticket()]
        with patch("features.tickets._scope_review_state", return_value="pass"), \
             patch("features.ticket_systems.make_ticket_system", return_value=system), \
             patch("features.tickets._create_pr",
                   side_effect=lambda c, t, s, b: {**s, "status": "in_review"}) as cp:
            scheduler._execute_create_pr("PROJ-1", {}, fake_config)
        cp.assert_called_once()
