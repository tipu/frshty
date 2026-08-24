from unittest.mock import MagicMock, patch

import features.tickets as tickets
import features.ticket_states as ticket_states
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

    def test_fail_holds_pr_without_enqueue(self, fake_config):
        ts = make_ticket_state(status="pr_ready")
        result, stop, eq, cp = self._handle(fake_config, ts, "fail")
        assert stop is True
        assert not any(c.args[2] == "scope_review" for c in eq.call_args_list)
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
             patch("features.tickets._check_in_review", side_effect=lambda c, t, s, b, **kw: s), \
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
