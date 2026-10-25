"""ci_passed must describe the current poll, not an earlier one.

The flag was written when every check went green and never cleared again.
features/ticket_states.py auto-merges on it, and manager/staleness.py reports
merge-ready on it, so a rerun that went pending, red or stalled left a ticket
merged and reported as green."""
from unittest.mock import MagicMock, patch

import core.state as state
import core.tasks.tickets as T
import features.ticket_states as ticket_states
import features.tickets as tickets
from features.platforms import BitbucketPlatform
from core.tasks.registry import TaskContext
from tests.conftest import make_ticket, make_ticket_state


def _bb_platform():
    config = {"job": {"platform": "bitbucket"}, "bitbucket": {"org": "o"},
              "workspace": {"repos": []}}
    with patch("features.platforms.resolve_env", return_value="x"), \
         patch("features.platforms.get_repos", return_value=[]):
        return BitbucketPlatform(config)


def _green_ts(**extra):
    ts = {"prs": [{"repo": "r", "id": 1, "url": "u"}], "status": "in_review",
          "ci_passed": True}
    ts.update(extra)
    return ts


class TestMonitorCI:
    def test_a_check_run_that_timed_out_clears_it(self):
        """The pending-timeout branch returns early, before the all_passed
        test, so it needs its own clear."""
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "INPROGRESS", "url": ""}]
        ts = _green_ts(checks_started_at="2020-01-01T00:00:00+00:00")
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, ts, "http://base")
        assert result.get("_ci_timeout_state", {}).get("strike_count") == "1"
        assert "ci_passed" not in result

    def test_a_fetch_that_raises_clears_it(self):
        """An unhandled platform exception used to abandon the whole ticket
        write, leaving the stored flag green until the next good poll."""
        p = _bb_platform()
        with patch.object(p, "get_pr_checks", side_effect=TimeoutError("network")), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, _green_ts(), "http://base")
        assert "ci_passed" not in result

    def test_a_rerun_that_went_pending_clears_it(self):
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "INPROGRESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, _green_ts(), "http://base")
        assert "ci_passed" not in result

    def test_a_fetch_that_returns_nothing_clears_it(self):
        p = _bb_platform()
        with patch.object(p, "get_pr_checks", return_value=None), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, _green_ts(), "http://base")
        assert "ci_passed" not in result

    def test_a_ticket_that_lost_its_prs_clears_it(self):
        """With no PR left there is no build to prove green. A stale flag
        also hid the ticket from the in_review_no_ci bucket, which selects on
        ci_passed being unset (manager/staleness.py:199)."""
        p = _bb_platform()
        with patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, _green_ts(prs=[]), "http://base")
        assert "ci_passed" not in result

    def test_the_ci_failure_handler_clears_it(self):
        ts = _green_ts()
        checks = [{"name": "Pipeline", "state": "FAILED", "url": ""}]
        with patch("features.tickets.log"), patch("features.tickets.state"):
            out = tickets._handle_ci_failure({"key": "DEV-1"}, ts, ts["prs"][0],
                                             checks, "http://base")
        assert "ci_passed" not in out

    def test_a_green_run_after_a_timeout_sets_it_again(self):
        """Negative control: clearing must not wedge the flag off."""
        p = _bb_platform()
        checks = [{"name": "Pipeline", "state": "SUCCESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, _green_ts(), "http://base")
        assert result["ci_passed"] is True


class TestUnrelatedHold:
    def test_a_held_pr_does_not_hide_a_real_failure_on_a_later_pr(self):
        p = _bb_platform()
        ts = _green_ts(prs=[{"repo": "r", "id": 1, "url": "u"},
                            {"repo": "s", "id": 2, "url": "v"}],
                       ci_unrelated_checks={"r/1": ["Pipeline"]})
        red = [{"name": "Pipeline", "state": "FAILED", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=red), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, ts, "http://base")
        assert result["_ci_failed"] is True
        assert result["pr"]["id"] == 2

    def test_a_ticket_held_only_on_unrelated_checks_is_flagged(self):
        p = _bb_platform()
        ts = _green_ts(prs=[{"repo": "r", "id": 1, "url": "u"},
                            {"repo": "s", "id": 2, "url": "v"}],
                       ci_unrelated_checks={"r/1": ["Pipeline"]})
        checks = {1: [{"name": "Pipeline", "state": "FAILED", "url": ""}],
                  2: [{"name": "Pipeline", "state": "SUCCESS", "url": ""}]}
        with patch.object(p, "get_pr_checks", side_effect=lambda repo, i: checks[i]), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, ts, "http://base")
        assert result["pr"]["id"] == 1
        with patch("features.tickets.log"), patch("features.tickets._enqueue_stage") as eq:
            out = tickets._handle_ci_failure({"key": "DEV-1"}, ts, result["pr"],
                                             result["checks"], "http://base", "inst")
        assert out["ci_held_unrelated"] is True
        assert "_ci_failed_pending" not in out
        eq.assert_not_called()

    def test_a_new_commit_on_a_later_held_pr_gets_a_fresh_triage(self):
        p = _bb_platform()
        ts = _green_ts(prs=[{"repo": "r", "id": 1, "url": "u"},
                            {"repo": "s", "id": 2, "url": "v"}],
                       ci_unrelated_checks={"r/1": ["build"], "s/2": ["build"]},
                       ci_fix_heads={"r/1": "a", "s/2": "b"})
        info = {("r", 1): {"head_sha": "a"}, ("s", 2): {"head_sha": "c"}}
        red = [{"name": "build", "state": "FAILED", "url": ""}]
        tickets._drop_stale_unrelated(ts, info)
        with patch.object(p, "get_pr_checks", return_value=red), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, ts, "http://base")
        assert result["pr"]["id"] == 2
        with patch("features.tickets.log"), patch("features.tickets._enqueue_stage") as eq, \
             patch("features.tickets.state"):
            out = tickets._handle_ci_failure({"key": "DEV-1"}, ts, result["pr"], red,
                                             "http://base", "inst", head_sha="c")
        assert "ci_held_unrelated" not in out
        assert out["_ci_failed_pending"] is True
        eq.assert_called_once_with("inst", "DEV-1", "fix_ci_failures")

    def test_an_unrecorded_head_keeps_the_verdict(self):
        ts = _green_ts(ci_unrelated_checks={"r/1": ["build"]})
        tickets._drop_stale_unrelated(ts, {("r", 1): {"head_sha": "c"}})
        assert ts["ci_unrelated_checks"] == {"r/1": ["build"]}

    def test_an_unrelated_verdict_records_the_head_it_judged(self, tmp_state):
        prs = [{"repo": "r", "id": 1, "url": "u"}, {"repo": "s", "id": 2, "url": "v"}]
        state.save_ticket("DEV-1", {"status": "in_review", "slug": "dev-1", "prs": prs,
                                    "_ci_failed_pending": True,
                                    "ci_fix_heads": {"r/1": "a"}})
        platform = MagicMock()
        platform.get_pr_info.side_effect = lambda repo, i: {"head_sha": {1: "a", 2: "b"}[i]}
        outcome = {"result": "unrelated", "failed_names": ["build"], "reason": "base"}
        ctx = TaskContext(instance_key="", ticket_key="DEV-1", task="fix_ci_failures",
                          payload={}, job_id=0, triggering_event_id=None,
                          config={"_base_url": "http://base"}, registry=None, now=None)
        with patch("core.tasks.tickets.make_platform", return_value=platform), \
             patch("core.tasks.tickets._ensure_pr_worktree", return_value="/wt"), \
             patch("features.pr_ci.triage_and_fix_pr", return_value=outcome), \
             patch("core.tasks.tickets.log"):
            T.fix_ci_failures(ctx)
        saved = state.load_ticket("DEV-1")
        assert saved["ci_fix_heads"] == {"r/1": "a", "s/2": "b"}
        assert saved["ci_unrelated_checks"] == {"r/1": ["build"], "s/2": ["build"]}

    def test_a_head_that_moves_during_triage_discards_the_verdict(self, tmp_state):
        prs = [{"repo": "r", "id": 1, "url": "u"}]
        state.save_ticket("DEV-1", {"status": "in_review", "slug": "dev-1", "prs": prs,
                                    "_ci_failed_pending": True})
        platform = MagicMock()
        platform.get_pr_info.side_effect = [{"head_sha": "h1"}, {"head_sha": "h2"}]
        outcome = {"result": "unrelated", "failed_names": ["build"], "reason": "base"}
        ctx = TaskContext(instance_key="", ticket_key="DEV-1", task="fix_ci_failures",
                          payload={}, job_id=0, triggering_event_id=None,
                          config={"_base_url": "http://base"}, registry=None, now=None)
        with patch("core.tasks.tickets.make_platform", return_value=platform), \
             patch("core.tasks.tickets._ensure_pr_worktree", return_value="/wt"), \
             patch("features.pr_ci.triage_and_fix_pr", return_value=outcome), \
             patch("core.tasks.tickets.log") as log:
            T.fix_ci_failures(ctx)
        saved = state.load_ticket("DEV-1")
        assert "ci_unrelated_checks" not in saved
        assert log.emit.call_args.args[0] == "ticket_checks_unrelated_discarded"

    def test_a_green_poll_forgets_the_unrelated_verdicts(self):
        p = _bb_platform()
        ts = _green_ts(ci_unrelated_checks={"r/1": ["build"]})
        green = [{"name": "build", "state": "SUCCESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=green), \
             patch("features.platforms.log"):
            result = p.monitor_ci({"key": "DEV-1"}, ts, "http://base")
        assert result["ci_passed"] is True
        assert "ci_unrelated_checks" not in result

    def test_a_failure_outside_the_unrelated_set_clears_the_flag(self):
        ts = _green_ts(ci_held_unrelated=True, ci_unrelated_checks={"r/1": ["lint"]})
        checks = [{"name": "tests", "state": "FAILED", "url": ""}]
        with patch("features.tickets.log"), patch("features.tickets._enqueue_stage"), \
             patch("features.tickets.state"):
            out = tickets._handle_ci_failure({"key": "DEV-1"}, ts, ts["prs"][0],
                                             checks, "http://base", "inst")
        assert "ci_held_unrelated" not in out
        assert out["_ci_failed_pending"] is True


def _reconciled(config, ticket, ts, base_url, **kwargs):
    """Stand in for _check_in_review with nothing owed.

    The merge gate refuses to merge when the reconciliation keys are absent,
    so a no-op patch here would assert that the merge is blocked by the gate
    rather than by the condition under test."""
    ts[tickets.RECONCILE_READ_KEY] = True
    ts[tickets.RECONCILE_OWED_KEY] = 0
    return ts


class TestAutoMergeIsNotFedStaleGreen:
    def _handle(self, fake_config, ts, ci_result):
        platform = MagicMock()
        platform.monitor_ci.return_value = ci_result
        with patch("features.tickets._scope_review_state", return_value="disabled"), \
             patch("features.tickets._resolve_conflicts_pending", return_value=False), \
             patch("features.tickets._build_pr_info_map", return_value={}), \
             patch("features.tickets._has_conflicting_pr", return_value=False), \
             patch("features.tickets._pr_base_moved", return_value=False), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._merge") as merge, \
             patch("features.tickets._check_in_review", side_effect=_reconciled), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.ticket_states.log"), \
             patch("features.tickets.log"), \
             patch("features.tickets.state"), \
             patch("features.ticket_states.state"):
            out, _ = ticket_states._handle_in_review_ticket(
                fake_config, make_ticket(), ts, "http://base", "inst", True)
        return out, merge

    def test_a_stalled_check_run_clears_it_and_blocks_the_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True)
        out, merge = self._handle(
            fake_config, ts, {"_ci_stalled": True, "pr": {"repo": "r", "id": 1}})
        assert "ci_passed" not in out
        merge.assert_not_called()

    def test_a_failed_check_run_blocks_the_merge(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True)
        _, merge = self._handle(
            fake_config, ts,
            {"_ci_failed": True, "pr": {"repo": "r", "id": 1},
             "checks": [{"name": "Pipeline", "state": "FAILED"}]})
        merge.assert_not_called()

    def test_a_green_poll_still_merges(self, fake_config):
        """Negative control: the gate must still let a real green through."""
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True)
        _, merge = self._handle(fake_config, ts, ts)
        merge.assert_called_once()
