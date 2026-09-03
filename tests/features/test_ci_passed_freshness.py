"""ci_passed must describe the current poll, not an earlier one.

The flag was written when every check went green and never cleared again.
features/ticket_states.py auto-merges on it, and manager/staleness.py reports
merge-ready on it, so a rerun that went pending, red or stalled left a ticket
merged and reported as green."""
from unittest.mock import MagicMock, patch

import features.ticket_states as ticket_states
import features.tickets as tickets
from features.platforms import BitbucketPlatform
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
             patch("features.tickets._check_in_review",
                   side_effect=lambda c, t, s, b, **kw: s), \
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
