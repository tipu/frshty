"""The checks window measures one CI run, and a failed read says why it failed.

A window opened on a head that never went green kept aging across every later
push, so a run six seconds old timed out "after 1663m". A failed read dropped
the gh stderr, so the event could not name its cause."""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import features.ticket_states as ticket_states
import features.tickets as tickets
from features.platforms import BitbucketPlatform, GitHubPlatform
from tests.conftest import make_ticket, make_ticket_state

OLD_WINDOW = "2020-01-01T00:00:00+00:00"
PR = {"repo": "r", "id": 1, "branch": "b", "url": "http://u/1"}


def _bb_platform():
    config = {"job": {"platform": "bitbucket"}, "bitbucket": {"org": "o"},
              "workspace": {"repos": []}}
    with patch("features.platforms.resolve_env", return_value="x"), \
         patch("features.platforms.get_repos", return_value=[]):
        return BitbucketPlatform(config)


def _gh_platform():
    config = {"job": {"platform": "github"}, "workspace": {"repos": []},
              "github": {"repo": "o/r"}}
    with patch("features.platforms.get_repos", return_value=[]):
        return GitHubPlatform(config)


def _handle(fake_config, ts, platform, head, info=None):
    with patch("features.tickets._scope_review_state", return_value="disabled"), \
         patch("features.tickets._build_pr_info_map",
               return_value=info or {("r", 1): {"head_sha": head}}), \
         patch("features.tickets.make_platform", return_value=platform), \
         patch("features.tickets._merge"), \
         patch("features.tickets._check_in_review", side_effect=lambda *a, **k: a[2]), \
         patch("features.tickets._enqueue_stage"), \
         patch("features.ticket_states.log"), \
         patch("features.tickets.log"), \
         patch("features.tickets.state"), \
         patch("features.ticket_states.state"):
        out, _ = ticket_states._handle_in_review_ticket(
            fake_config, make_ticket(), ts, "http://base", "inst", True)
    return out


class TestChecksWindowFollowsTheHead:
    def _ts(self, **extra):
        return make_ticket_state(status="in_review", prs=[dict(PR)],
                                 checks_started_at=OLD_WINDOW, **extra)

    def test_a_new_head_does_not_time_out_on_the_old_window(self, fake_config):
        p = _bb_platform()
        ts = self._ts(checks_heads={"r/1": "old"})
        checks = [{"name": "unit-tests", "state": "IN_PROGRESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log") as plog:
            out = _handle(fake_config, ts, p, "new")
        assert not [c for c in plog.emit.call_args_list if c.args[0] == "ticket_checks_timeout"]
        assert out["checks_started_at"] != OLD_WINDOW
        assert "_ci_timeout_state" not in out

    def test_a_new_head_with_no_checks_yet_is_not_passed_as_no_ci(self, fake_config):
        p = _bb_platform()
        ts = self._ts(checks_heads={"r/1": "old"})
        with patch.object(p, "get_pr_checks", return_value=[]), \
             patch("features.platforms.log"):
            out = _handle(fake_config, ts, p, "new")
        assert "ci_passed" not in out

    def test_the_same_head_keeps_the_window(self, fake_config):
        p = _bb_platform()
        ts = self._ts(checks_heads={"r/1": "same"})
        checks = [{"name": "unit-tests", "state": "IN_PROGRESS", "url": ""}]
        with patch.object(p, "get_pr_checks", return_value=checks), \
             patch("features.platforms.log") as plog:
            out = _handle(fake_config, ts, p, "same")
        assert out["checks_started_at"] == OLD_WINDOW
        assert [c for c in plog.emit.call_args_list if c.args[0] == "ticket_checks_timeout"]

    def test_an_unread_head_keeps_the_window(self, fake_config):
        heads = {"r/1": "same"}
        ts = self._ts(checks_heads=heads)
        platform = MagicMock()
        platform.monitor_ci.side_effect = lambda t, s, b: s
        out = _handle(fake_config, ts, platform, "")
        assert out["checks_started_at"] == OLD_WINDOW
        assert out["checks_heads"] == heads


    def test_an_unread_head_does_not_hide_a_move_on_another_pr(self, fake_config):
        other = {"repo": "s", "id": 2, "branch": "c", "url": "http://u/2"}
        ts = make_ticket_state(status="in_review", prs=[dict(PR), other],
                               checks_started_at=OLD_WINDOW,
                               checks_heads={"r/1": "old", "s/2": "kept"})
        platform = MagicMock()
        platform.monitor_ci.side_effect = lambda t, s, b: s
        out = _handle(fake_config, ts, platform, "",
                      info={("r", 1): {"head_sha": "new"}, ("s", 2): {}})
        assert "checks_started_at" not in out
        assert out["checks_heads"] == {"r/1": "new", "s/2": "kept"}


class TestRecoveryEndsTheWindow:
    def test_recovery_to_in_review_clears_the_window(self, fake_config):
        ts = make_ticket_state(status="pr_failed", prs=[dict(PR)], pr_failed_reason="pr_rejected",
                               checks_started_at=OLD_WINDOW,
                               _ci_timeout_state={"strike_count": "1"})
        platform = MagicMock()
        platform.get_pr_info.return_value = {"state": "OPEN", "approvers": []}
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.log"):
            out = tickets._recheck_pr_failed(
                fake_config, {"key": "PROJ-1", "summary": "", "url": "", "status": ""}, ts, "http://b")
        assert out["status"] == "in_review"
        assert "checks_started_at" not in out
        assert "_ci_timeout_state" not in out


class TestFailedReadNamesItsCause:
    DIAL_ERROR = 'Post "https://api.github.com/graphql": dial tcp 1.2.3.4:443: i/o timeout'

    def _emitted(self, plog, event):
        return [c.args[1] for c in plog.emit.call_args_list if c.args[0] == event]

    def test_github_comment_read_reports_the_gh_stderr(self):
        p = _gh_platform()
        failed = SimpleNamespace(returncode=1, stdout="", stderr=self.DIAL_ERROR)
        with patch.object(p, "_resolve_repo", return_value="o/r"), \
             patch.object(p, "_run_gh", return_value=failed), \
             patch("features.platforms.log") as plog:
            assert p.get_pr_comments("r", 1) is None
        assert any("i/o timeout" in s for s in self._emitted(plog, "gh_graphql_failed"))

    def test_github_comment_read_reports_invalid_json(self):
        p = _gh_platform()
        garbled = SimpleNamespace(returncode=0, stdout="<html>", stderr="")
        with patch.object(p, "_resolve_repo", return_value="o/r"), \
             patch.object(p, "_run_gh", return_value=garbled), \
             patch("features.platforms.log") as plog:
            assert p.get_pr_comments("r", 1) is None
        assert self._emitted(plog, "gh_graphql_failed")

    def test_github_checks_read_reports_the_gh_stderr(self):
        p = _gh_platform()
        failed = SimpleNamespace(returncode=1, stdout="", stderr=self.DIAL_ERROR)
        with patch.object(p, "_resolve_repo", return_value="o/r"), \
             patch.object(p, "_run_gh", return_value=failed), \
             patch("features.platforms.log") as plog:
            assert p.get_pr_checks("r", 1) is None
        assert any("i/o timeout" in s for s in self._emitted(plog, "gh_checks_read_failed"))

    def test_no_checks_reported_is_not_a_failed_read(self):
        p = _gh_platform()
        none = SimpleNamespace(returncode=1, stdout="", stderr="no checks reported on the branch")
        with patch.object(p, "_resolve_repo", return_value="o/r"), \
             patch.object(p, "_run_gh", return_value=none), \
             patch("features.platforms.log") as plog:
            assert p.get_pr_checks("r", 1) == []
        assert not plog.emit.called

    def test_bitbucket_reads_report_the_status(self):
        p = _bb_platform()
        client = MagicMock()
        client.__enter__.return_value.get.return_value = SimpleNamespace(status_code=503, text="busy")
        with patch("features.platforms.external_log.client", return_value=client), \
             patch("features.platforms.log") as plog:
            assert p.get_pr_comments("r", 1) is None
            assert p.get_pr_checks("r", 1) is None
        assert any("503" in s for s in self._emitted(plog, "bitbucket_comments_read_failed"))
        assert any("503" in s for s in self._emitted(plog, "bitbucket_checks_read_failed"))
