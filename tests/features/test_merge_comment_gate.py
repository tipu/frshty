"""A ticket must not merge before its comments have been reconciled.

Observed live as work item 9475: frshty merged LSC-78 while a review comment
was still open. features/ticket_states.py ran the auto-merge block before
_check_in_review in the same poll, so the merge decision was taken against a
comment list nobody had read yet, and web/tickets.py called _merge directly
with no gate at all. The gate now lives inside _merge, and the reconciliation
runs first and records what it found.
"""
import asyncio
from unittest.mock import MagicMock, patch

import features.ticket_states as ticket_states
import features.tickets as tickets
import web.state as web_state
import web.tickets as web_tickets
from tests.conftest import make_ticket, make_ticket_state


def _comment(**overrides):
    base = {
        "id": 100,
        "body": "This needs a guard clause",
        "author_id": "reviewer1",
        "author_name": "Reviewer",
        "path": "app.py",
        "line": 3,
        "parent_id": None,
        "resolved": False,
        "resolvable": True,
        "created_on": "2026-09-20T10:00:00Z",
        "created_at": "2026-09-20T10:00:00Z",
        "updated_at": "2026-09-20T10:00:00Z",
    }
    base.update(overrides)
    return base


class TestMergeWaitsForComments:
    def _platform(self, comments):
        platform = MagicMock()
        platform.self_id.return_value = "bot-self"
        platform.get_pr_info.return_value = {"state": "OPEN", "approvers": []}
        platform.get_pr_comments.return_value = comments
        platform.monitor_ci.side_effect = lambda t, s, b: {**s, "ci_passed": True}
        platform.merge_pr.return_value = {"status": "merged"}
        return platform

    def _handle(self, fake_config, platform):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(
            status="in_review", ci_passed=True,
            prs=[{"repo": "repo", "id": 99, "branch": "PROJ-1-do-the-thing",
                  "url": "http://u/99"}],
        )
        with patch("features.tickets._scope_review_state", return_value="disabled"), \
             patch("features.tickets._resolve_conflicts_pending", return_value=False), \
             patch("features.tickets._build_pr_info_map", return_value={}), \
             patch("features.tickets._has_conflicting_pr", return_value=False), \
             patch("features.tickets._pr_base_moved", return_value=False), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.ticket_states._t.make_platform", return_value=platform), \
             patch("features.tickets.get_repos", return_value=[]), \
             patch("features.tickets.run_balanced",
                   return_value='{"results": [{"i": 0, "actionable": false}]}'), \
             patch("features.tickets._draft_comment_reply", return_value="Noted."), \
             patch("features.tickets._substantiate_reply", return_value={}), \
             patch("features.tickets._load_pr_comments", return_value=[]), \
             patch("features.tickets._save_pr_comments"), \
             patch("features.tickets._enqueue_stage"), \
             patch("features.ticket_states.state"):
            out, _ = ticket_states._handle_in_review_ticket(
                fake_config, make_ticket(), ts, "http://base", "inst", True)
        return out

    def test_a_comment_posted_in_the_same_poll_blocks_the_merge(self, fake_config):
        platform = self._platform([_comment()])

        out = self._handle(fake_config, platform)

        platform.merge_pr.assert_not_called()
        assert out[tickets.RECONCILE_OWED_KEY] == 1
        assert out[tickets.RECONCILE_READ_KEY] is True

    def test_a_failed_comment_read_blocks_the_merge(self, fake_config):
        platform = self._platform(None)

        out = self._handle(fake_config, platform)

        platform.merge_pr.assert_not_called()
        assert out[tickets.RECONCILE_READ_KEY] is False

    def test_no_comments_still_merges(self, fake_config):
        """The control. Without it a gate stuck on 'hold' would read the same
        as a gate that works."""
        platform = self._platform([])

        out = self._handle(fake_config, platform)

        platform.merge_pr.assert_called_once_with("repo", 99)
        assert out[tickets.RECONCILE_OWED_KEY] == 0


class TestMergeHoldReason:
    def test_absent_keys_hold(self):
        assert tickets.merge_hold_reason({}) == "comments not reconciled"

    def test_failed_read_holds(self):
        ts = {tickets.RECONCILE_READ_KEY: False, tickets.RECONCILE_OWED_KEY: 0}
        assert tickets.merge_hold_reason(ts) == "comment read failed"

    def test_owed_comments_hold(self):
        ts = {tickets.RECONCILE_READ_KEY: True, tickets.RECONCILE_OWED_KEY: 2}
        assert tickets.merge_hold_reason(ts) == "2 comment(s) owed an answer"

    def test_a_clean_reconciliation_releases(self):
        ts = {tickets.RECONCILE_READ_KEY: True, tickets.RECONCILE_OWED_KEY: 0}
        assert tickets.merge_hold_reason(ts) == ""


class TestMergeGateInsideMerge:
    """The gate has to sit in _merge itself. web/tickets.py:1193 calls _merge
    directly, so a caller-side guard leaves the operator route open."""

    def _ts(self, **overrides):
        ts = make_ticket_state(
            status="in_review",
            prs=[{"repo": "repo", "id": 99, "branch": "b", "url": "http://u/99"}],
        )
        ts.update(overrides)
        return ts

    def _merge(self, fake_config, ts, force=False):
        platform = MagicMock()
        platform.merge_pr.return_value = {"status": "merged"}
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._mark_ticket_merged", side_effect=lambda c, t, s: s), \
             patch("features.tickets.log"):
            tickets._merge(fake_config, make_ticket(), ts, "http://base", force=force)
        return platform

    def test_unreconciled_state_is_not_merged(self, fake_config):
        platform = self._merge(fake_config, self._ts())
        platform.merge_pr.assert_not_called()

    def test_owed_comments_are_not_merged(self, fake_config):
        ts = self._ts(**{tickets.RECONCILE_READ_KEY: True,
                         tickets.RECONCILE_OWED_KEY: 1})
        platform = self._merge(fake_config, ts)
        platform.merge_pr.assert_not_called()

    def test_force_overrides_the_hold(self, fake_config):
        platform = self._merge(fake_config, self._ts(), force=True)
        platform.merge_pr.assert_called_once_with("repo", 99)


class TestOperatorMergeRoute:
    """web/tickets.py calls _merge directly. Without its own check the
    operator route merges a ticket with comments owed and no confirmation."""

    def _call(self, ts, body):
        class _Request:
            async def json(self):
                if body is None:
                    raise ValueError("no body")
                return body

        previous = web_state.primary_config()
        web_state.set_primary_config({"_base_url": "http://base"})
        platform = MagicMock()
        platform.merge_pr.return_value = {"status": "merged"}
        try:
            with patch("web.tickets.state.load_ticket", return_value=ts), \
                 patch("web.tickets.state.save_ticket"), \
                 patch("features.tickets.make_platform", return_value=platform), \
                 patch("features.tickets._mark_ticket_merged", side_effect=lambda c, t, s: s), \
                 patch("features.tickets.log"), \
                 patch("web.tickets.log"):
                result = asyncio.run(web_tickets.api_merge_ticket("PROJ-1", _Request()))
        finally:
            web_state.set_primary_config(previous)
        return result, platform

    def _ts(self, **overrides):
        ts = make_ticket_state(
            status="in_review",
            prs=[{"repo": "repo", "id": 99, "branch": "b", "url": "http://u/99",
                  "unresolved_comments": [{"author": "Reviewer", "loc": "app.py:3",
                                           "snippet": "needs a guard"}]}],
        )
        ts.update(overrides)
        return ts

    def test_owed_comments_answer_409_and_do_not_merge(self):
        ts = self._ts(**{tickets.RECONCILE_READ_KEY: True,
                         tickets.RECONCILE_OWED_KEY: 1})

        result, platform = self._call(ts, {})

        assert result.status_code == 409
        platform.merge_pr.assert_not_called()

    def test_a_request_with_no_body_is_not_a_confirmation(self):
        ts = self._ts(**{tickets.RECONCILE_READ_KEY: True,
                         tickets.RECONCILE_OWED_KEY: 1})

        result, platform = self._call(ts, None)

        assert result.status_code == 409
        platform.merge_pr.assert_not_called()

    def test_an_explicit_force_merges(self):
        ts = self._ts(**{tickets.RECONCILE_READ_KEY: True,
                         tickets.RECONCILE_OWED_KEY: 1})

        result, platform = self._call(ts, {"force": True})

        assert result["status"] == "ok"
        platform.merge_pr.assert_called_once_with("repo", 99)

    def test_a_clean_reconciliation_merges_without_a_confirmation(self):
        ts = self._ts(**{tickets.RECONCILE_READ_KEY: True,
                         tickets.RECONCILE_OWED_KEY: 0})

        result, platform = self._call(ts, {})

        assert result["status"] == "ok"
        platform.merge_pr.assert_called_once_with("repo", 99)


class TestCommentsOwed:
    def test_a_detected_unresolved_comment_is_owed(self):
        assert tickets._comments_owed([], {"1"}, {"1"}) == 1

    def test_an_addressed_entry_cancels_an_earlier_open_entry(self):
        entries = [{"id": 1, "status": "fix_failed"}, {"id": 1, "status": "addressed"}]
        assert tickets._comments_owed(entries, set(), {"1"}) == 0

    def test_a_needs_reply_entry_stays_owed(self):
        entries = [{"id": 1, "status": "needs_reply"}]
        assert tickets._comments_owed(entries, set(), {"1"}) == 1

    def test_a_resolved_comment_is_not_owed(self):
        """A non-resolvable comment frshty addressed reads as unresolved on
        the platform forever. Only an entry can settle it, and once it has
        one the merge is free."""
        entries = [{"id": 1, "status": "addressed"}]
        assert tickets._comments_owed(entries, {"1"}, {"1"}) == 0

    def test_baselined_history_is_not_owed(self):
        """A comment the engine never opened an entry for was baselined. It
        must not hold every merge on the PR for the life of the branch."""
        assert tickets._comments_owed([], set(), {"7"}) == 0
