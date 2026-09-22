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


class TestReconcileCommentsNow:
    """The operator route reads the comments again. A reviewer can write a
    comment between the last poll and the click, and the keys the poll left
    on the ticket state would still say nothing is owed."""

    def _ts(self):
        return make_ticket_state(
            status="in_review", slug="PROJ-1-do-the-thing",
            prs=[{"repo": "repo", "id": 99, "branch": "b", "url": "http://u/99"}],
        )

    def _platform(self, comments):
        platform = MagicMock()
        platform.self_id.return_value = "bot-self"
        platform.get_pr_comments.return_value = comments
        return platform

    def _run(self, fake_config, comments, entries=()):
        with patch("features.tickets.make_platform",
                   return_value=self._platform(comments)), \
             patch("features.tickets._load_pr_comments", return_value=list(entries)), \
             patch("features.tickets.pr_comments_readable", return_value=True):
            return tickets.reconcile_comments_now(fake_config, self._ts())

    def test_a_comment_written_since_the_last_poll_is_owed(self, fake_config):
        hold, owed = self._run(fake_config, [_comment()])
        assert hold == "1 comment(s) owed an answer"
        assert owed[0]["snippet"] == "This needs a guard clause"

    def test_nothing_unresolved_releases_the_merge(self, fake_config):
        hold, owed = self._run(fake_config, [_comment(resolved=True)])
        assert hold == ""
        assert owed == []

    def test_a_failed_read_holds_the_merge(self, fake_config):
        hold, owed = self._run(fake_config, None)
        assert hold == "comment read failed"

    def test_an_error_holds_the_merge(self, fake_config):
        with patch("features.tickets.make_platform", side_effect=RuntimeError("boom")):
            hold, owed = tickets.reconcile_comments_now(fake_config, self._ts())
        assert hold.startswith("comment read failed")


class TestOperatorMergeRoute:
    """web/tickets.py calls _merge directly. Without its own check the
    operator route merges a ticket with comments owed and no confirmation."""

    def _call(self, ts, body, hold="", owed_comments=()):
        owed_comments = list(owed_comments)

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
                 patch("features.tickets.reconcile_comments_now",
                       return_value=(hold, owed_comments)), \
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

    OWED = ("1 comment(s) owed an answer",
            [{"author": "Reviewer", "loc": "app.py:3", "snippet": "needs a guard"}])

    def test_owed_comments_answer_409_and_do_not_merge(self):
        result, platform = self._call(self._ts(), {}, *self.OWED)

        assert result.status_code == 409
        assert result.body.decode().count("needs a guard") == 1
        platform.merge_pr.assert_not_called()

    def test_a_request_with_no_body_is_not_a_confirmation(self):
        result, platform = self._call(self._ts(), None, *self.OWED)

        assert result.status_code == 409
        platform.merge_pr.assert_not_called()

    def test_a_truthy_value_that_is_not_true_is_not_a_confirmation(self):
        """Only Boolean true confirms. A JSON string "false" is truthy in
        Python and would otherwise merge a ticket with comments owed."""
        for value in ("false", 1, "yes", [1]):
            result, platform = self._call(self._ts(), {"force": value}, *self.OWED)
            assert result.status_code == 409, value
            platform.merge_pr.assert_not_called()

    def test_an_explicit_force_merges(self):
        result, platform = self._call(self._ts(), {"force": True}, *self.OWED)

        assert result["status"] == "ok"
        platform.merge_pr.assert_called_once_with("repo", 99)

    def test_a_clean_reconciliation_merges_without_a_confirmation(self):
        result, platform = self._call(self._ts(), {})

        assert result["status"] == "ok"
        platform.merge_pr.assert_called_once_with("repo", 99)

    def test_a_failed_read_holds_the_operator_merge(self):
        result, platform = self._call(self._ts(), {}, "comment read failed", [])

        assert result.status_code == 409
        platform.merge_pr.assert_not_called()


def _entry(comment_id, status, repo="repo", pr_id=99, kind="review"):
    return {"id": comment_id, "status": status, "pr_repo": repo, "pr_id": pr_id,
            "comment_kind": kind}


def _platform_comment(comment_id, kind="review", **overrides):
    c = _comment(id=comment_id, **overrides)
    c["comment_kind"] = kind
    return c


def _owed(entries, unresolved, detected=()):
    """_owed_on_pr against the registry the poll would have built."""
    live = tickets._live_comment_index(unresolved)
    latest = tickets._latest_comment_entries(entries, "repo", 99, live)
    return tickets._owed_on_pr(list(unresolved), list(detected), [], latest)


class TestCommentsOwed:
    def test_a_detected_unresolved_comment_is_owed(self):
        c = _platform_comment(1)
        assert _owed([], [c], [c]) == 1

    def test_an_addressed_entry_cancels_an_earlier_open_entry(self):
        c = _platform_comment(1)
        entries = [_entry(1, "fix_failed"), _entry(1, "addressed")]
        assert _owed(entries, [c]) == 0

    def test_a_needs_reply_entry_stays_owed(self):
        """needs_reply settles the cursor — the loop will not touch the
        comment again — but a human still owes the reply, so the merge
        waits."""
        c = _platform_comment(1)
        assert _owed([_entry(1, "needs_reply")], [c]) == 1

    def test_a_fix_that_was_never_pushed_stays_owed(self):
        c = _platform_comment(1)
        assert _owed([_entry(1, "fix_unpushed")], [c]) == 1

    def test_a_non_resolvable_comment_frshty_addressed_is_not_owed(self):
        """An issue comment reads as unresolved on the platform forever.
        Only its entry can settle it."""
        c = _platform_comment(1, kind="issue_comment")
        assert _owed([_entry(1, "addressed", kind="issue_comment")], [c]) == 0

    def test_baselined_history_is_not_owed(self):
        """A comment the engine never registered was baselined. It must not
        hold every merge on the pull request for the life of the branch."""
        assert _owed([], [_platform_comment(7)]) == 0

    def test_one_number_from_two_comment_sources_is_two_comments(self):
        """A review comment and an issue comment on one pull request come
        from id sequences that overlap. The kind separates them; without it
        the addressed one releases the owed one."""
        issue = _platform_comment(7, kind="issue_comment")
        review = _platform_comment(7, kind="review", body="still wrong")
        entries = [_entry(7, "addressed", kind="issue_comment")]
        assert _owed(entries, [issue, review], [review]) == 1

    def test_a_reply_is_owed_even_though_the_page_lists_roots(self):
        reply = _platform_comment(2, parent_id=1)
        assert _owed([], [reply], [reply]) == 1
