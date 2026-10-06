"""A fix push must not feed an endless automated re-review loop.

Observed live on rapid-analytics PR 25: the repository's review workflow
posts a full re-review on every push, and the ticket comment fixer fixed each
one and pushed again. The PR got 4 automated re-reviews in one hour. Only the
newest automated review per bot is current, and a PR gets at most
MAX_AUTOMATED_REVIEW_FIX_RUNS fix runs on automated reviews.
"""
from unittest.mock import MagicMock, patch

import core.comments as comments
import features.own_prs as own_prs
import features.tickets as tickets
from tests.conftest import make_ticket_state


def _review(cid, **overrides):
    base = {
        "id": cid,
        "body": f"PR re-review {cid}: fix the guard",
        "author_id": "github-actions",
        "author_name": "github-actions",
        "author_is_bot": True,
        "path": None,
        "line": None,
        "parent_id": None,
        "resolved": False,
        "resolvable": False,
        "comment_kind": "issue_comment",
        "created_on": "2026-10-06T20:00:00Z",
        "created_at": "2026-10-06T20:00:00Z",
        "updated_at": "2026-10-06T20:00:00Z",
    }
    base.update(overrides)
    return base


def _human(cid, **overrides):
    return _review(cid, author_id="reviewer1", author_name="Reviewer",
                   author_is_bot=False, body=f"Human comment {cid}", **overrides)


def _entry(cid, status):
    return {"id": cid, "comment_kind": "issue_comment", "pr_repo": "repo", "pr_id": 99,
            "body": f"PR re-review {cid}: fix the guard", "status": status,
            "created_at": "2026-10-06T20:00:00Z"}


def _key(c):
    return tickets._comment_key(c)


def _ts():
    return make_ticket_state(
        status="in_review",
        prs=[{"repo": "repo", "id": 99, "branch": "PROJ-1-do-the-thing", "url": "http://u/99"}],
    )


class TestAutomatedReviewSkips:
    def test_older_reviews_from_the_same_bot_are_superseded(self):
        live = [_review(1), _review(2), _review(3)]

        skips = comments.automated_review_skips(live, _key, set())

        assert skips == {_key(live[0]): "superseded", _key(live[1]): "superseded"}

    def test_the_newest_review_is_current_below_the_cap(self):
        live = [_review(1), _review(2)]

        skips = comments.automated_review_skips(live, _key, {_key(live[0])})

        assert _key(live[1]) not in skips

    def test_the_newest_review_is_capped_at_the_cap(self):
        live = [_review(1), _review(2), _review(3)]
        fixed = {_key(live[0]), _key(live[1])}

        skips = comments.automated_review_skips(live, _key, fixed)

        assert skips[_key(live[2])] == "capped"

    def test_a_review_that_had_its_fix_run_is_not_capped(self):
        live = [_review(1), _review(2)]

        skips = comments.automated_review_skips(live, _key, {_key(live[0]), _key(live[1])})

        assert _key(live[1]) not in skips

    def test_people_and_inline_bot_findings_are_never_skipped(self):
        live = [_human(1), _human(2),
                _review(3, comment_kind=None, path="app.py", line=4, resolvable=True),
                _review(4, comment_kind=None, path="app.py", line=9, resolvable=True)]

        assert comments.automated_review_skips(live, _key, set()) == {}


class TestTicketCommentPass:
    def _run(self, fake_config, live, registered=()):
        ticket = {"key": "PROJ-1", "summary": "Do thing", "url": "http://j/PROJ-1"}
        platform = MagicMock()
        platform.self_id.return_value = "bot-self"
        platform.get_pr_info.return_value = {"state": "OPEN", "approvers": []}
        platform.get_pr_comments.return_value = live
        classify = MagicMock(return_value='{"results": [{"i": 0, "actionable": false}]}')
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.get_repos", return_value=[]), \
             patch("features.tickets.run_balanced", classify), \
             patch("features.tickets._draft_comment_reply", return_value="Noted."), \
             patch("features.tickets._substantiate_reply", return_value={}), \
             patch("features.tickets._load_pr_comments", return_value=list(registered)), \
             patch("features.tickets._save_pr_comments"), \
             patch("features.tickets.log") as log:
            out = tickets._check_in_review(fake_config, ticket, _ts(), "http://base")
        prompts = [call.args[0] for call in classify.call_args_list
                   if call.args[0].startswith("Triage")]
        events = [call.args[0] for call in log.emit.call_args_list]
        return out, prompts, events

    def test_only_the_newest_automated_review_is_triaged(self, fake_config):
        out, prompts, events = self._run(fake_config, [_review(10), _review(11), _review(12)])

        assert len(prompts) == 1
        assert "PR re-review 12" in prompts[0]
        assert "PR re-review 10" not in prompts[0]
        assert "PR re-review 11" not in prompts[0]
        assert "ticket_pr_automated_review_skipped" in events

    def test_a_review_after_the_cap_is_not_triaged_and_not_owed(self, fake_config):
        live = [_review(10), _review(11), _review(12)]
        registered = [_entry(10, "addressed"), _entry(11, "addressed")]

        out, prompts, events = self._run(fake_config, live, registered)

        assert prompts == []
        assert out[tickets.RECONCILE_OWED_KEY] == 0
        assert "ticket_pr_automated_review_skipped" in events

    def test_a_superseded_failed_review_does_not_hold_the_merge(self, fake_config):
        live = [_review(10), _review(11)]
        registered = [{**_entry(10, "fix_failed"), "attempts": 2}, _entry(11, "addressed")]

        out, prompts, _ = self._run(fake_config, live, registered)

        assert prompts == []
        assert out[tickets.RECONCILE_OWED_KEY] == 0

    def test_a_person_is_still_triaged_after_the_cap(self, fake_config):
        """The control: the cap reaches automated reviews only."""
        live = [_review(10), _review(11), _review(12), _human(13)]
        registered = [_entry(10, "addressed"), _entry(11, "addressed")]

        out, prompts, _ = self._run(fake_config, live, registered)

        assert len(prompts) == 1
        assert "Human comment 13" in prompts[0]
        assert "PR re-review 12" not in prompts[0]
        assert out[tickets.RECONCILE_OWED_KEY] == 1


class TestReconcileNow:
    def _reason(self, fake_config, live, registered):
        platform = MagicMock()
        platform.self_id.return_value = "bot-self"
        platform.get_pr_comments.return_value = live
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._load_pr_comments", return_value=registered):
            return tickets.reconcile_comments_now(fake_config, _ts())[0]

    def test_a_superseded_failed_review_is_not_owed(self, fake_config):
        live = [_review(10), _review(11)]
        registered = [{**_entry(10, "fix_failed"), "attempts": 2}, _entry(11, "addressed")]

        assert self._reason(fake_config, live, registered) == ""

    def test_a_capped_review_is_not_owed(self, fake_config):
        live = [_review(10), _review(11), _review(12)]
        registered = [_entry(10, "addressed"), _entry(11, "addressed"),
                      _entry(12, "needs_reply")]

        assert self._reason(fake_config, live, registered) == ""

    def test_the_current_review_below_the_cap_is_owed(self, fake_config):
        """The control: the gate still holds on the one current review."""
        live = [_review(10), _review(11)]
        registered = [_entry(10, "addressed"), _entry(11, "needs_reply")]

        assert self._reason(fake_config, live, registered) == "1 comment(s) owed an answer"


class TestOwnPrs:
    def _drop(self, live, answered):
        pr = {"repo": "repo", "id": 99, "url": "http://u/99"}
        skips = own_prs._automated_review_skips(live, answered)
        with patch.object(comments, "mark_comment_seen") as seen, \
             patch("features.own_prs.log") as log:
            keep = own_prs._drop_skipped_automated_reviews(
                "inst", pr, "repo/99", "repo#99", "http://base", list(live), skips)
        return keep, seen, log

    def test_superseded_and_capped_reviews_are_dropped_and_baselined(self):
        live = [_review(10), _review(11), _review(12), _human(13)]

        keep, seen, log = self._drop(live, {"10", "11"})

        assert [c["id"] for c in keep] == [13]
        assert sorted(call.args[3] for call in seen.call_args_list) == ["10", "11", "12"]
        assert log.emit.call_args.args[0] == "pr_automated_review_skipped"

    def test_below_the_cap_only_the_superseded_review_is_dropped(self):
        live = [_review(10), _review(11)]

        keep, _, _ = self._drop(live, set())

        assert [c["id"] for c in keep] == [11]

    def test_a_person_with_the_id_of_a_superseded_review_is_kept(self):
        inline = _human(10, comment_kind=None, path="app.py", line=3, resolvable=True)
        live = [_review(10), _review(11), inline]

        keep, _, _ = self._drop(live, set())

        assert inline in keep

    def test_a_deferred_review_that_is_now_superseded_is_not_queued(self):
        live = [_review(10), _review(11)]
        pr = {"repo": "repo", "id": 99, "url": "http://u/99"}
        by_id = {str(c["id"]): c for c in live}
        skips = own_prs._automated_review_skips(live, set())
        with patch.object(comments, "get_deferred_comments",
                          return_value=[{"comment_id": "10"}, {"comment_id": "11"}]), \
             patch.object(comments, "mark_comment_seen") as seen, \
             patch.object(comments, "mark_comment_processing"), \
             patch("features.own_prs.q") as queue, \
             patch("features.own_prs.log"):
            own_prs._flush_deferred_comments({}, "inst", pr, "repo/99", "repo#99", "http://base",
                                             by_id, {}, None, skips)

        assert queue.enqueue_job.call_args.kwargs["payload"]["comment_ids"] == ["11"]
        assert [call.args[3] for call in seen.call_args_list] == ["10"]

    def test_a_queued_review_that_is_now_superseded_is_not_fixed(self):
        live = [_review(10), _review(11)]
        platform = MagicMock()
        platform.get_pr_comments.return_value = live
        config = {"job": {"key": "inst"}, "_base_url": "http://base"}
        payload = {"pr": {"repo": "repo", "id": 99, "url": "http://u/99"}, "comment_ids": [10]}
        with patch("features.own_prs.make_platform", return_value=platform), \
             patch.object(comments, "answered_comment_ids", return_value=set()), \
             patch.object(comments, "unowed_comment_ids", return_value=set()), \
             patch.object(comments, "mark_comment_seen") as seen, \
             patch("features.own_prs._self_id", return_value="bot-self"), \
             patch("features.own_prs._ensure_worktree") as worktree:
            ok, detail = own_prs.fix_comments_batch(config, payload)

        assert (ok, detail) == (True, "nothing left to fix")
        assert seen.call_args.args[3] == "10"
        worktree.assert_not_called()


class TestKindCollisions:
    def test_a_person_with_the_key_of_a_superseded_review_body_is_triaged(self, fake_config):
        inline = _human(10, comment_kind=None, path="app.py", line=3, resolvable=True)
        live = [_review(10, comment_kind="review_body"), _review(11, comment_kind="review_body"),
                inline]

        out, prompts, _ = TestTicketCommentPass()._run(fake_config, live)

        assert "Human comment 10" in prompts[0]
