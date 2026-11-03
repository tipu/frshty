"""A fix push must not feed an endless automated re-review loop.

Observed live on quick-insights PR 25: the repository's review workflow
posts a full re-review on every push, and the ticket comment fixer fixed each
one and pushed again. The PR got 4 automated re-reviews in one hour. Only the
newest automated review per bot is current, and a PR gets at most
MAX_AUTOMATED_REVIEW_FIX_RUNS fix runs on automated reviews.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import core.comments as comments
import features.own_prs as own_prs

_WRITTEN = (datetime.now(timezone.utc) - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")


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
        "created_on": _WRITTEN,
        "created_at": _WRITTEN,
        "updated_at": _WRITTEN,
    }
    base.update(overrides)
    return base


def _human(cid, **overrides):
    return _review(cid, author_id="reviewer1", author_name="Reviewer",
                   author_is_bot=False, body=f"Human comment {cid}", **overrides)


def _bb_report(cid, **overrides):
    base = _review(cid, author_id="app-pipeline", author_name="pipeline: Code Insights",
                   body=f"## Code Insights report for commit {cid}", resolvable=True)
    del base["comment_kind"]
    base.update(overrides)
    return base


def _bb_passed(cid, **overrides):
    return _bb_report(cid, body=f"## \u2705 All 6 checks passed \u2014 `{cid}`\n\n"
                                "See the Reports panel on this PR for details.", **overrides)


def _bb_failed(cid, **overrides):
    return _bb_report(cid, body=f"## \u274c 1 of 6 checks failed \u2014 `{cid}`\n\n"
                                "- Dependency Vulnerabilities (production)", **overrides)


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

    def test_a_passed_pipeline_report_is_dropped_and_baselined(self):
        live = [_bb_report(10), _bb_passed(11), _human(12)]

        keep, seen, _ = self._drop(live, set())

        assert [c["id"] for c in keep] == [12]
        assert sorted(call.args[3] for call in seen.call_args_list) == ["10", "11"]

    def test_a_failed_pipeline_report_is_dropped_and_baselined(self):
        live = [_bb_passed(10), _bb_failed(11), _human(12)]

        keep, seen, _ = self._drop(live, set())

        assert [c["id"] for c in keep] == [12]
        assert sorted(call.args[3] for call in seen.call_args_list) == ["10", "11"]

    def test_a_person_with_the_id_of_a_superseded_review_is_kept(self):
        inline = _human(10, comment_kind=None, path="app.py", line=3, resolvable=True)
        live = [_review(10), _review(11), inline]

        keep, _, _ = self._drop(live, set())

        assert inline in keep

    def test_an_abandoned_failed_pipeline_report_is_settled_by_the_reclaim_pass(self):
        live = [_bb_failed(10)]
        pr = {"repo": "repo", "id": 99, "url": "http://u/99"}
        skips = own_prs._automated_review_skips(live, set())
        row = {"comment_id": "10", "state": "new", "error_count": 6,
               "last_checked_at": _WRITTEN}
        with patch.object(comments, "get_unprocessed_comments", return_value=[row]), \
             patch.object(comments, "mark_comment_seen") as seen, \
             patch.object(comments, "mark_comment_deferred") as deferred, \
             patch("features.own_prs.log") as log:
            own_prs._reclaim_stuck_comments("inst", pr, "repo/99", "repo#99", "http://base",
                                            {"10": live[0]}, "bot-self", set(), {}, None, skips)

        assert [call.args[3] for call in seen.call_args_list] == ["10"]
        deferred.assert_not_called()
        assert log.emit.call_args_list == []

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


