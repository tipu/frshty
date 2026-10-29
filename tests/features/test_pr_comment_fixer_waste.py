"""The PR-comment fixer must not spend runs on comments that ask for nothing.

From 2026-09-01 to 2026-10-28, 100 of 153 fix_pr_comments runs failed on the
client instances. They came from 18 comment batches. A batch the fixer could
not change was charged as a failed attempt and ran again on the next poll, up
to five times. A comment whose triage failed went to the fixer untriaged, and
on one pull request it ran 27 times in a provider outage. Bot status reports
the triage read as needing no change sat in 'manual', and the morning digest
listed them as blocked work.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import core.comments as comments
import core.db as db
import core.state as state
import features.own_prs as own_prs
import manager.staleness as staleness


@pytest.fixture(autouse=True)
def _clear_tables():
    for table in ("comment_state", "kv"):
        try:
            db.execute(f"DELETE FROM {table}")
        except Exception:
            pass
    state._default_instance_key = "test"
    state._instance_key_cv.set("test")
    yield


PR = {"repo": "repo", "id": 7, "url": "http://pr/7", "branch": "b"}
PR_KEY = "repo/7"
NOT_ACTIONABLE = '{"results": [{"id": 0, "actionable": false, "reason": "status report"}]}'


def _now():
    return datetime.now(timezone.utc).isoformat()


def _comment(**overrides):
    stamp = _now()
    base = {
        "id": 500,
        "body": "Why did we pick this approach?",
        "author_id": "reviewer1",
        "author_name": "Reviewer",
        "path": "app.py",
        "line": 4,
        "parent_id": None,
        "resolved": False,
        "resolvable": True,
        "created_at": stamp,
        "updated_at": stamp,
    }
    base.update(overrides)
    return base


def _bot_report(**overrides):
    return _comment(id=600, author_id="github-actions", author_is_bot=True,
                    body="Job finished. Review steps complete.", path=None, line=None,
                    comment_kind="issue_comment", **overrides)


def _row(comment_id="500"):
    return db.query_one(
        "SELECT state, error_count, last_error FROM comment_state"
        " WHERE instance_key='test' AND resource_type='pr' AND resource_id=?"
        " AND comment_id=?",
        (PR_KEY, comment_id),
    )


def _triage(config, comment, answer=NOT_ACTIONABLE):
    with patch("features.own_prs.run_balanced", return_value=answer), \
         patch("features.own_prs.log"):
        own_prs._process_detected_comments(
            config, "test", MagicMock(), PR, "repo#7", "http://base",
            [comment], set(), {})


class TestABotReportIsNotBlockedWork:
    def test_a_bot_report_that_asks_for_nothing_is_settled(self, fake_config):
        _triage(fake_config, _bot_report())
        state.save("own_prs", {PR_KEY: {"title": "PR 7", "url": "http://pr/7"}})

        assert _row("600")["state"] == "processed"
        assert staleness.blocked_pr_comments("test") == []

    def test_a_pipeline_report_from_a_person_account_is_settled(self, fake_config):
        """A Bitbucket pipeline posts its report from a person's account."""
        report = _comment(id=601, body="## ✅ All 5 checks passed — `82866f7`")
        _triage(fake_config, report)

        assert _row("601")["state"] == "processed"

    def test_a_person_asking_for_nothing_still_goes_to_the_operator(self, fake_config):
        _triage(fake_config, _comment())
        state.save("own_prs", {PR_KEY: {"title": "PR 7", "url": "http://pr/7"}})

        assert _row()["state"] == "manual"
        assert [r["comment_id"] for r in staleness.blocked_pr_comments("test")] == ["500"]

    def test_a_bot_report_already_in_manual_leaves_on_the_next_poll(self):
        comments.mark_comment_processing("test", "pr", PR_KEY, "600", _now())
        comments.mark_comment_manual("test", "pr", PR_KEY, "600", "status report")

        own_prs._settle_manual_comments("test", PR_KEY, {"600": _bot_report()})

        assert _row("600")["state"] == "processed"


class TestAFailedTriageGoesBackToTriage:
    def _poll(self, config, platform, answer):
        with patch("features.own_prs.run_balanced", return_value=answer), \
             patch("features.own_prs.q.enqueue_job") as enqueue, \
             patch("features.own_prs.log"):
            own_prs._check_comments(config, "test", platform, PR, "http://base", seen={})
        return enqueue

    def test_the_fixer_never_sees_a_comment_nobody_triaged(self, fake_config):
        config = {**fake_config, "pr": {"comment_debounce_minutes": 0}}
        platform = MagicMock()
        platform.get_pr_comments.return_value = [_comment()]
        comments.mark_comment_seen("test", "pr", PR_KEY, "1", _now())

        first = self._poll(config, platform, None)
        assert _row()["last_error"] == own_prs.CLASSIFY_FAILED

        second = self._poll(config, platform, NOT_ACTIONABLE)

        first.assert_not_called()
        second.assert_not_called()
        assert _row()["state"] == "manual"

    def test_a_comment_triaged_as_a_request_after_a_failure_is_deferred(self, fake_config):
        platform = MagicMock()
        platform.get_pr_comments.return_value = [_comment(body="Rename this to count")]
        comments.mark_comment_seen("test", "pr", PR_KEY, "1", _now())

        self._poll(fake_config, platform, None)
        self._poll(fake_config, platform,
                   '{"results": [{"id": 0, "actionable": true, "reason": "rename"}]}')

        assert _row()["state"] == "deferred"


class TestARunThatChangesNothingRunsOnce:
    def _batch(self, tmp_path, fetched):
        platform = MagicMock()
        platform.get_pr_comments.return_value = fetched
        config = {"_state_dir": tmp_path, "_base_url": "http://base", "job": {"key": "test"}}
        for c in fetched:
            comments.mark_comment_processing("test", "pr", PR_KEY, str(c["id"]), c["updated_at"])
        with patch("features.own_prs.make_platform", return_value=platform), \
             patch("features.own_prs._ensure_worktree", return_value=tmp_path), \
             patch("features.own_prs._pre_dirty", return_value=set()), \
             patch("features.own_prs.git_util.head_sha", return_value="abc"), \
             patch("features.own_prs.run_claude_code", return_value="nothing to change"), \
             patch("features.own_prs._commit_fix", return_value=(False, own_prs.NO_CHANGES)), \
             patch("features.own_prs.log.emit"):
            return own_prs.fix_comments_batch(config, {"pr": PR, "comment_ids": [str(c["id"]) for c in fetched]})

    def test_the_comments_leave_the_retry_pool(self, tmp_path):
        ok, reason = self._batch(tmp_path, [_comment(), _bot_report(resolvable=False)])

        assert (ok, reason) == (False, own_prs.NO_CHANGES)
        assert _row()["state"] == "manual"
        assert _row()["last_error"] == own_prs.NO_CHANGE_REASON
        assert _row()["error_count"] == 0
        assert _row("600")["state"] == "processed"
        assert comments.get_unprocessed_comments("test", "pr", PR_KEY) == []
        assert comments.get_deferred_comments("test", "pr", PR_KEY) == []

