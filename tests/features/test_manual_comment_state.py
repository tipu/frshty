"""A comment nobody answered must not be written 'processed'.

features/own_prs.py flagged an ambiguous comment for a human and then called
mark_comment_processed on it in the next line. 'processed' is terminal: the
loop stopped tracking the comment while the thread stayed open on the pull
request and nobody had replied. 239 rows in the live database reached that
state. The non-terminal 'manual' state records the truth — the fixer will not
retry it, nothing counts it as answered, and the owed-comments selector always
shows it.
"""
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


def _comment(**overrides):
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
        "created_at": "2026-09-20T10:00:00Z",
        "updated_at": "2026-09-20T10:00:00Z",
    }
    base.update(overrides)
    return base


def _row(comment_id="500"):
    return db.query_one(
        "SELECT state, processed_at, last_error FROM comment_state"
        " WHERE instance_key='test' AND resource_type='pr' AND resource_id=?"
        " AND comment_id=?",
        (PR_KEY, comment_id),
    )


def _flag_manual(config, comment):
    with patch("features.own_prs.run_balanced",
               return_value='{"results": [{"id": 0, "actionable": false, '
                            '"reason": "open question"}]}'), \
         patch("features.own_prs.log"):
        own_prs._process_detected_comments(
            config, "test", MagicMock(), PR, "repo#7", "http://base",
            [comment], set(), {})


class TestNonActionableCommentStaysOpen:
    def test_it_is_not_terminal(self, fake_config):
        _flag_manual(fake_config, _comment())

        row = _row()
        assert row["state"] == "manual"
        assert row["processed_at"] is None

    def test_the_fixer_does_not_pick_it_up(self, fake_config):
        _flag_manual(fake_config, _comment())

        assert comments.get_unprocessed_comments("test", "pr", PR_KEY) == []
        assert comments.get_deferred_comments("test", "pr", PR_KEY) == []

    def test_it_does_not_count_as_answered(self, fake_config):
        _flag_manual(fake_config, _comment())

        assert comments.answered_comment_ids("test", "pr", PR_KEY) == set()
        assert comments.settled_comment_ids("test", "pr", PR_KEY) == set()

    def test_the_owed_comments_selector_returns_it(self, fake_config):
        _flag_manual(fake_config, _comment())
        state.save("own_prs", {PR_KEY: {"title": "PR 7", "url": "http://pr/7"}})

        out = staleness.blocked_pr_comments("test")

        assert [r["comment_id"] for r in out] == ["500"]
        assert out[0]["reason_kind"] == "manual"

    def test_a_reviewer_resolving_the_thread_clears_it(self, fake_config):
        _flag_manual(fake_config, _comment())

        own_prs._settle_manual_comments(
            "test", PR_KEY, {"500": _comment(resolved=True)})

        assert _row()["state"] == "processed"
        assert staleness.blocked_pr_comments("test") == []

    def test_a_deleted_comment_clears_it(self, fake_config):
        _flag_manual(fake_config, _comment())

        own_prs._settle_manual_comments("test", PR_KEY, {})

        assert _row()["state"] == "deleted"
        assert staleness.blocked_pr_comments("test") == []


class TestManualDoesNotReopenAnsweredThreads:
    """_reopen_answered_threads reopens a resolved thread that still owes an
    answer on some but not all of its comments. A manual comment is owed by a
    human, not by frshty, so a thread the reviewer resolved after our reply
    must stay resolved. Counting it as owed reopens that thread on every
    poll."""

    def test_a_manual_comment_does_not_reopen_its_thread(self, fake_config):
        _flag_manual(fake_config, _comment())
        thread = [
            _comment(resolved=True),
            _comment(id=501, author_id="bot-self", parent_id=500, resolved=True),
        ]

        reopened = own_prs._reopen_answered_threads(
            thread,
            comments.unowed_comment_ids("test", "pr", PR_KEY,
                                        {"500", "501"}),
            "bot-self")

        assert reopened == []
        assert [c["resolved"] for c in thread] == [True, True]

    def test_an_unanswered_comment_still_reopens_its_thread(self):
        """The control: with no manual row the same thread reopens, so the
        assertion above measures the manual state and not the shape of the
        thread."""
        thread = [
            _comment(resolved=True),
            _comment(id=501, author_id="bot-self", parent_id=500, resolved=True),
        ]

        reopened = own_prs._reopen_answered_threads(
            thread,
            comments.unowed_comment_ids("test", "pr", PR_KEY, {"500", "501"}),
            "bot-self")

        assert reopened != []
        assert [c["resolved"] for c in thread] == [False, False]
