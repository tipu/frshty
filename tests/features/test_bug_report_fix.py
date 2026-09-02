"""The reported-bug loop: a QA comment on a ticket has to reach the fixer,
the fixer has to prove it changed code, and the comment must not be closed
until it did.

DEV-635 is the case these cover. A QA note listing four named failures was
detected, a fix_reported_bug job ran for six minutes, reported ok, produced no
commit, and closed the comment. The prompt never carried the note.
"""
from unittest.mock import MagicMock, patch

import pytest

import core.comments as comments
import core.state as state
import features.tickets as tickets
from core.tasks.registry import TaskContext
from core.tasks.tickets import (
    GENERIC_BUG_REPORT_PROMPT, _bug_report_prompt, fix_reported_bug,
)

from tests.conftest import make_ticket


QA_NOTE = (
    "DEV-635 QA Testing Notes. Test 3 — Write column can be turned on and off: "
    "FAIL. When you add a folder and click allow write, the Write check box is "
    "still unchecked after Add."
)


class TestBugReportPrompt:
    def test_the_prompt_quotes_the_report(self):
        prompt = _bug_report_prompt("DEV-635", "File explorer tool", [
            {"comment_id": "17285", "author": "Dan Brisco",
             "created_at": "2026-09-01T18:22:03+00:00",
             "body": QA_NOTE, "reason": "QA found the write toggle does not save"},
        ])
        assert QA_NOTE in prompt
        assert "Dan Brisco" in prompt
        assert "File explorer tool" in prompt
        assert "QA found the write toggle does not save" in prompt

    def test_every_report_reaches_the_prompt(self):
        prompt = _bug_report_prompt("DEV-635", "File explorer tool", [
            {"comment_id": "1", "body": "first failure", "author": "Dan"},
            {"comment_id": "2", "body": "second failure", "author": "Dan"},
        ])
        assert "first failure" in prompt
        assert "second failure" in prompt

    def test_no_reports_falls_back_to_the_generic_prompt(self):
        assert _bug_report_prompt("DEV-635", "x", []) == GENERIC_BUG_REPORT_PROMPT


def _ctx(tmp_path, payload=None, slug="DEV-635-file-explorer-tool"):
    (tmp_path / "tickets" / slug / "workspace").mkdir(parents=True)
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "_base_url": "http://localhost:8000",
    }
    return TaskContext(
        instance_key="aimyable", ticket_key="DEV-635", task="fix_reported_bug",
        payload=payload if payload is not None else {}, job_id=0,
        triggering_event_id=None, config=config, registry=None, now=None,
    )


REPORTS = [{"comment_id": "17285", "author": "Dan Brisco", "body": QA_NOTE,
            "reason": "the write toggle does not save"}]


class TestFixReportedBugProvesItsWork:
    def test_a_run_that_commits_nothing_fails_and_reopens_the_comment(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS, "ticket_summary": "File explorer tool"})
        heads = {"saas-dashboard": "aaa"}
        with patch("core.state.load_ticket", return_value={"slug": "DEV-635-file-explorer-tool"}), \
             patch("core.tasks.tickets._capture_repo_heads", return_value=heads), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=[]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"), \
             patch("core.tasks.tickets.comments.mark_comment_error") as err, \
             patch("core.tasks.tickets.comments.mark_comment_processed") as done:
            result = fix_reported_bug(ctx)
        assert result.status == "failed"
        assert "no code change" in result.reason
        err.assert_called_once()
        assert err.call_args.args[3] == "17285"
        done.assert_not_called()

    def test_a_run_that_commits_passes_and_closes_the_comment(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS, "ticket_summary": "File explorer tool"})
        moved = iter([{"saas-dashboard": "aaa"}, {"saas-dashboard": "bbb"}])
        with patch("core.state.load_ticket", return_value={"slug": "DEV-635-file-explorer-tool"}), \
             patch("core.tasks.tickets._capture_repo_heads", side_effect=lambda _d: next(moved)), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"), \
             patch("core.tasks.tickets.comments.mark_comment_error") as err, \
             patch("core.tasks.tickets.comments.mark_comment_processed") as done:
            result = fix_reported_bug(ctx)
        assert result.status == "ok"
        assert result.artifacts["repos"] == ["saas-dashboard"]
        done.assert_called_once()
        err.assert_not_called()

    def test_the_report_is_what_claude_is_asked_to_fix(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS, "ticket_summary": "File explorer tool"})
        moved = iter([{"r": "aaa"}, {"r": "bbb"}])
        with patch("core.state.load_ticket", return_value={"slug": "DEV-635-file-explorer-tool"}), \
             patch("core.tasks.tickets._capture_repo_heads", side_effect=lambda _d: next(moved)), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=["r"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done") as rc, \
             patch("core.tasks.tickets.comments.mark_comment_processed"):
            fix_reported_bug(ctx)
        assert QA_NOTE in rc.call_args.args[0]

    def test_a_dead_claude_run_reopens_the_comment(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS})
        with patch("core.state.load_ticket", return_value={"slug": "DEV-635-file-explorer-tool"}), \
             patch("core.tasks.tickets._capture_repo_heads", return_value={}), \
             patch("core.tasks.tickets.run_claude_code", return_value=None), \
             patch("core.tasks.tickets.comments.mark_comment_error") as err:
            result = fix_reported_bug(ctx)
        assert result.status == "failed"
        err.assert_called_once()


class TestFixReportedBugPushesToThePr:
    def test_a_committed_fix_reaches_the_open_pr_branch(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS})
        slug = "DEV-635-file-explorer-tool"
        wt = tmp_path / "tickets" / slug / "workspace" / "saas-dashboard"
        wt.mkdir(parents=True)
        ticket_state = {
            "slug": slug, "branch": "danial/feature/DEV-635",
            "prs": [{"repo": "saas-dashboard", "id": 248,
                     "branch": "danial/feature/DEV-635"}],
        }
        moved = iter([{"saas-dashboard": "aaa"}, {"saas-dashboard": "bbb"}])
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": True}
        with patch("core.state.load_ticket", return_value=ticket_state), \
             patch("core.tasks.tickets._capture_repo_heads", side_effect=lambda _d: next(moved)), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"), \
             patch("core.tasks.tickets.ticket_worktree_path", return_value=wt), \
             patch("core.tasks.tickets.make_platform", return_value=platform), \
             patch("core.tasks.tickets.comments.mark_comment_processed"):
            result = fix_reported_bug(ctx)
        assert result.artifacts["pushed"] == ["saas-dashboard"]
        platform.push_branch.assert_called_once_with(wt, "danial/feature/DEV-635")

    def test_a_ticket_with_no_pr_is_left_unpushed(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS})
        moved = iter([{"saas-dashboard": "aaa"}, {"saas-dashboard": "bbb"}])
        platform = MagicMock()
        with patch("core.state.load_ticket",
                   return_value={"slug": "DEV-635-file-explorer-tool", "prs": []}), \
             patch("core.tasks.tickets._capture_repo_heads", side_effect=lambda _d: next(moved)), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"), \
             patch("core.tasks.tickets.make_platform", return_value=platform), \
             patch("core.tasks.tickets.comments.mark_comment_processed"):
            result = fix_reported_bug(ctx)
        assert result.artifacts["pushed"] == []
        platform.push_branch.assert_not_called()

    def test_a_failed_push_is_reported_not_swallowed(self, tmp_path):
        ctx = _ctx(tmp_path, {"reports": REPORTS})
        slug = "DEV-635-file-explorer-tool"
        wt = tmp_path / "tickets" / slug / "workspace" / "saas-dashboard"
        wt.mkdir(parents=True)
        moved = iter([{"saas-dashboard": "aaa"}, {"saas-dashboard": "bbb"}])
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": False, "error": "rejected"}
        with patch("core.state.load_ticket",
                   return_value={"slug": slug, "branch": "b",
                                 "prs": [{"repo": "saas-dashboard", "id": 248}]}), \
             patch("core.tasks.tickets._capture_repo_heads", side_effect=lambda _d: next(moved)), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"), \
             patch("core.tasks.tickets.ticket_worktree_path", return_value=wt), \
             patch("core.tasks.tickets.make_platform", return_value=platform), \
             patch("core.tasks.tickets.comments.mark_comment_processed"):
            result = fix_reported_bug(ctx)
        assert result.artifacts["push_failed"] == ["saas-dashboard"]
        assert result.status == "ok"


COMMENT = {
    "id": "17285",
    "author_name": "Dan Brisco",
    "body": QA_NOTE,
    "created_at": "2026-09-01T18:22:03+00:00",
    "updated_at": "2026-09-01T18:22:03+00:00",
}


def _run_poll(fake_config, comment, ts, detection=None, unsettled=None,
              enqueue_returns=1):
    detection = detection or {"new": [comment], "edited": [], "deleted": [],
                              "unchanged_count": 0}
    ticket = make_ticket(key="DEV-635", summary="File explorer tool",
                         updated_at=comment["updated_at"])
    with patch("features.tickets.get_repos", return_value=[{"name": "repo"}]), \
         patch("features.tickets._fetch_ticket_comments", return_value=[comment]), \
         patch("features.tickets.comments.fetch_and_detect_comments", return_value=detection), \
         patch("features.tickets.comments.get_unprocessed_comments",
               return_value=unsettled or []), \
         patch("features.tickets.comments.mark_comment_processing"), \
         patch("features.tickets.comments.mark_comment_processed") as processed, \
         patch("features.tickets.comments.mark_comment_retryable") as retryable, \
         patch("features.tickets._is_issue_comment",
               return_value={"issue": True, "reason": "QA found four failures"}), \
         patch("features.tickets._ensure_worktree", return_value={"ok": True}), \
         patch("features.tickets._enqueue_stage", return_value=enqueue_returns) as eq, \
         patch("features.tickets.log.emit"):
        tickets._process_ticket_comments(fake_config, "DEV-635", ts, ticket,
                                         "http://localhost:8000", "aimyable")
    return eq, processed, retryable


class TestTheReportReachesTheJob:
    def test_the_comment_body_is_carried_in_the_job_payload(self, fake_config):
        eq, _, _ = _run_poll(fake_config, dict(COMMENT), {"slug": "s", "status": "in_review"})
        eq.assert_called_once()
        payload = eq.call_args.kwargs["payload"]
        assert payload["reports"][0]["body"] == QA_NOTE
        assert payload["reports"][0]["comment_id"] == "17285"
        assert payload["reports"][0]["author"] == "Dan Brisco"
        assert payload["ticket_summary"] == "File explorer tool"

    def test_the_comment_is_not_closed_at_enqueue_time(self, fake_config):
        """The job decides the outcome. Closing here is what made a six-minute
        run that changed nothing look like a fix."""
        _, processed, _ = _run_poll(fake_config, dict(COMMENT),
                                    {"slug": "s", "status": "in_review"})
        processed.assert_not_called()

    def test_a_refused_enqueue_sends_the_comment_back(self, fake_config):
        _, processed, retryable = _run_poll(fake_config, dict(COMMENT),
                                            {"slug": "s", "status": "in_review"},
                                            enqueue_returns=None)
        processed.assert_not_called()
        retryable.assert_called_once()

    def test_one_job_carries_every_report_in_the_pass(self, fake_config):
        second = dict(COMMENT, id="17286", body="Test 5 also fails")
        detection = {"new": [dict(COMMENT), second], "edited": [], "deleted": [],
                     "unchanged_count": 0}
        ticket = make_ticket(key="DEV-635", summary="File explorer tool",
                             updated_at=COMMENT["updated_at"])
        with patch("features.tickets.get_repos", return_value=[{"name": "repo"}]), \
             patch("features.tickets._fetch_ticket_comments",
                   return_value=[dict(COMMENT), second]), \
             patch("features.tickets.comments.fetch_and_detect_comments", return_value=detection), \
             patch("features.tickets.comments.get_unprocessed_comments", return_value=[]), \
             patch("features.tickets.comments.mark_comment_processing"), \
             patch("features.tickets.comments.mark_comment_processed"), \
             patch("features.tickets._is_issue_comment",
                   return_value={"issue": True, "reason": "r"}), \
             patch("features.tickets._ensure_worktree", return_value={"ok": True}), \
             patch("features.tickets._enqueue_stage", return_value=1) as eq, \
             patch("features.tickets.log.emit"):
            tickets._process_ticket_comments(fake_config, "DEV-635",
                                             {"slug": "s", "status": "in_review"},
                                             ticket, "http://localhost:8000", "aimyable")
        eq.assert_called_once()
        ids = [r["comment_id"] for r in eq.call_args.kwargs["payload"]["reports"]]
        assert ids == ["17285", "17286"]


class TestEditedAndUnsettledComments:
    def test_an_edited_report_is_processed(self, fake_config):
        """The snapshot only knows ids. An edit keeps the id, so filtering on
        'id not seen before' threw every edited bug report away."""
        edited = dict(COMMENT, updated_at="2026-09-01T19:00:00+00:00",
                      previously_at="2026-09-01T18:22:03+00:00")
        ts = {"slug": "s", "status": "in_review",
              "ticket_comment_snapshot": {"count": 1, "comment_ids": ["17285"]}}
        detection = {"new": [], "edited": [edited], "deleted": [], "unchanged_count": 0}
        eq, _, _ = _run_poll(fake_config, edited, ts, detection=detection)
        eq.assert_called_once()

    def test_an_unsettled_report_is_offered_again(self, fake_config):
        """A comment left in 'processing' keeps its upstream timestamp, so the
        detector calls it unchanged forever. The sweep is the only way back."""
        ts = {"slug": "s", "status": "in_review",
              "comments_checked_issue_updated_at": COMMENT["updated_at"],
              "ticket_comment_snapshot": {"count": 1, "comment_ids": ["17285"]}}
        detection = {"new": [], "edited": [], "deleted": [], "unchanged_count": 1}
        unsettled = [{"comment_id": "17285", "state": "new", "error_count": 1,
                      "last_checked_at": "2026-09-01T18:22:14+00:00",
                      "comment_edited_at": COMMENT["updated_at"]}]
        eq, _, _ = _run_poll(fake_config, dict(COMMENT), ts, detection=detection,
                             unsettled=unsettled)
        eq.assert_called_once()

    def test_nothing_unsettled_keeps_the_upstream_poll_short(self, fake_config):
        ts = {"slug": "s", "status": "in_review",
              "comments_checked_issue_updated_at": COMMENT["updated_at"]}
        ticket = make_ticket(key="DEV-635", updated_at=COMMENT["updated_at"])
        with patch("features.tickets.get_repos", return_value=[{"name": "repo"}]), \
             patch("features.tickets.comments.get_unprocessed_comments", return_value=[]), \
             patch("features.tickets._fetch_ticket_comments") as fetch:
            tickets._process_ticket_comments(fake_config, "DEV-635", ts, ticket,
                                             "http://localhost:8000", "aimyable")
        fetch.assert_not_called()


class TestUnsettledSweep:
    def test_a_fresh_processing_row_is_left_alone(self, fresh_db):
        state.init("sweep-a")
        comments.mark_comment_processing("sweep-a", "ticket", "DEV-1", "1", "t")
        assert tickets._unsettled_ticket_comments("sweep-a", "DEV-1") == []

    def test_a_stale_processing_row_comes_back(self, fresh_db):
        state.init("sweep-b")
        comments.mark_comment_processing("sweep-b", "ticket", "DEV-2", "1", "t")
        with patch("features.tickets.TICKET_COMMENT_RECLAIM_SECONDS", -1):
            rows = tickets._unsettled_ticket_comments("sweep-b", "DEV-2")
        assert [r["comment_id"] for r in rows] == ["1"]

    def test_a_comment_past_its_retry_budget_is_dropped(self, fresh_db):
        state.init("sweep-c")
        comments.mark_comment_processing("sweep-c", "ticket", "DEV-3", "1", "t")
        for _ in range(tickets.MAX_TICKET_COMMENT_RETRIES + 1):
            comments.mark_comment_error("sweep-c", "ticket", "DEV-3", "1", "no code change produced")
        assert tickets._unsettled_ticket_comments("sweep-c", "DEV-3") == []

    def test_giving_up_is_announced_once(self, fresh_db):
        state.init("sweep-d")
        comments.mark_comment_processing("sweep-d", "ticket", "DEV-4", "1", "t")
        for _ in range(tickets.MAX_TICKET_COMMENT_RETRIES + 1):
            comments.mark_comment_error("sweep-d", "ticket", "DEV-4", "1", "no code change produced")
        ts = {}
        with patch("features.tickets.log.emit") as emit:
            tickets._abandoned_ticket_comments("sweep-d", "DEV-4", ts, {}, "http://x")
            tickets._abandoned_ticket_comments("sweep-d", "DEV-4", ts, {}, "http://x")
        events = [c.args[0] for c in emit.call_args_list]
        assert events == ["ticket_comment_fix_abandoned"]
