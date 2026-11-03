"""The ticket layer on the own-PR engine: the comment_state merge gate, the
one-time copy of pr_comments.json, the ticket comment route and the status
moves the ticket keeps."""
import json
import uuid
from unittest.mock import MagicMock, patch

import pytest

import core.comments as comments
import core.state as state
import core.tasks.tickets as T
import features.own_prs as own_prs
import features.ticket_pr as ticket_pr
import features.ticket_states as ticket_states
from core.tasks.registry import TaskContext
from tests.conftest import make_comment, make_ticket, make_ticket_state

PR = {"repo": "api", "id": 7, "url": "http://u/7", "branch": "PROJ-1-x"}
WRITTEN = "2026-01-01T00:00:00Z"


@pytest.fixture()
def inst(fake_config, tmp_state):
    key = f"tp-{uuid.uuid4().hex[:8]}"
    fake_config["job"]["key"] = key
    return key


def _ts(**extra):
    return make_ticket_state(status="in_review", prs=[dict(PR)], **extra)


def _read(**seen):
    state.save("own_prs", {"api/7": {"comments_read_ok": True, **seen}})


def _ledger(fake_config, ts, rows):
    path = fake_config["workspace"]["root"] / "tickets" / ts["slug"] / "pr_comments.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows) if isinstance(rows, list) else rows)


def _row(inst, cid):
    return next(r for r in comments.get_comment_rows(inst, "pr", "api/7")
                if r["comment_id"] == str(cid))


class TestMergeHoldReason:
    def test_a_pr_the_engine_has_not_read_holds(self, fake_config, inst):
        assert ticket_pr.merge_hold_reason(fake_config, _ts()) == "comments not reconciled"

    def test_a_failed_read_holds(self, fake_config, inst):
        state.save("own_prs", {"api/7": {"comments_read_ok": False}})
        assert ticket_pr.merge_hold_reason(fake_config, _ts()) == "comment read failed"

    def test_a_comment_that_waits_for_a_reply_holds(self, fake_config, inst):
        _read()
        comments.mark_comment_processing(inst, "pr", "api/7", "5", WRITTEN)
        comments.mark_comment_manual(inst, "pr", "api/7", "5", "needs a reply")
        assert ticket_pr.merge_hold_reason(fake_config, _ts()) == "1 comment(s) owed an answer"

    def test_a_settled_comment_does_not_hold(self, fake_config, inst):
        _read()
        comments.mark_comment_seen(inst, "pr", "api/7", "5", WRITTEN)
        assert ticket_pr.merge_hold_reason(fake_config, _ts()) == ""


class TestReconcileCommentsNow:
    def _run(self, fake_config, live):
        platform = MagicMock()
        platform.self_id.return_value = "me"
        platform.get_pr_comments.return_value = live
        with patch("features.ticket_pr.make_platform", return_value=platform):
            return ticket_pr.reconcile_comments_now(fake_config, _ts())

    def test_a_comment_the_engine_has_not_seen_is_owed(self, fake_config, inst):
        hold, rows = self._run(fake_config, [make_comment(id=9, body="new ask")])
        assert hold == "1 comment(s) owed an answer"
        assert rows[0]["snippet"] == "new ask"

    def test_a_settled_resolved_or_own_comment_is_not_owed(self, fake_config, inst):
        comments.mark_comment_seen(inst, "pr", "api/7", "9", WRITTEN)
        live = [make_comment(id=9), make_comment(id=10, resolved=True),
                make_comment(id=11, author_id="me")]
        assert self._run(fake_config, live) == ("", [])

    def test_a_deleted_row_the_platform_still_reports_is_owed(self, fake_config, inst):
        comments.mark_comment_seen(inst, "pr", "api/7", "9", WRITTEN)
        comments.mark_comment_deleted(inst, "pr", "api/7", "9")
        hold, rows = self._run(fake_config, [make_comment(id=9, body="still open")])
        assert hold == "1 comment(s) owed an answer"
        assert rows[0]["snippet"] == "still open"

    def test_a_failed_read_holds(self, fake_config, inst):
        assert self._run(fake_config, None)[0] == "comment read failed"


class TestImportLedger:
    def test_each_status_lands_in_its_comment_state(self, fake_config, inst):
        ts = _ts()
        _ledger(fake_config, ts, [
            {"id": 1, "comment_kind": "review", "pr_repo": "api", "pr_id": 7, "body": "a",
             "status": "addressed"},
            {"id": 2, "comment_kind": "review", "pr_repo": "api", "pr_id": 7, "body": "b",
             "status": "needs_reply", "suggested_reply": "because",
             "defence": {"verdict": "pending"}},
            {"id": 3, "comment_kind": "review", "pr_repo": "api", "pr_id": 7, "body": "c",
             "status": "fix_failed", "attempts": 1},
            {"id": 4, "comment_kind": "review", "pr_repo": "api", "pr_id": 7, "body": "d",
             "status": "fix_failed", "attempts": 2},
            {"id": 5, "comment_kind": "review", "pr_repo": "api", "pr_id": 8, "body": "other pr",
             "status": "needs_reply"},
        ])
        live = [make_comment(id=i, body=b, updated_at=WRITTEN)
                for i, b in ((1, "a"), (2, "b"), (3, "c"), (4, "d"))]
        assert ticket_pr.import_ledger(fake_config, inst, ts, PR, live) == 4
        assert _row(inst, 1)["state"] == "processed" and _row(inst, 1)["fix_count"] == 1
        assert _row(inst, 2)["state"] == "manual"
        assert _row(inst, 2)["suggested_reply"] == "because"
        assert json.loads(_row(inst, 2)["defence"]) == {"verdict": "pending"}
        assert _row(inst, 3)["state"] == "new" and _row(inst, 3)["error_count"] == 1
        assert _row(inst, 4)["state"] == "manual"
        assert _row(inst, 1)["comment_edited_at"] == WRITTEN

    def test_a_comment_the_cursor_passed_is_settled(self, fake_config, inst):
        ts = _ts(last_comment_ids={"api/7": 5})
        live = [make_comment(id=4), make_comment(id=6)]
        assert ticket_pr.import_ledger(fake_config, inst, ts, PR, live) == 1
        assert _row(inst, 4)["state"] == "processed"
        assert [r["comment_id"] for r in comments.get_comment_rows(inst, "pr", "api/7")] == ["4"]

    def test_an_unreadable_ledger_returns_none(self, fake_config, inst):
        ts = _ts()
        _ledger(fake_config, ts, "{not json")
        assert ticket_pr.import_ledger(fake_config, inst, ts, PR, []) is None

    def test_a_ticket_pr_imports_once_before_detection(self, fake_config, inst):
        ts = _ts()
        state.save_ticket("PROJ-1", ts)
        _ledger(fake_config, ts, [{"id": 1, "comment_kind": "review", "pr_repo": "api",
                                   "pr_id": 7, "body": "a", "status": "addressed"}])
        platform = MagicMock()
        platform.get_pr_comments.return_value = [make_comment(id=1, body="a", updated_at=WRITTEN)]
        seen: dict = {}
        with patch("features.own_prs.log"), \
             patch("features.ticket_pr.import_ledger", wraps=ticket_pr.import_ledger) as imp:
            own_prs._check_comments(fake_config, inst, platform, PR, "b", seen=seen,
                                    ticket_key="PROJ-1")
            own_prs._check_comments(fake_config, inst, platform, PR, "b", seen=seen,
                                    ticket_key="PROJ-1")
        assert imp.call_count == 1
        assert seen["ledger_imported"] is True and seen["comments_read_ok"] is True
        assert _row(inst, 1)["state"] == "processed"


class TestTicketCommentRoute:
    def _route(self, fake_config, inst, no_reply=False, draft="It reads the cache.",
               commits=False):
        comments.mark_comment_processing(inst, "pr", "api/7", "100", WRITTEN)
        seen: dict = {}
        with patch("features.own_prs.ticket_pr.comment_needs_no_reply", return_value=no_reply), \
             patch("features.own_prs.ticket_pr.draft_comment_reply", return_value=draft), \
             patch("features.own_prs.ticket_pr.reply_commits_to_change", return_value=commits), \
             patch("features.own_prs.ticket_pr.substantiate_reply",
                   return_value={"verdict": "pending"}) as sub, \
             patch("features.own_prs.log"):
            own_prs._route_ticket_comment(fake_config, inst, PR, "api#7", make_comment(),
                                          WRITTEN, "a question", {}, {}, seen, "PROJ-1")
        return _row(inst, 100), seen, sub

    def test_a_statement_needs_no_reply_and_closes(self, fake_config, inst):
        row, _, sub = self._route(fake_config, inst, no_reply=True)
        assert row["state"] == "processed"
        sub.assert_not_called()

    def test_a_question_waits_for_the_operator_with_a_draft(self, fake_config, inst):
        row, seen, sub = self._route(fake_config, inst)
        assert row["state"] == "manual"
        assert row["suggested_reply"] == "It reads the cache."
        assert json.loads(row["defence"]) == {"verdict": "pending"}
        assert "fix_deadline" not in seen
        sub.assert_called_once()

    def test_a_draft_that_promises_a_change_goes_to_the_fixer(self, fake_config, inst):
        row, seen, sub = self._route(fake_config, inst, commits=True)
        assert row["state"] == "deferred"
        assert "fix_deadline" in seen
        sub.assert_not_called()

    def test_the_ticket_page_reads_the_row(self, fake_config, inst):
        self._route(fake_config, inst)
        comments.record_comment_detail(inst, "pr", "api/7", make_comment(body="Why?"))
        rows = ticket_pr.comment_rows(fake_config, _ts())
        assert [(r["id"], r["status"], r["body"]) for r in rows] == [(100, "needs_reply", "Why?")]


class TestStatusMoves:
    def _handle(self, fake_config, ts, info_map, ci_result=None):
        platform = MagicMock()
        platform.monitor_ci.return_value = ci_result or ts
        with patch("features.tickets._scope_review_state", return_value="disabled"), \
             patch("features.tickets._build_pr_info_map", return_value=info_map), \
             patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets._check_in_review", side_effect=lambda c, t, s, b, **kw: s), \
             patch("features.tickets._merge") as merge, \
             patch("features.ticket_states.log") as log:
            out, _ = ticket_states._handle_in_review_ticket(
                fake_config, make_ticket(), ts, "http://base", "inst", True)
        return out, merge, log

    def test_a_spent_base_sync_on_a_conflict_parks_the_ticket(self, fake_config, inst):
        _read(base_sync_attempts=2)
        out, merge, _ = self._handle(fake_config, _ts(), {("api", 7): {"mergeable": "CONFLICTING"}})
        assert (out["status"], out["pr_failed_reason"]) == ("pr_failed", "conflict_failed")
        merge.assert_not_called()

    def test_a_conflict_with_budget_left_waits_for_the_engine(self, fake_config, inst):
        _read(base_sync_attempts=1)
        out, _, _ = self._handle(fake_config, _ts(), {("api", 7): {"mergeable": "CONFLICTING"}})
        assert out["status"] == "in_review"

    def test_a_spent_ci_budget_parks_the_ticket(self, fake_config, inst):
        _read(ci_cap_emitted=True)
        failed = {"_ci_failed": True, "pr": dict(PR), "checks": [{"name": "lint", "state": "FAILED"}]}
        out, _, _ = self._handle(fake_config, _ts(ci_passed=True),
                                 {("api", 7): {"head_sha": "abc"}}, failed)
        assert (out["status"], out["pr_failed_reason"]) == ("pr_failed", "ci_failed")
        assert out["ci_fix_heads"] == {"api/7": "abc"}
        assert "ci_passed" not in out

    def test_a_red_check_with_budget_left_only_drops_the_green(self, fake_config, inst):
        _read()
        failed = {"_ci_failed": True, "pr": dict(PR), "checks": [{"name": "lint", "state": "FAILED"}]}
        out, _, _ = self._handle(fake_config, _ts(ci_passed=True), {}, failed)
        assert out["status"] == "in_review"
        assert "ci_passed" not in out


class TestSubstantiateReplyTask:
    def _ctx(self, fake_config, inst):
        return TaskContext(instance_key=inst, ticket_key="PROJ-1", task="substantiate_reply",
                           payload={"repo": "api", "pr_id": 7, "comment_id": "100"},
                           job_id=1, triggering_event_id=None, config=fake_config,
                           registry=None, now=None)

    def test_the_verdict_is_written_onto_the_comment(self, fake_config, inst, tmp_path):
        comments.mark_comment_processing(inst, "pr", "api/7", "100", WRITTEN)
        comments.set_comment_reply(inst, "pr", "api/7", "100", "It is covered.")
        wt = tmp_path / "wt"
        (wt / ".git").mkdir(parents=True)
        result = MagicMock()
        result.to_dict.return_value = {"verdict": "substantiated"}
        with patch("core.tasks.tickets.state.load_ticket", return_value=_ts()), \
             patch("core.tasks.tickets._ensure_pr_worktree", return_value=wt), \
             patch("core.tasks.tickets.defence.substantiate", return_value=result) as sub:
            out = T.substantiate_reply(self._ctx(fake_config, inst))
        assert out.status == "ok"
        assert sub.call_args[0][1] == "It is covered."
        assert json.loads(_row(inst, 100)["defence"]) == {"verdict": "substantiated"}

    def test_a_comment_no_longer_tracked_is_skipped(self, fake_config, inst):
        out = T.substantiate_reply(self._ctx(fake_config, inst))
        assert out.status == "skipped"


class TestTicketCiBudget:
    def _ci(self, tmp_path, seen, outcome, ticket_key="PROJ-1"):
        platform = MagicMock()
        platform.get_pr_checks.return_value = [{"state": "FAILED", "name": "lint"}]
        worktree = tmp_path / "wt"
        worktree.mkdir(exist_ok=True)
        with patch("features.own_prs._ensure_worktree", return_value=worktree), \
             patch("features.own_prs.subprocess.run",
                   return_value=MagicMock(returncode=0, stdout="abc\n")), \
             patch("features.pr_ci.triage_and_fix_pr", return_value=outcome) as triage, \
             patch("features.own_prs.log"):
            own_prs._check_ci({"_state_dir": tmp_path}, platform, dict(PR), seen, "http://base",
                              ticket_key=ticket_key)
        return triage

    def test_a_ticket_fix_run_that_produced_nothing_spends_the_budget(self, tmp_path):
        seen: dict = {}
        self._ci(tmp_path, seen, {"result": "fix_failed", "attempts": 0, "failed_names": ["lint"]})
        assert seen["ci_fix_attempts"] == 1

    def test_an_own_pr_fix_run_that_produced_nothing_keeps_its_budget(self, tmp_path):
        seen: dict = {}
        self._ci(tmp_path, seen, {"result": "fix_failed", "attempts": 0, "failed_names": ["lint"]},
                 ticket_key="")
        assert "ci_fix_attempts" not in seen

    def test_the_head_a_fix_pushed_is_triaged_again_once_ci_had_time(self, tmp_path):
        recent = {"ci_fix_sha": "abc", "ci_fix_at": "2999-01-01T00:00:00+00:00"}
        assert self._ci(tmp_path, recent, {"result": "no_failing"}).call_count == 0
        settled = {"ci_fix_sha": "abc", "ci_fix_at": "2000-01-01T00:00:00+00:00"}
        assert self._ci(tmp_path, settled, {"result": "no_failing"}).call_count == 1

    def test_a_capped_head_stays_skipped(self, tmp_path):
        assert self._ci(tmp_path, {"ci_fix_sha": "abc"}, {"result": "no_failing"}).call_count == 0


def test_the_same_branch_in_two_repositories_gets_two_worktrees(tmp_path):
    config = {"_state_dir": tmp_path}
    api = own_prs._worktree_path(config, {"repo": "api", "branch": "PROJ-1-x"})
    web = own_prs._worktree_path(config, {"repo": "web", "branch": "PROJ-1-x"})
    assert api != web
    assert api.name == web.name == "PROJ-1-x"


def test_a_ticket_fixer_that_fails_at_the_cap_parks_the_pr(tmp_path):
    seen = {"ci_fix_attempts": 2}
    TestTicketCiBudget()._ci(tmp_path, seen, {"result": "haiku_empty", "attempts": 2,
                                              "failed_names": ["lint"]})
    assert seen["ci_cap_emitted"] is True
    assert seen["ci_fix_attempts"] == 2
