from unittest.mock import MagicMock, patch

import features.ticket_pr as ticket_pr
import features.tickets as tickets
import features.ticket_states as ticket_states
import web.tickets as web_tickets
from tests.conftest import make_ticket, make_ticket_state
from tests.features.test_scope_review import _reconciled

PRS = [{"repo": "api", "id": 1, "url": ""}, {"repo": "web", "id": 2, "url": ""}]


def _info(*approvers, author="author-login"):
    return {"state": "OPEN", "author_id": author, "approver_ids": list(approvers)}


def _handle(config, ts, info_map, scope="disabled"):
    platform = MagicMock()
    platform.monitor_ci.return_value = ts
    with patch("features.tickets._scope_review_state", return_value=scope), \
         patch("features.tickets._build_pr_info_map", return_value=info_map), \
         patch("features.tickets.make_platform", return_value=platform), \
         patch("features.tickets._merge") as merge, \
         patch("features.tickets._check_in_review", side_effect=_reconciled), \
         patch("features.tickets._enqueue_stage"), \
         patch("features.ticket_states.state"):
        ticket_states._handle_in_review_ticket(
            config, make_ticket(), ts, "http://base", "inst", True)
    return merge


class TestMergeWhenAllApproved:
    def test_merges_when_every_pr_is_approved(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                          ("web", 2): _info("reviewer-login")})
        merge.assert_called_once()
        assert "merge_held_unapproved" not in ts

    def test_one_unapproved_pr_holds_every_pr(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        with patch("features.tickets.log") as log:
            merge = _handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                              ("web", 2): _info()})
        merge.assert_not_called()
        assert ts["merge_held_unapproved"] == ["web#2"]
        assert log.emit.call_args[0][0] == "ticket_merge_held_for_approval"

    def test_the_author_approving_their_own_pr_does_not_count(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {("api", 1): _info("author-login"),
                                          ("web", 2): _info("reviewer-login")})
        merge.assert_not_called()

    def test_an_unknown_author_holds_the_merge(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {("api", 1): _info("reviewer-login", author=""),
                                          ("web", 2): _info("reviewer-login")})
        merge.assert_not_called()

    def test_a_pr_with_no_info_holds_the_merge(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {("api", 1): _info("reviewer-login")})
        merge.assert_not_called()

    def test_auto_merge_also_waits_for_approvals(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {("api", 1): _info(), ("web", 2): _info()})
        merge.assert_not_called()

    def test_without_the_rule_nothing_merges(self, fake_config):
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                          ("web", 2): _info("reviewer-login")})
        merge.assert_not_called()

    def test_auto_merge_alone_ignores_approvals(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = _handle(fake_config, ts, {})
        merge.assert_called_once()

    def test_the_hold_event_fires_once_per_waiting_set(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(prs=PRS)
        info = {("api", 1): _info("reviewer-login"), ("web", 2): _info()}
        with patch("features.tickets.log") as log, \
             patch("features.tickets.make_platform"):
            assert tickets._hold_for_approvals(fake_config, make_ticket(), ts, "b", info)
            assert tickets._hold_for_approvals(fake_config, make_ticket(), ts, "b", info)
        assert log.emit.call_count == 1


_REAL_NO_REPLY = ticket_pr.comment_needs_no_reply


class TestApprovalsOverrideAScopeFail:
    def _handle(self, config, ts, info_map, scope):
        return _handle(config, ts, info_map, scope)

    def test_all_approved_merges_over_a_scope_fail(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        with patch("features.tickets.log") as log:
            merge = self._handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                                   ("web", 2): _info("reviewer-login")}, "fail")
        merge.assert_called_once()
        assert log.emit.call_args[0][0] == "ticket_scope_fail_overridden_by_approval"

    def test_an_unfinished_scope_correction_still_holds(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS,
                               scope_fix={"incomplete": "a commit was blocked part way"})
        merge = self._handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                               ("web", 2): _info("reviewer-login")}, "fail")
        merge.assert_not_called()

    def test_a_scope_fail_still_holds_while_a_pr_waits(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = self._handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                               ("web", 2): _info()}, "fail")
        merge.assert_not_called()

    def test_a_pending_scope_review_still_holds(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = self._handle(fake_config, ts, {("api", 1): _info("reviewer-login"),
                                               ("web", 2): _info("reviewer-login")}, "pending")
        merge.assert_not_called()

    def test_auto_merge_alone_keeps_the_scope_fail_hold(self, fake_config):
        fake_config["pr"]["auto_merge"] = True
        ts = make_ticket_state(status="in_review", ci_passed=True, prs=PRS)
        merge = self._handle(fake_config, ts, {}, "fail")
        merge.assert_not_called()


class TestStatementCommentsNeedNoReply:
    def test_the_check_reads_the_model_answer(self):
        with patch("features.ticket_pr.run_haiku", return_value='{"needs_reply": false}'):
            assert _REAL_NO_REPLY("Review done. Approved.") is True
        with patch("features.ticket_pr.run_haiku", return_value='{"needs_reply": true}'):
            assert _REAL_NO_REPLY("Why this approach?") is False

    def test_no_answer_counts_as_a_reply_needed(self):
        with patch("features.ticket_pr.run_haiku", return_value=None):
            assert _REAL_NO_REPLY("Approved.") is False
        with patch("features.ticket_pr.run_haiku", return_value="not json"):
            assert _REAL_NO_REPLY("Approved.") is False

def test_the_pr_board_counts_a_no_reply_comment_as_done():
    entries = [{"pr_repo": "api", "pr_id": 1, "status": "no_reply_needed"}]
    assert web_tickets._pr_comment_breakdown(entries, "api", 1) == {
        "done": 1, "not_done": 0, "discuss": 0}


GREEN = [{"name": "build", "state": "SUCCESS", "url": ""}]
RED = [{"name": "build", "state": "FAILED", "url": ""}]


def _merge_foreign(config, ts, infos, checks):
    platform = MagicMock()
    platform.get_pr_info.side_effect = lambda repo, pr_id: infos[(repo, pr_id)]
    platform.get_pr_checks.side_effect = lambda repo, pr_id: checks[(repo, pr_id)]
    platform.merge_pr.return_value = {}
    with patch("features.tickets.make_platform", return_value=platform), \
         patch("features.tickets._fetch_ticket_comments", return_value=[]), \
         patch("features.tickets.log"):
        out = tickets._merge_approved_foreign_prs(config, make_ticket(), ts, "http://base")
    return out, platform


class TestMergeApprovedForeignPrs:
    def _ts(self, status="pr_ready"):
        return make_ticket_state(status=status, foreign_prs=[dict(p) for p in PRS])

    def test_merges_every_approved_green_pr_and_marks_the_ticket_merged(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        out, platform = _merge_foreign(
            fake_config, self._ts(),
            {("api", 1): _info("reviewer-login"), ("web", 2): _info("reviewer-login")},
            {("api", 1): GREEN, ("web", 2): GREEN})
        assert platform.merge_pr.call_count == 2
        assert out["status"] == "merged"
        assert "foreign_prs" not in out
        assert [p["id"] for p in out["prs"]] == [1, 2]

    def test_failing_ci_holds_every_pr(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        out, platform = _merge_foreign(
            fake_config, self._ts(),
            {("api", 1): _info("reviewer-login"), ("web", 2): _info("reviewer-login")},
            {("api", 1): GREEN, ("web", 2): RED})
        platform.merge_pr.assert_not_called()
        assert out["status"] == "pr_ready"
        assert out["foreign_prs"] == PRS

    def test_pending_or_missing_ci_holds(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        for checks in (None, [], [{"name": "build", "state": "INPROGRESS", "url": ""}]):
            _, platform = _merge_foreign(
                fake_config, self._ts(),
                {("api", 1): _info("reviewer-login"), ("web", 2): _info("reviewer-login")},
                {("api", 1): GREEN, ("web", 2): checks})
            platform.merge_pr.assert_not_called()

    def test_one_unapproved_pr_holds_every_pr(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        _, platform = _merge_foreign(
            fake_config, self._ts(),
            {("api", 1): _info("reviewer-login"), ("web", 2): _info("author-login")},
            {("api", 1): GREEN, ("web", 2): GREEN})
        platform.merge_pr.assert_not_called()

    def test_a_declined_pr_does_not_block_the_approved_one(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        declined = {**_info(), "state": "DECLINED"}
        out, platform = _merge_foreign(
            fake_config, self._ts(),
            {("api", 1): _info("reviewer-login"), ("web", 2): declined},
            {("api", 1): GREEN, ("web", 2): RED})
        platform.merge_pr.assert_called_once_with("api", 1)
        assert out["status"] == "merged"
        assert [p["id"] for p in out["prs"]] == [1]

    def test_rule_off_does_nothing(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = False
        out, platform = _merge_foreign(
            fake_config, self._ts(),
            {("api", 1): _info("reviewer-login"), ("web", 2): _info("reviewer-login")},
            {("api", 1): GREEN, ("web", 2): GREEN})
        platform.merge_pr.assert_not_called()
        assert out["foreign_prs"] == PRS

    def test_a_merge_error_keeps_the_hold(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        platform = MagicMock()
        platform.get_pr_info.return_value = _info("reviewer-login")
        platform.get_pr_checks.return_value = GREEN
        platform.merge_pr.return_value = {"error": "conflict"}
        with patch("features.tickets.make_platform", return_value=platform), \
             patch("features.tickets.log"):
            out = tickets._merge_approved_foreign_prs(fake_config, make_ticket(), self._ts(), "b")
        assert out["status"] == "pr_ready"
        assert out["foreign_prs"] == PRS

    def test_a_status_that_cannot_reach_merged_keeps_the_hold_for_release(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        out, platform = _merge_foreign(
            fake_config, self._ts(status="planning"),
            {("api", 1): _info("reviewer-login"), ("web", 2): _info("reviewer-login")},
            {("api", 1): GREEN, ("web", 2): GREEN})
        assert platform.merge_pr.call_count == 2
        assert out["status"] == "planning"
        assert out["foreign_prs"] == PRS

    def test_an_unreadable_pr_state_holds_every_pr(self, fake_config):
        fake_config["pr"]["merge_when_all_approved"] = True
        for unknown in ({}, {"state": None}, {"state": "WEIRD"}):
            out, platform = _merge_foreign(
                fake_config, self._ts(),
                {("api", 1): _info("reviewer-login"), ("web", 2): unknown},
                {("api", 1): GREEN, ("web", 2): GREEN})
            platform.merge_pr.assert_not_called()
            assert out["foreign_prs"] == PRS
