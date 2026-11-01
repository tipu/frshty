from unittest.mock import MagicMock, patch

import features.tickets as tickets
import features.ticket_states as ticket_states
from tests.conftest import make_ticket, make_ticket_state
from tests.features.test_scope_review import _reconciled

PRS = [{"repo": "api", "id": 1, "url": ""}, {"repo": "web", "id": 2, "url": ""}]


def _info(*approvers, author="author-login"):
    return {"state": "OPEN", "author_id": author, "approver_ids": list(approvers)}


def _handle(config, ts, info_map):
    platform = MagicMock()
    platform.monitor_ci.return_value = ts
    with patch("features.tickets._scope_review_state", return_value="disabled"), \
         patch("features.tickets._resolve_conflicts_pending", return_value=False), \
         patch("features.tickets._build_pr_info_map", return_value=info_map), \
         patch("features.tickets._has_conflicting_pr", return_value=False), \
         patch("features.tickets._pr_base_moved", return_value=False), \
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
