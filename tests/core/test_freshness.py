"""The verification ledger records claims. It must not decide anything.

Every test here holds the ledger to two properties. It tells a claim that
still stands from one an event has staled, and a failure to write it never
reaches the caller."""
from unittest.mock import patch

import pytest

import core.db as db
import core.freshness as freshness
import core.state as state


_counter = 0


@pytest.fixture
def inst():
    global _counter
    _counter += 1
    return f"ledger-test-{_counter}"


def _rows(instance_key):
    return db.query_all(
        "SELECT * FROM verification_claim WHERE instance_key=? ORDER BY claim",
        (instance_key,))


class TestRecord:
    def test_a_claim_is_established_against_its_subject(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        rows = _rows(inst)
        assert len(rows) == 1
        assert rows[0]["claim"] == "proof"
        assert rows[0]["established_against"] == "r:abc"
        assert rows[0]["invalidated_at"] is None

    def test_re_recording_the_same_subject_keeps_the_first_time(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        first = _rows(inst)[0]["established_at"]
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        assert _rows(inst)[0]["established_at"] == first

    def test_a_new_subject_re_establishes_the_claim(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.record("DEV-1", "proof", "r:def", instance_key=inst)
        rows = _rows(inst)
        assert len(rows) == 1
        assert rows[0]["established_against"] == "r:def"

    def test_a_stale_claim_is_established_again_even_on_the_same_subject(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.invalidate("DEV-1", "proof", "ticket_base_synced",
                             instance_key=inst)
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        row = _rows(inst)[0]
        assert row["invalidated_at"] is None
        assert row["invalidated_by"] is None

    def test_claims_are_kept_apart_per_ticket_and_per_claim(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.record("DEV-1", "ci", "repo/1@sha", instance_key=inst)
        freshness.record("DEV-2", "proof", "r:zzz", instance_key=inst)
        assert len(_rows(inst)) == 3

    def test_the_active_instance_is_used_when_none_is_given(self, tmp_state):
        freshness.record("DEV-9", "proof", "r:abc")
        assert _rows(tmp_state.name)[0]["ticket_key"] == "DEV-9"

    def test_a_write_with_no_instance_is_reported_not_swallowed(self):
        state._instance_key_cv.set(None)
        with patch.object(state, "_default_instance_key", None), \
             patch.object(freshness, "log") as fake_log:
            freshness.record("DEV-1", "proof", "r:abc")
        assert fake_log.emit.call_args.args[0] == "freshness_ledger_skipped"


class TestInvalidate:
    def test_an_event_stales_a_standing_claim(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.invalidate("DEV-1", "proof", "ticket_base_synced",
                             instance_key=inst)
        row = _rows(inst)[0]
        assert row["invalidated_at"] is not None
        assert row["invalidated_by"] == "ticket_base_synced"

    def test_the_first_event_to_stale_a_claim_is_the_one_kept(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.invalidate("DEV-1", "proof", "ticket_base_synced",
                             instance_key=inst)
        freshness.invalidate("DEV-1", "proof", "ticket_pr_comment_fixed",
                             instance_key=inst)
        assert _rows(inst)[0]["invalidated_by"] == "ticket_base_synced"

    def test_invalidating_a_claim_that_was_never_made_writes_nothing(self, inst):
        freshness.invalidate("DEV-1", "proof", "ticket_base_synced",
                             instance_key=inst)
        assert _rows(inst) == []

    def test_one_claim_going_stale_leaves_the_others_standing(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.record("DEV-1", "comments", "{}", instance_key=inst)
        freshness.invalidate("DEV-1", "proof", "ticket_base_synced",
                             instance_key=inst)
        by_claim = {r["claim"]: r for r in _rows(inst)}
        assert by_claim["proof"]["invalidated_at"] is not None
        assert by_claim["comments"]["invalidated_at"] is None


class TestClaims:
    def test_the_page_reads_back_what_was_recorded(self, inst):
        freshness.record("DEV-1", "proof", "r:abc", instance_key=inst)
        freshness.invalidate("DEV-1", "proof", "ticket_base_synced",
                             instance_key=inst)
        out = freshness.claims(inst, "DEV-1")
        assert [c["claim"] for c in out] == ["proof"]
        assert out[0]["invalidated_by"] == "ticket_base_synced"

    def test_a_ticket_with_no_claims_reads_empty(self, inst):
        assert freshness.claims(inst, "DEV-1") == []


def _survives(call, broken):
    """Run call() with a broken database and report what escaped.

    Returns (raised, emitted). A caller of the ledger must see no exception,
    and the event feed must see the failure. Reported rather than allowed to
    propagate so a regression is a decided assertion and not a crash."""
    raised = None
    with patch.object(freshness.db, broken,
                      side_effect=RuntimeError("disk is gone")), \
         patch.object(freshness, "log") as fake_log:
        try:
            call()
        except Exception as e:
            raised = e
    emitted = fake_log.emit.call_args
    return raised, emitted


class TestTheLedgerNeverBreaksItsCaller:
    def test_a_failed_write_is_emitted_and_not_raised(self, inst):
        raised, emitted = _survives(
            lambda: freshness.record("DEV-1", "proof", "r:abc", instance_key=inst),
            "execute")
        assert raised is None, f"the ledger raised {raised!r} at its caller"
        assert emitted.args[0] == "freshness_ledger_failed"
        assert "disk is gone" in emitted.args[1]

    def test_a_failed_invalidate_is_emitted_and_not_raised(self, inst):
        raised, emitted = _survives(
            lambda: freshness.invalidate("DEV-1", "proof", "x", instance_key=inst),
            "execute")
        assert raised is None, f"the ledger raised {raised!r} at its caller"
        assert emitted.args[0] == "freshness_ledger_failed"

    def test_a_failed_read_gives_the_page_an_empty_list(self, inst):
        out = []
        raised, emitted = _survives(
            lambda: out.append(freshness.claims(inst, "DEV-1")), "query_all")
        assert raised is None, f"the ledger raised {raised!r} at the page"
        assert out == [[]]
        assert emitted.args[0] == "freshness_ledger_failed"


class TestTheLever:
    def test_the_ledger_is_written_by_default(self):
        assert freshness.enabled({}) is True
        assert freshness.enabled(None) is True

    def test_the_operator_can_turn_the_writing_off(self):
        assert freshness.enabled({"freshness": {"enabled": False}}) is False


class TestSubjects:
    def test_pr_heads_name_every_pr_and_its_head(self):
        prs = [{"repo": "api", "id": 2}, {"repo": "web", "id": 1}]
        info = {("api", 2): {"head_sha": "aaa"}, ("web", 1): {"head_sha": "bbb"}}
        assert freshness.pr_heads(prs, info) == "api/2@aaa web/1@bbb"

    def test_pr_heads_are_stable_under_reordering(self):
        info = {("api", 2): {"head_sha": "aaa"}, ("web", 1): {"head_sha": "bbb"}}
        first = freshness.pr_heads([{"repo": "api", "id": 2},
                                    {"repo": "web", "id": 1}], info)
        second = freshness.pr_heads([{"repo": "web", "id": 1},
                                     {"repo": "api", "id": 2}], info)
        assert first == second

    def test_a_moved_head_changes_the_subject(self):
        prs = [{"repo": "api", "id": 2}]
        before = freshness.pr_heads(prs, {("api", 2): {"head_sha": "aaa"}})
        after = freshness.pr_heads(prs, {("api", 2): {"head_sha": "ccc"}})
        assert before != after

    def test_the_watermark_carries_both_comment_kinds(self):
        subject = freshness.watermark({"last_comment_ids": {"r/1": 7},
                                       "last_issue_comment_ids": {"r/1": 3}})
        assert '"review": {"r/1": 7}' in subject
        assert '"issue": {"r/1": 3}' in subject

    def test_a_moved_watermark_changes_the_subject(self):
        before = freshness.watermark({"last_comment_ids": {"r/1": 7}})
        after = freshness.watermark({"last_comment_ids": {"r/1": 9}})
        assert before != after

    def test_the_digest_is_order_independent_and_content_sensitive(self):
        assert freshness.digest(["a", "b"]) == freshness.digest(["b", "a"])
        assert freshness.digest(["a", "b"]) != freshness.digest(["a", "c"])
        assert freshness.digest([]) == ""
