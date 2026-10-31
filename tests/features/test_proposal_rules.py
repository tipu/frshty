"""The rules that keep a proposal off the board before it is filed.

A decline holds the same objective for DECLINE_HOLD_DAYS. A merge or review
of a client pull request is not filed, and the morning digest names it
instead. A class the operator declines nearly every time is muted. Each rule
raises the same "not filed" result a duplicate proposal key raises, and each
reports itself once in the event feed."""
from datetime import datetime, timedelta, timezone

import pytest

import core.db as db
import core.state as state
from manager import staleness
from services import work_launch, work_store

INSTANCE = "saidanlabs"
CLIENT = "saidanlabs,slack"


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path, monkeypatch):
    state.init(tmp_path)
    state._default_instance_key = INSTANCE
    state._instance_key_cv.set(INSTANCE)
    monkeypatch.setattr(work_launch, "_board_work", lambda: {})
    monkeypatch.setattr(work_launch, "_instance_config",
                        lambda key: {"job": {"key": key}} if key == INSTANCE else None)
    monkeypatch.setattr(work_launch, "_config_on_disk", lambda key: None)
    yield


def _file(objective, instance_key=INSTANCE, contexts=CLIENT, **kwargs):
    return work_store.create_proposal(objective, instance_key=instance_key,
                                      contexts=contexts, **kwargs)


def _events(event):
    return db.query_all("SELECT summary, meta, instance_key FROM log_events"
                        " WHERE event = ? ORDER BY ts", (event,))


def _count(objective):
    return db.query_one("SELECT COUNT(*) AS n FROM work_items WHERE objective = ?",
                        (objective,))["n"]


def _age_decline(item_id, days):
    at = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    db.execute("UPDATE work_items SET archived_at = ? WHERE id = ?", (at, item_id))


class TestDeclineHold:
    OBJECTIVE = "Unblock WB-209 — it has sat in stuck since 2026-09-17"

    def test_a_declined_objective_is_not_filed_again_within_the_hold(self):
        first = _file(self.OBJECTIVE)
        work_store.apply_action(first, "decline")

        with pytest.raises(work_store.ProposalKeyHeld) as raised:
            _file("Unblock WB-209 — it has sat in stuck since 2026-09-18")

        assert isinstance(raised.value, work_store.ProposalSuppressed)
        assert raised.value.holder["id"] == first
        assert db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"] == 1
        [event] = _events(work_store.PROPOSAL_DECLINE_HELD_EVENT)
        assert event["instance_key"] == INSTANCE
        assert f"#{first}" in event["summary"]

    def test_a_refusal_is_reported_once_however_often_it_repeats(self):
        work_store.apply_action(_file(self.OBJECTIVE), "decline")
        for _ in range(3):
            with pytest.raises(work_store.ProposalSuppressed):
                _file(self.OBJECTIVE)
        assert len(_events(work_store.PROPOSAL_DECLINE_HELD_EVENT)) == 1

    def test_the_hold_ends_after_seven_days(self):
        first = _file(self.OBJECTIVE)
        work_store.apply_action(first, "decline")
        _age_decline(first, work_store.DECLINE_HOLD_DAYS + 1)

        assert _file(self.OBJECTIVE) > first

    def test_a_decline_holds_only_its_own_instance_and_objective(self):
        work_store.apply_action(_file(self.OBJECTIVE), "decline")

        assert _file(self.OBJECTIVE, instance_key="stepioxcorp")
        assert _file("Unblock WB-283 — it has sat in stuck since 2026-09-17")

    def test_an_approved_proposal_holds_nothing(self):
        assert work_store.claim_proposal(_file(self.OBJECTIVE))
        assert _file(self.OBJECTIVE)

    def test_a_follow_up_is_held_only_by_its_own_task(self):
        """Two tasks can leave the same generic step on two branches."""
        source = work_store.create_item("ship the fix")
        other = work_store.create_item("ship another fix")
        first = _file("open the pull request for the pushed branch",
                      source_item_id=source)
        work_store.apply_action(first, "decline")

        assert _file("open the pull request for the pushed branch", source_item_id=other)
        with pytest.raises(work_store.ProposalSuppressed):
            _file("open the pull request for the pushed branch", source_item_id=source)

    def test_a_caller_with_its_own_hold_can_skip_this_one(self):
        work_store.apply_action(_file(self.OBJECTIVE), "decline")
        assert _file(self.OBJECTIVE, decline_hold=False)


class TestProposalClass:
    @pytest.mark.parametrize("objective, expected", [
        ("Merge vudruco-web PR #368 (https://bitbucket.org/x/y/pull-requests/368)", "merge_pr"),
        ("Merge all approved pull requests associated with ticket 644.", "merge_pr"),
        ("Approve PR #12 on the api repository", "merge_pr"),
        ("[stepioxcorp] Merge https://github.com/x/y/pull/4", "merge_pr"),
        ("Review, merge and deploy vudruco-web PR #365 first", "review_pr"),
        ("Review pull request #286 at bitbucket.org/x/y", "review_pr"),
        ("Follow up with Si Fokon on email: send the invoice", "follow_up_person"),
        ("[stepioxcorp] Inspect the blocked worktrees", "health_doctor"),
        ("Doctor DEV-4242 for the watchdog", "health_doctor"),
        ("Restore the Upwork inbox reader on personal.", "health_doctor"),
        ("Merge the two config files into one", "other"),
        ("Review the onboarding copy", "other"),
        ("Unblock WB-209", "other"),
        ("", "other"),
    ])
    def test_the_class_is_read_off_the_first_words(self, objective, expected):
        assert work_store.proposal_class(objective) == expected


class TestClientPullRequestProposals:
    MERGE = "Review and merge PR #260, then tell Si Fokon that it is merged"

    def test_a_client_merge_is_not_filed_and_the_digest_says_so(self):
        with pytest.raises(work_store.ProposalSuppressed) as raised:
            _file(self.MERGE)

        assert raised.value.event == work_store.PROPOSAL_CLIENT_PR_SKIPPED_EVENT
        assert _count(self.MERGE) == 0
        [event] = _events(work_store.PROPOSAL_CLIENT_PR_SKIPPED_EVENT)
        assert "saidanlabs" in event["summary"]
        [line] = staleness.client_pr_proposals_skipped(INSTANCE)
        assert line["count"] == 1
        assert line["examples"] == [self.MERGE]
        assert staleness.client_pr_proposals_skipped("stepioxcorp") == []

    def test_the_digest_bucket_carries_the_skipped_proposals(self):
        with pytest.raises(work_store.ProposalSuppressed):
            _file(self.MERGE)
        buckets = staleness.aggregate_all(INSTANCE)
        assert buckets["client_pr_proposals_skipped"][0]["count"] == 1

    def test_a_project_the_operator_reviews_himself_keeps_its_merge(self):
        assert _file(self.MERGE, contexts="frshty")

    def test_a_project_listed_under_work_projects_is_a_client(self, monkeypatch):
        monkeypatch.setattr(work_launch, "_board_work",
                            lambda: {"projects": {"stepioxcorp": "/srv/stepioxcorp"}})
        with pytest.raises(work_store.ProposalSuppressed):
            _file(self.MERGE, contexts="stepioxcorp")

    @pytest.mark.parametrize("contexts", ["upwork", "followup", "slack_int"])
    def test_a_source_tag_is_not_a_client_project(self, contexts):
        assert _file(self.MERGE, contexts=contexts)

    def test_a_proposal_that_names_no_project_is_not_a_client_s(self):
        assert _file(self.MERGE, contexts="")

    def test_a_board_that_asks_for_client_merges_gets_them(self, monkeypatch):
        monkeypatch.setattr(work_launch, "_board_work",
                            lambda: {"client_pr_proposals": True})
        assert _file(self.MERGE)

    def test_other_work_on_a_client_project_is_filed(self):
        assert _file("Move WB-412 to the PLT board.")

    def test_the_board_key_is_read_off_the_work_table(self, monkeypatch):
        monkeypatch.undo()
        monkeypatch.setattr(work_launch, "personal_config",
                            lambda: {"work": {"client_pr_proposals": True,
                                              "mute_declined_proposals": False}})
        assert work_launch.proposal_policy() == {"client_pr_proposals": True,
                                                 "mute_declined_proposals": False}
        monkeypatch.setattr(work_launch, "personal_config", lambda: {"work": {}})
        assert work_launch.proposal_policy() == {"client_pr_proposals": False,
                                                 "mute_declined_proposals": True}

    def test_a_proposal_over_http_reports_the_refusal(self, monkeypatch):
        monkeypatch.setattr(work_launch, "project_entries",
                            lambda: [{"key": "saidanlabs", "root": "/tmp"}])
        out = work_launch.propose(self.MERGE, ["saidanlabs"])
        assert "client project" in out["error"]
        assert "item_id" not in out


class TestMutedClass:
    @staticmethod
    def _history(declined, total=work_store.MUTE_WINDOW, klass="health_doctor"):
        for i in range(total):
            objective = {"health_doctor": f"Doctor the queue, pass {i}",
                         "follow_up_person": f"Follow up with Si Fokon on email: answer message {i}",
                         }.get(klass, f"Tidy the notes, part {i}")
            item_id = _file(objective, contexts="saidanlabs,followup")
            if i < declined:
                work_store.apply_action(item_id, "decline")
            else:
                assert work_store.claim_proposal(item_id)

    NEXT = "Restore the backup of the notes"

    def test_a_class_declined_more_than_eighty_percent_is_muted(self):
        self._history(declined=17)

        with pytest.raises(work_store.ProposalSuppressed) as raised:
            _file(self.NEXT, contexts="saidanlabs,followup")

        assert raised.value.event == work_store.PROPOSAL_CLASS_MUTED_EVENT
        assert raised.value.proposal_class == "health_doctor"
        assert _count(self.NEXT) == 0
        [event] = _events(work_store.PROPOSAL_CLASS_MUTED_EVENT)
        assert "17 of its last 20" in event["summary"]

    def test_eighty_percent_exactly_is_not_muted(self):
        self._history(declined=16)
        assert _file(self.NEXT, contexts="saidanlabs,followup")

    def test_a_class_with_fewer_than_twenty_decided_proposals_is_not_muted(self):
        self._history(declined=19, total=19)
        assert _file(self.NEXT, contexts="saidanlabs,followup")

    def test_the_mute_is_per_instance(self):
        self._history(declined=20)
        assert _file(self.NEXT, instance_key="stepioxcorp", contexts="stepioxcorp")

    def test_class_other_is_never_muted(self):
        self._history(declined=20, klass="other")
        assert _file("Tidy the notes, part 99", contexts="saidanlabs,followup")

    def test_a_follow_up_is_never_muted(self):
        self._history(declined=20, klass="follow_up_person")
        assert _file("Follow up with Stean Piostol on text message: confirm the time",
                     contexts="saidanlabs,followup")

    def test_a_waiting_proposal_does_not_count(self):
        self._history(declined=16)
        for i in range(3):
            _file(f"Doctor the queue, waiting {i}", contexts="saidanlabs,followup")
        assert _file(self.NEXT, contexts="saidanlabs,followup")

    @staticmethod
    def _age_declines(days):
        at = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
        db.execute("UPDATE work_items SET archived_at = ? WHERE stop_reason = ?",
                   (at, work_store.DECLINED_REASON))

    def test_the_mute_lifts_after_mute_days(self):
        self._history(declined=20)
        self._age_declines(work_store.MUTE_DAYS + 1)
        assert _file(self.NEXT, contexts="saidanlabs,followup")

    def test_the_mute_holds_within_mute_days(self):
        self._history(declined=20)
        self._age_declines(work_store.MUTE_DAYS - 1)
        with pytest.raises(work_store.ProposalSuppressed) as raised:
            _file(self.NEXT, contexts="saidanlabs,followup")
        assert raised.value.event == work_store.PROPOSAL_CLASS_MUTED_EVENT

    def test_a_decline_after_the_mute_lifts_renews_it(self):
        self._history(declined=20)
        self._age_declines(work_store.MUTE_DAYS + 1)
        work_store.apply_action(_file(self.NEXT, contexts="saidanlabs,followup"), "decline")
        with pytest.raises(work_store.ProposalSuppressed):
            _file("Doctor the queue, after the lift", contexts="saidanlabs,followup")

    def test_the_latest_decline_holds_the_mute_not_the_newest_proposal(self):
        self._history(declined=20)
        self._age_declines(work_store.MUTE_DAYS + 1)
        older = db.query_all("SELECT id FROM work_items ORDER BY id")[0]["id"]
        _age_decline(older, 0)
        with pytest.raises(work_store.ProposalSuppressed):
            _file(self.NEXT, contexts="saidanlabs,followup")

    def test_the_board_can_turn_the_mute_off(self, monkeypatch):
        self._history(declined=20)
        monkeypatch.setattr(work_launch, "_board_work",
                            lambda: {"mute_declined_proposals": False})
        assert _file(self.NEXT, contexts="saidanlabs,followup")
