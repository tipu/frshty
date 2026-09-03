"""The pipeline tasks that had no test at all.

Coverage showed these task bodies never executed under the suite: create_pr,
resolve_conflicts, sync_pr_base, validate_merged_ticket, scope_review,
setup_prd_ticket and backfill_artifacts. Each one owns a ticket transition or
a guard, so a regression in one moved a ticket to the wrong state silently.
These tests drive each body directly and pin the transition it reports, the
guard that stops it, and the failure it converts an exception into.
"""
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import core.state as state
from core.tasks import tickets as T
from core.tasks.registry import TaskContext
from tests.conftest import make_ticket_state


SLUG = "PROJ-1-do-the-thing"


def _ctx(config, task, key="PROJ-1", payload=None, base_url="http://base"):
    return TaskContext(
        instance_key="inst",
        ticket_key=key,
        task=task,
        payload=payload or {},
        job_id=0,
        triggering_event_id=None,
        config={**config, "_base_url": base_url},
        registry=SimpleNamespace(base_url=base_url),
        now=None,
    )


def _seed(status="pr_ready", **extra):
    ts = make_ticket_state(status=status, slug=SLUG, summary="Do the thing",
                           description="Description text")
    ts.update(extra)
    state.save("tickets", {"PROJ-1": ts})
    return ts


def _ticket_dir(config):
    d = config["workspace"]["root"] / "tickets" / SLUG
    (d / "docs").mkdir(parents=True, exist_ok=True)
    return d


class TestCreatePr:
    def test_reports_the_status_the_feature_layer_returned(self, fake_config, tmp_state):
        _seed()
        with patch("features.tickets._create_pr",
                   return_value={**_seed(), "status": "in_review"}):
            result = T.create_pr(_ctx(fake_config, "create_pr"))
        assert result.status == "ok"
        assert result.artifacts["transitioned_to"] == "in_review"
        assert state.load_ticket("PROJ-1")["status"] == "in_review"

    def test_a_missing_status_is_reported_as_pr_failed(self, fake_config, tmp_state):
        _seed()
        with patch("features.tickets._create_pr", return_value={"slug": SLUG}):
            result = T.create_pr(_ctx(fake_config, "create_pr"))
        assert result.artifacts["transitioned_to"] == "pr_failed"

    def test_an_exception_moves_the_ticket_to_pr_failed(self, fake_config, tmp_state):
        _seed()
        with patch("features.tickets._create_pr", side_effect=RuntimeError("boom")):
            result = T.create_pr(_ctx(fake_config, "create_pr"))
        assert result.status == "failed"
        assert "RuntimeError: boom" in result.reason
        assert result.artifacts["transitioned_to"] == "pr_failed"
        assert state.load_ticket("PROJ-1")["status"] == "pr_failed"

    def test_a_missing_ticket_fails(self, fake_config, tmp_state):
        state.save("tickets", {})
        result = T.create_pr(_ctx(fake_config, "create_pr"))
        assert result.status == "failed"
        assert result.reason == "ticket not found"


class TestResolveConflicts:
    def test_reports_the_transition_and_the_attempt_delta(self, fake_config, tmp_state):
        _seed(status="in_review", conflict_resolution_attempts=1)
        updated = {**state.load_ticket("PROJ-1"), "status": "in_review",
                   "conflict_resolution_attempts": 2}
        with patch("features.tickets._resolve_conflicts", return_value=updated):
            result = T.resolve_conflicts(_ctx(fake_config, "resolve_conflicts"))
        assert result.status == "ok"
        assert result.artifacts["attempts"] == 2
        assert result.artifacts["attempts_delta"] == 1
        assert result.artifacts["status_changed"] is False

    def test_a_status_change_is_reported(self, fake_config, tmp_state):
        _seed(status="in_review")
        updated = {**state.load_ticket("PROJ-1"), "status": "pr_failed"}
        with patch("features.tickets._resolve_conflicts", return_value=updated):
            result = T.resolve_conflicts(_ctx(fake_config, "resolve_conflicts"))
        assert result.artifacts["status_changed"] is True
        assert result.artifacts["transitioned_to"] == "pr_failed"

    def test_an_exception_is_logged_and_fails_the_task(self, fake_config, tmp_state):
        _seed(status="in_review")
        emitted = MagicMock()
        with patch("features.tickets._resolve_conflicts", side_effect=ValueError("nope")), \
             patch("core.tasks.tickets.log.emit", emitted):
            result = T.resolve_conflicts(_ctx(fake_config, "resolve_conflicts"))
        assert result.status == "failed"
        assert "ValueError: nope" in result.reason
        assert emitted.call_args.args[0] == "resolve_conflicts_error"

    def test_a_missing_ticket_key_fails(self, fake_config, tmp_state):
        result = T.resolve_conflicts(_ctx(fake_config, "resolve_conflicts", key=None))
        assert result.status == "failed"
        assert result.reason == "ticket_key missing"


class TestSyncPrBase:
    def test_saves_what_the_feature_layer_returned(self, fake_config, tmp_state):
        _seed(status="in_review")
        updated = {**state.load_ticket("PROJ-1"), "base_synced_at": "2026-09-02T00:00:00Z"}
        with patch("features.tickets._sync_pr_base", return_value=updated):
            result = T.sync_pr_base(_ctx(fake_config, "sync_pr_base"))
        assert result.status == "ok"
        assert state.load_ticket("PROJ-1")["base_synced_at"] == "2026-09-02T00:00:00Z"

    def test_an_exception_is_logged_and_fails_the_task(self, fake_config, tmp_state):
        _seed(status="in_review")
        emitted = MagicMock()
        with patch("features.tickets._sync_pr_base", side_effect=OSError("git gone")), \
             patch("core.tasks.tickets.log.emit", emitted):
            result = T.sync_pr_base(_ctx(fake_config, "sync_pr_base"))
        assert result.status == "failed"
        assert "OSError: git gone" in result.reason
        assert emitted.call_args.args[0] == "sync_pr_base_error"

    def test_a_missing_ticket_fails(self, fake_config, tmp_state):
        state.save("tickets", {})
        result = T.sync_pr_base(_ctx(fake_config, "sync_pr_base"))
        assert result.status == "failed"
        assert result.reason == "ticket not found"


class TestValidateMergedTicket:
    def test_no_live_url_skips_instead_of_failing(self, fake_config, tmp_state):
        _seed(status="done")
        emitted = MagicMock()
        with patch("core.tasks.tickets.log.emit", emitted):
            result = T.validate_merged_ticket(_ctx(fake_config, "validate_merged_ticket"))
        assert result.status == "ok"
        assert result.artifacts == {"skipped": True}
        assert emitted.call_args.args[0] == "validation_skipped"

    def test_a_live_url_runs_the_validator_and_returns_its_summary(self, fake_config, tmp_state):
        _seed(status="done")
        cfg = {**fake_config, "validation": {"live_url": "http://live",
                                             "persistent_browser_context": True}}
        with patch("features.validation.validate_merged",
                   return_value={"verdict": "pass", "checks": 3}) as v:
            result = T.validate_merged_ticket(_ctx(cfg, "validate_merged_ticket"))
        assert result.status == "ok"
        assert result.artifacts == {"verdict": "pass", "checks": 3}
        assert v.call_args.kwargs["persistent_context"] is True
        assert v.call_args.args[3] == "http://live"

    def test_a_validator_exception_is_logged_and_fails(self, fake_config, tmp_state):
        _seed(status="done")
        cfg = {**fake_config, "validation": {"live_url": "http://live"}}
        emitted = MagicMock()
        with patch("features.validation.validate_merged", side_effect=RuntimeError("browser")), \
             patch("core.tasks.tickets.log.emit", emitted):
            result = T.validate_merged_ticket(_ctx(cfg, "validate_merged_ticket"))
        assert result.status == "failed"
        assert "RuntimeError: browser" in result.reason
        assert emitted.call_args.args[0] == "validation_error"

    def test_a_missing_ticket_fails(self, fake_config, tmp_state):
        state.save("tickets", {})
        result = T.validate_merged_ticket(_ctx(fake_config, "validate_merged_ticket"))
        assert result.status == "failed"
        assert result.reason == "ticket not found"


class TestScopeReview:
    def test_no_fingerprint_skips(self, fake_config, tmp_state):
        _seed()
        _ticket_dir(fake_config)
        with patch("core.tasks.tickets.scope_fingerprint", return_value=""):
            result = T.scope_review(_ctx(fake_config, "scope_review"))
        assert result.status == "skipped"
        assert result.reason == "no branch diff to review"

    def test_a_missing_ticket_dir_fails(self, fake_config, tmp_state):
        _seed()
        result = T.scope_review(_ctx(fake_config, "scope_review"))
        assert result.status == "failed"
        assert "ticket dir missing" in result.reason

    def test_a_pass_is_recorded_against_the_fingerprint_taken_first(self, fake_config, tmp_state):
        _seed()
        _ticket_dir(fake_config)
        with patch("core.tasks.tickets.scope_fingerprint", return_value="fp-1"), \
             patch("core.tasks.tickets.run_scope_review", return_value=("pass", "in scope")), \
             patch("core.tasks.tickets.log.emit") as emitted:
            result = T.scope_review(_ctx(fake_config, "scope_review"))
        assert result.status == "ok"
        recorded = state.load_ticket("PROJ-1")["scope_review"]
        assert recorded["verdict"] == "pass"
        assert recorded["fingerprint"] == "fp-1"
        assert emitted.call_args.args[0] == "ticket_scope_review_passed"

    def test_a_fail_emits_the_failed_event(self, fake_config, tmp_state):
        _seed()
        _ticket_dir(fake_config)
        with patch("core.tasks.tickets.scope_fingerprint", return_value="fp-1"), \
             patch("core.tasks.tickets.run_scope_review", return_value=("fail", "out of scope")), \
             patch("core.tasks.tickets.log.emit") as emitted:
            T.scope_review(_ctx(fake_config, "scope_review"))
        assert state.load_ticket("PROJ-1")["scope_review"]["verdict"] == "fail"
        assert emitted.call_args.args[0] == "ticket_scope_review_failed"

    def test_a_review_that_returns_no_verdict_fails(self, fake_config, tmp_state):
        _seed()
        _ticket_dir(fake_config)
        with patch("core.tasks.tickets.scope_fingerprint", return_value="fp-1"), \
             patch("core.tasks.tickets.run_scope_review", return_value=(None, "model unavailable")):
            result = T.scope_review(_ctx(fake_config, "scope_review"))
        assert result.status == "failed"
        assert result.reason == "model unavailable"
        assert "scope_review" not in state.load_ticket("PROJ-1")


class TestSetupPrdTicket:
    def _prd(self, **extra):
        return _seed(status="new", source="prd", approval_status="approved",
                     slug="", **extra)

    def test_materializes_and_enqueues_planning(self, fake_config, tmp_state):
        self._prd()
        materialized = {**state.load_ticket("PROJ-1"), "slug": SLUG, "branch": SLUG}
        with patch("features.tickets.materialize_prd_ticket", return_value=materialized), \
             patch("features.tickets._repo_gate_blocked", return_value=None), \
             patch("core.queue.enqueue_job") as eq:
            result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "ok"
        assert result.artifacts == {"slug": SLUG, "branch": SLUG}
        assert [c.args[1] for c in eq.call_args_list] == ["address_pm_findings", "start_planning"]

    def test_a_non_prd_ticket_is_skipped(self, fake_config, tmp_state):
        _seed(status="new", slug="")
        result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "skipped"
        assert result.reason == "not a PRD-source ticket"

    def test_an_unapproved_ticket_is_skipped(self, fake_config, tmp_state):
        _seed(status="new", source="prd", approval_status="pending", slug="")
        result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "skipped"
        assert result.reason == "ticket not approved"

    def test_an_already_materialized_ticket_is_skipped(self, fake_config, tmp_state):
        _seed(status="new", source="prd", approval_status="approved")
        result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "skipped"
        assert result.reason == "ticket already materialized"

    def test_a_busy_repo_skips_without_enqueueing(self, fake_config, tmp_state):
        self._prd()
        with patch("features.tickets._repo_gate_blocked", return_value="PROJ-9"), \
             patch("core.queue.enqueue_job") as eq:
            result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "skipped"
        assert result.reason == "repo busy with PROJ-9"
        eq.assert_not_called()

    def test_a_materialize_error_fails_without_enqueueing(self, fake_config, tmp_state):
        self._prd()
        with patch("features.tickets.materialize_prd_ticket",
                   side_effect=RuntimeError("no repo configured")), \
             patch("features.tickets._repo_gate_blocked", return_value=None), \
             patch("core.queue.enqueue_job") as eq:
            result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "failed"
        assert result.reason == "no repo configured"
        eq.assert_not_called()

    def test_a_missing_ticket_fails(self, fake_config, tmp_state):
        state.save("tickets", {})
        result = T.setup_prd_ticket(_ctx(fake_config, "setup_prd_ticket"))
        assert result.status == "failed"
        assert result.reason == "ticket not found"


class TestBackfillArtifacts:
    def test_a_missing_ticket_dir_fails(self, fake_config, tmp_state):
        _seed()
        result = T.backfill_artifacts(
            _ctx(fake_config, "backfill_artifacts",
                 payload={"pr_url": "http://pr/1", "repo": "myrepo"}))
        assert result.status == "failed"
        assert "ticket dir missing" in result.reason

    @pytest.mark.parametrize("payload", [
        {}, {"pr_url": "http://pr/1"}, {"repo": "myrepo"}])
    def test_an_incomplete_payload_fails(self, fake_config, tmp_state, payload):
        """run_claude_code is patched so that dropping the guard fails this
        test instead of shelling out to the real CLI and hanging it."""
        _seed()
        _ticket_dir(fake_config)
        runner = MagicMock(return_value="done")
        with patch("core.tasks.tickets.run_claude_code", runner), \
             patch("core.tasks.tickets.log.emit"):
            result = T.backfill_artifacts(
                _ctx(fake_config, "backfill_artifacts", payload=payload))
        runner.assert_not_called()
        assert result.status == "failed"
        assert result.reason == "payload must include pr_url and repo"

    def test_the_prompt_names_the_pr_the_repo_and_the_three_docs(self, fake_config, tmp_state):
        _seed()
        _ticket_dir(fake_config)
        runner = MagicMock(return_value="done")
        with patch("core.tasks.tickets.run_claude_code", runner), \
             patch("core.tasks.tickets.log.emit"):
            result = T.backfill_artifacts(
                _ctx(fake_config, "backfill_artifacts",
                     payload={"pr_url": "http://pr/1", "repo": "myrepo"}))
        assert result.status == "ok"
        prompt = runner.call_args.args[0]
        assert "http://pr/1" in prompt
        assert "./myrepo/" in prompt
        for doc in ("docs/technical-plan.md", "docs/change-manifest.md", "docs/tri-review.md"):
            assert doc in prompt

    def test_a_runner_that_returns_nothing_fails(self, fake_config, tmp_state):
        _seed()
        _ticket_dir(fake_config)
        with patch("core.tasks.tickets.run_claude_code", return_value=None), \
             patch("core.tasks.tickets.log.emit"):
            result = T.backfill_artifacts(
                _ctx(fake_config, "backfill_artifacts",
                     payload={"pr_url": "http://pr/1", "repo": "myrepo"}))
        assert result.status == "failed"
        assert result.reason == "claude returned non-zero or empty"
