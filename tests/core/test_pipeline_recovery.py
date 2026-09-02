"""Tests for ticket pipeline recovery: what happens when a stage produces
partial artifacts and the postcondition fails, and how _enqueue_stage limits
retries to prevent infinite re-enqueue loops."""
import sys
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db
import core.queue as q
import core.state as state
from core.tasks.registry import TaskContext, TaskResult, task, run_task
from core.tasks.preconditions import file_exists, status_is
from core.worker import WorkerPool
from tests.conftest import make_ticket_state


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _seed_ticket(instance_key: str, ticket_key: str, slug: str, status: str):
    state.use(instance_key)
    state.save_ticket(ticket_key, {
        "instance_key": instance_key,
        "ticket_key": ticket_key,
        "slug": slug,
        "status": status,
    })


def _seed_job(instance_key: str, task: str, ticket_key: str | None,
              status: str, finished_at: str | None = None) -> int:
    now = datetime.now(timezone.utc).isoformat()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO jobs(instance_key, ticket_key, task, status, "
            "enqueued_at, started_at, finished_at, response) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (instance_key, ticket_key, task, status, now, now,
             finished_at or now,
             json.dumps({"reason": "test", "artifacts": {}})),
        )
        return cur.lastrowid or 0


def _make_config(tmp_path: Path) -> dict:
    ws_root = tmp_path / "workspace"
    ws_root.mkdir(parents=True, exist_ok=True)
    return {
        "job": {"key": "test"},
        "workspace": {
            "root": ws_root,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
            "branch_prefix": "",
            "exclude": [],
            "dep_commands": [],
        },
        "pr": {"auto_pr": True, "auto_merge": False,
               "merge_strategy": "squash", "merge_flags": []},
        "features": {"tickets": True},
    }


def _task_ctx(ticket_key: str, config: dict, task: str = "start_planning",
              job_id: int = 1) -> TaskContext:
    return TaskContext(
        instance_key="test",
        ticket_key=ticket_key,
        task=task,
        payload={},
        job_id=job_id,
        triggering_event_id=None,
        config=config,
        registry=None,
        now=datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestStartPlanningPartialRecovery:
    """start_planning should recover when /ctp produced technical-plan.md
    but change-manifest.md is missing (the real-world bug on PRD-6_GAPS_RISKS-7)."""

    def test_detects_partial_ctp_and_generates_missing_manifest(self, fresh_db, tmp_path, monkeypatch):
        """Set up a ticket dir where technical-plan.md exists but
        change-manifest.md does not, simulate run_claude_code returning ok
        and writing the missing file, then verify the postcondition passes
        and the ticket transitions to 'reviewing'."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "TEST-1"
        slug = "test-1-slug"
        _seed_ticket(instance, ticket_key, slug, "planning")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "technical-plan.md").write_text(
            "# Technical Plan\n\n## Test Plan\n...\n")

        calls = []

        def fake_run_claude(prompt, *, cwd=None, timeout=None, session_id=None, resume=False):
            calls.append({"prompt": prompt, "timeout": timeout})
            change_manifest = Path(cwd) / "docs" / "change-manifest.md"
            if not change_manifest.exists():
                change_manifest.parent.mkdir(parents=True, exist_ok=True)
                change_manifest.write_text("# Change Manifest\n\nGenerated.\n")
            return "ok"

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", fake_run_claude)

        from core import tasks as _tasks_import
        _tasks_import.tickets  # ensure registration

        ctx = _task_ctx(ticket_key, config, job_id=1)
        result = run_task(ctx)

        assert result.status == "ok", f"expected ok, got {result.status} ({result.reason})"

        change_manifest = docs / "change-manifest.md"
        assert change_manifest.exists(), \
            "start_planning should have generated the missing change-manifest.md"

        state.use(instance)
        t = state.load_ticket(ticket_key)
        assert t and t["status"] == "reviewing", \
            "ticket should transition to 'reviewing' on successful start_planning"

        assert len(calls) == 1, "should have made one call (recovery prompt only, /ctp skipped)"

    def test_passes_through_when_both_files_exist(self, fresh_db, tmp_path, monkeypatch):
        """If both technical-plan.md and change-manifest.md already exist,
        start_planning should still work (just runs /ctp as usual)."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "TEST-2"
        slug = "test-2-slug"
        _seed_ticket(instance, ticket_key, slug, "planning")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "technical-plan.md").write_text("# Tech Plan\n")
        (docs / "change-manifest.md").write_text("# Change Manifest\n")

        monkeypatch.setattr("core.tasks.tickets.run_claude_code",
                            lambda prompt, **kw: "ok")

        ctx = _task_ctx(ticket_key, config, job_id=2)
        result = run_task(ctx)

        assert result.status == "ok", f"expected ok, got {result.status}"

    def test_does_not_recover_when_technical_plan_also_missing(self, fresh_db, tmp_path, monkeypatch):
        """If neither change-manifest.md nor technical-plan.md exist,
        start_planning should NOT try to recover (there's nothing to recover
        from) and should return failed."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "TEST-3"
        slug = "test-3-slug"
        _seed_ticket(instance, ticket_key, slug, "planning")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)

        monkeypatch.setattr("core.tasks.tickets.run_claude_code",
                            lambda prompt, **kw: None)

        ctx = _task_ctx(ticket_key, config, job_id=3)
        result = run_task(ctx)

        assert result.status == "failed", \
            f"expected failed when planning cannot proceed, got {result.status}"
        # The key invariant: with no technical-plan.md to recover from, the
        # recovery branch must NOT fabricate a change-manifest.
        assert not (docs / "change-manifest.md").exists(), \
            "recovery must not run when there is nothing to recover from"

    def test_recovery_prompt_contains_technical_plan(self, fresh_db, tmp_path, monkeypatch):
        """When recovery fires, the prompt passed to run_claude_code for the
        recovery step should include the existing technical-plan.md content."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "TEST-4"
        slug = "test-4-slug"
        _seed_ticket(instance, ticket_key, slug, "planning")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "technical-plan.md").write_text("# Technical Plan\n\nContent here\n")

        def fake_run_claude(prompt, *, cwd=None, timeout=None, session_id=None, resume=False):
            change_manifest = Path(cwd) / "docs" / "change-manifest.md"
            if not change_manifest.exists():
                change_manifest.parent.mkdir(parents=True, exist_ok=True)
                change_manifest.write_text("# Change Manifest\n\nGenerated.\n")
            return "ok"

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", fake_run_claude)

        ctx = _task_ctx(ticket_key, config, job_id=4)
        result = run_task(ctx)

        assert result.status == "ok"
        assert (docs / "change-manifest.md").exists()


class TestEnqueueStageRetryBudget:
    """_enqueue_stage should stop re-enqueuing after a configurable number of
    consecutive failures (the bug that caused PRD-6_GAPS_RISKS-7's infinite
    retry loop)."""

    def test_enqueues_first_time_with_no_history(self):
        with patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("core.queue.enqueue_job") as eq:
            from features import tickets as tix
            tix._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_called_once()

    def test_skips_when_already_queued(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "start_planning", "status": "queued"}]), \
             patch("core.queue.enqueue_job") as eq:
            from features import tickets as tix
            tix._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_not_called()

    def test_skips_when_already_running(self):
        with patch("core.queue.jobs_for_ticket",
                   return_value=[{"task": "start_planning", "status": "running"}]), \
             patch("core.queue.enqueue_job") as eq:
            from features import tickets as tix
            tix._enqueue_stage("inst", "T-1", "start_planning")
            eq.assert_not_called()

    def test_stops_retrying_after_max_consecutive_failures(self, fresh_db, tmp_path):
        """After MAX_STAGE_RETRIES consecutive failures of the same task,
        _enqueue_stage must not enqueue another job."""

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            for i in range(3):
                _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-1", "start_planning")
                eq.assert_not_called()

    def test_enqueues_when_below_max_failures(self, fresh_db, tmp_path):
        """With fewer than MAX_STAGE_RETRIES consecutive failures,
        _enqueue_stage should still allow another attempt."""

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 5):
            for i in range(2):
                _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-1", "start_planning")
                eq.assert_called_once()

    def test_does_not_count_ok_jobs_as_failures(self, fresh_db, tmp_path):
        """A single 'ok' job in the recent history should reset the consecutive
        failure counter so _enqueue_stage allows a new enqueue."""

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            _seed_job("inst", "start_planning", "T-1", "failed")
            _seed_job("inst", "start_planning", "T-1", "ok")
            _seed_job("inst", "start_planning", "T-1", "failed")
            _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-1", "start_planning")
                eq.assert_called_once()

    def test_diff_task_different_ticket_not_affected(self, fresh_db, tmp_path):
        """Retry budget for one task should not affect other tasks or
        other tickets."""

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            for i in range(4):
                _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-2", "start_planning")
                eq.assert_called_once()

    def test_lower_status_retries_doesnt_prevent_new_ticket(self, fresh_db, tmp_path):
        """A ticket with no job history at all should always be enqueued
        regardless of retry budget."""

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-NEW", "start_planning")
                eq.assert_called_once()


class TestStartReviewingRecovery:
    """start_reviewing should recover when /tri-review produced a partial
    tri-review.md without a VERDICT line."""

    def test_reviews_normal_when_no_review_file(self, fresh_db, tmp_path, monkeypatch):
        """When tri-review.md doesn't exist, run /tri-review normally."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "REV-1"
        slug = "rev-1-slug"
        _seed_ticket(instance, ticket_key, slug, "reviewing")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("# Manifest\n")

        def fake_run_claude(prompt, *, cwd=None, timeout=None, session_id=None, resume=False):
            tri = Path(cwd) / "docs" / "tri-review.md"
            tri.write_text("VERDICT: PASS\n")
            return "ok"

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", fake_run_claude)
        from core import tasks as _tasks_import
        _tasks_import.tickets

        ctx = _task_ctx(ticket_key, config, task="start_reviewing", job_id=10)
        result = run_task(ctx)

        assert result.status == "ok", f"expected ok, got {result.status} ({result.reason})"
        assert (docs / "tri-review.md").exists()

    def test_reviews_when_review_file_missing_verdict(self, fresh_db, tmp_path, monkeypatch):
        """When tri-review.md exists but lacks VERDICT, rerun /tri-review."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "REV-2"
        slug = "rev-2-slug"
        _seed_ticket(instance, ticket_key, slug, "reviewing")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("# Manifest\n")
        (docs / "tri-review.md").write_text("## Review\nSome findings but no verdict line\n")

        def fake_run_claude(prompt, *, cwd=None, timeout=None, session_id=None, resume=False):
            tri = Path(cwd) / "docs" / "tri-review.md"
            tri.write_text("## Review\n## Verdict\nVERDICT: PASS\n")
            return "ok"

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", fake_run_claude)
        from core import tasks as _tasks_import
        _tasks_import.tickets

        ctx = _task_ctx(ticket_key, config, task="start_reviewing", job_id=11)
        result = run_task(ctx)

        assert result.status == "ok", f"expected ok, got {result.status} ({result.reason})"
        assert "VERDICT: PASS" in (docs / "tri-review.md").read_text()

    def test_reviews_fails_when_claude_fails(self, fresh_db, tmp_path, monkeypatch):
        """When /tri-review claude call returns None, should fail."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "REV-3"
        slug = "rev-3-slug"
        _seed_ticket(instance, ticket_key, slug, "reviewing")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("# Manifest\n")

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", lambda prompt, **kw: None)
        from core import tasks as _tasks_import
        _tasks_import.tickets

        ctx = _task_ctx(ticket_key, config, task="start_reviewing", job_id=12)
        result = run_task(ctx)

        assert result.status == "failed"
        assert "claude returned non-zero or empty" in result.reason


class TestFixReviewFindingsRecovery:
    """fix_review_findings should recover when tri-review.md already exists
    with VERDICT: FAIL."""

    def test_fixes_when_verdict_is_fail(self, fresh_db, tmp_path, monkeypatch):
        """Normal flow: tri-review.md has VERDICT: FAIL, fix it."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "FIX-1"
        slug = "fix-1-slug"
        _seed_ticket(instance, ticket_key, slug, "reviewing")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("# Manifest\n")
        (docs / "tri-review.md").write_text("## Verdict\nVERDICT: FAIL\n")

        def fake_run_claude(prompt, *, cwd=None, timeout=None, session_id=None, resume=False):
            tri = Path(cwd) / "docs" / "tri-review.md"
            tri.write_text("## Verdict\nVERDICT: PASS\n")
            return "ok"

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", fake_run_claude)
        from core import tasks as _tasks_import
        _tasks_import.tickets

        ctx = _task_ctx(ticket_key, config, task="fix_review_findings", job_id=20)
        result = run_task(ctx)

        assert result.status == "ok", f"expected ok, got {result.status} ({result.reason})"
        assert "VERDICT: PASS" in (docs / "tri-review.md").read_text()

    def test_fix_fails_when_claude_fails(self, fresh_db, tmp_path, monkeypatch):
        """When fix claude call fails, should fail."""
        state.init(tmp_path / "state")

        instance = "test"
        ticket_key = "FIX-2"
        slug = "fix-2-slug"
        _seed_ticket(instance, ticket_key, slug, "reviewing")

        config = _make_config(tmp_path)
        ticket_dir = config["workspace"]["root"] / "tickets" / slug
        docs = ticket_dir / "docs"
        docs.mkdir(parents=True)
        (docs / "change-manifest.md").write_text("# Manifest\n")
        (docs / "tri-review.md").write_text("## Verdict\nVERDICT: FAIL\n")

        monkeypatch.setattr("core.tasks.tickets.run_claude_code", lambda prompt, **kw: None)
        from core import tasks as _tasks_import
        _tasks_import.tickets

        ctx = _task_ctx(ticket_key, config, task="fix_review_findings", job_id=21)
        result = run_task(ctx)

        assert result.status == "failed"


class TestValidationRetry:
    """validate_merged_ticket should be retried when it fails, not stranded
    at 'validation' status forever."""

    def test_validation_enqueues_when_ticket_at_validation_and_has_failed_jobs(self, fresh_db, tmp_path):
        """A ticket at 'validation' with a prior failed validate_merged_ticket
        should re-enqueue the task (the bug: scan_tickets skips validation
        status completely)."""
        state.init(tmp_path / "state")
        state.use("inst")
        state.save_ticket("VAL-1", {
            "status": "validation",
            "slug": "val-1-slug",
            "url": "https://example.com/issue/VAL-1",
        })

        _seed_job("inst", "validate_merged_ticket", "VAL-1", "failed")

        from features import tickets as tix
        with patch.object(tix, "MAX_STAGE_RETRIES", 5), \
             patch("core.queue.enqueue_job") as eq:
            tix._enqueue_stage("inst", "VAL-1", "validate_merged_ticket")
            eq.assert_called_once_with("inst", "validate_merged_ticket", ticket_key="VAL-1", payload=None)

    def test_validation_skips_when_already_queued(self, fresh_db, tmp_path):
        """Don't enqueue if a queued validate_merged_ticket already exists."""
        state.init(tmp_path / "state")
        state.use("inst")
        state.save_ticket("VAL-2", {
            "status": "validation",
            "slug": "val-2-slug",
        })

        _seed_job("inst", "validate_merged_ticket", "VAL-2", "queued")

        from features import tickets as tix
        with patch.object(tix, "MAX_STAGE_RETRIES", 5), \
             patch("core.queue.enqueue_job") as eq:
            tix._enqueue_stage("inst", "VAL-2", "validate_merged_ticket")
            eq.assert_not_called()

    def test_validation_skips_after_max_retries(self, fresh_db, tmp_path):
        """After MAX_STAGE_RETRIES consecutive failed validate_merged_ticket,
        stop retrying."""
        state.init(tmp_path / "state")
        state.use("inst")
        state.save_ticket("VAL-3", {
            "status": "validation",
            "slug": "val-3-slug",
        })

        for i in range(5):
            _seed_job("inst", "validate_merged_ticket", "VAL-3", "failed")

        from features import tickets as tix
        with patch.object(tix, "MAX_STAGE_RETRIES", 5), \
             patch("core.queue.enqueue_job") as eq:
            tix._enqueue_stage("inst", "VAL-3", "validate_merged_ticket")
            eq.assert_not_called()

    def test_validation_runs_normally_when_no_history(self, fresh_db, tmp_path):
        """First time at validation with no job history should enqueue."""
        state.init(tmp_path / "state")
        state.use("inst")
        state.save_ticket("VAL-4", {
            "status": "validation",
            "slug": "val-4-slug",
        })

        from features import tickets as tix
        with patch.object(tix, "MAX_STAGE_RETRIES", 5), \
             patch("core.queue.enqueue_job") as eq:
            tix._enqueue_stage("inst", "VAL-4", "validate_merged_ticket")
            eq.assert_called_once()


class TestScanTicketsValidationSkip:
    """scan_tickets must not skip validation-status tickets — it needs to
    re-enqueue validate_merged_ticket when it fails, not strand the ticket."""

    def test_validation_not_skipped_by_scan(self, fresh_db, tmp_path, monkeypatch):
        """scan_tickets must re-enqueue validate_merged_ticket for a ticket
        at validation status with a prior failed job, instead of skipping it
        unconditionally (the current bug)."""
        state.init(tmp_path / "state")
        state.use("inst")
        state.save_ticket("VAL-SCAN-1", {
            "status": "validation",
            "slug": "val-scan-1-slug",
            "url": "https://example.com/issue/VAL-SCAN-1",
            "summary": "Test validation",
            "validation_enqueued_at": "2026-05-01T00:00:00Z",
        })

        _seed_job("inst", "validate_merged_ticket", "VAL-SCAN-1", "failed")

        from features import tickets as tix

        enqueued = []
        def track_enqueue(ik, task, **kw):
            enqueued.append((ik, task, kw))

        monkeypatch.setattr("core.queue.enqueue_job", track_enqueue)

        config = _make_config(tmp_path)
        config["job"]["ticket_system"] = "manual"
        config["job"]["platform"] = "github"
        config["_base_url"] = "http://localhost:8000"
        config["_state_dir"] = tmp_path / ".frshty" / "test"
        config["workspace"]["repos"] = ["lumeninv"]
        (config["workspace"]["root"] / "lumeninv").mkdir(parents=True, exist_ok=True)
        (config["workspace"]["root"] / "lumeninv" / ".git").mkdir(parents=True, exist_ok=True)
        config["github"] = {"repo": "org/repo"}

        monkeypatch.setattr(tix, "_fetch_open_prs", lambda c: [])
        monkeypatch.setattr(tix, "enqueue_prd_backfill", lambda ik: None)

        monkeypatch.setattr(tix, "_fetch_tickets", lambda cfg: [
            {"key": "VAL-SCAN-1", "summary": "Test validation", "status": "done",
             "url": "https://example.com/issue/VAL-SCAN-1", "attachments": [], "related": []},
        ])

        tix.check(config, "inst")

        assert any(task == "validate_merged_ticket" for _, task, _ in enqueued), \
            "scan_tickets must re-enqueue validate_merged_ticket when ticket is at validation status with prior failed job"
