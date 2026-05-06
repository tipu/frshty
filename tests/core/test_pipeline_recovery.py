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

    def test_detects_partial_ctp_and_generates_missing_manifest(self, tmp_path, monkeypatch):
        """Set up a ticket dir where technical-plan.md exists but
        change-manifest.md does not, simulate run_claude_code returning ok
        and writing the missing file, then verify the postcondition passes
        and the ticket transitions to 'reviewing'."""
        db.init(tmp_path / "t.db", ROOT / "migrations")
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

        def fake_run_claude(prompt, *, cwd=None, timeout=None):
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

    def test_passes_through_when_both_files_exist(self, tmp_path, monkeypatch):
        """If both technical-plan.md and change-manifest.md already exist,
        start_planning should still work (just runs /ctp as usual)."""
        db.init(tmp_path / "t.db", ROOT / "migrations")
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

    def test_does_not_recover_when_technical_plan_also_missing(self, tmp_path, monkeypatch):
        """If neither change-manifest.md nor technical-plan.md exist,
        start_planning should NOT try to recover (there's nothing to recover
        from) and should return failed."""
        db.init(tmp_path / "t.db", ROOT / "migrations")
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
            f"expected failed when claude returns None, got {result.status}"
        assert "claude returned non-zero or empty" in result.reason

    def test_recovery_prompt_contains_technical_plan(self, tmp_path, monkeypatch):
        """When recovery fires, the prompt passed to run_claude_code for the
        recovery step should include the existing technical-plan.md content."""
        db.init(tmp_path / "t.db", ROOT / "migrations")
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

        def fake_run_claude(prompt, *, cwd=None, timeout=None):
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

    def test_stops_retrying_after_max_consecutive_failures(self, tmp_path):
        """After MAX_STAGE_RETRIES consecutive failures of the same task,
        _enqueue_stage must not enqueue another job."""
        db.init(tmp_path / "t.db", ROOT / "migrations")

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            for i in range(3):
                _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-1", "start_planning")
                eq.assert_not_called()

    def test_enqueues_when_below_max_failures(self, tmp_path):
        """With fewer than MAX_STAGE_RETRIES consecutive failures,
        _enqueue_stage should still allow another attempt."""
        db.init(tmp_path / "t.db", ROOT / "migrations")

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 5):
            for i in range(2):
                _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-1", "start_planning")
                eq.assert_called_once()

    def test_does_not_count_ok_jobs_as_failures(self, tmp_path):
        """A single 'ok' job in the recent history should reset the consecutive
        failure counter so _enqueue_stage allows a new enqueue."""
        db.init(tmp_path / "t.db", ROOT / "migrations")

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            _seed_job("inst", "start_planning", "T-1", "failed")
            _seed_job("inst", "start_planning", "T-1", "ok")
            _seed_job("inst", "start_planning", "T-1", "failed")
            _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-1", "start_planning")
                eq.assert_called_once()

    def test_diff_task_different_ticket_not_affected(self, tmp_path):
        """Retry budget for one task should not affect other tasks or
        other tickets."""
        db.init(tmp_path / "t.db", ROOT / "migrations")

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            for i in range(4):
                _seed_job("inst", "start_planning", "T-1", "failed")

            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-2", "start_planning")
                eq.assert_called_once()

    def test_lower_status_retries_doesnt_prevent_new_ticket(self, tmp_path):
        """A ticket with no job history at all should always be enqueued
        regardless of retry budget."""
        db.init(tmp_path / "t.db", ROOT / "migrations")

        from features import tickets as tix

        with patch.object(tix, "MAX_STAGE_RETRIES", 3):
            with patch("core.queue.enqueue_job") as eq:
                tix._enqueue_stage("inst", "T-NEW", "start_planning")
                eq.assert_called_once()
