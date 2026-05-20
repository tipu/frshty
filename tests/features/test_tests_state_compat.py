"""Backward-compat tests for in-flight tickets when the testing-state feature
ships.

Every ticket that passes tri-review enters the `testing` state. TESTING.md
controls how plan_tests prompts claude, but never gates entry.
"""
import pytest
from unittest.mock import patch

from core.tasks.tickets import enter_testing
from core.tasks.registry import TaskContext, TaskResult


def _make_ctx(tmp_path, ticket_key="PROJ-1", slug="PROJ-1-do-the-thing"):
    ticket_dir = tmp_path / "tickets" / slug
    (ticket_dir / "docs").mkdir(parents=True)
    (ticket_dir / "docs" / "tri-review.md").write_text("VERDICT: PASS\n")
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "_base_url": "http://localhost:8000",
    }
    return TaskContext(
        instance_key="aimyable",
        ticket_key=ticket_key,
        task="enter_testing",
        payload={},
        job_id=0,
        triggering_event_id=None,
        config=config,
        registry=None,
        now=None,
    )


class TestEnterTestingIsPureTransition:
    def test_no_testing_md_still_enters_testing(self, tmp_path):
        """Removal of the gate: tickets without a TESTING.md still go through
        the testing state. plan_tests handles the missing-guide case by
        telling claude to investigate the codebase."""
        ctx = _make_ctx(tmp_path)
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._dirty_workspace_repos", return_value=[]):
            result = enter_testing(ctx)
        assert result.status == "ok"
        # ticket_dev_complete must NOT fire here — it fires later in mark_ready
        # after tests have actually passed.
        dispatch.assert_not_called()

    def test_substantive_testing_md_enters_testing(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        (tmp_path / "TESTING.md").write_text("# Testing\n\n" + "real content " * 50)
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._dirty_workspace_repos", return_value=[]):
            result = enter_testing(ctx)
        assert result.status == "ok"
        dispatch.assert_not_called()


class TestWorktreeGuard:
    def test_dirty_worktree_blocks_enter_testing(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._dirty_workspace_repos",
                   return_value=["saas-dashboard"]):
            result = enter_testing(ctx)
        assert result.status == "failed"
        assert "uncommitted" in result.reason
        dispatch.assert_not_called()
