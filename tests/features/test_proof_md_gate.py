"""Tests for the PROOF.md gate.

Unlike TESTING.md (which is a hint, not a gate), PROOF.md is a true gate:
- present → enter_proving transitions testing → proving, prove runs
- absent → enter_proving fires ticket_dev_complete inline and transitions
  testing → pr_ready directly (skip path)
"""
import pytest
from unittest.mock import patch

from core.tasks.tickets import enter_proving, _enter_proving_target
from core.tasks.registry import TaskContext, TaskResult


def _make_ctx(tmp_path, ticket_key="PROJ-1", slug="PROJ-1-do-the-thing"):
    ticket_dir = tmp_path / "tickets" / slug
    (ticket_dir / "docs").mkdir(parents=True)
    (ticket_dir / "docs" / "test-runs.md").write_text("VERDICT: PASS\n")
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "_base_url": "http://localhost:8000",
    }
    return TaskContext(
        instance_key="aimyable",
        ticket_key=ticket_key,
        task="enter_proving",
        payload={},
        job_id=0,
        triggering_event_id=None,
        config=config,
        registry=None,
        now=None,
    )


class TestEnterProvingSkipPath:
    def test_no_proof_md_fires_event_and_returns_skip_artifact(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing",
                                 "branch": "b",
                                 "estimate_seconds": 0,
                                 "discovered_at": ""}), \
             patch("core.tasks.tickets._dirty_workspace_repos", return_value=[]):
            result = enter_proving(ctx)
        assert result.status == "ok"
        assert result.artifacts.get("has_proof_md") is False
        dispatch.assert_called_once()
        evt_name, payload, _ = dispatch.call_args.args
        assert evt_name == "ticket_dev_complete"
        assert payload["ticket_key"] == "PROJ-1"

    def test_empty_proof_md_takes_skip_path(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        (tmp_path / "PROOF.md").write_text("")
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing",
                                 "branch": "b",
                                 "estimate_seconds": 0,
                                 "discovered_at": ""}), \
             patch("core.tasks.tickets._dirty_workspace_repos", return_value=[]):
            result = enter_proving(ctx)
        assert result.artifacts.get("has_proof_md") is False
        dispatch.assert_called_once()

    def test_whitespace_only_proof_md_takes_skip_path(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        (tmp_path / "PROOF.md").write_text("\n\n   \t\n")
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing",
                                 "branch": "b",
                                 "estimate_seconds": 0,
                                 "discovered_at": ""}), \
             patch("core.tasks.tickets._dirty_workspace_repos", return_value=[]):
            result = enter_proving(ctx)
        assert result.artifacts.get("has_proof_md") is False
        dispatch.assert_called_once()


class TestEnterProvingActivatePath:
    def test_present_proof_md_does_not_fire_event_yet(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        (tmp_path / "PROOF.md").write_text("# How to prove\n\nUse playwright.\n")
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._dirty_workspace_repos", return_value=[]):
            result = enter_proving(ctx)
        assert result.status == "ok"
        assert result.artifacts.get("has_proof_md") is True
        # Event must fire later in mark_ready, not here.
        dispatch.assert_not_called()


class TestWorktreeGuard:
    def test_dirty_worktree_blocks_enter_proving(self, tmp_path):
        ctx = _make_ctx(tmp_path)
        with patch("core.events.dispatch") as dispatch, \
             patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._dirty_workspace_repos",
                   return_value=["saas-dashboard"]):
            result = enter_proving(ctx)
        assert result.status == "failed"
        assert "uncommitted" in result.reason
        dispatch.assert_not_called()


class TestEnterProvingTarget:
    def _ctx(self):
        return TaskContext(
            instance_key="aimyable", ticket_key="PROJ-1", task="enter_proving",
            payload={}, job_id=0, triggering_event_id=None, config={},
            registry=None, now=None,
        )

    def test_routes_to_proving_when_proof_present(self):
        result = TaskResult("ok", artifacts={"has_proof_md": True})
        assert _enter_proving_target(self._ctx(), result) == "proving"

    def test_routes_to_pr_ready_when_proof_absent(self):
        result = TaskResult("ok", artifacts={"has_proof_md": False})
        assert _enter_proving_target(self._ctx(), result) == "pr_ready"

    def test_routes_to_pr_ready_when_artifacts_missing_key(self):
        """Default safety: missing key → skip path (pr_ready), not
        unintended activation of proving."""
        result = TaskResult("ok", artifacts={})
        assert _enter_proving_target(self._ctx(), result) == "pr_ready"
