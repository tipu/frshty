"""Tests for `_handle_proving_ticket` dispatcher routing and the prove task."""
import subprocess

import pytest
from unittest.mock import patch

import features.ticket_states as ts_mod
from core.tasks import tickets as T
from core.tasks.tickets import prove
from core.tasks.registry import TaskContext


def _seed(tmp_path, ticket_key="PROJ-1", slug="PROJ-1-do-the-thing"):
    docs = tmp_path / "tickets" / slug / "docs"
    docs.mkdir(parents=True)
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "_base_url": "http://localhost:8000",
    }
    ticket = {"key": ticket_key}
    state_dict = {"status": "proving", "slug": slug}
    return config, ticket, state_dict, docs


class TestHandlerRouting:
    def test_no_proof_yet_enqueues_prove(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_proving_ticket(config, ticket, state_dict,
                                           config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "prove")

    def test_proof_exists_enqueues_mark_ready(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "proof.md").write_text("PROOF: DEMOED\n\nRecorded docs/demo.webm\n")
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_proving_ticket(config, ticket, state_dict,
                                           config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "mark_ready")

    def test_not_applicable_proof_also_enqueues_mark_ready(self, tmp_path):
        """A 'NOT_APPLICABLE' verdict still completes the proving step —
        claude assessed that the change isn't demoable, and we accept that
        as a valid outcome."""
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "proof.md").write_text("PROOF: NOT_APPLICABLE\n\nPure refactor.\n")
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_proving_ticket(config, ticket, state_dict,
                                           config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "mark_ready")

    def test_no_instance_key_short_circuits(self, tmp_path):
        config, ticket, state_dict, _ = _seed(tmp_path)
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_proving_ticket(config, ticket, state_dict,
                                           config["_base_url"], "", True)
        eq.assert_not_called()


class TestProveTask:
    def _ctx(self, tmp_path, slug="PROJ-1-do-the-thing"):
        ticket_dir = tmp_path / "tickets" / slug
        (ticket_dir / "docs").mkdir(parents=True)
        config = {
            "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
            "_base_url": "http://localhost:8000",
        }
        return TaskContext(
            instance_key="aimyable", ticket_key="PROJ-1", task="prove",
            payload={}, job_id=0, triggering_event_id=None, config=config,
            registry=None, now=None,
        )

    def test_prove_runs_claude_with_proof_md_inlined(self, tmp_path):
        ctx = self._ctx(tmp_path)
        proof_content = "# How to prove\n\n1. Use playwright. 2. Record demo.webm.\n"
        (tmp_path / "PROOF.md").write_text(proof_content)
        with patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._claim_session",
                   return_value=(None, False)), \
             patch("core.tasks.tickets.run_claude_code",
                   return_value="proof-done") as rc:
            result = prove(ctx)
        assert result.status == "ok"
        rc.assert_called_once()
        prompt = rc.call_args.args[0]
        # PROOF.md content must be inlined verbatim into the prompt
        assert proof_content in prompt
        assert "follow it" in prompt.lower() or "follow the" in prompt.lower()

    def test_prove_with_missing_proof_md_writes_not_applicable(self, tmp_path):
        """Defensive: if PROOF.md vanished between enter_proving and prove
        (race or operator deletion), write a NOT_APPLICABLE proof.md and
        exit ok rather than spinning."""
        ctx = self._ctx(tmp_path)
        with patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets.run_claude_code") as rc:
            result = prove(ctx)
        assert result.status == "ok"
        assert result.artifacts.get("skipped") is True
        # Did NOT call claude — short-circuited
        rc.assert_not_called()
        proof = tmp_path / "tickets" / "PROJ-1-do-the-thing" / "docs" / "proof.md"
        assert proof.exists()
        assert "NOT_APPLICABLE" in proof.read_text()

    def test_prove_claude_failure_returns_failed(self, tmp_path):
        ctx = self._ctx(tmp_path)
        (tmp_path / "PROOF.md").write_text("# proof guide\n\ncontent\n")
        with patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._claim_session",
                   return_value=(None, False)), \
             patch("core.tasks.tickets.run_claude_code", return_value=None):
            result = prove(ctx)
        assert result.status == "failed"


class TestProofChangeScope:
    def test_uses_configured_base_branch_and_lists_only_changed_repo(self, tmp_path):
        repo = tmp_path / "ticket" / "rapid-analytics-ui"
        (repo / ".git").mkdir(parents=True)
        config = {
            "workspace": {
                "base_branch": "main",
                "base_branches": {"rapid-analytics-ui": "development"},
            }
        }
        completed = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="src/analytics.ts\n", stderr=""
        )
        with patch.object(T.subprocess, "run", return_value=completed) as run:
            scope = T._proof_change_scope(tmp_path / "ticket", config)

        assert "rapid-analytics-ui (base: development)" in scope
        assert "src/analytics.ts" in scope
        assert run.call_args.args[0][-1] == "origin/development...HEAD"

    def test_prompt_marks_omitted_repositories_out_of_scope(self, tmp_path):
        ctx = TestProveTask()._ctx(tmp_path)
        (tmp_path / "PROOF.md").write_text("# proof guide\n")
        with patch("core.state.load_ticket",
                   return_value={"slug": "PROJ-1-do-the-thing"}), \
             patch("core.tasks.tickets._claim_session",
                   return_value=(None, False)), \
             patch("core.tasks.tickets._proof_change_scope",
                   return_value="- backend (base: development)\n  - src/api.py"), \
             patch("core.tasks.tickets.run_claude_code",
                   return_value="proof-done") as rc:
            result = prove(ctx)

        assert result.status == "ok"
        prompt = rc.call_args.args[0]
        assert "backend (base: development)" in prompt
        assert "Treat every repository omitted from this list as unchanged" in prompt
