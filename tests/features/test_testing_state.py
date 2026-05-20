"""Tests for `_handle_testing_ticket` dispatcher routing.

Walks the state machine inside the `testing` status: deciding which task to
enqueue next based on which docs artifacts are on disk and the verdict in
`docs/test-runs.md`. Mocks `_enqueue_stage` and the runner functions so the
tests run in seconds and don't burn LLM quota.
"""
import pytest
from unittest.mock import patch

import features.ticket_states as ts_mod
from features.tickets import MAX_TEST_FIX_ATTEMPTS


def _seed(tmp_path, ticket_key="PROJ-1", slug="PROJ-1-do-the-thing"):
    docs = tmp_path / "tickets" / slug / "docs"
    docs.mkdir(parents=True)
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "_base_url": "http://localhost:8000",
    }
    ticket = {"key": ticket_key}
    state_dict = {"status": "testing", "slug": slug}
    return config, ticket, state_dict, docs


class TestHandlerRouting:
    def test_no_test_plan_enqueues_plan_tests(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "plan_tests")

    def test_plan_exists_no_files_written_enqueues_write_tests(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "write_tests")

    def test_files_written_no_runs_enqueues_run_tests_and_fix(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        (docs / "test-files-written.txt").write_text("repo/tests/foo.test.ts\n")
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "run_tests_and_fix")

    def test_verdict_pass_enqueues_enter_proving(self, tmp_path):
        """PASS now hops through enter_proving, which gates on PROOF.md. If
        PROOF.md is absent, enter_proving short-circuits straight to
        pr_ready; otherwise it routes to proving. Either way the next
        scheduled job from this handler is enter_proving, not mark_ready."""
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        (docs / "test-files-written.txt").write_text("x")
        (docs / "test-runs.md").write_text("# attempt 1\nVERDICT: PASS\n")
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "enter_proving")

    def test_verdict_fail_under_cap_enqueues_run_tests_and_fix(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        (docs / "test-files-written.txt").write_text("x")
        (docs / "test-runs.md").write_text("# attempt 2\nVERDICT: FAIL\n")
        state_dict["test_fix_attempts"] = 2  # < cap of 3
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "run_tests_and_fix")

    def test_no_instance_key_short_circuits(self, tmp_path):
        config, ticket, state_dict, _ = _seed(tmp_path)
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "", True)
        eq.assert_not_called()


class TestCapBoundary:
    def test_third_fail_transitions_to_tests_failed(self, tmp_path):
        """When attempts == MAX_TEST_FIX_ATTEMPTS and verdict is still FAIL,
        the handler transitions the ticket to tests_failed (no further runs).
        """
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        (docs / "test-files-written.txt").write_text("x")
        (docs / "test-runs.md").write_text("# attempt 3\nVERDICT: FAIL\n")
        state_dict["test_fix_attempts"] = MAX_TEST_FIX_ATTEMPTS

        with patch("features.tickets._enqueue_stage") as eq, \
             patch("core.state.transition_ticket") as trans, \
             patch("core.log.emit") as emit:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)

        trans.assert_called_once_with("PROJ-1", "tests_failed")
        # No further run_tests_and_fix invocations once we've hit the cap
        for call in eq.call_args_list:
            assert call.args[2] != "run_tests_and_fix"
        # emit_once should have fired the human-attention event exactly once
        events = [c.args[0] for c in emit.call_args_list]
        assert "ticket_tests_failed" in events

    def test_cap_minus_one_still_retries(self, tmp_path):
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        (docs / "test-files-written.txt").write_text("x")
        (docs / "test-runs.md").write_text("# attempt 2\nVERDICT: FAIL\n")
        state_dict["test_fix_attempts"] = MAX_TEST_FIX_ATTEMPTS - 1
        with patch("features.tickets._enqueue_stage") as eq, \
             patch("core.state.transition_ticket") as trans:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        trans.assert_not_called()
        eq.assert_called_once_with("aimyable", "PROJ-1", "run_tests_and_fix")


class TestUnparseableVerdict:
    def test_no_verdict_line_treated_as_rerun(self, tmp_path):
        """test-runs.md exists but is malformed (LLM wrote weird stuff). Don't
        crash; just rerun."""
        config, ticket, state_dict, docs = _seed(tmp_path)
        (docs / "test-plan.md").write_text("plan")
        (docs / "test-files-written.txt").write_text("x")
        (docs / "test-runs.md").write_text("# attempt 1\n(no verdict line here)\n")
        with patch("features.tickets._enqueue_stage") as eq:
            ts_mod._handle_testing_ticket(config, ticket, state_dict,
                                          config["_base_url"], "aimyable", True)
        eq.assert_called_once_with("aimyable", "PROJ-1", "run_tests_and_fix")
