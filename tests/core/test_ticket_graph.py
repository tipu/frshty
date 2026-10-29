"""The graph in core/ticket_status.py is the one place that selects edges.

It holds the legal edges, the edge each task takes on success, and the
bounded retry loops. These tests read the expected values from literals, not
from the code under test.
"""
import pytest

from core.branch_sync import MAX_BASE_SYNC_ATTEMPTS
from core.comments import MAX_AUTOMATED_REVIEW_FIX_RUNS
from core.consensus_scope import MAX_SCOPE_FIX_ATTEMPTS
from core.tasks.registry import _REGISTRY, task
from core.ticket_status import (AUTOMATED_REVIEW_FIX, BASE_SYNC, CYCLES, PR_AUTOFIX,
                                SCOPE_FIX, SUCCESS_EDGES, TicketStatus, can_transition,
                                success_target)
from features.pr_autofix import MAX_ATTEMPTS as PR_AUTOFIX_MAX_ATTEMPTS

EXPECTED_SUCCESS = {
    "start_planning": "reviewing",
    "enter_testing": "testing",
    "mark_ready": "pr_ready",
    "do_research": "done",
    "validate_merged_ticket": "done",
}


class TestSuccessEdges:
    def test_every_success_edge_names_a_registered_task(self):
        assert set(SUCCESS_EDGES) <= set(_REGISTRY)

    @pytest.mark.parametrize("name,target", sorted(EXPECTED_SUCCESS.items()))
    def test_fixed_edges(self, name, target):
        assert success_target(name, None, {}) == target

    def test_enter_proving_selects_by_proof(self):
        assert success_target("enter_proving", "testing", {"has_proof_md": True}) == "proving"
        assert success_target("enter_proving", "testing", {"has_proof_md": False}) == "pr_ready"
        assert success_target("enter_proving", "testing", {}) == "pr_ready"

    def test_scope_fix_returns_to_proving_only_from_pr_ready(self):
        assert success_target("fix_scope_findings", "pr_ready", {}) == "proving"
        assert success_target("fix_scope_findings", "proving", {}) is None
        assert success_target("fix_scope_findings", "in_review", {}) is None

    def test_a_task_outside_the_table_has_no_edge(self):
        assert success_target("sync_pr_base", "in_review", {}) is None

    def test_registry_reads_the_table(self):
        for name in EXPECTED_SUCCESS:
            assert _REGISTRY[name]["on_success_status"] == EXPECTED_SUCCESS[name]

    def test_a_graph_task_cannot_declare_its_own_edge(self):
        with pytest.raises(ValueError, match="SUCCESS_EDGES"):
            task("mark_ready", on_success_status="in_review")
        assert _REGISTRY["mark_ready"]["on_success_status"] == "pr_ready"


class TestCycles:
    def test_the_four_loops_are_declared(self):
        assert {c.name for c in CYCLES} == {"scope_fix", "base_sync", "pr_autofix",
                                            "automated_review_fix"}

    def test_budgets(self):
        assert (SCOPE_FIX.budget, BASE_SYNC.budget, PR_AUTOFIX.budget,
                AUTOMATED_REVIEW_FIX.budget) == (2, 2, 3, 2)

    def test_module_constants_read_the_graph(self):
        assert MAX_SCOPE_FIX_ATTEMPTS == SCOPE_FIX.budget
        assert MAX_BASE_SYNC_ATTEMPTS == BASE_SYNC.budget
        assert PR_AUTOFIX_MAX_ATTEMPTS == PR_AUTOFIX.budget
        assert MAX_AUTOMATED_REVIEW_FIX_RUNS == AUTOMATED_REVIEW_FIX.budget

    @pytest.mark.parametrize("cycle", CYCLES, ids=lambda c: c.name)
    def test_every_cycle_task_is_registered(self, cycle):
        assert cycle.task in _REGISTRY

    @pytest.mark.parametrize("cycle", [c for c in CYCLES if c.node], ids=lambda c: c.name)
    def test_a_ticket_cycle_can_return_to_its_node(self, cycle):
        assert can_transition(cycle.node.value, cycle.node.value)

    def test_scope_fix_reenters_proving_from_pr_ready(self):
        assert SCOPE_FIX.node is TicketStatus.proving
        assert can_transition("pr_ready", "proving")

    @pytest.mark.parametrize("cycle", CYCLES, ids=lambda c: c.name)
    def test_exit_check(self, cycle):
        for attempts in range(cycle.budget):
            assert cycle.exit_reason(attempts) is None
            assert cycle.may_continue(attempts)
            assert cycle.exit_reason(attempts, converged=True) == "converged"
        assert cycle.exit_reason(cycle.budget) == "budget_spent"
        assert not cycle.may_continue(cycle.budget)
        assert cycle.exit_reason(cycle.budget + 1) == "budget_spent"
        assert cycle.exit_reason(cycle.budget, converged=True) == "converged"
