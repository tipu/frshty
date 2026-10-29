from dataclasses import dataclass
from enum import Enum
from typing import Callable


class TicketStatus(str, Enum):
    pending_approval = "pending_approval"
    new = "new"
    researching = "researching"
    planning = "planning"
    reviewing = "reviewing"
    testing = "testing"
    tests_failed = "tests_failed"
    proving = "proving"
    pr_ready = "pr_ready"
    in_review = "in_review"
    merged = "merged"
    validation = "validation"
    pr_failed = "pr_failed"
    blocked = "blocked"
    done = "done"
    # Container issues (Jira Epic, Linear Project, etc.) — frshty tracks them
    # for UI grouping of their child stories but never plans / codes / PRs them.
    # Terminal: the only legal transition out is `done` (handled implicitly by
    # `transition()` so every status can reach done).
    epic = "epic"
    # Manually ignored. Terminal except for an explicit un-ignore back to new.
    # check() never dispatches it (no status handler) and discovery never
    # resurrects it because its row stays in state.
    ignored = "ignored"


_ALLOWED: dict[TicketStatus, set[TicketStatus]] = {
    TicketStatus.pending_approval: {TicketStatus.new, TicketStatus.done},
    TicketStatus.new:        {TicketStatus.planning, TicketStatus.researching, TicketStatus.merged, TicketStatus.epic},
    TicketStatus.researching: {TicketStatus.done, TicketStatus.new},
    TicketStatus.planning:   {TicketStatus.reviewing, TicketStatus.blocked},
    TicketStatus.reviewing:  {TicketStatus.testing, TicketStatus.pr_ready, TicketStatus.planning, TicketStatus.blocked},
    TicketStatus.testing:    {TicketStatus.proving, TicketStatus.pr_ready, TicketStatus.tests_failed, TicketStatus.reviewing, TicketStatus.blocked},
    TicketStatus.tests_failed: {TicketStatus.testing, TicketStatus.reviewing, TicketStatus.pr_ready, TicketStatus.done},
    TicketStatus.proving:    {TicketStatus.pr_ready, TicketStatus.testing, TicketStatus.reviewing, TicketStatus.blocked},
    TicketStatus.pr_ready:   {TicketStatus.testing, TicketStatus.proving, TicketStatus.reviewing, TicketStatus.in_review, TicketStatus.pr_failed, TicketStatus.merged},
    TicketStatus.in_review:  {TicketStatus.merged, TicketStatus.in_review, TicketStatus.pr_failed, TicketStatus.proving},
    TicketStatus.merged:     {TicketStatus.validation, TicketStatus.new},
    TicketStatus.validation: {TicketStatus.done, TicketStatus.new},
    TicketStatus.pr_failed:  {TicketStatus.pr_ready, TicketStatus.in_review, TicketStatus.merged},
    TicketStatus.done:       {TicketStatus.new, TicketStatus.pr_ready, TicketStatus.testing, TicketStatus.proving, TicketStatus.in_review},
    TicketStatus.epic:       set(),  # epic is terminal; transition() still permits → done universally
    TicketStatus.ignored:    {TicketStatus.new},  # only legal exit is un-ignore → new
    TicketStatus.blocked:    {TicketStatus.new, TicketStatus.planning,
                             TicketStatus.reviewing, TicketStatus.testing,
                             TicketStatus.proving},
}


def can_transition(current: str, target: str, *, blocked_from: str | None = None) -> bool:
    try:
        transition(current, target, blocked_from=blocked_from)
    except ValueError:
        return False
    return True


def transition(current: str, target: str, *, blocked_from: str | None = None) -> str:
    """The legal next status, or ValueError.

    `blocked_from` is the status a blocked ticket was parked from. The table
    above says which shapes of resume exist at all; this argument says which
    one this ticket has earned. A caller that does not supply it leaves
    `new` as the only exit, which is what `blocked` meant before resume
    existed, so an unknown history never widens anything.
    """
    cur = TicketStatus(current)
    tgt = TicketStatus(target)
    if tgt == cur:
        return tgt.value
    if tgt == TicketStatus.done:
        return tgt.value
    if tgt not in _ALLOWED.get(cur, set()):
        raise ValueError(f"Illegal transition: {cur.value} -> {tgt.value}")
    if cur is TicketStatus.blocked and tgt is not TicketStatus.new \
            and tgt.value != blocked_from:
        raise ValueError(
            f"Illegal transition: {cur.value} -> {tgt.value}; "
            f"blocked from {blocked_from or 'nowhere recorded'}"
        )
    return tgt.value


def _enter_proving_edge(status: str | None, artifacts: dict) -> str:
    """enter_proving routes to `proving` when a PROOF.md exists at
    workspace.root, otherwise it skips straight to `pr_ready`. A missing
    artifact takes the skip path."""
    return "proving" if artifacts.get("has_proof_md") else "pr_ready"


def _scope_fix_edge(status: str | None, artifacts: dict) -> str | None:
    """fix_scope_findings goes back to `proving` only while the ticket is
    still holding its PR. A ticket corrected before its proof is already in
    `proving` and stays there. A ticket whose PR the operator opened over the
    verdict keeps its status, because the body pushed to that PR."""
    return "proving" if status == "pr_ready" else None


SuccessEdge = str | Callable[[str | None, dict], str | None]

SUCCESS_EDGES: dict[str, SuccessEdge] = {
    "start_planning": TicketStatus.reviewing.value,
    "enter_testing": TicketStatus.testing.value,
    "enter_proving": _enter_proving_edge,
    "mark_ready": TicketStatus.pr_ready.value,
    "fix_scope_findings": _scope_fix_edge,
    "do_research": TicketStatus.done.value,
    "validate_merged_ticket": TicketStatus.done.value,
}


def success_target(task: str, status: str | None, artifacts: dict) -> str | None:
    """The status a successful run of `task` moves the ticket to, or None.

    SUCCESS_EDGES holds the edge each task takes when it succeeds. A selector
    reads the ticket status at the moment of success and the artifacts of the
    run. The task registry reads this table, so a task declares no success
    edge of its own."""
    edge = SUCCESS_EDGES.get(task)
    return edge(status, artifacts) if callable(edge) else edge


@dataclass(frozen=True)
class Cycle:
    """A bounded retry loop in the graph.

    `node` is the ticket status the loop runs on, or None for a loop that
    runs on any pull request, a ticket PR or not. `task` runs one pass. `budget` is
    the number of passes the loop may spend. `exit_reason` is the check every
    caller makes before it starts another pass. `exit_edge` is the status the
    ticket moves to when the budget is spent, or None when the loop stops in
    place."""
    name: str
    node: TicketStatus | None
    task: str
    budget: int
    exit_edge: TicketStatus | None = None

    def exit_reason(self, attempts: int, converged: bool = False) -> str | None:
        """Why the loop stops, or None while another pass may run.

        A converged loop stops whatever budget is left. A loop that spent its
        budget stops even when it did not converge."""
        if converged:
            return "converged"
        if attempts >= self.budget:
            return "budget_spent"
        return None

    def may_continue(self, attempts: int, converged: bool = False) -> bool:
        return self.exit_reason(attempts, converged) is None


# A correction pass is one LLM run plus a fresh proof, so it is not free, and a
# third pass would mean the reviewers and the fixer disagree about what the
# ticket is. That disagreement is the operator's to settle, so the gate goes
# back to holding the branch once the budget is spent.
SCOPE_FIX = Cycle("scope_fix", TicketStatus.proving, "fix_scope_findings", 2)
BASE_SYNC = Cycle("base_sync", TicketStatus.in_review, "sync_pr_base", 2)
PR_AUTOFIX = Cycle("pr_autofix", None, "pr_autofix_run", 3)
AUTOMATED_REVIEW_FIX = Cycle("automated_review_fix", None, "fix_pr_comments", 2)
TEST_FIX = Cycle("test_fix", TicketStatus.testing, "run_tests_and_fix", 3,
                 exit_edge=TicketStatus.tests_failed)

CYCLES: tuple[Cycle, ...] = (SCOPE_FIX, BASE_SYNC, PR_AUTOFIX, AUTOMATED_REVIEW_FIX, TEST_FIX)


def epic_edge(issue_type: str | None) -> str | None:
    """`epic` for a container issue (a Jira Epic, a Linear Project), else None.

    frshty tracks an epic for UI grouping and never plans, codes or PRs it."""
    return TicketStatus.epic.value if issue_type == "Epic" else None


def approval_edge(approval_required: bool, approval_status: str | None) -> str | None:
    """`pending_approval` for a `new` ticket that waits for a human, else None.

    The handler writes this status with the ticket state and not through
    transition(), so `new -> pending_approval` is not in _ALLOWED."""
    if approval_required and approval_status not in ("approved", "rejected"):
        return TicketStatus.pending_approval.value
    return None


def tests_failed_edge(attempts: int) -> str | None:
    """The status a `testing` ticket with a FAIL verdict moves to, or None
    while TEST_FIX may run another pass."""
    if TEST_FIX.may_continue(attempts):
        return None
    return TEST_FIX.exit_edge.value
