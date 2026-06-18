from enum import Enum


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
    TicketStatus.in_review:  {TicketStatus.merged, TicketStatus.in_review, TicketStatus.pr_failed},
    TicketStatus.merged:     {TicketStatus.validation, TicketStatus.new},
    TicketStatus.validation: {TicketStatus.done, TicketStatus.new},
    TicketStatus.pr_failed:  {TicketStatus.pr_ready, TicketStatus.in_review, TicketStatus.merged},
    TicketStatus.done:       {TicketStatus.new, TicketStatus.pr_ready, TicketStatus.testing, TicketStatus.proving, TicketStatus.in_review},
    TicketStatus.epic:       set(),  # epic is terminal; transition() still permits → done universally
    TicketStatus.ignored:    {TicketStatus.new},  # only legal exit is un-ignore → new
    TicketStatus.blocked:    {TicketStatus.new},
}


def transition(current: str, target: str) -> str:
    cur = TicketStatus(current)
    tgt = TicketStatus(target)
    if tgt == cur:
        return tgt.value
    if tgt == TicketStatus.done:
        return tgt.value
    if tgt not in _ALLOWED.get(cur, set()):
        raise ValueError(f"Illegal transition: {cur.value} -> {tgt.value}")
    return tgt.value
