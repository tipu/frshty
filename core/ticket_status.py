from enum import Enum


class TicketStatus(str, Enum):
    pending_approval = "pending_approval"
    new = "new"
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
    done = "done"


_ALLOWED: dict[TicketStatus, set[TicketStatus]] = {
    TicketStatus.pending_approval: {TicketStatus.new, TicketStatus.done},
    TicketStatus.new:        {TicketStatus.planning, TicketStatus.merged},
    TicketStatus.planning:   {TicketStatus.reviewing},
    TicketStatus.reviewing:  {TicketStatus.testing, TicketStatus.pr_ready, TicketStatus.planning},
    TicketStatus.testing:    {TicketStatus.proving, TicketStatus.pr_ready, TicketStatus.tests_failed, TicketStatus.reviewing},
    TicketStatus.tests_failed: {TicketStatus.testing, TicketStatus.reviewing, TicketStatus.pr_ready, TicketStatus.done},
    TicketStatus.proving:    {TicketStatus.pr_ready, TicketStatus.testing, TicketStatus.reviewing},
    TicketStatus.pr_ready:   {TicketStatus.testing, TicketStatus.proving, TicketStatus.reviewing, TicketStatus.in_review, TicketStatus.pr_failed, TicketStatus.merged},
    TicketStatus.in_review:  {TicketStatus.merged, TicketStatus.in_review, TicketStatus.pr_failed},
    TicketStatus.merged:     {TicketStatus.validation, TicketStatus.new},
    TicketStatus.validation: {TicketStatus.done, TicketStatus.new},
    TicketStatus.pr_failed:  {TicketStatus.pr_ready, TicketStatus.in_review, TicketStatus.merged},
    TicketStatus.done:       {TicketStatus.new, TicketStatus.pr_ready, TicketStatus.testing, TicketStatus.proving, TicketStatus.in_review},
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
