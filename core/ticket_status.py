from enum import Enum


class TicketStatus(str, Enum):
    new = "new"
    planning = "planning"
    reviewing = "reviewing"
    pr_ready = "pr_ready"
    pr_created = "pr_created"
    in_review = "in_review"
    merged = "merged"
    pr_failed = "pr_failed"
    done = "done"


_ALLOWED: dict[TicketStatus, set[TicketStatus]] = {
    TicketStatus.new:        {TicketStatus.planning},
    TicketStatus.planning:   {TicketStatus.reviewing},
    TicketStatus.reviewing:  {TicketStatus.pr_ready, TicketStatus.planning},
    TicketStatus.pr_ready:   {TicketStatus.pr_created, TicketStatus.pr_failed, TicketStatus.merged},
    TicketStatus.pr_created: {TicketStatus.merged, TicketStatus.in_review, TicketStatus.pr_failed},
    TicketStatus.in_review:  {TicketStatus.merged, TicketStatus.pr_created, TicketStatus.in_review, TicketStatus.pr_failed},
    TicketStatus.merged:     {TicketStatus.new},
    TicketStatus.pr_failed:  {TicketStatus.pr_ready, TicketStatus.merged},
    TicketStatus.done:       {TicketStatus.new, TicketStatus.pr_ready, TicketStatus.pr_created},
}


def transition(current: str, target: str) -> str:
    cur = TicketStatus(current)
    tgt = TicketStatus(target)
    if tgt == TicketStatus.done:
        return tgt.value
    if tgt not in _ALLOWED.get(cur, set()):
        raise ValueError(f"Illegal transition: {cur.value} -> {tgt.value}")
    return tgt.value
