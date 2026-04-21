import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

_REGISTRY: dict[str, dict] = {}


@dataclass
class TaskContext:
    instance_key: str
    ticket_key: str | None
    task: str
    payload: dict
    job_id: int
    triggering_event_id: int | None
    config: dict
    registry: Any
    now: datetime


@dataclass
class TaskResult:
    status: str
    reason: str = ""
    artifacts: dict = field(default_factory=dict)
    next_events: list[dict] = field(default_factory=list)


Precondition = Callable[[TaskContext], tuple[bool, str]]
Postcondition = Callable[[TaskContext], tuple[bool, str]]
SuccessStatus = str | Callable[[TaskContext, "TaskResult"], str | None] | None


def task(name: str,
         preconditions: list[Precondition] | None = None,
         postconditions: list[Postcondition] | None = None,
         on_entry_status: str | None = None,
         on_success_status: SuccessStatus = None,
         timeout: int = 60):
    def deco(fn):
        _REGISTRY[name] = {
            "fn": fn,
            "preconditions": list(preconditions or []),
            "postconditions": list(postconditions or []),
            "on_entry_status": on_entry_status,
            "on_success_status": on_success_status,
            "timeout": timeout,
        }
        return fn
    return deco


def get_task(name: str) -> dict | None:
    return _REGISTRY.get(name)


def all_tasks() -> list[str]:
    return sorted(_REGISTRY.keys())


def _apply_status(ctx: TaskContext, target: str) -> TaskResult | None:
    """Apply target status via state.transition_ticket. Returns a failed
    TaskResult on invariant violation, None on success or if no-op."""
    if not ctx.ticket_key or not target:
        return None
    import core.state as state
    try:
        state.transition_ticket(ctx.ticket_key, target)
    except state.TicketStateError as e:
        return TaskResult("failed", f"transition to {target}: {e}")
    return None


def run_task(ctx: TaskContext) -> TaskResult:
    entry = _REGISTRY.get(ctx.task)
    if not entry:
        return TaskResult("failed", f"unknown task: {ctx.task}")
    fn = entry["fn"]
    preconds = entry["preconditions"]
    postconds = entry["postconditions"]
    on_entry = entry.get("on_entry_status")
    on_success = entry.get("on_success_status")
    for p in preconds:
        try:
            ok, reason = p(ctx)
        except Exception as e:
            return TaskResult("failed", f"precondition errored: {type(e).__name__}: {e}")
        if not ok:
            return TaskResult("skipped", f"precondition: {reason}")
    if on_entry:
        err = _apply_status(ctx, on_entry)
        if err is not None:
            return err
    try:
        result = fn(ctx)
        if result is None:
            result = TaskResult("ok")
        elif not isinstance(result, TaskResult):
            result = TaskResult("ok", artifacts={"return": result})
    except Exception as e:
        return TaskResult("failed", f"{type(e).__name__}: {e}",
                          artifacts={"traceback": traceback.format_exc()})
    if result.status != "ok":
        return result
    for p in postconds:
        try:
            ok, reason = p(ctx)
        except Exception as e:
            return TaskResult("failed", f"postcondition errored: {type(e).__name__}: {e}",
                              artifacts=result.artifacts, next_events=result.next_events)
        if not ok:
            return TaskResult("failed", f"postcondition: {reason}",
                              artifacts=result.artifacts, next_events=result.next_events)
    if on_success is not None:
        target = on_success(ctx, result) if callable(on_success) else on_success
        if target:
            err = _apply_status(ctx, target)
            if err is not None:
                return TaskResult("failed", err.reason,
                                  artifacts=result.artifacts, next_events=result.next_events)
    return result
