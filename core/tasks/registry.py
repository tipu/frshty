import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

_REGISTRY: dict[str, tuple[Callable, list[Callable], int]] = {}


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


def task(name: str, preconditions: list[Precondition] | None = None, timeout: int = 60):
    def deco(fn):
        _REGISTRY[name] = (fn, list(preconditions or []), timeout)
        return fn
    return deco


def get_task(name: str) -> tuple[Callable, list[Callable], int] | None:
    return _REGISTRY.get(name)


def all_tasks() -> list[str]:
    return sorted(_REGISTRY.keys())


def run_task(ctx: TaskContext) -> TaskResult:
    entry = _REGISTRY.get(ctx.task)
    if not entry:
        return TaskResult("failed", f"unknown task: {ctx.task}")
    fn, preconds, _ = entry
    for p in preconds:
        try:
            ok, reason = p(ctx)
        except Exception as e:
            return TaskResult("failed", f"precondition errored: {type(e).__name__}: {e}")
        if not ok:
            return TaskResult("skipped", f"precondition: {reason}")
    try:
        result = fn(ctx)
        if result is None:
            return TaskResult("ok")
        if isinstance(result, TaskResult):
            return result
        return TaskResult("ok", artifacts={"return": result})
    except Exception as e:
        return TaskResult("failed", f"{type(e).__name__}: {e}",
                          artifacts={"traceback": traceback.format_exc()})
