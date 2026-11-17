"""Autopilot sweep task."""
from core.tasks.registry import TaskContext, TaskResult, task

AUTOPILOT_TIMEOUT = 600


@task("autopilot_sweep", timeout=AUTOPILOT_TIMEOUT)
def autopilot_sweep(ctx: TaskContext) -> TaskResult:
    """Read what changed for the operator and start the agents that handle it.

    features.autopilot is imported here, not at the top: it reaches core.runtime
    through services.work_launch, and core.runtime imports this package."""
    import features.autopilot as autopilot
    out = autopilot.sweep(ctx.config, ctx.instance_key, now=ctx.now)
    failed = any(s.get("error") for s in out.get("started", []))
    return TaskResult("partial" if failed or out.get("errors") else "ok", artifacts=out)
