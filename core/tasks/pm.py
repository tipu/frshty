"""PM agent tasks. Advisory; never block the pipeline."""
import core.log as log
import core.state as state
from core.tasks.registry import TaskContext, TaskResult, task


PM_TIMEOUT = 180


@task("pm_pre_approval", timeout=PM_TIMEOUT)
def pm_pre_approval(ctx: TaskContext) -> TaskResult:
    if not ctx.ticket_key:
        return TaskResult("failed", "ticket_key missing")
    ts = state.load_ticket(ctx.ticket_key) or {}
    if not ts:
        return TaskResult("failed", "ticket not found")
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    try:
        from pm import runner
        review = runner.run_pre_approval(ctx.instance_key, ctx.ticket_key, ts)
        if review is None:
            return TaskResult("ok", artifacts={"skipped": "pm review unavailable"})
        return TaskResult("ok", artifacts={
            "verdict": review["verdict"],
            "findings_count": len(review["findings"]),
        })
    except Exception as e:
        log.emit("pm_pre_approval_error",
                 f"[{ctx.instance_key}] {ctx.ticket_key}: {type(e).__name__}: {e}",
                 meta={"ticket": ctx.ticket_key})
        return TaskResult("failed", f"{type(e).__name__}: {e}")
