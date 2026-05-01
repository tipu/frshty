"""PM agent tasks. Advisory; never block the pipeline."""
import core.log as log
import core.state as state
from core.tasks.registry import TaskContext, TaskResult, task


PM_TIMEOUT = 180


@task("pm_prd_update", timeout=PM_TIMEOUT)
def pm_prd_update(ctx: TaskContext) -> TaskResult:
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    section_id = ctx.payload.get("section_id")
    old_section = ctx.payload.get("old") or {}
    new_section = ctx.payload.get("new") or {}
    if not section_id:
        return TaskResult("failed", "section_id missing")
    try:
        from pm import runner
        result = runner.run_prd_update(ctx.instance_key, section_id, old_section, new_section)
        if result is None:
            return TaskResult("ok", artifacts={"skipped": "pm review unavailable"})
        return TaskResult("ok", artifacts={
            "verdict": result.get("verdict"),
            "findings_count": len(result.get("findings", [])),
        })
    except Exception as e:
        log.emit("pm_prd_update_error",
                 f"[{ctx.instance_key}] section {section_id}: {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")


@task("pm_post_shipping", timeout=PM_TIMEOUT * 2)
def pm_post_shipping(ctx: TaskContext) -> TaskResult:
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    try:
        from pm import runner
        result = runner.run_post_shipping(ctx.instance_key)
        if result is None:
            return TaskResult("ok", artifacts={"skipped": "no PRD or no shipped tickets"})
        return TaskResult("ok", artifacts={
            "verdict": result.get("verdict"),
            "findings_count": len(result.get("findings", [])),
        })
    except Exception as e:
        log.emit("pm_post_shipping_error",
                 f"[{ctx.instance_key}] {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")


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
