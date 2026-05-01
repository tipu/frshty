"""PRD scan task."""
import core.log as log
from core.tasks.registry import TaskContext, TaskResult, task


PRD_SCAN_TIMEOUT = 600


@task("parse_prd", timeout=PRD_SCAN_TIMEOUT)
def parse_prd(ctx: TaskContext) -> TaskResult:
    cfg = ctx.config.get("prd") or {}
    if not cfg.get("enabled"):
        return TaskResult("ok", artifacts={"skipped": "prd disabled"})
    try:
        from prd import orchestrator
        summary = orchestrator.scan(ctx.config, ctx.instance_key)
        return TaskResult("ok", artifacts=summary)
    except Exception as e:
        log.emit("parse_prd_error",
                 f"[{ctx.instance_key}] {type(e).__name__}: {e}",
                 meta={"error_type": type(e).__name__})
        return TaskResult("failed", f"{type(e).__name__}: {e}")
