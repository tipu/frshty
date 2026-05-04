"""Manager daily digest task."""
import core.log as log
from core.tasks.registry import TaskContext, TaskResult, task


MANAGER_TIMEOUT = 240


@task("manager_daily_digest", timeout=MANAGER_TIMEOUT)
def manager_daily_digest(ctx: TaskContext) -> TaskResult:
    cfg = ctx.config.get("manager") or {}
    if not cfg.get("enabled"):
        return TaskResult("ok", artifacts={"skipped": "manager disabled"})
    try:
        from manager import runner
        out = runner.run_daily_digest(ctx.instance_key, ctx.config)
        if out is None:
            return TaskResult("ok", artifacts={"skipped": "haiku unavailable"})
        return TaskResult("ok", artifacts={
            "counts": out.get("counts"),
            "empty": out.get("empty", False),
        })
    except Exception as e:
        log.emit("manager_digest_error",
                 f"[{ctx.instance_key}] {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")
