"""Release inspection task. Runs once per release-done transition; idempotent."""
import core.log as log
from core.tasks.registry import TaskContext, TaskResult, task


@task("release_inspect", timeout=600)
def release_inspect(ctx: TaskContext) -> TaskResult:
    if not (ctx.config.get("features") or {}).get("releases"):
        return TaskResult("ok", artifacts={"skipped": "releases feature disabled"})
    rid = ctx.payload.get("release_id")
    force = bool(ctx.payload.get("force"))
    if not rid:
        return TaskResult("failed", "release_id missing from payload")
    try:
        from pm import release_runner
        result = release_runner.run_release_inspection(
            ctx.instance_key, int(rid), ctx.config, force=force,
        )
    except Exception as e:
        log.emit("release_inspect_error",
                 f"[{ctx.instance_key}] release_id={rid}: {type(e).__name__}: {e}",
                 meta={"release_id": rid})
        return TaskResult("failed", f"{type(e).__name__}: {e}")
    if result is None:
        return TaskResult("ok", artifacts={"skipped": "no-op or unavailable"})
    return TaskResult("ok", artifacts={"verdict": result.get("verdict"),
                                        "findings_count": len(result.get("findings", []))})
