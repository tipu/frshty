from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled

UPWORK_SCAN_TIMEOUT = 600


@task("upwork_scan", preconditions=[feature_enabled("upwork")],
      timeout=UPWORK_SCAN_TIMEOUT)
def upwork_scan(ctx: TaskContext) -> TaskResult:
    """Read this instance's Upwork inbox and propose the work it asks for."""
    from features import upwork_inbox
    return TaskResult("ok", artifacts=upwork_inbox.check(
        ctx.config, instance_key=ctx.instance_key, now=ctx.now))
