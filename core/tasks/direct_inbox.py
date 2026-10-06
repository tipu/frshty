from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled

DIRECT_INBOX_SCAN_TIMEOUT = 900


@task("direct_inbox_scan", preconditions=[feature_enabled("direct_inbox")],
      timeout=DIRECT_INBOX_SCAN_TIMEOUT)
def direct_inbox_scan(ctx: TaskContext) -> TaskResult:
    """Read the operator's direct emails and texts and propose the follow-ups."""
    from features import direct_inbox
    return TaskResult("ok", artifacts=direct_inbox.check(
        ctx.config, instance_key=ctx.instance_key, now=ctx.now))
