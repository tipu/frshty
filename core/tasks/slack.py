from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled


@task("slack_scan", preconditions=[feature_enabled("slack")], timeout=60)
def slack_scan(ctx: TaskContext) -> TaskResult:
    from features import slack_monitor
    slack_monitor.check(ctx.config)
    return TaskResult("ok")


@task("handle_slack_message", timeout=30)
def handle_slack_message(ctx: TaskContext) -> TaskResult:
    return TaskResult("ok", artifacts={"message_ts": ctx.payload.get("ts")})
