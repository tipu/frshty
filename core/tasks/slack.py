from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled

CONVERSATION_SCAN_TIMEOUT = 600


@task("slack_scan", preconditions=[feature_enabled("slack")], timeout=60)
def slack_scan(ctx: TaskContext) -> TaskResult:
    from features import slack_monitor
    slack_monitor.check(ctx.config)
    return TaskResult("ok")


@task("slack_conversation_scan", preconditions=[feature_enabled("slack")],
      timeout=CONVERSATION_SCAN_TIMEOUT)
def slack_conversation_scan(ctx: TaskContext) -> TaskResult:
    """Index this instance's Slack conversations and propose the work they ask for."""
    from features import slack_conversations
    return TaskResult("ok", artifacts=slack_conversations.check(
        ctx.config, instance_key=ctx.instance_key, now=ctx.now))


@task("handle_slack_message", timeout=30)
def handle_slack_message(ctx: TaskContext) -> TaskResult:
    return TaskResult("ok", artifacts={"message_ts": ctx.payload.get("ts")})
