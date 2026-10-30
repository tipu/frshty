import core.slack_capture as slack_capture
from core.tasks.registry import TaskContext, TaskResult, read_result, task
from core.tasks.preconditions import feature_enabled

CONVERSATION_SCAN_TIMEOUT = 600


@task("slack_scan", preconditions=[feature_enabled("slack")], timeout=60)
def slack_scan(ctx: TaskContext) -> TaskResult:
    from features import slack_monitor
    for view in slack_capture.workspace_configs(ctx.config):
        slack_monitor.check(view)
    return TaskResult("ok")


@task("slack_conversation_scan", preconditions=[feature_enabled("slack")],
      timeout=CONVERSATION_SCAN_TIMEOUT)
def slack_conversation_scan(ctx: TaskContext) -> TaskResult:
    """Index this instance's Slack conversations and propose the work they ask for."""
    from features import slack_conversations
    views = slack_capture.workspace_configs(ctx.config)
    if len(views) == 1:
        return read_result(slack_conversations.check(
            views[0], instance_key=ctx.instance_key, now=ctx.now))
    scans = {slack_capture.scope(view): slack_conversations.check(
        view, instance_key=ctx.instance_key, now=ctx.now) for view in views}
    return read_result({"workspaces": scans,
                        "complete": all(scan.get("complete", True) for scan in scans.values())})


@task("handle_slack_message", timeout=30)
def handle_slack_message(ctx: TaskContext) -> TaskResult:
    return TaskResult("ok", artifacts={"message_ts": ctx.payload.get("ts")})
