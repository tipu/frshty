from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled


@task("billing_check", preconditions=[feature_enabled("billing")], timeout=120)
def billing_check(ctx: TaskContext) -> TaskResult:
    from features import billing
    billing.check(ctx.config)
    return TaskResult("ok")
