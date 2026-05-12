from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled


@task("poll_own_prs", preconditions=[feature_enabled("review_prs")], timeout=180)
def poll_own_prs(ctx: TaskContext) -> TaskResult:
    from features import own_prs
    own_prs.check(ctx.config)
    return TaskResult("ok")


@task("poll_reviewer", preconditions=[feature_enabled("review_prs")], timeout=120)
def poll_reviewer(ctx: TaskContext) -> TaskResult:
    from features import reviewer
    reviewer.check(ctx.config)
    return TaskResult("ok")


@task("poll_peer_reviews", preconditions=[feature_enabled("review_prs")], timeout=60)
def poll_peer_reviews(ctx: TaskContext) -> TaskResult:
    from features import peer_reviews
    peer_reviews.refresh(ctx.config)
    return TaskResult("ok")


@task("timesheet_check", preconditions=[feature_enabled("timesheet")], timeout=120)
def timesheet_check(ctx: TaskContext) -> TaskResult:
    from features import timesheet
    timesheet.check(ctx.config)
    return TaskResult("ok")


@task("poll_billing_invoices", preconditions=[feature_enabled("billing")], timeout=60)
def poll_billing_invoices(ctx: TaskContext) -> TaskResult:
    from features import billing_snapshot
    billing_snapshot.refresh(ctx.config)
    return TaskResult("ok")


@task("scheduler_check", timeout=60)
def scheduler_check(ctx: TaskContext) -> TaskResult:
    from core import scheduler
    scheduler.check_due(ctx.config)
    return TaskResult("ok")
