from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import feature_enabled


@task("poll_own_prs", preconditions=[feature_enabled("review_prs")], timeout=180)
def poll_own_prs(ctx: TaskContext) -> TaskResult:
    from features import own_prs
    own_prs.check(ctx.config)
    return TaskResult("ok")


@task("fix_pr_comment", preconditions=[feature_enabled("review_prs")], timeout=900)
def fix_pr_comment(ctx: TaskContext) -> TaskResult:
    from features import own_prs
    ok, reason = own_prs.fix_comment(ctx.config, ctx.payload)
    return TaskResult("ok") if ok else TaskResult("failed", reason)


@task("fix_pr_comments", preconditions=[feature_enabled("review_prs")], timeout=1800)
def fix_pr_comments(ctx: TaskContext) -> TaskResult:
    from features import own_prs
    ok, reason = own_prs.fix_comments_batch(ctx.config, ctx.payload)
    return TaskResult("ok", reason or "") if ok else TaskResult("failed", reason)


@task("poll_reviewer",
      preconditions=[feature_enabled("review_prs"), feature_enabled("auto_review")],
      timeout=120)
def poll_reviewer(ctx: TaskContext) -> TaskResult:
    from features import reviewer
    reviewer.check(ctx.config)
    return TaskResult("ok")


@task("poll_peer_reviews", preconditions=[feature_enabled("review_prs")], timeout=60)
def poll_peer_reviews(ctx: TaskContext) -> TaskResult:
    from features import peer_reviews
    peer_reviews.refresh(ctx.config)
    return TaskResult("ok")


@task("poll_pr_autofix", preconditions=[feature_enabled("pr_autofix")], timeout=180)
def poll_pr_autofix(ctx: TaskContext) -> TaskResult:
    from features import pr_autofix
    pr_autofix.check(ctx.config)
    return TaskResult("ok")


@task("pr_autofix_run", preconditions=[feature_enabled("pr_autofix")], timeout=3300)
def pr_autofix_run(ctx: TaskContext) -> TaskResult:
    from features import pr_autofix
    ok, reason = pr_autofix.run(ctx.config, ctx.payload)
    return TaskResult("ok", reason or "") if ok else TaskResult("failed", reason)


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


@task("watchdog_scan", timeout=300)
def watchdog_scan(ctx: TaskContext) -> TaskResult:
    from manager import watchdog
    opened = watchdog.run(ctx.config, instance_key=ctx.instance_key)
    return TaskResult("ok", artifacts={"opened": opened})


@task("scheduler_check", timeout=60)
def scheduler_check(ctx: TaskContext) -> TaskResult:
    from core import scheduler
    scheduler.check_due(ctx.config)
    return TaskResult("ok")


@task("dep_store_gc", timeout=600)
def dep_store_gc(ctx: TaskContext) -> TaskResult:
    from core import deps
    result = deps.gc_dep_stores(ctx.config)
    return TaskResult("ok", artifacts=result)
