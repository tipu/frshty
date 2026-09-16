"""The three beats of the standup: open the day, nudge, close the day."""
import core.log as log
from core.tasks.registry import TaskContext, TaskResult, task


@task("standup_open", timeout=120)
def standup_open(ctx: TaskContext) -> TaskResult:
    """Draft the day and open it.

    Opening is what starts the nudge clock, so a morning frshty never drafted
    can never nudge. The draft is written once; a second fire of the same day
    finds the row and leaves the operator's edits alone."""
    from services import standup
    if not standup.enabled(ctx.config):
        return TaskResult("ok", artifacts={"skipped": "standup disabled"})
    day = standup.today_key()
    if not standup.working_day(day, ctx.config):
        return TaskResult("ok", artifacts={"skipped": f"{day} is not a working day"})
    try:
        standup_id = standup.ensure_day(ctx.config, day)
        view = standup.open_day(standup_id)
    except Exception as e:
        log.emit("standup_open_failed",
                 f"[{ctx.instance_key}] could not open the standup for {day}: "
                 f"{type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")
    return TaskResult("ok", artifacts={"day": day, "items": len(view["items"])})


@task("standup_tick", timeout=180)
def standup_tick(ctx: TaskContext) -> TaskResult:
    """Run the gates over the day's action items and fire at most one nudge.

    The cron fan-out routes this on every tick. standup.tick claims the run
    against the standup row, so the configured interval is what paces the loop
    and two instances routing the same tick cannot both nudge."""
    from services import standup
    if not standup.enabled(ctx.config):
        return TaskResult("ok", artifacts={"skipped": "standup disabled"})
    try:
        moved = standup.sweep_completed_tasks()
        out = standup.tick(ctx.config)
    except Exception as e:
        log.emit("standup_tick_failed",
                 f"[{ctx.instance_key}] the standup tick failed: "
                 f"{type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")
    return TaskResult("ok", artifacts={"awaiting_check": moved, **out})


@task("standup_close", timeout=120)
def standup_close(ctx: TaskContext) -> TaskResult:
    """Ask carry, park or drop on everything still open, then freeze the day."""
    from services import standup
    if not standup.enabled(ctx.config):
        return TaskResult("ok", artifacts={"skipped": "standup disabled"})
    day = standup.today_key()
    view = standup.day_view(day, ctx.config)
    if not view["exists"] or view["state"] != standup.OPEN:
        return TaskResult("ok", artifacts={"skipped": f"no open standup for {day}"})
    try:
        closed = standup.close_day(view["id"], ctx.config)
    except Exception as e:
        log.emit("standup_close_failed",
                 f"[{ctx.instance_key}] could not close the standup for {day}: "
                 f"{type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")
    asked = sum(1 for i in closed["items"] if i.get("question"))
    return TaskResult("ok", artifacts={"day": day, "asked": asked})
