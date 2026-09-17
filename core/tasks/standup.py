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

    The cron fan-out routes this on every tick, which is what makes it the one
    beat that is always current: it also opens or closes the day when the
    restart that skipped those two schedules rolled them to tomorrow.
    standup.tick paces itself against the standup row, and the budget it
    reserves is what bounds the day when two instances both route the tick."""
    from services import standup
    if not standup.enabled(ctx.config):
        return TaskResult("ok", artifacts={"skipped": "standup disabled"})
    try:
        caught_up = standup.catch_up(ctx.config)
        moved = standup.sweep_completed_tasks()
        out = standup.tick(ctx.config)
    except Exception as e:
        log.emit("standup_tick_failed",
                 f"[{ctx.instance_key}] the standup tick failed: "
                 f"{type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")
    return TaskResult("ok", artifacts={"awaiting_check": moved,
                                       "catch_up": caught_up, **out})


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
