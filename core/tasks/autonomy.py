"""Today-agent autonomy: a thin planner that turns /today into an action surface.

Each tick: load the day's plan from KV; if missing or for an older date, build
a fresh one via manager.planner. The actual ticket advancement is owned by the
existing pipeline (features.tickets.check + worker pool); this task only
declares focus and prunes goals whose tickets have reached their target state.

Interrupt protocol (no code change in registry needed): tasks that need human
input — 2FA, approval, destructive confirm, blocking ambiguity — return
TaskResult(status="blocked", reason=<question>, artifacts={
    "kind":              "auth_required" | "approval_required"
                       | "destructive_confirm" | "ambiguity_blocking",
    "expected_input":    "text" | "yes_no" | "typed_confirmation",
    "deferred_payload":  <dict to feed back into a fresh enqueue when answered>,
}).

claim_next() only picks status='queued' so a 'blocked' job sits unconsumed.
The /api/today/answer endpoint marks it consumed and re-enqueues a fresh job
with the answer attached to its payload.
"""
from __future__ import annotations

from datetime import datetime, timezone

import core.log as log
import core.state as state
from core.tasks.registry import TaskContext, TaskResult, task


TICK_TIMEOUT = 60
KV_KEY = "today_agent"
TERMINAL_TICKET_STATES = {"merged", "validation", "done"}


def _today_iso() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def _ticket_status(ticket_key: str) -> str | None:
    try:
        t = state.load_ticket(ticket_key)
    except Exception:
        return None
    return (t or {}).get("status") if t else None


def _prune_goals(goals: list[dict]) -> tuple[list[dict], list[dict]]:
    """Move goals whose ticket reached target state (or terminal) to done."""
    active: list[dict] = []
    completed: list[dict] = []
    for g in goals:
        tk = g.get("ticket_key")
        target = g.get("target_state")
        if not tk:
            continue
        cur = _ticket_status(tk)
        if cur is None:
            active.append(g)
            continue
        g = {**g, "current_state": cur}
        if cur == target or cur in TERMINAL_TICKET_STATES:
            completed.append(g)
        else:
            active.append(g)
    return active, completed


@task("today_agent_tick", timeout=TICK_TIMEOUT)
def today_agent_tick(ctx: TaskContext) -> TaskResult:
    today_cfg = ctx.config.get("today_agent") or {}
    if not today_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "today_agent disabled"})

    blob = state.load(KV_KEY) or {}
    plan = blob.get("plan") or {}
    plan_date = plan.get("date")
    paused = bool(blob.get("paused"))

    rebuild = (not plan) or (plan_date != _today_iso())
    if rebuild and not paused:
        try:
            from manager.planner import build_plan
            plan = build_plan(ctx.instance_key, ctx.config,
                              use_llm=today_cfg.get("use_llm", True))
        except Exception as e:
            log.emit("today_planner_error",
                     f"[{ctx.instance_key}] {type(e).__name__}: {e}")
            return TaskResult("failed", f"planner: {type(e).__name__}: {e}")
        blob = {**blob, "plan": plan, "paused": False, "skipped": []}

    goals = (plan.get("goals") or [])
    skipped_keys = set(blob.get("skipped") or [])
    goals = [g for g in goals if g.get("ticket_key") not in skipped_keys]
    active, completed = _prune_goals(goals)

    plan_out = dict(plan)
    plan_out["goals"] = active
    plan_out["completed"] = (plan.get("completed") or []) + completed
    plan_out["last_tick_at"] = datetime.now(timezone.utc).isoformat()
    blob["plan"] = plan_out
    state.save(KV_KEY, blob)

    if rebuild:
        log.emit("today_plan_built",
                 f"[{ctx.instance_key}] today plan: "
                 + ", ".join(f"{g.get('ticket_key')}→{g.get('target_state')}"
                             for g in active),
                 meta={"goal_count": len(active)})

    return TaskResult("ok", artifacts={
        "active": len(active),
        "completed": len(completed),
        "rebuild": rebuild,
    })
