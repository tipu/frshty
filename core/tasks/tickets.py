"""Ticket pipeline tasks. Headless claude -p invocations, postcondition-gated."""
from pathlib import Path

import core.log as log
import core.state as state
from core.claude_runner import run_claude_code
from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import (
    status_is, auto_pr_true, file_exists, file_contains, feature_enabled,
)


PLAN_TIMEOUT = 1800
REVIEW_TIMEOUT = 900
FIX_TIMEOUT = 1800


def _set_status(ctx: TaskContext, new_status: str) -> None:
    tickets = state.load("tickets")
    ts = tickets.get(ctx.ticket_key or "")
    if ts is None:
        return
    ts["status"] = new_status
    tickets[ctx.ticket_key] = ts
    state.save("tickets", tickets)


def _ticket_dir(ctx: TaskContext) -> Path:
    ws = ctx.config["workspace"]
    tickets = state.load("tickets")
    ts = tickets.get(ctx.ticket_key or "", {})
    slug = ts.get("slug") or (ctx.ticket_key or "")
    root = Path(ws["root"]) if isinstance(ws["root"], str) else ws["root"]
    return root / ws["tickets_dir"] / slug


@task("scan_tickets", preconditions=[feature_enabled("tickets")], timeout=120)
def scan_tickets(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    try:
        tix.check(ctx.config, ctx.instance_key)
        return TaskResult("ok")
    except Exception as e:
        log.emit("scan_tickets_error", f"[{ctx.instance_key}] {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")


@task("start_planning",
      preconditions=[status_is("new", "planning")],
      postconditions=[file_exists("docs/change-manifest.md")],
      timeout=PLAN_TIMEOUT)
def start_planning(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    _set_status(ctx, "planning")
    log.emit("ticket_planning_started", f"Headless /confer-technical-plan for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    result = run_claude_code("/confer-technical-plan docs/", cwd=ticket_dir, timeout=PLAN_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    _set_status(ctx, "reviewing")
    return TaskResult("ok", artifacts={"transitioned_to": "reviewing"})


@task("start_reviewing",
      preconditions=[status_is("reviewing"), file_exists("docs/change-manifest.md")],
      postconditions=[file_contains("docs/tri-review.md", r"VERDICT:\s*(PASS|FAIL)")],
      timeout=REVIEW_TIMEOUT)
def start_reviewing(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    prompt = (
        "Run /tri-review and save the full output to docs/tri-review.md. "
        "In the Verdict section, include a line reading exactly 'VERDICT: PASS' "
        "if no blocking findings remain unresolved, or 'VERDICT: FAIL' otherwise."
    )
    log.emit("ticket_review_started", f"Headless /tri-review for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=REVIEW_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


@task("fix_review_findings",
      preconditions=[status_is("reviewing"),
                     file_contains("docs/tri-review.md", r"VERDICT:\s*FAIL")],
      postconditions=[file_contains("docs/tri-review.md", r"VERDICT:\s*(PASS|FAIL)")],
      timeout=FIX_TIMEOUT)
def fix_review_findings(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    prompt = (
        "Read docs/tri-review.md. Fix all blocking findings in the workspace. "
        "Run relevant tests to verify the fixes. Then re-run /tri-review and save "
        "the full output to docs/tri-review.md, replacing the previous version, "
        "with a line reading exactly 'VERDICT: PASS' or 'VERDICT: FAIL' in the "
        "Verdict section."
    )
    log.emit("ticket_review_fixing", f"Headless fix+rereview for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=FIX_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


@task("mark_ready",
      preconditions=[status_is("reviewing"),
                     file_contains("docs/tri-review.md", r"VERDICT:\s*PASS")],
      timeout=15)
def mark_ready(ctx: TaskContext) -> TaskResult:
    import core.events as events
    tickets = state.load("tickets")
    ts = tickets.get(ctx.ticket_key or "", {})
    _set_status(ctx, "pr_ready")
    events.dispatch("ticket_dev_complete", {
        "ticket_key": ctx.ticket_key,
        "estimate_seconds": ts.get("estimate_seconds", 0),
        "discovered_at": ts.get("discovered_at", ""),
        "slug": ts.get("slug", ""),
        "branch": ts.get("branch", ""),
    }, ctx.config)
    return TaskResult("ok", artifacts={"transitioned_to": "pr_ready"})


@task("create_pr",
      preconditions=[status_is("pr_ready"), auto_pr_true],
      timeout=300)
def create_pr(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    tickets = state.load("tickets")
    ts = tickets.get(ctx.ticket_key or "")
    if ts is None:
        return TaskResult("failed", "ticket not found")
    ticket = {"key": ctx.ticket_key, "summary": ts.get("summary", ""),
              "description": ts.get("description", ""), "url": ts.get("url", "")}
    try:
        updated = tix._create_pr(ctx.config, ticket, ts, ctx.registry.base_url)
        new_status = updated.get("status", "pr_failed")
        tickets[ctx.ticket_key] = updated
        state.save("tickets", tickets)
        return TaskResult("ok", artifacts={"transitioned_to": new_status})
    except Exception as e:
        _set_status(ctx, "pr_failed")
        return TaskResult("failed", f"{type(e).__name__}: {e}",
                          artifacts={"transitioned_to": "pr_failed"})


@task("apply_note_reset", timeout=30)
def apply_note_reset(ctx: TaskContext) -> TaskResult:
    import shutil
    from datetime import datetime, timezone
    note = ctx.payload.get("note", "")
    ws = ctx.config["workspace"]
    tickets = state.load("tickets")
    ts = tickets.get(ctx.ticket_key or "")
    if ts is None:
        return TaskResult("failed", "ticket not found")
    root = ws["root"] if isinstance(ws["root"], Path) else Path(ws["root"])
    ticket_dir = root / ws["tickets_dir"] / (ts.get("slug") or ctx.ticket_key or "")
    docs = ticket_dir / "docs"
    now = datetime.now(timezone.utc).isoformat()
    archived_to = None
    if note:
        ticket_md = docs / "ticket.md"
        ticket_md.parent.mkdir(parents=True, exist_ok=True)
        with ticket_md.open("a") as f:
            f.write(f"\n\n## Note ({now})\n{note}\n")
    archive = docs / "archive" / now.replace(":", "-")
    moved = []
    for fname in ("change-manifest.md", "tri-review.md", "technical-plan.md"):
        src = docs / fname
        if src.exists():
            archive.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(archive / fname))
            moved.append(fname)
    if moved:
        archived_to = str(archive)

    ts["status"] = "new"
    tickets[ctx.ticket_key] = ts
    state.save("tickets", tickets)
    return TaskResult("ok", artifacts={"archived_to": archived_to, "moved": moved,
                                        "transitioned_to": "new"})


@task("set_state", timeout=15)
def set_state(ctx: TaskContext) -> TaskResult:
    target = ctx.payload.get("target", "")
    if not target:
        return TaskResult("failed", "target state missing")
    _set_status(ctx, target)
    return TaskResult("ok", artifacts={"transitioned_to": target})
