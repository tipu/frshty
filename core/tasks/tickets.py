"""Ticket pipeline tasks. Thin wrappers around features.tickets functions."""
from datetime import datetime, timezone

import core.db as db
import core.log as log
from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import (
    status_is, auto_pr_true, file_exists, file_contains, feature_enabled,
)


def _set_status(ctx: TaskContext, new_status: str) -> None:
    db.execute(
        "UPDATE tickets SET status=?, updated_at=? WHERE instance_key=? AND ticket_key=?",
        (new_status, datetime.now(timezone.utc).isoformat(), ctx.instance_key, ctx.ticket_key),
    )


@task("scan_tickets", preconditions=[feature_enabled("tickets")], timeout=120)
def scan_tickets(ctx: TaskContext) -> TaskResult:
    """Wrap features.tickets.check. Reads ticket system, enqueues per-ticket follow-ups."""
    from features import tickets as tix
    try:
        tix.check(ctx.config)
        return TaskResult("ok")
    except Exception as e:
        log.emit("scan_tickets_error", f"[{ctx.instance_key}] {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")


@task("start_planning",
      preconditions=[status_is("new")],
      timeout=30)
def start_planning(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    row = db.query_one(
        "SELECT slug, branch FROM tickets WHERE instance_key=? AND ticket_key=?",
        (ctx.instance_key, ctx.ticket_key),
    )
    if not row:
        return TaskResult("failed", "ticket row missing")
    ts = {"slug": row["slug"], "branch": row["branch"], "status": "new"}
    tix.restart_session(ctx.config, ctx.ticket_key, ts, base_url=ctx.registry.base_url)
    _set_status(ctx, "planning")
    return TaskResult("ok", artifacts={"transitioned_to": "planning"})


@task("start_reviewing",
      preconditions=[status_is("planning"), file_exists("docs/change-manifest.md")],
      timeout=30)
def start_reviewing(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    row = db.query_one(
        "SELECT slug, branch FROM tickets WHERE instance_key=? AND ticket_key=?",
        (ctx.instance_key, ctx.ticket_key),
    )
    ts = {"slug": row["slug"] if row else "", "branch": row["branch"] if row else "",
          "status": "reviewing"}
    _set_status(ctx, "reviewing")
    tix.restart_session(ctx.config, ctx.ticket_key, ts, base_url=ctx.registry.base_url)
    return TaskResult("ok", artifacts={"transitioned_to": "reviewing"})


@task("mark_ready",
      preconditions=[status_is("reviewing"),
                     file_contains("docs/tri-review.md", r"VERDICT:\s*PASS")],
      timeout=15)
def mark_ready(ctx: TaskContext) -> TaskResult:
    _set_status(ctx, "pr_ready")
    return TaskResult("ok", artifacts={"transitioned_to": "pr_ready"})


@task("retry_plan",
      preconditions=[status_is("reviewing"),
                     file_contains("docs/tri-review.md", r"VERDICT:\s*FAIL")],
      timeout=30)
def retry_plan(ctx: TaskContext) -> TaskResult:
    _set_status(ctx, "planning")
    return TaskResult("ok", artifacts={"transitioned_to": "planning"})


@task("create_pr",
      preconditions=[status_is("pr_ready"), auto_pr_true],
      timeout=300)
def create_pr(ctx: TaskContext) -> TaskResult:
    """Placeholder for PR creation; production code lives in features/tickets.py."""
    from features import tickets as tix
    row = db.query_one(
        "SELECT slug, branch, data FROM tickets WHERE instance_key=? AND ticket_key=?",
        (ctx.instance_key, ctx.ticket_key),
    )
    if not row:
        return TaskResult("failed", "ticket row missing")
    ts = {
        "slug": row["slug"], "branch": row["branch"], "status": "pr_ready",
        **db.load_json(row, "data"),
    }
    ticket_state = db.query_all(
        "SELECT ticket_key, data, status, slug, branch FROM tickets WHERE instance_key=?",
        (ctx.instance_key,),
    )
    tickets_dict = {r["ticket_key"]: {"status": r["status"], "slug": r["slug"],
                                       "branch": r["branch"], **db.load_json(r, "data")}
                    for r in ticket_state}
    ticket = {"key": ctx.ticket_key, "summary": ts.get("summary", ""),
              "description": ts.get("description", ""), "url": ts.get("url", "")}
    try:
        updated = tix._create_pr(ctx.config, ticket, ts, ctx.registry.base_url)
        new_status = updated.get("status", "pr_failed")
        _set_status(ctx, new_status)
        return TaskResult("ok", artifacts={"transitioned_to": new_status})
    except Exception as e:
        _set_status(ctx, "pr_failed")
        return TaskResult("failed", f"{type(e).__name__}: {e}",
                          artifacts={"transitioned_to": "pr_failed"})


@task("apply_note_reset", timeout=30)
def apply_note_reset(ctx: TaskContext) -> TaskResult:
    import shutil
    from pathlib import Path
    note = ctx.payload.get("note", "")
    ws = ctx.config["workspace"]
    row = db.query_one(
        "SELECT slug FROM tickets WHERE instance_key=? AND ticket_key=?",
        (ctx.instance_key, ctx.ticket_key),
    )
    if not row:
        return TaskResult("failed", "ticket not found")
    root = ws["root"] if isinstance(ws["root"], Path) else Path(ws["root"])
    ticket_dir = root / ws["tickets_dir"] / (row["slug"] or ctx.ticket_key or "")
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

    try:
        from core import terminal
        terminal.kill_terminal(ctx.ticket_key or "")
    except Exception:
        pass

    _set_status(ctx, "new")
    return TaskResult("ok", artifacts={"archived_to": archived_to, "moved": moved,
                                        "transitioned_to": "new"})


@task("set_state", timeout=15)
def set_state(ctx: TaskContext) -> TaskResult:
    target = ctx.payload.get("target", "")
    if not target:
        return TaskResult("failed", "target state missing")
    _set_status(ctx, target)
    return TaskResult("ok", artifacts={"transitioned_to": target})
