"""PM agent tasks. Advisory; never block the pipeline."""
from pathlib import Path

import core.log as log
import core.state as state
from core.tasks.registry import TaskContext, TaskResult, task


PM_TIMEOUT = 180


@task("pm_prd_update", timeout=PM_TIMEOUT)
def pm_prd_update(ctx: TaskContext) -> TaskResult:
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    section_id = ctx.payload.get("section_id")
    old_section = ctx.payload.get("old") or {}
    new_section = ctx.payload.get("new") or {}
    if not section_id:
        return TaskResult("failed", "section_id missing")
    try:
        from pm import runner
        result = runner.run_prd_update(ctx.instance_key, section_id, old_section, new_section)
        if result is None:
            return TaskResult("ok", artifacts={"skipped": "pm review unavailable"})
        return TaskResult("ok", artifacts={
            "verdict": result.get("verdict"),
            "findings_count": len(result.get("findings", [])),
        })
    except Exception as e:
        log.emit("pm_prd_update_error",
                 f"[{ctx.instance_key}] section {section_id}: {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")


@task("pm_post_shipping", timeout=PM_TIMEOUT * 2)
def pm_post_shipping(ctx: TaskContext) -> TaskResult:
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    try:
        from pm import runner
        result = runner.run_post_shipping(ctx.instance_key)
        if result is None:
            return TaskResult("ok", artifacts={"skipped": "no PRD or no shipped tickets"})
        return TaskResult("ok", artifacts={
            "verdict": result.get("verdict"),
            "findings_count": len(result.get("findings", [])),
        })
    except Exception as e:
        log.emit("pm_post_shipping_error",
                 f"[{ctx.instance_key}] {type(e).__name__}: {e}")
        return TaskResult("failed", f"{type(e).__name__}: {e}")


def _ticket_worktree(ctx: TaskContext) -> Path | None:
    if not ctx.ticket_key:
        return None
    ts = state.load_ticket(ctx.ticket_key) or {}
    slug = ts.get("slug")
    if not slug:
        return None
    ws = ctx.config.get("workspace") or {}
    root = ws.get("root")
    tickets_dir = ws.get("tickets_dir")
    if not root or not tickets_dir:
        return None
    root_path = Path(root) if isinstance(root, str) else root
    return root_path / tickets_dir / slug


def _render_pm_findings_md(review: dict) -> str:
    lines = ["# PM Pre-Approval Findings", ""]
    lines.append(f"verdict: {review.get('verdict', 'unknown')}")
    lines.append(f"reviewed_at: {review.get('created_at', '')}")
    lines.append("")
    findings = review.get("findings") or []
    for i, f in enumerate(findings, 1):
        sev = f.get("severity", "info")
        cat = f.get("category", "")
        msg = f.get("message", "")
        lines.append(f"## Finding {i} — {sev} / {cat}")
        lines.append("")
        lines.append(msg)
        refs = f.get("related_ticket_keys") or []
        if refs:
            lines.append("")
            lines.append(f"refs: {', '.join(refs)}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


@task("address_pm_findings", timeout=60)
def address_pm_findings(ctx: TaskContext) -> TaskResult:
    if not ctx.ticket_key:
        return TaskResult("failed", "ticket_key missing")
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    from pm import runner
    review = runner.latest_pre_approval_review(ctx.instance_key, ctx.ticket_key)
    if not review or not review.get("findings"):
        return TaskResult("ok", artifacts={"skipped": "no findings"})
    worktree = _ticket_worktree(ctx)
    if worktree is None or not worktree.is_dir():
        return TaskResult("ok", artifacts={"skipped": "worktree not yet materialized"})
    docs_dir = worktree / "docs"
    docs_dir.mkdir(parents=True, exist_ok=True)
    out = docs_dir / "pm-findings.md"
    out.write_text(_render_pm_findings_md(review))
    log.emit("pm_findings_written_to_worktree",
             f"[{ctx.instance_key}] wrote {out} ({len(review['findings'])} findings)",
             meta={"ticket": ctx.ticket_key, "path": str(out)})
    return TaskResult("ok", artifacts={"path": str(out),
                                        "findings_count": len(review["findings"])})


@task("pm_pre_approval", timeout=PM_TIMEOUT)
def pm_pre_approval(ctx: TaskContext) -> TaskResult:
    if not ctx.ticket_key:
        return TaskResult("failed", "ticket_key missing")
    ts = state.load_ticket(ctx.ticket_key) or {}
    if not ts:
        return TaskResult("failed", "ticket not found")
    pm_cfg = ctx.config.get("pm_agent") or {}
    if not pm_cfg.get("enabled", True):
        return TaskResult("ok", artifacts={"skipped": "pm disabled"})
    try:
        from pm import runner
        review = runner.run_pre_approval(ctx.instance_key, ctx.ticket_key, ts)
        if review is None:
            return TaskResult("ok", artifacts={"skipped": "pm review unavailable"})
        return TaskResult("ok", artifacts={
            "verdict": review["verdict"],
            "findings_count": len(review["findings"]),
        })
    except Exception as e:
        log.emit("pm_pre_approval_error",
                 f"[{ctx.instance_key}] {ctx.ticket_key}: {type(e).__name__}: {e}",
                 meta={"ticket": ctx.ticket_key})
        return TaskResult("failed", f"{type(e).__name__}: {e}")
