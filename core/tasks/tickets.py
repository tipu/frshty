"""Ticket pipeline tasks. Headless claude -p invocations, postcondition-gated."""
from pathlib import Path

import core.log as log
import core.state as state
from core.claude_runner import run_claude_code
from core.config import ticket_worktree_path
from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import (
    status_is, auto_pr_true, file_exists, file_contains, feature_enabled, has_flag,
)
from features.platforms import make_platform


PLAN_TIMEOUT = 1800
REVIEW_TIMEOUT = 900
FIX_TIMEOUT = 1800


def _ticket_dir(ctx: TaskContext) -> Path:
    ws = ctx.config["workspace"]
    ts = state.load_ticket(ctx.ticket_key or "") or {}
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


@task("setup_prd_ticket",
      preconditions=[status_is("new", "planning")],
      postconditions=[file_exists("docs/ticket.md")],
      timeout=300)
def setup_prd_ticket(ctx: TaskContext) -> TaskResult:
    """Materialize an approved PRD ticket: slug + branch + per-repo worktree(s)
    + docs/ticket.md. Enqueues start_planning on success."""
    import core.queue as q
    from features import tickets as tix
    if not ctx.ticket_key:
        return TaskResult("failed", "ticket_key missing")
    ts = state.load_ticket(ctx.ticket_key) or {}
    if not ts:
        return TaskResult("failed", "ticket not found")
    if ts.get("source") != "prd":
        return TaskResult("skipped", "not a PRD-source ticket")
    if ts.get("approval_status") != "approved":
        return TaskResult("skipped", "ticket not approved")
    if ts.get("slug"):
        return TaskResult("skipped", "ticket already materialized")
    base_url = ctx.config.get("_base_url", "")
    try:
        updated = tix.materialize_prd_ticket(ctx.config, ctx.ticket_key, ts, base_url)
    except RuntimeError as e:
        return TaskResult("failed", str(e))
    state.save_ticket(ctx.ticket_key, updated)
    q.enqueue_job(ctx.instance_key, "address_pm_findings", ticket_key=ctx.ticket_key)
    q.enqueue_job(ctx.instance_key, "start_planning", ticket_key=ctx.ticket_key)
    return TaskResult("ok", artifacts={"slug": updated.get("slug"),
                                        "branch": updated.get("branch")})


@task("start_planning",
      preconditions=[status_is("new", "planning")],
      postconditions=[file_exists("docs/change-manifest.md")],
      on_entry_status="planning",
      on_success_status="reviewing",
      timeout=PLAN_TIMEOUT)
def start_planning(ctx: TaskContext) -> TaskResult:
    from features import acceptance
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    if ctx.ticket_key:
        ts = state.load_ticket(ctx.ticket_key) or {}
        if ts:
            before = bool(acceptance.is_structured(ts))
            acceptance.ensure_structured(ts)
            if not before and acceptance.is_structured(ts):
                state.save_ticket(ctx.ticket_key, ts)
                log.emit("ticket_acceptance_enriched",
                         f"Structured criteria filled for {ctx.ticket_key}",
                         meta={"ticket": ctx.ticket_key,
                               "n_criteria": len(acceptance.all_criteria(ts))})
    prompt = (
        "Run /ctp docs/. The resulting docs/technical-plan.md MUST include a "
        "'Test Plan' section listing unit, integration, and playwright tests for "
        "each acceptance criterion. If the ticket has structured acceptance criteria "
        "(in the ticket data), translate each criterion's playwright steps into a "
        "concrete e2e test. "
        "If docs/pm-findings.md exists, read it and address each concern in "
        "docs/technical-plan.md; if a concern is invalid, explain why."
    )
    log.emit("ticket_planning_started", f"Headless /ctp for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    if not (ticket_dir / "docs" / "technical-plan.md").exists():
        result = run_claude_code(prompt, cwd=ticket_dir, timeout=PLAN_TIMEOUT)
        if result is None:
            return TaskResult("failed", "claude returned non-zero or empty")
    else:
        log.emit("ticket_planning_skipped_ctp",
                 f"technical-plan.md exists, skipping /ctp for {ctx.ticket_key}",
                 meta={"ticket": ctx.ticket_key})

    change_manifest = ticket_dir / "docs" / "change-manifest.md"
    technical_plan = ticket_dir / "docs" / "technical-plan.md"
    if not change_manifest.exists() and technical_plan.exists():
        log.emit("ticket_planning_recovery",
                 f"change-manifest.md missing but technical-plan.md exists for "
                 f"{ctx.ticket_key}; generating from existing docs",
                 meta={"ticket": ctx.ticket_key})
        recovery_prompt = (
            "Read docs/technical-plan.md. Then write docs/change-manifest.md as a "
            "change manifest at technical-product-owner altitude. Include capability "
            "delivered, release-note framing, problem and approach, what changed by "
            "area, new surfaces, changed surfaces, integration obligations, tradeoffs "
            "accepted, what could break, what tests prove. Ground it in the actual "
            "technical-plan content."
        )
        recovery_result = run_claude_code(recovery_prompt, cwd=ticket_dir, timeout=300)
        if recovery_result is None:
            return TaskResult("failed", "recovery: claude returned non-zero or empty "
                                        "while generating change-manifest.md")
    return TaskResult("ok")


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
      postconditions=[file_contains("docs/tri-review.md", r"VERDICT:\s*PASS")],
      timeout=FIX_TIMEOUT)
def fix_review_findings(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    prompt = (
        "Read docs/tri-review.md and identify the blocking findings (those flagged as "
        "blocking, not suggestions). Fix each one in the workspace. Run any tests directly "
        "relevant to the changed files. Then update docs/tri-review.md: under the Verdict "
        "section, replace the previous verdict with a single line. Write 'VERDICT: PASS' "
        "if and only if every blocking finding from the original review has been addressed "
        "by your changes. Otherwise list which blocking findings remain unfixed (one bullet "
        "each, citing file:line) and write 'VERDICT: FAIL'. Do NOT re-run /tri-review or "
        "spawn additional persona reviews — this is a focused verification of your own fix, "
        "not a fresh review."
    )
    log.emit("ticket_review_fixing", f"Headless fix+rereview for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=FIX_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


@task("fix_ci_failures",
      preconditions=[status_is("pr_created", "in_review"),
                     has_flag("_ci_failed_pending")],
      timeout=FIX_TIMEOUT)
def fix_ci_failures(ctx: TaskContext) -> TaskResult:
    from features.tickets import MAX_CI_FIX_ATTEMPTS
    from features.pr_ci import triage_and_fix_pr

    ts = state.load_ticket(ctx.ticket_key or "") or {}
    slug = ts.get("slug", "")
    prs = ts.get("prs", [])
    base_url = ctx.config.get("_base_url", "")
    ticket_link = {"detail": f"{base_url}/tickets/{ctx.ticket_key}"}

    try:
        if not prs:
            return TaskResult("failed", "no prs on ticket")

        platform = make_platform(ctx.config)

        for pr in prs:
            wt = ticket_worktree_path(ctx.config, slug, pr["repo"])
            outcome = triage_and_fix_pr(
                platform, pr["repo"], pr["id"],
                label=f"{ctx.ticket_key} {pr['repo']}#{pr['id']}",
                worktree=wt,
                attempts=ts.get("ci_fix_attempts", 0),
                max_attempts=MAX_CI_FIX_ATTEMPTS,
            )
            kind = outcome["result"]
            failed_names = outcome.get("failed_names", [])
            pr_link = {**ticket_link, "pr": pr.get("url", "")}
            meta = {"ticket": ctx.ticket_key, "repo": pr["repo"], "pr_id": pr["id"],
                    "failed_checks": failed_names}

            if kind == "worktree_missing":
                log.emit("ticket_ci_fix_skipped",
                         f"Skipped CI fix for {slug or ctx.ticket_key}/{pr['repo']}: worktree missing",
                         links=ticket_link, meta={**meta, "reason": "worktree_missing"})
            elif kind == "unrelated":
                log.emit("ticket_checks_unrelated",
                         f"CI failure for {slug or ctx.ticket_key} not caused by our changes: "
                         f"{outcome.get('reason','')[:100]}",
                         links=pr_link, meta={**meta, "reason": outcome.get("reason", "")})
            elif kind == "fixed":
                def _bump(current):
                    if not current:
                        return None
                    current["ci_fix_attempts"] = current.get("ci_fix_attempts", 0) + 1
                    current.pop("ci_passed", None)
                    current.pop("checks_started_at", None)
                    return current
                updated = state.update_ticket(ctx.ticket_key or "", _bump) or {}
                ts = updated
                log.emit("ticket_ci_fix_sent",
                         f"Sent CI fix to {slug or ctx.ticket_key} (attempt "
                         f"{updated.get('ci_fix_attempts', 0)}): {outcome.get('fix_hint','')[:80]}",
                         links=pr_link,
                         meta={**meta, "fix_hint": outcome.get("fix_hint", "")})
            # no_failing / capped / haiku_* / fix_failed: silent by design

        return TaskResult("ok", artifacts={"ci_fix_attempts": ts.get("ci_fix_attempts", 0)})
    finally:
        if ctx.ticket_key:
            def _clear(current):
                if current is None:
                    return None
                current.pop("_ci_failed_pending", None)
                return current
            state.update_ticket(ctx.ticket_key, _clear)


BACKFILL_TIMEOUT = 2400


@task("backfill_artifacts",
      postconditions=[file_contains("docs/tri-review.md", r"VERDICT:\s*(PASS|FAIL)")],
      timeout=BACKFILL_TIMEOUT)
def backfill_artifacts(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    pr_url = ctx.payload.get("pr_url", "")
    repo = ctx.payload.get("repo", "")
    if not pr_url or not repo:
        return TaskResult("failed", "payload must include pr_url and repo")
    prompt = (
        f"You are backfilling planning/review docs for a ticket whose PR is already open.\n\n"
        f"Context:\n"
        f"- Ticket: {ctx.ticket_key}\n"
        f"- PR: {pr_url}\n"
        f"- PR branch checked out at ./{repo}/ (git worktree)\n"
        f"- Current dir has docs/ and {repo}/ as siblings\n\n"
        f"Tasks in order:\n"
        f"1. Read docs/ticket.md for ticket context.\n"
        f"2. Examine PR changes: cd {repo}/ && git diff --stat origin/main...HEAD ; git diff origin/main...HEAD\n"
        f"3. Write docs/technical-plan.md as a retrospective technical plan explaining the approach in this PR. Cover architecture, data flow, files modified, key types/interfaces.\n"
        f"4. Write docs/change-manifest.md as a change manifest at technical-product-owner altitude. Include capability delivered, release-note framing, problem and approach, what changed by area, new surfaces, changed surfaces, integration obligations, tradeoffs accepted, what could break, what tests prove. Ground it in the actual diff.\n"
        f"5. Run /tri-review and save the full output to docs/tri-review.md, including a line reading exactly 'VERDICT: PASS' or 'VERDICT: FAIL' in the Verdict section.\n\n"
        f"Write all three docs. Do not modify code in the repo."
    )
    log.emit("ticket_backfill_started", f"Backfilling artifacts for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key, "pr_url": pr_url})
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=BACKFILL_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


@task("mark_ready",
      preconditions=[status_is("reviewing"),
                     file_contains("docs/tri-review.md", r"VERDICT:\s*PASS")],
      on_success_status="pr_ready",
      timeout=15)
def mark_ready(ctx: TaskContext) -> TaskResult:
    import core.events as events
    ts = state.load_ticket(ctx.ticket_key or "") or {}
    events.dispatch("ticket_dev_complete", {
        "ticket_key": ctx.ticket_key,
        "estimate_seconds": ts.get("estimate_seconds", 0),
        "discovered_at": ts.get("discovered_at", ""),
        "slug": ts.get("slug", ""),
        "branch": ts.get("branch", ""),
    }, ctx.config)
    return TaskResult("ok")


@task("fix_reported_bug",
      timeout=FIX_TIMEOUT)
def fix_reported_bug(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    prompt = (
        "A user reported that this ticket's fix is not working or has regressed. "
        "Re-investigate the issue in the current worktree state, identify why it's failing, "
        "and fix it. Then run relevant tests to verify the fix works. Commit your changes."
    )
    log.emit("ticket_bug_fix_started", f"Investigating reported bug for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=FIX_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


@task("create_pr",
      preconditions=[status_is("pr_ready"), auto_pr_true],
      timeout=300)
def create_pr(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    ts = state.load_ticket(ctx.ticket_key or "")
    if ts is None:
        return TaskResult("failed", "ticket not found")
    ticket = {"key": ctx.ticket_key, "summary": ts.get("summary", ""),
              "description": ts.get("description", ""), "url": ts.get("url", "")}
    try:
        updated = tix._create_pr(ctx.config, ticket, ts, ctx.registry.base_url)
        new_status = updated.get("status", "pr_failed")
        state.save_ticket(ctx.ticket_key or "", updated)
        return TaskResult("ok", artifacts={"transitioned_to": new_status})
    except Exception as e:
        if ctx.ticket_key:
            try:
                state.transition_ticket(ctx.ticket_key, "pr_failed")
            except state.TicketStateError:
                pass
        return TaskResult("failed", f"{type(e).__name__}: {e}",
                          artifacts={"transitioned_to": "pr_failed"})


@task("apply_note_reset",
      on_success_status="new",
      timeout=30)
def apply_note_reset(ctx: TaskContext) -> TaskResult:
    import shutil
    from datetime import datetime, timezone
    note = ctx.payload.get("note", "")
    ws = ctx.config["workspace"]
    ts = state.load_ticket(ctx.ticket_key or "")
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

    return TaskResult("ok", artifacts={"archived_to": archived_to, "moved": moved})


@task("set_state",
      on_success_status=lambda ctx, _r: ctx.payload.get("target", "") or None,
      timeout=15)
def set_state(ctx: TaskContext) -> TaskResult:
    target = ctx.payload.get("target", "")
    if not target:
        return TaskResult("failed", "target state missing")
    return TaskResult("ok", artifacts={"transitioned_to": target})


VALIDATION_TIMEOUT = 1800


@task("validate_merged_ticket",
      preconditions=[status_is("merged", "validation")],
      on_entry_status="validation",
      on_success_status="done",
      timeout=VALIDATION_TIMEOUT)
def validate_merged_ticket(ctx: TaskContext) -> TaskResult:
    from features import validation
    if not ctx.ticket_key:
        return TaskResult("failed", "ticket_key missing")
    ts = state.load_ticket(ctx.ticket_key) or {}
    if not ts:
        return TaskResult("failed", "ticket not found")
    val_cfg = ctx.config.get("validation", {})
    base_url = val_cfg.get("live_url") or ""
    if not base_url:
        log.emit("validation_skipped",
                 f"No [validation].live_url configured; skipping for {ctx.ticket_key}",
                 meta={"ticket": ctx.ticket_key})
        return TaskResult("ok", artifacts={"skipped": True})
    persistent = bool(val_cfg.get("persistent_browser_context"))
    try:
        summary = validation.validate_merged(
            ctx.instance_key, ctx.ticket_key, ts, base_url,
            persistent_context=persistent,
        )
        return TaskResult("ok", artifacts=summary)
    except Exception as e:
        log.emit("validation_error",
                 f"[{ctx.instance_key}] {ctx.ticket_key}: {type(e).__name__}: {e}",
                 meta={"ticket": ctx.ticket_key})
        return TaskResult("failed", f"{type(e).__name__}: {e}")
