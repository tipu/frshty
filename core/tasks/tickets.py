"""Ticket pipeline tasks. Headless claude -p invocations, postcondition-gated."""
import json
from pathlib import Path

import core.log as log
import core.state as state
from core.claude_runner import run_claude_code, run_haiku, extract_json
from core.config import ticket_worktree_path
from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import (
    status_is, auto_pr_true, file_exists, file_contains, feature_enabled, has_flag,
)
from features.platforms import make_platform


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


@task("fix_ci_failures",
      preconditions=[status_is("pr_created", "in_review"),
                     has_flag("_ci_failed_pending")],
      timeout=FIX_TIMEOUT)
def fix_ci_failures(ctx: TaskContext) -> TaskResult:
    from features.tickets import MAX_CI_FIX_ATTEMPTS

    tickets = state.load("tickets")
    ts = tickets.get(ctx.ticket_key or "") or {}
    slug = ts.get("slug", "")
    prs = ts.get("prs", [])
    base_url = ctx.config.get("_base_url", "")
    ticket_link = {"detail": f"{base_url}/tickets/{ctx.ticket_key}"}

    try:
        if not prs:
            return TaskResult("failed", "no prs on ticket")

        platform = make_platform(ctx.config)

        for pr in prs:
            checks = platform.get_pr_checks(pr["repo"], pr["id"]) or []
            failed_names = [c["name"] for c in checks if c["state"].upper() in ("FAILURE", "FAILED")]
            if not failed_names:
                continue

            fix_attempts = ts.get("ci_fix_attempts", 0)
            if fix_attempts >= MAX_CI_FIX_ATTEMPTS:
                continue

            wt = ticket_worktree_path(ctx.config, slug, pr["repo"])
            if not wt.is_dir():
                log.emit("ticket_ci_fix_skipped",
                         f"Skipped CI fix for {slug or ctx.ticket_key}/{pr['repo']}: worktree missing",
                         links=ticket_link,
                         meta={"ticket": ctx.ticket_key, "repo": pr["repo"], "reason": "worktree_missing"})
                continue

            failure_logs = platform.get_failed_logs(pr["repo"], pr["id"]) or ""
            pr_diff = platform.get_pr_diff(pr["repo"], pr["id"]) or ""
            causality_prompt = (
                "CI checks failed on a PR created by our automated pipeline. "
                "Determine if this is caused by our changes or is pre-existing/unrelated.\n\n"
                f"Ticket: {ctx.ticket_key} — {ts.get('summary', '')}\n"
                f"Failed checks: {', '.join(failed_names)}\n"
                f"Fix attempt: {fix_attempts + 1}/{MAX_CI_FIX_ATTEMPTS}\n\n"
                f"PR diff (what we changed):\n{pr_diff[:4000]}\n\n"
                f"Failure logs:\n{failure_logs[:4000]}\n\n"
                "Analyze causality:\n"
                "1. Could our diff have caused these failures? Consider both direct changes and indirect effects.\n"
                "2. Or are these pre-existing failures, flaky tests, or infra issues unrelated to our changes?\n\n"
                "Reply with EXACTLY one JSON object:\n"
                '{"caused_by_us": true/false, "reason": "brief explanation", "fix_hint": "what to change if caused_by_us"}'
            )
            classification = run_haiku(causality_prompt, timeout=120)
            if not classification:
                continue
            try:
                analysis = extract_json(classification) or json.loads(classification.strip())
            except (json.JSONDecodeError, TypeError):
                continue

            if not analysis.get("caused_by_us", False):
                reason = analysis.get("reason", "")
                log.emit("ticket_checks_unrelated",
                         f"CI failure for {slug or ctx.ticket_key} not caused by our changes: {reason[:100]}",
                         links={**ticket_link, "pr": pr.get("url", "")},
                         meta={"ticket": ctx.ticket_key, "failed_checks": failed_names, "reason": reason})
                continue

            fix_hint = analysis.get("fix_hint", "")
            fix_prompt = (
                f"CI checks failed: {', '.join(failed_names)}. This is caused by our changes. "
                f"Fix the issue: {fix_hint}. Run the failing tests locally if you can, "
                f"then commit with --no-verify and push."
            )
            result = run_claude_code(fix_prompt, cwd=wt, timeout=FIX_TIMEOUT)
            if result is None:
                continue

            ts["ci_fix_attempts"] = fix_attempts + 1
            ts.pop("ci_passed", None)
            ts.pop("checks_started_at", None)
            log.emit("ticket_ci_fix_sent",
                     f"Sent CI fix to {slug or ctx.ticket_key} (attempt {ts['ci_fix_attempts']}): {fix_hint[:80]}",
                     links={**ticket_link, "pr": pr.get("url", "")},
                     meta={"ticket": ctx.ticket_key, "failed_checks": failed_names, "fix_hint": fix_hint})

        return TaskResult("ok", artifacts={"ci_fix_attempts": ts.get("ci_fix_attempts", 0)})
    finally:
        ts.pop("_ci_failed_pending", None)
        if ctx.ticket_key:
            tickets[ctx.ticket_key] = ts
            state.save("tickets", tickets)


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
