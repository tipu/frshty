import json
import re
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
import core.terminal as terminal
import manager.runner as runner
import manager.staleness as staleness
from core.config import get_repos
from core.tasks.autonomy import KV_KEY as _TODAY_KV_KEY
from features.tickets import _load_pr_comments
from manager.planner import build_plan
from services import work_store
from web.state import _config


router = APIRouter()

_ALLOWED_LOOPS = frozenset({
    "needs_classification",
    "blocked_pr_comments",
    "merge_ready", "ready_to_submit", "pr_comments_needs_reply",
    "peer_pr_reviews", "pickup_new", "in_review_no_ci", "pr_failed_tickets",
    "stale_own_prs", "stale_unattended", "pending_approvals_stuck",
    "regressions_recent", "timesheet_underfilled", "billcom_invoice_due",
})

_LAUNCHABLE_LOOPS = frozenset({
    "merge_ready", "ready_to_submit", "pr_comments_needs_reply",
    "pickup_new", "in_review_no_ci", "pr_failed_tickets",
    "pending_approvals_stuck", "stale_own_prs", "peer_pr_reviews",
})

_LAUNCH_NS = uuid.UUID("6f72736d-0000-0000-0000-667273687479")
_LAUNCH_STORE = "today_launch"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today_blob() -> dict:
    return state.load(_TODAY_KV_KEY) or {}


def _today_save(blob: dict) -> None:
    state.save(_TODAY_KV_KEY, blob)


def _ticket_status_safe(key: str) -> str | None:
    try:
        t = state.load_ticket(key)
    except Exception as e:
        log.emit("today_plan_ticket_unreadable",
                 f"could not read ticket {key}: {type(e).__name__}: {e}",
                 meta={"ticket": key})
        return None
    return (t or {}).get("status") if t else None


@router.get("/api/today/plan")
def api_today_plan():
    """The day's declared focus: the goals the planner picked, each with the
    ticket's live status so a goal the pipeline already reached reads as done."""
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"empty": True}
    blob = _today_blob()
    plan = blob.get("plan") or {}
    goals = plan.get("goals") or []
    skipped_keys = set(blob.get("skipped") or [])
    enriched = []
    for g in goals:
        tk = g.get("ticket_key")
        if not tk or tk in skipped_keys:
            continue
        cur = _ticket_status_safe(tk)
        enriched.append({**g, "current_state": cur,
                         "completed": cur == g.get("target_state")})
    return {
        "empty": not enriched,
        "instance_key": instance_key,
        "date": plan.get("date"),
        "generated_at": plan.get("generated_at"),
        "goals": enriched,
        "completed": plan.get("completed") or [],
        "paused": bool(blob.get("paused")),
        "skipped_keys": sorted(skipped_keys),
        "bucket_counts": plan.get("bucket_counts") or {},
    }


@router.get("/api/today/questions")
def api_today_questions():
    """Jobs parked in status='blocked' because a task needs an answer.
    claim_next only picks 'queued', so a blocked job waits here until answered."""
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return {"questions": []}
    rows = db.query_all(
        "SELECT id, ticket_key, task, payload, response, finished_at"
        " FROM jobs WHERE instance_key=? AND status='blocked'"
        " ORDER BY finished_at DESC LIMIT 50",
        (instance_key,),
    )
    out: list[dict] = []
    for r in rows:
        try:
            resp = json.loads(r.get("response") or "{}")
        except (json.JSONDecodeError, TypeError):
            resp = {}
        artifacts = resp.get("artifacts") or {}
        out.append({
            "job_id": r["id"],
            "ticket_key": r.get("ticket_key"),
            "task": r.get("task"),
            "question": resp.get("reason") or "",
            "kind": artifacts.get("kind") or "ambiguity_blocking",
            "expected_input": artifacts.get("expected_input") or "text",
            "asked_at": r.get("finished_at"),
        })
    return {"questions": out}


@router.post("/api/today/answer")
def api_today_answer(body: dict):
    """Consume a blocked job and re-enqueue the same task with the answer."""
    job_id = body.get("job_id")
    answer = body.get("answer", "")
    if not job_id:
        return JSONResponse({"error": "job_id required"}, status_code=400)
    row = db.query_one(
        "SELECT id, instance_key, ticket_key, task, payload, response, status"
        " FROM jobs WHERE id=?",
        (job_id,),
    )
    if not row:
        return JSONResponse({"error": "job not found"}, status_code=404)
    if row.get("status") != "blocked":
        return JSONResponse({"error": f"job not blocked (status={row.get('status')})"},
                            status_code=409)
    try:
        resp = json.loads(row.get("response") or "{}")
    except (json.JSONDecodeError, TypeError):
        resp = {}
    artifacts = resp.get("artifacts") or {}
    deferred = artifacts.get("deferred_payload") or {}
    try:
        original_payload = json.loads(row.get("payload") or "{}")
    except (json.JSONDecodeError, TypeError):
        original_payload = {}
    new_payload = {**original_payload, **deferred,
                   "_resume_from_job": job_id, "_answer": answer}
    db.execute(
        "UPDATE jobs SET status='answered', response=? WHERE id=?",
        (json.dumps({**resp, "answered_with": "<redacted>"}), job_id),
    )
    new_id = q.enqueue_job(row["instance_key"], row["task"], new_payload,
                           ticket_key=row.get("ticket_key"))
    log.emit("today_answered",
             f"answered job {job_id} -> re-enqueued as {new_id}",
             meta={"ticket": row.get("ticket_key"), "task": row.get("task")})
    return {"ok": True, "new_job_id": new_id}


@router.post("/api/today/steer")
def api_today_steer(body: dict):
    """Operator control over the day's plan: pause, resume, skip a ticket, or
    rebuild the plan now."""
    action = body.get("action") or ""
    blob = _today_blob()
    if action == "pause":
        blob["paused"] = True
    elif action == "resume":
        blob["paused"] = False
    elif action == "skip":
        ticket = body.get("ticket_key")
        reason = body.get("reason") or ""
        if not ticket:
            return JSONResponse({"error": "ticket_key required"}, status_code=400)
        skipped = list(blob.get("skipped") or [])
        if ticket not in skipped:
            skipped.append(ticket)
        blob["skipped"] = skipped
        reasons = blob.get("skip_reasons") or {}
        reasons[ticket] = reason
        blob["skip_reasons"] = reasons
    elif action == "replan":
        instance_key = _config.get("job", {}).get("key", "")
        if not instance_key:
            return JSONResponse({"error": "no instance"}, status_code=400)
        blob["plan"] = build_plan(
            instance_key, _config,
            use_llm=(_config.get("today_agent") or {}).get("use_llm", True))
        blob["paused"] = False
    else:
        return JSONResponse({"error": f"unknown action: {action}"}, status_code=400)
    _today_save(blob)
    return {"ok": True}


@router.get("/api/today/loops")
def api_today_loops(live: int = 0):
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    thresholds = (_config.get("manager") or {}).get("thresholds") or {}
    try:
        loops = staleness.aggregate_all(instance_key, config=_config,
                                        thresholds=thresholds, live=bool(live))
    except Exception as e:
        log.emit("today_loops_aggregate_failed",
                 f"[{instance_key}] aggregate_all crashed: {type(e).__name__}: {e}")
        return JSONResponse({"error": f"aggregate_all failed: {e}"}, status_code=500)

    counts = {k: len(v) for k, v in loops.items()}
    snoozed = _list_active_snoozes(instance_key)

    latest = runner.latest(instance_key)
    current_hash = runner.current_priorities_hash(_config)
    last_hash = (latest or {}).get("priorities_hash") or ""
    policy_stale = bool(current_hash and last_hash and current_hash != last_hash)

    return {
        "instance_key": instance_key,
        "generated_at": _now_iso(),
        "loops": loops,
        "counts": counts,
        "snoozed": snoozed,
        "policy_stale": policy_stale,
        "manager_latest": latest,
        "live": bool(live),
        "errors": [],
    }


@router.post("/api/today/snoozes")
async def api_today_snooze_create(body: dict):
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    loop_type = (body.get("loop_type") or "").strip()
    entity_id = (body.get("entity_id") or "").strip()
    if not loop_type or not entity_id:
        return JSONResponse({"error": "loop_type and entity_id required"}, status_code=400)
    if loop_type not in _ALLOWED_LOOPS:
        return JSONResponse({"error": f"unknown loop_type: {loop_type}"}, status_code=400)
    snooze_until = body.get("snooze_until")
    reason = (body.get("reason") or "").strip() or None
    _upsert_snooze(instance_key, loop_type, entity_id, snooze_until, reason)
    return {"status": "snoozed", "loop_type": loop_type,
            "entity_id": entity_id, "snooze_until": snooze_until}


@router.delete("/api/today/snoozes/{loop_type}/{entity_id:path}")
def api_today_snooze_delete(loop_type: str, entity_id: str):
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    _delete_snooze(instance_key, loop_type, entity_id)
    return {"status": "removed"}


def _upsert_snooze(instance_key, loop_type, entity_id, snooze_until, reason):
    db.execute(
        "INSERT INTO today_snoozes(instance_key, loop_type, entity_id, snooze_until, created_at, reason)"
        " VALUES (?, ?, ?, ?, ?, ?)"
        " ON CONFLICT(instance_key, loop_type, entity_id) DO UPDATE SET"
        " snooze_until=excluded.snooze_until, created_at=excluded.created_at, reason=excluded.reason",
        (instance_key, loop_type, entity_id, snooze_until, _now_iso(), reason),
    )


def _delete_snooze(instance_key, loop_type, entity_id):
    db.execute(
        "DELETE FROM today_snoozes WHERE instance_key=? AND loop_type=? AND entity_id=?",
        (instance_key, loop_type, entity_id),
    )


def _list_active_snoozes(instance_key: str) -> list[dict]:
    rows = db.query_all(
        "SELECT loop_type, entity_id, snooze_until, created_at, reason"
        " FROM today_snoozes"
        " WHERE instance_key=?"
        " AND (snooze_until IS NULL OR datetime(snooze_until) > datetime('now'))",
        (instance_key,),
    )
    return [dict(r) for r in rows]


def _sanitize(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "-", s or "")[:80]


def _launch_key(loop_type: str, entity_id: str) -> str:
    return f"today-{_sanitize(loop_type)}-{_sanitize(entity_id)}"


def _session_uuid(instance_key: str, loop_type: str, entity_id: str) -> str:
    return str(uuid.uuid5(_LAUNCH_NS, f"{instance_key}|{loop_type}|{entity_id}"))


def _resolve_launch_cwd(ticket_key: str | None, repo: str | None) -> str | None:
    if ticket_key:
        cwd = terminal._resolve_cwd(_config, ticket_key)
        if cwd:
            return cwd
    repos = get_repos(_config)
    if repo:
        for r in repos:
            if r.get("name") == repo:
                return str(r["path"])
    if repos:
        return str(repos[0]["path"])
    root = (_config.get("workspace") or {}).get("root")
    return str(root) if root else None


def _title(loop_type: str, ticket_key: str | None, repo: str | None, pr_id) -> str:
    if ticket_key:
        return ticket_key
    if repo:
        return f"{repo}#{pr_id}" if pr_id is not None else repo
    return loop_type


def _build_context(loop_type: str, ticket_key: str | None, repo: str | None, pr_id) -> str:
    ticket = state.load_ticket(ticket_key) if ticket_key else None
    summary = (ticket or {}).get("summary", "") if ticket else ""
    head = f"{ticket_key}: {summary}".strip(": ").strip() or _title(loop_type, ticket_key, repo, pr_id)

    if loop_type == "pr_comments_needs_reply" and ticket:
        slug = ticket.get("slug", "")
        comments = [c for c in _load_pr_comments(_config, slug) if c.get("status") == "needs_reply"]
        pr_url = ""
        for pr in ticket.get("prs") or []:
            if pr.get("url"):
                pr_url = pr["url"]
                break
        lines = [
            f"# Reply to PR review comments — {head}", "",
            f"You are in the worktree for {ticket_key}. {len(comments)} review comment(s) need a reply.",
            "Drafts below are a starting point. Some answers depend on input from teammates — if you are not sure of the answer, ask me before posting. Post each reply with `gh` only after I confirm the wording.", "",
        ]
        for c in comments:
            lines.append(f"## Comment #{c.get('id')} on {c.get('pr_repo')}#{c.get('pr_id')} ({c.get('path')}:{c.get('line')})")
            if c.get("pr_url") or pr_url:
                lines.append(c.get("pr_url") or pr_url)
            lines.append("")
            lines.append("> " + (c.get("body") or "").replace("\n", "\n> "))
            if c.get("diff_hunk"):
                lines.append("\nDiff hunk:\n```\n" + c["diff_hunk"] + "\n```")
            lines.append(f"\nDrafted reply: {c.get('suggested_reply') or '(none yet — draft one)'}\n")
        return "\n".join(lines)

    if loop_type == "merge_ready" and ticket:
        prs = "\n".join(
            f"- {p.get('repo')}#{p.get('id')} {p.get('url','')} (approvers: {', '.join(p.get('approvers') or []) or 'none'})"
            for p in (ticket.get("prs") or [])
        )
        return (
            f"# Merge check — {head}\n\n"
            "These PRs are approved. Before merging, confirm every sibling PR for this ticket is green and approved and that linked repos have no unmerged dependencies, then merge in dependency order. Ask me before merging if anything looks off.\n\n"
            f"PRs:\n{prs}\n"
        )

    if loop_type in ("pickup_new", "ready_to_submit") and ticket:
        desc = (ticket.get("description") or "")[:4000]
        repos = ", ".join(r.get("name", "") for r in get_repos(_config))
        verb = "Plan and implement it (e.g. `/ctp docs/`)." if loop_type == "pickup_new" else "Finish the work and open the PR."
        return f"# {head}\n\n{desc}\n\nRepos: {repos}\n\n{verb}"

    if loop_type in ("pr_failed_tickets", "in_review_no_ci") and ticket:
        prs = "\n".join(f"- {p.get('repo')}#{p.get('id')} {p.get('url','')}" for p in (ticket.get("prs") or []))
        reason = ticket.get("pr_failed_reason", "")
        attempts = ticket.get("ci_fix_attempts", 0)
        return (
            f"# Fix failing CI — {head}\n\n"
            f"ci_fix_attempts: {attempts}; reason: {reason}\n\n"
            f"PRs:\n{prs}\n\n"
            "Reproduce the failure locally first (`gh run view --log-failed`), find the local equivalent of each failing step, fix it, and push."
        )

    if loop_type in ("stale_own_prs", "peer_pr_reviews"):
        return (
            f"# {head}\n\n"
            f"PR {repo}#{pr_id} needs attention. Review the diff and decide next steps (nudge reviewers, address feedback, or rebase)."
        )

    if loop_type == "pending_approvals_stuck" and ticket:
        return (
            f"# Decision needed — {head}\n\n"
            f"{ticket.get('url','')}\n\n"
            "This ticket is awaiting your decision before frshty can proceed. Review it and tell me to approve or reject."
        )

    return f"# {head}\n\nReview this item and tell me what you want to do."


def _ensure_work_item(instance_key: str, m: dict, key: str, cwd: str):
    try:
        existing = db.query_one("SELECT id FROM work_runs WHERE session_id = ?", (m["sid"],))
        if existing:
            return
        scope = "ticket" if m.get("ticket_key") else "pr"
        scope_ref = m.get("ticket_key") or f"{m.get('repo')}/{m.get('pr_id')}"
        item_id = work_store.create_item(m.get("title") or scope_ref, scope=scope,
                                         scope_ref=scope_ref, instance_key=instance_key,
                                         tags=instance_key)
        work_store.add_run(item_id, m["sid"], key, cwd)
    except Exception as e:
        log.emit("work_item_link_failed", f"[{instance_key}] {key}: {type(e).__name__}: {e}")


@router.post("/api/today/launch")
def api_today_launch(body: dict):
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)

    store = state.load(_LAUNCH_STORE) or {}
    key_in = (body.get("key") or "").strip() or None

    if key_in and key_in in store:
        m = store[key_in]
        key = key_in
        loop_type = m["loop_type"]
        ticket_key = m.get("ticket_key")
        repo = m.get("repo")
        pr_id = m.get("pr_id")
    else:
        loop_type = (body.get("loop_type") or "").strip()
        ticket_key = (body.get("ticket_key") or "").strip() or None
        repo = (body.get("repo") or "").strip() or None
        pr_id = body.get("pr_id")
        if loop_type not in _LAUNCHABLE_LOOPS:
            return JSONResponse({"error": f"loop not launchable: {loop_type}"}, status_code=400)
        entity_id = ticket_key or (f"{repo}/{pr_id}" if repo else "")
        if not entity_id:
            return JSONResponse({"error": "ticket_key or repo required"}, status_code=400)
        key = _launch_key(loop_type, entity_id)
        m = store.get(key) or {
            "loop_type": loop_type, "ticket_key": ticket_key, "repo": repo, "pr_id": pr_id,
            "sid": _session_uuid(instance_key, loop_type, entity_id), "seeded": False,
            "created_at": _now_iso(),
        }

    cwd = _resolve_launch_cwd(ticket_key, repo)
    if not cwd:
        return JSONResponse({"error": "no working directory for this item"}, status_code=400)

    m.setdefault("title", _title(loop_type, ticket_key, repo, pr_id))
    health = terminal.session_healthy(key)
    if health.get("alive") and health.get("agent_running"):
        store[key] = m
        state.save(_LAUNCH_STORE, store)
        _ensure_work_item(instance_key, m, key, cwd)
        return {"key": key, "status": "running", "title": m["title"], "session_id": m["sid"]}

    first_run = not m.get("seeded")
    context = _build_context(loop_type, ticket_key, repo, pr_id) if first_run else ""
    try:
        terminal.launch_claude(key, cwd, m["sid"], context, first_run, config=_config)
    except Exception as e:
        log.emit("today_launch_failed",
                 f"[{instance_key}] launch {key} failed: {type(e).__name__}: {e}")
        return JSONResponse({"error": f"launch failed: {e}"}, status_code=500)

    m["seeded"] = True
    m["launched_at"] = _now_iso()
    store[key] = m
    state.save(_LAUNCH_STORE, store)
    _ensure_work_item(instance_key, m, key, cwd)
    return {"key": key, "status": "launched", "title": m["title"], "session_id": m["sid"]}
