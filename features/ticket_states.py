"""Per-status handler functions for features.tickets.check().

Each handler has the signature
    (config, ticket, ts, base_url, instance_key, existing) -> (ts, stop_ticket)
where stop_ticket=True tells check()'s dispatcher loop to break out of further
status-handler dispatch for this ticket. The dispatcher then state.save_ticket()s
and moves to the next ticket.

This module exists to separate the dispatch table from the helpers it calls.
Handlers reference helpers via `_t.<name>` (where `_t = features.tickets`) so
that test patches at `features.tickets.<name>` flow through the alias — bare-name
`from features.tickets import _setup_ticket` would bind to the original function
object at module-load time and miss patches applied later.

Side-effect idempotency: see emit_once() in features/tickets.py for the canonical
"fire once per ticket lifetime" helper and TestCheckIdempotentSecondCycle for the
meta-invariant test that asserts a second check() cycle produces zero new
log_events.
"""
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, TypedDict

import core.freshness as freshness
import core.log as log
import core.queue as q
import core.state as state
from core.ticket_status import (DIRTY_REVIEW, TicketStatus, approval_edge, epic_edge, tests_failed_edge,
                                transition)
from pm import runner as pm_runner

import features.pr_steward as pr_steward
import features.ticket_pr as ticket_pr
from features.pr_ci import failing_check_names
import features.tickets as _t


def _pm_pre_approval_needed(config: dict, instance_key: str, key: str) -> bool:
    """Pre-approval PM review is a once-per-ticket gate. A ticket sits in
    pending_approval until a human acts, and _enqueue_stage does not block
    re-enqueue after a successful run, so enqueuing it every dispatcher tick
    re-runs the LLM review and rewrites pm-findings.md on a loop. Only enqueue
    when no pre_approval review exists yet (a failed run writes no row, so it
    still retries until one succeeds)."""
    if not instance_key or not (config.get("pm_agent") or {}).get("enabled", True):
        return False
    return pm_runner.latest_pre_approval_review(instance_key, key) is None


class StateHandler(Protocol):
    """Contract every per-status handler must satisfy.

    A handler takes the dispatcher's per-ticket context (config, ticket,
    ts, base_url, instance_key, existing) and returns (updated_ts, stop_ticket).
    stop_ticket=True signals the dispatcher loop to break out of further
    status-handler dispatch for this ticket — the dispatcher then
    state.save_ticket()s and moves to the next ticket.

    Idempotency contract: handlers MUST treat every side-effect emit as
    "fire once per ticket lifetime" unless there is a documented reason
    otherwise. Use emit_once(ts, marker_field, ...) from features.tickets
    for the canonical pattern. The meta-invariant test
    TestCheckIdempotentSecondCycle asserts that a second check() cycle
    produces zero new log_events for any ticket in any non-terminal status.

    Future direction: if the free-function handler style proves to need
    more structure (e.g. distinct entry/tick/transitions methods to make
    one-shot vs per-poll work explicit), upgrade individual handlers to
    classes that implement this Protocol via __call__. The Protocol shape
    lets free functions and classes coexist in the same dispatch tuple.
    """
    def __call__(
        self,
        config: dict,
        ticket: dict,
        ts: dict,
        base_url: str,
        instance_key: str,
        existing: bool,
    ) -> tuple[dict, bool]: ...


class TicketState(TypedDict, total=False):
    """Declared fields valid on a ticket-state dict.

    All fields are optional (total=False) because tickets in different
    statuses populate different subsets. This is a documentation/type-hint
    layer only — there is no runtime enforcement. Treat the dispatcher's
    docstring at features.tickets.check() as the canonical reference for
    when each field is set and cleared.

    Field categories:
    - lifecycle: status, external_status, url, source, summary, description,
      done_at, requeued_at, reopened_count, slug, branch
    - setup markers: discovered_at, setup_failed_at
    - approval markers: approval_status
    - merge markers: merged_at, merged_external_status, merged_comment_snapshot,
      last_merged_at, last_merged_external_status, last_merged_comment_snapshot,
      ticket_comment_snapshot
    - PR markers: prs, pr_scheduled_at, pr_attempts, pr_failed_reason
    - conflict markers: conflict_resolution_attempts, last_conflict_error
    - CI markers: ci_fix_attempts, ci_fix_heads, ci_passed, checks_started_at,
      checks_heads, _ci_timeout_state, ci_held_unrelated
    - scope review markers: scope_review
    - resume markers: dirty_review_attempts
    - misc: llm_sessions
    """
    status: str
    external_status: str
    url: str
    source: str
    summary: str
    description: str
    done_at: str
    done_reopen_checked_updated_at: str
    requeued_at: str
    reopened_count: int
    slug: str
    branch: str
    discovered_at: str
    setup_failed_at: str
    approval_status: str
    merged_at: str
    merged_external_status: str
    merged_comment_snapshot: dict
    last_merged_at: str
    last_merged_external_status: str
    last_merged_comment_snapshot: dict
    ticket_comment_snapshot: dict
    prs: list
    pr_scheduled_at: str
    pr_attempts: int
    pr_failed_reason: str
    conflict_resolution_attempts: int
    last_conflict_error: str
    ci_fix_attempts: int
    ci_fix_heads: dict
    ci_passed: bool
    checks_started_at: str
    checks_heads: dict
    _ci_timeout_state: dict
    ci_held_unrelated: bool
    scope_review: dict
    dirty_review_attempts: int
    llm_sessions: dict


def _handle_new_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    # Epics (Jira) / projects (Linear) are container issues, not units of work.
    # frshty tracks them so the UI can group their child stories together but
    # never plans / codes / PRs them. Short-circuit before any pipeline kicks
    # in: park them in the terminal `epic` status, record the summary so the
    # UI can render the epic header, and return without enqueueing.
    epic = epic_edge(ticket.get("issue_type"))
    if epic:
        if ts.get("status") != epic:
            try:
                state.transition_ticket(key, epic, reason="auto: issue_type=Epic")
            except state.TicketStateError:
                ts["status"] = epic
            if not existing:
                log.emit("epic_found",
                         f"Epic tracked (not planned): {key} — {ticket.get('summary', '')}",
                         links={"ticket": ticket.get("url", ""),
                                "detail": f"{base_url}/tickets/{key}"},
                         meta={"ticket": key, "issue_type": "Epic"})
        if "summary" not in ts:
            ts["summary"] = ticket.get("summary", "")
        state.save_ticket(key, ts)
        return ts, True
    if "source" not in ts:
        ts["source"] = _t._ticket_source(config)
    source = ts.get("source", _t._ticket_source(config))
    approval = approval_edge(_t._approval_required(config, source), ts.get("approval_status"))
    if approval:
        ts["status"] = approval
        ts["approval_status"] = "pending"
        if "discovered_at" not in ts:
            ts["discovered_at"] = datetime.now(timezone.utc).isoformat()
        if "summary" not in ts:
            ts["summary"] = ticket.get("summary", "")
        if "description" not in ts:
            ts["description"] = ticket.get("description", "")
        state.save_ticket(key, ts)
        log.emit("ticket_pending_approval",
                 f"Ticket {key} awaiting approval (source={source})",
                 links={"detail": f"{base_url}/tickets/{key}"},
                 meta={"ticket": key, "source": source})
        if _pm_pre_approval_needed(config, instance_key, key):
            _t._enqueue_stage(instance_key, key, "pm_pre_approval")
        return ts, True
    if source == "prd":
        # PRD tickets are materialized by setup_prd_ticket (not by the
        # _setup_ticket path that jira/linear use). setup_prd_ticket
        # populates slug + enqueues start_planning exactly once. But if that
        # start_planning is skipped (e.g. repo gate is busy with a sibling
        # PRD ticket) nothing re-enqueues it — the PRD ticket sits in "new"
        # until the next process restart. Re-enqueue on every dispatcher tick
        # when the ticket has been materialized but hasn't moved out of "new".
        # _enqueue_stage dedupes against queued/running jobs and the
        # consecutive-failure cap, so this is a no-op when planning is already
        # in flight or already gave up.
        def _keep_task_fields(cur: dict) -> dict:
            merged = dict(ts)
            for field in ("status", "slug", "branch", "discovered_at"):
                if cur.get(field) is not None:
                    merged[field] = cur[field]
            return merged

        ts = state.update_ticket(key, _keep_task_fields) or ts
        if instance_key and ts.get("slug"):
            _t._enqueue_stage(instance_key, key, "start_planning")
        return ts, True
    if not existing:
        log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
            meta={"ticket": key})
    if not ts.get("discovered_at") and not ts.get("setup_failed_at"):
        pre_merged = _t._find_pre_merged_pr(config, ticket)
        if pre_merged:
            ts = _t._short_circuit_to_merged(config, ticket, ts, pre_merged, base_url)
            state.save_ticket(key, ts)
            return ts, True
        ts = _t._merge_setup(ts, _t._setup_ticket(config, ticket, base_url))
    if "source" not in ts:
        ts["source"] = source
    if ts.get("discovered_at") and instance_key:
        work_type = _t._ensure_work_type(config, ticket, ts)
        if work_type == "research":
            _t._enqueue_stage(instance_key, key, "do_research")
        elif work_type == "unknown":
            # Held for manual classification; surfaced via the
            # needs_classification Today bucket. No pipeline task is enqueued.
            pass
        else:
            if _pm_pre_approval_needed(config, instance_key, key):
                _t._enqueue_stage(instance_key, key, "pm_pre_approval")
            _t._enqueue_stage(instance_key, key, "start_planning")
    return ts, False


def _handle_pending_approval_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    return ts, True


def _handle_planning_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if "Epic" in (ticket.get("issue_type"), ts.get("issue_type")):
        return ts, True
    if instance_key:
        _t._enqueue_stage(instance_key, ticket["key"], "start_planning")
    return ts, False


def _handle_reviewing_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if not instance_key:
        return ts, False
    key = ticket["key"]
    ws = config["workspace"]
    slug = ts.get("slug", "")
    docs = ws["root"] / ws["tickets_dir"] / slug / "docs"
    review_file = docs / "tri-review.md"
    if review_file.exists():
        verdict = _t._VERDICT_RE.search(review_file.read_text())
        if verdict and verdict.group(1).upper() == "PASS":
            _t._enqueue_stage(instance_key, key, "enter_testing")
        elif verdict and verdict.group(1).upper() == "FAIL":
            _t._enqueue_stage(instance_key, key, "fix_review_findings")
        else:
            _t._enqueue_stage(instance_key, key, "start_reviewing")
        return ts, False
    flow_doc = docs / "FLOW.html"
    if not flow_doc.exists() and not ts.get("flow_doc_requested"):
        ts["flow_doc_requested"] = True
        state.save_ticket(key, ts)
        _t._enqueue_stage(instance_key, key, "generate_flow_doc")
        return ts, False
    _t._enqueue_stage(instance_key, key, "start_reviewing")
    return ts, False


def _handle_testing_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if not instance_key:
        return ts, False
    key = ticket["key"]
    ws = config["workspace"]
    slug = ts.get("slug", "")
    docs = ws["root"] / ws["tickets_dir"] / slug / "docs"
    test_plan = docs / "test-plan.md"
    test_files = docs / "test-files-written.txt"
    test_runs = docs / "test-runs.md"

    if not test_plan.exists():
        _t._enqueue_stage(instance_key, key, "plan_tests")
        return ts, False
    if not test_files.exists():
        _t._enqueue_stage(instance_key, key, "write_tests")
        return ts, False
    if not test_runs.exists():
        _t._enqueue_stage(instance_key, key, "run_tests_and_fix")
        return ts, False

    verdict_match = _t._VERDICT_RE.search(test_runs.read_text())
    verdict = verdict_match.group(1).upper() if verdict_match else None

    if verdict == "PASS":
        _t._enqueue_stage(instance_key, key, "enter_proving")
    elif verdict == "FAIL":
        attempts = int(ts.get("test_fix_attempts", 0))
        exhausted = tests_failed_edge(attempts)
        if exhausted:
            try:
                state.transition_ticket(key, exhausted)
                ts["status"] = exhausted
                _t.emit_once(
                    ts, "tests_failed_at", "ticket_tests_failed",
                    f"{key}: tests failed after {attempts} attempts; "
                    f"needs human review",
                    links={"detail": f"{base_url}/tickets/{key}"},
                    meta={"ticket": key, "attempts": attempts},
                )
            except state.TicketStateError:
                pass
        else:
            _t._enqueue_stage(instance_key, key, "run_tests_and_fix")
    else:
        _t._enqueue_stage(instance_key, key, "run_tests_and_fix")
    return ts, False


def _handle_proving_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    """Route a proving ticket, after the consensus scope gate clears it.

    The gate reads the branch the ticket would ship, so it belongs after the
    implementation is complete and before the proof. A branch that carries
    changes the ticket does not ask for is then corrected once and proved
    once. The gate used to run only at pr_ready, which proved the branch,
    corrected it, and proved it again.

    A FAIL whose correction budget is spent falls through to the proof. The
    reviewers and the fixer disagree about what the ticket is, that is the
    operator's call, and the pr_ready gate still holds the PR until the
    operator makes it.
    """
    if not instance_key:
        return ts, False
    key = ticket["key"]
    scope = _t._scope_review_state(config, ts)
    if scope == "pending":
        _t._enqueue_stage(instance_key, key, "scope_review")
        state.save_ticket(key, ts)
        return ts, True
    if scope == "fail" and not _t._scope_fix_budget_spent(instance_key, key, ts):
        _t._enqueue_scope_fix(instance_key, key, ts)
        state.save_ticket(key, ts)
        return ts, True
    ws = config["workspace"]
    slug = ts.get("slug", "")
    proof = ws["root"] / ws["tickets_dir"] / slug / "docs" / "proof.md"
    if proof.exists() and not _t._proof_is_stale(config, ts):
        _t._enqueue_stage(instance_key, key, "mark_ready")
    else:
        _t._enqueue_stage(instance_key, key, "prove")
    return ts, False


def _change_manifest_exists(config: dict, ts: dict, key: str) -> bool:
    ws = config["workspace"]
    slug = ts.get("slug") or key
    root = Path(ws["root"]) if isinstance(ws["root"], str) else ws["root"]
    return (root / ws["tickets_dir"] / slug / "docs" / "change-manifest.md").exists()


def _handle_pr_ready_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if instance_key and not ts.get("pr_descriptions_generated_at") and _change_manifest_exists(config, ts, key):
        _t._enqueue_stage(instance_key, key, "generate_pr_descriptions")
    if instance_key:
        scope = _t._scope_review_state(config, ts)
        if scope in ("pending", "fail"):
            if scope == "pending":
                _t._enqueue_stage(instance_key, key, "scope_review")
            else:
                _t._enqueue_scope_fix(instance_key, key, ts)
            state.save_ticket(key, ts)
            return ts, True
    if config.get("pr", {}).get("auto_pr") and not ts.get("pr_scheduled_at"):
        if instance_key and _t._repo_gate_blocked(instance_key, key, config):
            state.save_ticket(key, ts)
            return ts, True
        ts = _t._create_pr(config, ticket, ts, base_url)
    return ts, False


def _restart_checks_window_on_new_head(ts: dict, pr_info_map: dict) -> None:
    """A CI run belongs to a head, so the checks window restarts when a head
    moves. Only a green result used to end the window, so a window opened on a
    head that never went green kept aging across every later push. An unread
    head says nothing about a move, so it keeps its stored value."""
    stored = ts.get("checks_heads") or {}
    heads = dict(stored)
    for pr in ts.get("prs", []):
        sha = (pr_info_map.get((pr["repo"], pr["id"])) or {}).get("head_sha")
        if sha:
            heads[f"{pr['repo']}/{pr['id']}"] = sha
    if heads != stored:
        ts["checks_heads"] = heads
        ts.pop("checks_started_at", None)
        ts.pop("_ci_timeout_state", None)


def _handle_in_review_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    pr_info_map = _t._build_pr_info_map(_t.make_platform(config), ts.get("prs", []))
    ts = _t._check_in_review(config, ticket, ts, base_url, pr_info_map=pr_info_map)

    if ts["status"] == "in_review":
        conflicted = ticket_pr.conflict_capped(ts, pr_info_map)
        if conflicted:
            log.emit("ticket_conflict_failed",
                f"Conflict resolution failed for {_t._label(key, ts)}: the base sync budget is spent "
                f"on {', '.join(conflicted)}",
                links={"detail": f"{base_url}/tickets/{key}"},
                meta={"ticket": key, "prs": conflicted})
            ts["status"] = transition(ts["status"], "pr_failed")
            ts["pr_failed_reason"] = "conflict_failed"

    scope = _t._scope_review_state(config, ts) if instance_key else "disabled"
    if ts["status"] == "in_review" and instance_key and scope == "pending":
        _t._enqueue_stage(instance_key, key, "scope_review")

    if ts["status"] == "in_review":
        platform = _t.make_platform(config)
        ts.pop("ci_held_unrelated", None)
        _restart_checks_window_on_new_head(ts, pr_info_map)
        result = platform.monitor_ci(ticket, ts, base_url)
        if result.get("_ci_failed"):
            ts.pop("ci_passed", None)
            capped = ticket_pr.ci_capped(ts)
            if capped:
                failed_pr = result["pr"]
                log.emit("ticket_checks_failed",
                    f"CI failed for {_t._label(key, ts)} after the CI fix budget was spent: "
                    f"{', '.join(failing_check_names(result['checks']))}",
                    links={"detail": f"{base_url}/tickets/{key}", "pr": failed_pr.get("url", "")},
                    meta={"ticket": key, "prs": capped,
                          "failed_checks": failing_check_names(result["checks"])})
                ts["ci_fix_heads"] = {f"{p['repo']}/{p['id']}": (pr_info_map.get((p["repo"], p["id"])) or {}).get("head_sha", "")
                                      for p in ts.get("prs", [])}
                ts["status"] = transition(ts["status"], "pr_failed")
                ts["pr_failed_reason"] = "ci_failed"
        elif result.get("_ci_stalled"):
            pr = result.get("pr") or {}
            log.emit("ticket_checks_stall_repolling",
                f"CI stalled for {_t._label(ticket['key'], ts)} PR #{pr.get('id', '?')}; "
                f"resetting checks window and re-polling on next cycle",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr.get("repo"), "pr_id": pr.get("id")})
            ts.pop("_ci_timeout_state", None)
            ts.pop("checks_started_at", None)
            ts.pop("ci_passed", None)
        else:
            ts = result
            if ts.get("ci_passed") and freshness.enabled(config):
                freshness.record(key, "ci",
                                 freshness.pr_heads(ts.get("prs", []), pr_info_map),
                                 instance_key=instance_key)

    if ts["status"] == "in_review" and freshness.enabled(config):
        freshness.record(key, "comments", freshness.watermark(ts, instance_key),
                         instance_key=instance_key)

    pr_cfg = config.get("pr", {})
    approval_rule = bool(pr_cfg.get("merge_when_all_approved"))
    scope_ok = (scope in ("disabled", "pass")
                or (approval_rule and scope == "fail"
                    and not _t._scope_fix_incomplete(ts)))
    if (ts["status"] == "in_review" and ts.get("ci_passed")
            and (pr_cfg.get("auto_merge") or approval_rule)
            and scope_ok
            and not _t._hold_for_approvals(config, ticket, ts, base_url, pr_info_map)):
        if scope == "fail":
            _t.emit_once(ts, "scope_fail_approval_override",
                "ticket_scope_fail_overridden_by_approval",
                f"{_t._label(key, ts)}: every PR is approved; the approvals replace the "
                f"failed scope review and the merge proceeds",
                links={"detail": f"{base_url}/tickets/{key}"},
                meta={"ticket": key, "scope_review": ts.get("scope_review") or {}})
        ts = _t._merge(config, ticket, ts, base_url)
    if ts["status"] == "in_review":
        try:
            pr_steward.observe(config, ticket, ts, base_url, instance_key, pr_info_map)
        except Exception as e:
            log.emit("ticket_pr_steward_failed",
                f"{_t._label(key, ts)}: the shadow steward fingerprint failed: {type(e).__name__}: {e}",
                links={"detail": f"{base_url}/tickets/{key}"},
                meta={"ticket": key, "reason": "observe_error", "error": type(e).__name__})
    return ts, False


def _handle_merged_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    curr_ext = ticket.get("status", "")
    if not ts.get("merged_external_status"):
        ts["merged_external_status"] = curr_ext or "_merged_"
        state.save_ticket(key, ts)
        return ts, True
    if (curr_ext and curr_ext != ts["merged_external_status"]
            and (not _t._is_past_review(config, curr_ext)
                 or _t._done_ticket_has_new_comments(config, key, ts, ticket))):
        ts = _t._reingest_merged_ticket(config, ticket, ts, base_url)
        state.save_ticket(key, ts)
        if instance_key:
            _t._enqueue_stage(instance_key, key, "start_planning")
        return ts, True
    if instance_key:
        _t._enqueue_stage(instance_key, key, "validate_merged_ticket")
    return ts, True


def _handle_validation_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    if instance_key:
        _t._enqueue_stage(instance_key, ticket["key"], "validate_merged_ticket")
    return ts, True


def _handle_pr_failed_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if ts.get("prs"):
        ts = _t._recheck_pr_failed(config, ticket, ts, base_url)
        state.save_ticket(key, ts)
    if ts["status"] == TicketStatus.pr_failed:
        return ts, True
    return ts, False


def _parked_by_dirty_gate(instance_key: str, key: str) -> bool:
    """True when the newest stage job of the ticket is enter_testing refusing
    uncommitted work. advance_ticket only dispatches, so it is not a stage."""
    for job in q.jobs_for_ticket(instance_key, key):
        if job["task"] == "advance_ticket":
            continue
        return (job["task"] == "enter_testing" and job["status"] == "failed"
                and "uncommitted changes" in (job["response"] or ""))
    return False


def _handle_blocked_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    """Send a ticket that enter_testing refused back through review.

    Every other block waits for the operator. This one has a known remedy:
    start_reviewing commits the leftover work and reviews it. The review on
    disk read the branch without that work, so it goes, and the ticket earns a
    fresh verdict instead of carrying the old one over unreviewed code.
    DIRTY_REVIEW bounds the loop for a review that leaves new work behind each
    time. The ticket waits while another ticket holds the repo gate."""
    key = ticket["key"]
    if not instance_key or not _parked_by_dirty_gate(instance_key, key):
        return ts, True
    attempts = ts.get("dirty_review_attempts", 0)
    if not DIRTY_REVIEW.may_continue(attempts):
        return ts, True
    ws = config["workspace"]
    review_file = ws["root"] / ws["tickets_dir"] / ts.get("slug", "") / "docs" / "tri-review.md"
    with _t._gate_lock_for(instance_key):
        if _t._repo_gate_blocked(instance_key, key, config):
            return ts, True
        review_file.unlink(missing_ok=True)
        moved = state.transition_ticket(key, TicketStatus.reviewing.value,
                                        reason="uncommitted work goes back through review",
                                        dirty_review_attempts=attempts + 1)
    log.emit("ticket_dirty_review_resumed",
             f"{key}: enter_testing found uncommitted work; reviewing it again",
             meta={"ticket": key, "attempt": attempts + 1})
    ts.pop("blocked_from", None)
    return {**ts, "status": moved["status"], "dirty_review_attempts": attempts + 1}, True


_PRE_DISPATCH_HANDLERS: tuple[tuple[str, StateHandler], ...] = (
    (TicketStatus.merged, _handle_merged_ticket),
    (TicketStatus.validation, _handle_validation_ticket),
    (TicketStatus.pr_failed, _handle_pr_failed_ticket),
)


_STATUS_HANDLERS: tuple[tuple[str, StateHandler], ...] = (
    ("new", _handle_new_ticket),
    ("pending_approval", _handle_pending_approval_ticket),
    ("planning", _handle_planning_ticket),
    ("reviewing", _handle_reviewing_ticket),
    ("testing", _handle_testing_ticket),
    ("proving", _handle_proving_ticket),
    ("pr_ready", _handle_pr_ready_ticket),
    ("in_review", _handle_in_review_ticket),
    ("blocked", _handle_blocked_ticket),
)
