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
from typing import Protocol, TypedDict

import core.log as log
import core.state as state
from core.ticket_status import TicketStatus

import features.tickets as _t


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
    - CI markers: ci_fix_attempts, ci_passed, checks_started_at,
      _ci_failed_pending, _ci_timeout_state
    - comment markers: last_comment_ids, comment_fix_attempts
    - misc: llm_sessions
    """
    status: str
    external_status: str
    url: str
    source: str
    summary: str
    description: str
    done_at: str
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
    ci_passed: bool
    checks_started_at: str
    _ci_failed_pending: bool
    _ci_timeout_state: dict
    last_comment_ids: dict
    comment_fix_attempts: dict
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
    if "source" not in ts:
        ts["source"] = _t._ticket_source(config)
    source = ts.get("source", _t._ticket_source(config))
    if _t._approval_required(config, source) and ts.get("approval_status") not in ("approved", "rejected"):
        ts["status"] = TicketStatus.pending_approval.value
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
        if instance_key and (config.get("pm_agent") or {}).get("enabled", True):
            _t._enqueue_stage(instance_key, key, "pm_pre_approval")
        return ts, True
    if source == "prd":
        state.save_ticket(key, ts)
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
        ts = _t._setup_ticket(config, ticket, base_url)
    if "source" not in ts:
        ts["source"] = source
    if ts.get("discovered_at") and instance_key:
        if (config.get("pm_agent") or {}).get("enabled", True):
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
    review_file = ws["root"] / ws["tickets_dir"] / slug / "docs" / "tri-review.md"
    if review_file.exists():
        verdict = _t._VERDICT_RE.search(review_file.read_text())
        if verdict and verdict.group(1).upper() == "PASS":
            _t._enqueue_stage(instance_key, key, "enter_testing")
        elif verdict and verdict.group(1).upper() == "FAIL":
            _t._enqueue_stage(instance_key, key, "fix_review_findings")
        else:
            _t._enqueue_stage(instance_key, key, "start_reviewing")
    else:
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
        if attempts >= _t.MAX_TEST_FIX_ATTEMPTS:
            try:
                state.transition_ticket(key, "tests_failed")
                ts["status"] = "tests_failed"
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
    if not instance_key:
        return ts, False
    key = ticket["key"]
    ws = config["workspace"]
    slug = ts.get("slug", "")
    proof = ws["root"] / ws["tickets_dir"] / slug / "docs" / "proof.md"
    if proof.exists():
        _t._enqueue_stage(instance_key, key, "mark_ready")
    else:
        _t._enqueue_stage(instance_key, key, "prove")
    return ts, False


def _handle_pr_ready_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if instance_key and not ts.get("pr_descriptions"):
        _t._enqueue_stage(instance_key, key, "generate_pr_descriptions")
    if config.get("pr", {}).get("auto_pr") and not ts.get("pr_scheduled_at"):
        if instance_key and _t._repo_gate_blocked(instance_key, key):
            state.save_ticket(key, ts)
            return ts, True
        ts = _t._create_pr(config, ticket, ts, base_url)
    return ts, False


def _handle_in_review_ticket(
    config: dict,
    ticket: dict,
    ts: dict,
    base_url: str,
    instance_key: str,
    existing: bool,
) -> tuple[dict, bool]:
    key = ticket["key"]
    if instance_key:
        if _t._resolve_conflicts_pending(instance_key, key):
            state.save_ticket(key, ts)
            return ts, True
        if _t._has_conflicting_pr(config, ts):
            _t._enqueue_stage(instance_key, key, "resolve_conflicts")
            state.save_ticket(key, ts)
            return ts, True
    else:
        ts = _t._resolve_conflicts(config, ticket, ts, base_url)

    if ts["status"] == "in_review":
        platform = _t.make_platform(config)
        result = platform.monitor_ci(ticket, ts, base_url)
        if result.get("_ci_failed"):
            ts = _t._handle_ci_failure(ticket, ts, result["pr"], result["checks"], base_url, instance_key)
        elif result.get("_ci_stalled"):
            pr = result.get("pr") or {}
            log.emit("ticket_checks_stall_repolling",
                f"CI stalled for {_t._label(ticket['key'], ts)} PR #{pr.get('id', '?')}; "
                f"resetting checks window and re-polling on next cycle",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr.get("repo"), "pr_id": pr.get("id")})
            ts.pop("_ci_timeout_state", None)
            ts.pop("checks_started_at", None)
        else:
            ts = result

    if ts["status"] == "in_review" and ts.get("ci_passed") and config.get("pr", {}).get("auto_merge"):
        ts = _t._merge(config, ticket, ts, base_url)

    if ts["status"] == "in_review":
        ts = _t._check_in_review(config, ticket, ts, base_url)
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
    if curr_ext and curr_ext != ts["merged_external_status"]:
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
)
