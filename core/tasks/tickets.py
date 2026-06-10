"""Ticket pipeline tasks. Headless claude -p invocations, postcondition-gated."""
import json
import os
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

import core.log as log
import core.state as state
from core.claude_runner import run_claude_code, run_haiku, extract_json
from core.config import get_repos, ticket_worktree_path
from core.deps import relink_shared_venv
from core.consensus_plan import run_consensus_plan
from core.tasks.registry import TaskContext, TaskResult, task
from core.tasks.preconditions import (
    status_is, auto_pr_true, file_exists, file_contains, feature_enabled, has_flag,
    repo_gate_clear,
)
from features.platforms import make_platform


PLAN_TIMEOUT = 1800
REVIEW_TIMEOUT = 900
FIX_TIMEOUT = 1800
TEST_PLAN_TIMEOUT = 1800
TEST_WRITE_TIMEOUT = 3600
TEST_RUN_TIMEOUT = 5400
PROOF_TIMEOUT = 3600
MIN_TESTING_MD_BYTES = 200


def _testing_md_is_substantive(path: Path) -> bool:
    """True iff TESTING.md exists, is a file, and has >= MIN_TESTING_MD_BYTES
    of non-whitespace content."""
    try:
        if not path.is_file():
            return False
        content = path.read_text()
    except OSError:
        return False
    return len(content.strip()) >= MIN_TESTING_MD_BYTES


_NO_LOCAL_PY_VENV_SENTINEL = "__NO_LOCAL_PY_VENV__"


def _detect_runner(repo_dir: Path) -> tuple[list[str], dict[str, str]] | None:
    """Return (cmd_argv, env_extras) for the repo's native test runner, or
    None if no runner is detectable. package.json takes priority over
    pyproject/go/cargo for polyglot repos. For Python: never falls back to
    a system-PATH `pytest` — if the repo has Python test indicators but no
    discoverable local venv (.venv/bin/pytest) and no recognized env-manager
    lockfile (Pipfile, uv.lock, poetry.lock), returns a sentinel cmd so the
    caller can record a loud failure rather than silently invoking a system
    pytest that almost certainly lacks the repo's deps."""
    if (repo_dir / "package.json").exists():
        try:
            pkg = json.loads((repo_dir / "package.json").read_text())
        except (json.JSONDecodeError, OSError):
            pkg = {}
        scripts = pkg.get("scripts", {}) if isinstance(pkg, dict) else {}
        for candidate in ("test", "test:unit", "test:ci"):
            if candidate in scripts:
                if (repo_dir / "pnpm-lock.yaml").exists():
                    return (["pnpm", "run", candidate], {})
                if (repo_dir / "yarn.lock").exists():
                    return (["yarn", "run", candidate], {})
                return (["npm", "run", candidate], {})
    if (repo_dir / "pyproject.toml").exists() or \
       (repo_dir / "pytest.ini").exists() or \
       (repo_dir / "tests").is_dir():
        venv_pytest = repo_dir / ".venv" / "bin" / "pytest"
        if venv_pytest.exists():
            return ([str(venv_pytest), "-q"], {})
        if (repo_dir / "Pipfile").exists():
            return (["pipenv", "run", "pytest", "-q"], {})
        if (repo_dir / "uv.lock").exists():
            return (["uv", "run", "pytest", "-q"], {})
        if (repo_dir / "poetry.lock").exists():
            return (["poetry", "run", "pytest", "-q"], {})
        return ([_NO_LOCAL_PY_VENV_SENTINEL], {})
    if (repo_dir / "go.mod").exists():
        return (["go", "test", "./..."], {})
    if (repo_dir / "Cargo.toml").exists():
        return (["cargo", "test", "--quiet"], {})
    return None


def _repo_has_changes_vs_base(repo_dir: Path, base_branch: str) -> bool:
    """True iff `git diff --name-only origin/<base>...HEAD` lists any file.

    Used by run_tests_and_fix to skip repos the current ticket never touched,
    so DEV-475's verdict isn't blocked by pre-existing test failures in a repo
    DEV-475 has zero commits in. Falls back to True on any git error (safer to
    run a test than silently skip a repo we couldn't introspect)."""
    try:
        result = subprocess.run(
            ["git", "diff", "--name-only", f"origin/{base_branch}...HEAD"],
            cwd=str(repo_dir), capture_output=True, text=True, timeout=30,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return True
    if result.returncode != 0:
        return True
    return any(line.strip() for line in result.stdout.splitlines())


def _run_repo_tests(repo_dir: Path, cmd: list[str], env: dict[str, str],
                    timeout: int) -> dict:
    full_env = {**os.environ, **env}
    try:
        completed = subprocess.run(
            cmd, cwd=repo_dir, capture_output=True, text=True,
            timeout=timeout, env=full_env,
        )
    except subprocess.TimeoutExpired as e:
        tail = (e.stdout or "")[-2000:] + "\n---STDERR---\n" + (e.stderr or "")[-1000:]
        return {"result": "timeout", "exit_code": -1, "tail": tail}
    except (FileNotFoundError, OSError) as e:
        return {"result": "error", "exit_code": -1,
                "tail": f"{type(e).__name__}: {e}"}
    if completed.returncode == 0:
        result = "pass"
    elif completed.returncode == 5:
        result = "no_runner"
    else:
        result = "fail"
    return {
        "result": result,
        "exit_code": completed.returncode,
        "tail": (completed.stdout or "")[-3000:]
                + "\n---STDERR---\n"
                + (completed.stderr or "")[-1500:],
    }


def _write_test_runs(ticket_dir: Path, attempt: int,
                     per_repo: list[dict], verdict: str) -> None:
    lines = [
        f"# Test runs — attempt {attempt}\n",
        f"VERDICT: {verdict}\n\n",
    ]
    for r in per_repo:
        lines.append(f"## {r['repo']}\n")
        lines.append(f"- result: {r['result']}\n")
        if r.get("cmd"):
            lines.append(f"- command: `{r['cmd']}`\n")
        if r.get("exit_code") is not None:
            lines.append(f"- exit_code: {r['exit_code']}\n")
        if r.get("tail"):
            lines.append("\n```\n" + r["tail"] + "\n```\n")
        lines.append("\n")
    (ticket_dir / "docs" / "test-runs.md").write_text("".join(lines))


def _build_fix_prompt(per_repo: list[dict]) -> str:
    sections = []
    for r in per_repo:
        if r["result"] in ("pass", "no_runner", "skipped"):
            continue
        sections.append(
            f"--- {r['repo']} ({r['result']}, exit={r.get('exit_code', '?')}) ---\n"
            f"command: {r.get('cmd', '?')}\n\n"
            f"{r.get('tail', '')[:3500]}\n"
        )
    failures = "\n".join(sections) or "(no failure tails captured)"
    return (
        "The test runs below failed. Fix the failures by modifying the "
        "PRODUCTION code in this worktree (not the tests, unless a test is "
        "clearly wrong). Then commit your changes.\n\n"
        "Constraint: do NOT introduce mocks, skip markers, or conditional "
        "early returns to make failing tests pass. If a test reveals a real "
        "product bug, fix the bug.\n\n"
        f"{failures}"
    )


_PLAN_TESTS_PROMPT_TEMPLATE = """Read these files in order:
- docs/ticket.md
- docs/comments.md (upstream reviewer/QA discussion, if present)
- docs/notes/*.md (human guidance, if present)
- docs/change-manifest.md
- docs/tri-review.md

{testing_md_section}

Use any structured acceptance criteria enumerated in docs/ticket.md as the authoritative catalog. If absent, infer testable behavior from docs/change-manifest.md.

Produce docs/test-plan.md listing automated test cases that cover PRODUCT edge cases. For each acceptance criterion, write 2-6 test cases.

REQUIRED scope — test these:
- form-validation edges (empty, max-length, special chars, mixed casing, unicode/RTL)
- empty-state and zero-result UI paths
- permission / role boundaries (viewer vs editor vs admin; cross-tenant denial)
- pagination off-by-one (page 1, last page, page beyond last, page=0, negative)
- state-machine corners (cancelling mid-flight, replaying completed action, double-submit)
- business-logic boundaries (currency rounding at half-cent, integer overflow, date math at DST/month/year ends, sort stability with ties)
- multi-actor races where two users touch the same record back-to-back

EXPLICITLY OUT OF SCOPE — do NOT write tests for:
- redis / cache disconnects or eviction
- database connection drops, migration failures, lock timeouts
- S3 / blob-store outages
- network partitions, HTTP 5xx from upstream services, DNS failures
- container restarts, OOM, signal handling, file descriptor exhaustion
- TLS handshake failures, clock skew between hosts

Those are infrastructure failure modes and belong in a separate non-functional test track. If you find yourself writing one, delete it.

Output: one table row per test case:
| # | Acceptance criterion | Layer | Repo | File path | Test name | Assertion (plain English) | Preconditions |

Layer in {{unit, integration, e2e}}. Tests must FAIL on missing preconditions, never silently skip.

Finish with: Layer counts: unit: N  integration: M  e2e: K  total: T

If no testable product behavior exists, write a '## No Testable Criteria' section with rationale and counts 0/0/0/0.
"""


_PROVE_PROMPT_TEMPLATE = """A PROOF.md guide is provided below. Follow it.

PROJECT PROOF GUIDE (from workspace.root/PROOF.md):
{proof_md}
END OF PROOF GUIDE

The guide tells you (a) how to decide if this ticket's change is demoable / provable, and (b) if so, what concrete artifacts to produce (screenshots, recordings, traces, screen captures, exports, etc.).

Per the guide, do exactly one of:
  - Carry out the proof (capture whatever artifacts it asks for, save them under docs/ in this ticket directory).
  - Decide it isn't demoable and record that decision.

In every case, write a `docs/proof.md` summarizing what you did:
- A first-line marker: `PROOF: DEMOED` or `PROOF: NOT_APPLICABLE`
- A brief explanation (what you proved, or why no proof is applicable)
- Paths to any artifacts you produced under docs/ (relative to the ticket dir)

That `docs/proof.md` is the postcondition gate for this step.
"""


_WRITE_TESTS_PROMPT = """Read docs/test-plan.md. For each test case in the table, implement it in the repo's native framework. The test must be a real test the project's runner can discover.

Per repo in workspace/<repo>/:
1. Detect framework via repo-root files (package.json with vitest/jest/mocha/playwright; pyproject.toml or pytest.ini; go.mod; Cargo.toml).
2. Place each test in the canonical location — sibling to existing tests. Grep the tree first.
3. Reuse existing factories / fixtures / helpers. Only create new ones if there's a justified gap.
4. Tests MUST FAIL on missing preconditions. Use explicit assertions; never `if (!items) return`.
5. Do NOT modify production code yet. If a test fails because implementation is incomplete, that's fine — leave a one-line comment. The run_tests_and_fix loop iterates on production code afterwards.

After writing, append one line per file written to docs/test-files-written.txt:
    <repo>/<rel-path>

That file is the postcondition gate.
"""


def _claim_session(ctx: TaskContext, task_name: str) -> tuple[str | None, bool]:
    """Returns (session_id, resume). Reuses an existing UUID stored in ticket
    state so subsequent calls for this (ticket, task) chain into the same
    Claude CLI session and benefit from prior cache_creation. First call
    creates a fresh UUID and stores it; later calls return resume=True."""
    if not ctx.ticket_key:
        return None, False
    existing = state.load_ticket(ctx.ticket_key) or {}
    sessions = existing.get("llm_sessions") or {}
    sid = sessions.get(task_name)
    if sid:
        return sid, True
    sid = str(uuid.uuid4())
    def _set(t: dict) -> dict:
        sess = dict(t.get("llm_sessions") or {})
        sess[task_name] = sid
        t["llm_sessions"] = sess
        return t
    state.update_ticket(ctx.ticket_key, _set)
    return sid, False


def _drop_session(ctx: TaskContext, task_name: str) -> None:
    """Forget the stored session_id for (ticket, task) so the next call starts
    fresh. Use after a resume=True call fails — the on-disk session may have
    been cleaned up and continuing to --resume against it will error every time."""
    if not ctx.ticket_key:
        return
    def _del(t: dict) -> dict:
        sess = dict(t.get("llm_sessions") or {})
        sess.pop(task_name, None)
        t["llm_sessions"] = sess
        return t
    state.update_ticket(ctx.ticket_key, _del)


_NOISE_ONLY = {"Pipfile", "Pipfile.lock", "pnpm-lock.yaml", "yarn.lock",
               "package-lock.json", "uv.lock", "poetry.lock"}

_SCRATCH_DIRS = (".playwright-cli", "test-results")


def _clean_workspace_scratch(ticket_dir: Path) -> list[str]:
    """Remove ephemeral tool scratch (playwright artifacts, test output) that
    agents leave in worktrees. These are never source — left in place they
    block mark_ready's clean-worktree gate or get swept into the PR."""
    workspace = ticket_dir / "workspace"
    search_root = workspace if workspace.is_dir() else ticket_dir
    removed: list[str] = []
    for child in sorted(search_root.iterdir()):
        if not (child.is_dir() and (child / ".git").exists()):
            continue
        result = subprocess.run(
            ["git", "clean", "-fd", "--", *_SCRATCH_DIRS],
            cwd=child, capture_output=True, text=True,
        )
        for line in result.stdout.splitlines():
            if line.startswith("Removing "):
                removed.append(f"{child.name}/{line[len('Removing '):]}")
    return removed


def _dirty_workspace_repos(ticket_dir: Path) -> list[str]:
    """Return names of workspace repos that have meaningful uncommitted changes
    (excluding Pipfile / Pipfile.lock noise)."""
    workspace = ticket_dir / "workspace"
    search_root = workspace if workspace.is_dir() else ticket_dir
    dirty: list[str] = []
    for child in sorted(search_root.iterdir()):
        if not (child.is_dir() and (child / ".git").exists()):
            continue
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=child, capture_output=True, text=True,
        )
        lines = [l for l in status.stdout.splitlines() if l.strip()]
        meaningful = [
            l for l in lines
            if Path(l[3:].split(" -> ")[-1]).name not in _NOISE_ONLY
        ]
        if meaningful:
            dirty.append(child.name)
    return dirty


def _commit_workspace_changes(ticket_dir: Path, ticket_key: str,
                              message: str | None = None) -> list[str]:
    """Commit meaningful worktree changes left uncommitted by a Claude run.

    Skips any repo whose only dirty files are Pipfile / Pipfile.lock — those
    appear when pipenv resolves deps and carry no intentional code change.
    Returns the list of repo names that were actually committed.

    `message` defaults to the historical tri-review wording for backward-
    compat with existing callers; new callers should pass an explicit message
    that names the step (e.g. 'test: scaffold tests' / 'fix: failing tests')."""
    candidates: list[Path] = []
    workspace = ticket_dir / "workspace"
    search_root = workspace if workspace.is_dir() else ticket_dir
    for child in sorted(search_root.iterdir()):
        if child.is_dir() and (child / ".git").exists():
            candidates.append(child)

    commit_msg = message or f"fix: address tri-review findings for {ticket_key}"

    committed: list[str] = []
    for repo_dir in candidates:
        status = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=repo_dir, capture_output=True, text=True,
        )
        lines = [l for l in status.stdout.splitlines() if l.strip()]
        meaningful = [
            l for l in lines
            if Path(l[3:].split(" -> ")[-1]).name not in _NOISE_ONLY
        ]
        if not meaningful:
            continue
        subprocess.run(["git", "add", "-A"], cwd=repo_dir, check=True)
        from core.git_util import commit_with_hooks
        commit_with_hooks(repo_dir, message=commit_msg, check=True, timeout=900)
        committed.append(repo_dir.name)
    return committed


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


@task("rank_new_tickets", preconditions=[feature_enabled("tickets")], timeout=90)
def rank_new_tickets(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    try:
        result = tix._rank_new_tickets(ctx.instance_key)
        return TaskResult("ok", artifacts=result)
    except Exception as e:
        log.emit("rank_new_tickets_error", f"[{ctx.instance_key}] {type(e).__name__}: {e}",
                 meta={"error": type(e).__name__})
        return TaskResult("failed", f"{type(e).__name__}: {e}")


@task("setup_prd_ticket",
      preconditions=[status_is("new", "planning"), repo_gate_clear],
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
    with tix._gate_lock_for(ctx.instance_key):
        blocker = tix._repo_gate_blocked(ctx.instance_key, ctx.ticket_key, ctx.config)
        if blocker:
            return TaskResult("skipped", f"repo busy with {blocker}")
        try:
            updated = tix.materialize_prd_ticket(
                ctx.config, ctx.ticket_key, ts, base_url,
                instance_key=ctx.instance_key,
            )
        except RuntimeError as e:
            return TaskResult("failed", str(e))
        state.save_ticket(ctx.ticket_key, updated)
    q.enqueue_job(ctx.instance_key, "address_pm_findings", ticket_key=ctx.ticket_key)
    q.enqueue_job(ctx.instance_key, "start_planning", ticket_key=ctx.ticket_key)
    return TaskResult("ok", artifacts={"slug": updated.get("slug"),
                                        "branch": updated.get("branch")})


@task("start_planning",
      preconditions=[status_is("new", "planning"), repo_gate_clear],
      postconditions=[file_exists("docs/change-manifest.md")],
      on_success_status="reviewing",
      timeout=PLAN_TIMEOUT)
def start_planning(ctx: TaskContext) -> TaskResult:
    from features import acceptance
    from features import tickets as tix
    with tix._gate_lock_for(ctx.instance_key):
        blocker = tix._repo_gate_blocked(ctx.instance_key, ctx.ticket_key or "", ctx.config)
        if blocker:
            return TaskResult("skipped", f"repo busy with {blocker}")
        try:
            state.transition_ticket(ctx.ticket_key or "", "planning")
        except state.TicketStateError as e:
            return TaskResult("failed", f"transition to planning: {e}")
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    # Refresh every per-repo worktree onto the latest origin/<base_branch>
    # before Claude starts editing. Without this, a ticket whose worktree was
    # created earlier (e.g., concurrent PRD intake) ends up with a stale base
    # — when it later opens a PR, the auto-merge step hits a conflict against
    # whatever sibling tickets merged in between. The per-repo gate
    # serialises planning/reviewing/testing/proving but the worktree itself
    # only gets seeded once at setup time; refresh here closes that gap.
    if ctx.ticket_key:
        ts = state.load_ticket(ctx.ticket_key) or {}
        slug = ts.get("slug", "")
        if slug:
            ws = ctx.config["workspace"]
            base_branch = ws.get("base_branch", "main")
            for repo in get_repos(ctx.config):
                wt = ticket_worktree_path(ctx.config, slug, repo["name"])
                if not wt.is_dir():
                    continue
                subprocess.run(["git", "fetch", "origin", base_branch],
                               cwd=str(wt), capture_output=True, timeout=60)
                subprocess.run(["git", "reset", "--hard", f"origin/{base_branch}"],
                               cwd=str(wt), capture_output=True, timeout=60)
                subprocess.run(["git", "clean", "-fd"],
                               cwd=str(wt), capture_output=True, timeout=60)
                relink_shared_venv(ctx.config, repo["name"], wt)
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
    ts = state.load_ticket(ctx.ticket_key or "") or {}
    slug = ts.get("slug") or (ctx.ticket_key or "")
    log.emit("ticket_planning_started", f"Consensus planning for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    if not (ticket_dir / "docs" / "technical-plan.md").exists():
        ok, reason = run_consensus_plan(ctx.config, ticket_dir, slug,
                                        ticket_key=ctx.ticket_key or "")
        if not ok:
            return TaskResult("failed", reason)
    else:
        log.emit("ticket_planning_skipped_ctp",
                 f"technical-plan.md exists, skipping consensus plan for {ctx.ticket_key}",
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

    fix_prompt = (
        "Read docs/tri-review.md and identify the blocking findings (those flagged as "
        "blocking, not suggestions). Fix each one in the workspace. Run any tests directly "
        "relevant to the changed files. Do NOT modify docs/tri-review.md — leave the verdict "
        "section untouched; a separate verifier will assess your work."
    )
    log.emit("ticket_review_fixing", f"Apply fix for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    sid, resume = _claim_session(ctx, "fix_review_findings")
    fix_result = run_claude_code(fix_prompt, cwd=ticket_dir, timeout=FIX_TIMEOUT,
                                 session_id=sid, resume=resume)
    if fix_result is None:
        if resume:
            _drop_session(ctx, "fix_review_findings")
        return TaskResult("failed", "fix step: claude returned non-zero or empty")

    verify_prompt = (
        "Independent verification step. Read docs/tri-review.md to see the original blocking "
        "findings. Then run `git diff` (and `git diff --staged` if relevant) to see the changes "
        "that were just made. For each blocking finding from the original review, decide whether "
        "the diff actually addresses it — be skeptical, this is a fresh review, not a confirmation "
        "of a prior judgement.\n\n"
        "Update the Verdict section of docs/tri-review.md (replace whatever is there) with exactly "
        "one of:\n"
        "  VERDICT: PASS    — every blocking finding is genuinely addressed by the diff\n"
        "  VERDICT: FAIL    — one or more blocking findings remain; list each unfixed finding as a "
        "bullet citing file:line and what's missing\n\n"
        "Do NOT re-run /tri-review and do NOT spawn persona sub-agents. Do NOT make any code edits. "
        "This is verification only."
    )
    log.emit("ticket_review_verifying", f"Fresh verify for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    verify_result = run_claude_code(verify_prompt, cwd=ticket_dir, timeout=REVIEW_TIMEOUT)
    if verify_result is None:
        return TaskResult("failed", "verify step: claude returned non-zero or empty")
    committed = _commit_workspace_changes(ticket_dir, ctx.ticket_key or "")
    if committed:
        log.emit("ticket_review_committed",
                 f"Committed fix changes for {ctx.ticket_key}: {', '.join(committed)}",
                 meta={"ticket": ctx.ticket_key, "repos": committed})
    return TaskResult("ok")


@task("fix_ci_failures",
      preconditions=[status_is("in_review"),
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
            elif kind in ("fixed", "fix_failed", "haiku_empty", "haiku_parse_error"):
                # Every outcome that burned a real fix cycle counts toward the
                # cap — not just successful sends. Otherwise a CI failure the
                # auto-fixer can't resolve (fix_failed/haiku_*) loops forever,
                # never increments ci_fix_attempts, never trips
                # MAX_CI_FIX_ATTEMPTS in _handle_ci_failure, and wedges the
                # repo gate indefinitely. 'unrelated' and 'no_failing' don't
                # count (nothing of ours was attempted).
                def _bump(current):
                    if not current:
                        return None
                    current["ci_fix_attempts"] = current.get("ci_fix_attempts", 0) + 1
                    current.pop("ci_passed", None)
                    current.pop("checks_started_at", None)
                    return current
                updated = state.update_ticket(ctx.ticket_key or "", _bump) or {}
                ts = updated
                if kind == "fixed":
                    log.emit("ticket_ci_fix_sent",
                             f"Sent CI fix to {slug or ctx.ticket_key} (attempt "
                             f"{updated.get('ci_fix_attempts', 0)}): {outcome.get('fix_hint','')[:80]}",
                             links=pr_link,
                             meta={**meta, "fix_hint": outcome.get("fix_hint", "")})
                else:
                    log.emit("ticket_ci_fix_attempt_failed",
                             f"CI fix attempt for {slug or ctx.ticket_key} did not produce a fix "
                             f"({kind}, attempt {updated.get('ci_fix_attempts', 0)}/{MAX_CI_FIX_ATTEMPTS})",
                             links=pr_link, meta={**meta, "kind": kind})
            # no_failing / capped / unrelated / worktree_missing: don't count toward cap

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
        f"1. Read docs/ticket.md (and docs/comments.md if present, for upstream reviewer/QA context) for ticket context.\n"
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


@task("enter_testing",
      preconditions=[status_is("reviewing"),
                     file_contains("docs/tri-review.md", r"VERDICT:\s*PASS")],
      on_success_status="testing",
      timeout=15)
def enter_testing(ctx: TaskContext) -> TaskResult:
    """Pure state transition reviewing → testing. TESTING.md is read later
    by plan_tests as a hint, not as a gate — every ticket that passes
    tri-review goes through the testing state."""
    ticket_dir = _ticket_dir(ctx)
    dirty = _dirty_workspace_repos(ticket_dir)
    if dirty:
        return TaskResult("failed",
                          f"worktree has uncommitted changes in: {', '.join(dirty)}")
    return TaskResult("ok")


@task("plan_tests",
      preconditions=[status_is("testing"),
                     file_exists("docs/change-manifest.md")],
      postconditions=[file_exists("docs/test-plan.md")],
      timeout=TEST_PLAN_TIMEOUT)
def plan_tests(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    workspace_root = Path(ctx.config["workspace"]["root"])
    testing_md_path = workspace_root / "TESTING.md"
    prompt = _PLAN_TESTS_PROMPT_TEMPLATE.format(
        testing_md_section=_render_testing_md_section(testing_md_path)
    )
    log.emit("ticket_test_planning_started",
             f"Headless plan_tests for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key,
                   "has_testing_guide": _testing_md_is_substantive(testing_md_path)})
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=TEST_PLAN_TIMEOUT)
    if result is None:
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


def _render_testing_md_section(path: Path) -> str:
    """Build the prompt's TESTING.md block. When the guide is substantive,
    inline its content as authoritative. When it's missing or trivial, tell
    claude to investigate the codebase itself."""
    if _testing_md_is_substantive(path):
        try:
            content = path.read_text()
        except OSError:
            content = ""
        return (
            "PROJECT TESTING GUIDE (from workspace.root/TESTING.md):\n"
            f"{content}\n"
            "END OF TESTING GUIDE\n\n"
            "Treat the testing guide above as authoritative for: which "
            "frameworks to use, where tests live, what fixtures already "
            "exist, what patterns are established. Prefer the guide over "
            "general best practices when they conflict."
        )
    return (
        "NO PROJECT TESTING GUIDE PROVIDED.\n\n"
        "Investigate the workspace yourself before planning. For each "
        "workspace/<repo>/ subdirectory:\n"
        "- detect the language and test framework from repo-root files "
        "(package.json devDependencies, pyproject.toml/pytest.ini, "
        "go.mod, Cargo.toml)\n"
        "- grep existing test files to learn the directory layout, "
        "naming conventions, and which fixtures/factories already exist\n"
        "- reuse those existing helpers rather than inventing parallel ones\n"
        "Plan tests in whatever framework you find."
    )


@task("write_tests",
      preconditions=[status_is("testing"),
                     file_exists("docs/test-plan.md")],
      postconditions=[file_exists("docs/test-files-written.txt")],
      timeout=TEST_WRITE_TIMEOUT)
def write_tests(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    log.emit("ticket_test_writing_started",
             f"Headless write_tests for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    sid, resume = _claim_session(ctx, "write_tests")
    result = run_claude_code(_WRITE_TESTS_PROMPT, cwd=ticket_dir,
                             timeout=TEST_WRITE_TIMEOUT,
                             session_id=sid, resume=resume)
    if result is None:
        if resume:
            _drop_session(ctx, "write_tests")
        return TaskResult("failed", "claude returned non-zero or empty")
    _commit_workspace_changes(ticket_dir, ctx.ticket_key or "",
                              message=f"test: scaffold tests for {ctx.ticket_key}")
    return TaskResult("ok")


@task("run_tests_and_fix",
      preconditions=[status_is("testing"),
                     file_exists("docs/test-plan.md")],
      postconditions=[file_contains("docs/test-runs.md", r"VERDICT:\s*(PASS|FAIL)")],
      timeout=TEST_RUN_TIMEOUT)
def run_tests_and_fix(ctx: TaskContext) -> TaskResult:
    """Run tests once per task invocation. If FAIL and cap not reached, ask
    claude to fix and let the dispatcher re-enqueue us next cycle. Each
    invocation writes a fresh VERDICT into docs/test-runs.md."""
    from features.tickets import MAX_TEST_FIX_ATTEMPTS
    ts = state.load_ticket(ctx.ticket_key or "") or {}
    ticket_dir = _ticket_dir(ctx)
    workspace = ticket_dir / "workspace"
    if not workspace.is_dir():
        workspace = ticket_dir

    attempt = int(ts.get("test_fix_attempts", 0)) + 1
    base_branch = ctx.config["workspace"].get("base_branch", "main")

    per_repo: list[dict] = []
    for repo_dir in sorted(p for p in workspace.iterdir() if p.is_dir()):
        if not (repo_dir / ".git").exists():
            continue
        if not _repo_has_changes_vs_base(repo_dir, base_branch):
            per_repo.append({"repo": repo_dir.name, "result": "skipped",
                             "tail": f"no changes vs origin/{base_branch} — "
                             f"out of scope for this ticket"})
            continue
        runner = _detect_runner(repo_dir)
        if runner is None:
            per_repo.append({"repo": repo_dir.name, "result": "no_runner"})
            log.emit("test_runner_not_detected",
                     f"{ctx.ticket_key}/{repo_dir.name}: no runner detected",
                     meta={"ticket": ctx.ticket_key, "repo": repo_dir.name})
            continue
        cmd, env = runner
        if cmd and cmd[0] == _NO_LOCAL_PY_VENV_SENTINEL:
            msg = (f"{ctx.ticket_key}/{repo_dir.name}: Python tests detected "
                   f"but no local virtualenv solution found. Expected one of: "
                   f".venv/bin/pytest, Pipfile, uv.lock, poetry.lock. Refusing "
                   f"to invoke system pytest (deps almost certainly missing). "
                   f"Install dependencies into a local .venv before retrying.")
            log.emit("test_runner_no_local_venv", msg,
                     meta={"ticket": ctx.ticket_key, "repo": repo_dir.name,
                           "repo_dir": str(repo_dir)})
            per_repo.append({"repo": repo_dir.name, "result": "fail",
                             "exit_code": -1, "cmd": "(no local venv)",
                             "tail": msg})
            continue
        outcome = _run_repo_tests(repo_dir, cmd, env,
                                  timeout=TEST_RUN_TIMEOUT // 3)
        outcome["repo"] = repo_dir.name
        outcome["cmd"] = " ".join(cmd)
        per_repo.append(outcome)

    # Permissive verdict: PASS if nothing failed. Repos with no detectable
    # runner don't count toward FAIL (otherwise testless workspaces would
    # never make it to pr_ready). Repos with no changes vs base ("skipped")
    # don't count either — they're out of scope for this ticket. Pass
    # through with a log event.
    failures = [o for o in per_repo
                if o["result"] not in ("pass", "no_runner", "skipped")]
    overall_pass = not failures
    verdict = "PASS" if overall_pass else "FAIL"

    _write_test_runs(ticket_dir, attempt, per_repo, verdict)

    if overall_pass:
        return TaskResult("ok", artifacts={"attempt": attempt, "verdict": "PASS"})

    state.update_ticket(ctx.ticket_key or "",
                        lambda t: {**t, "test_fix_attempts": attempt} if t else t)

    if attempt >= MAX_TEST_FIX_ATTEMPTS:
        return TaskResult("ok", artifacts={"attempt": attempt, "verdict": "FAIL",
                                            "cap_reached": True})

    fix_prompt = _build_fix_prompt(per_repo)
    sid, resume = _claim_session(ctx, "run_tests_and_fix")
    fix_result = run_claude_code(fix_prompt, cwd=ticket_dir,
                                 timeout=TEST_RUN_TIMEOUT,
                                 session_id=sid, resume=resume)
    if fix_result is None:
        if resume:
            _drop_session(ctx, "run_tests_and_fix")
        return TaskResult("failed", "fix step: claude returned non-zero")

    _commit_workspace_changes(
        ticket_dir, ctx.ticket_key or "",
        message=f"fix: address failing tests for {ctx.ticket_key} (attempt {attempt})")
    return TaskResult("ok", artifacts={"attempt": attempt, "verdict": "FAIL",
                                        "fix_dispatched": True})


def _fire_ticket_dev_complete(ctx: TaskContext) -> None:
    import core.events as events
    ts = state.load_ticket(ctx.ticket_key or "") or {}
    events.dispatch("ticket_dev_complete", {
        "ticket_key": ctx.ticket_key,
        "estimate_seconds": ts.get("estimate_seconds", 0),
        "discovered_at": ts.get("discovered_at", ""),
        "slug": ts.get("slug", ""),
        "branch": ts.get("branch", ""),
    }, ctx.config)


def _enter_proving_target(ctx: TaskContext, result: TaskResult) -> str:
    """Callable on_success_status for enter_proving: route to `proving` when
    a PROOF.md exists at workspace.root, otherwise skip straight to
    `pr_ready`. The body fires ticket_dev_complete only on the skip path so
    the event is dispatched exactly once per ticket either way."""
    return "proving" if result.artifacts.get("has_proof_md") else "pr_ready"


@task("enter_proving",
      preconditions=[status_is("testing"),
                     file_contains("docs/test-runs.md", r"VERDICT:\s*PASS")],
      on_success_status=_enter_proving_target,
      timeout=15)
def enter_proving(ctx: TaskContext) -> TaskResult:
    """Bridge from testing to either proving (PROOF.md present) or pr_ready
    (PROOF.md absent — skip the proof step). On the skip path, fires
    ticket_dev_complete inline so downstream listeners (schedule_pr) trigger
    immediately, matching the no-proof-needed flow."""
    ticket_dir = _ticket_dir(ctx)
    dirty = _dirty_workspace_repos(ticket_dir)
    if dirty:
        return TaskResult("failed",
                          f"worktree has uncommitted changes in: {', '.join(dirty)}")
    workspace_root = Path(ctx.config["workspace"]["root"])
    proof_md = workspace_root / "PROOF.md"
    has_proof = proof_md.is_file() and len(proof_md.read_text().strip()) > 0
    if not has_proof:
        _fire_ticket_dev_complete(ctx)
        log.emit("ticket_proof_skipped",
                 f"{ctx.ticket_key}: no PROOF.md at {workspace_root}; "
                 f"skipping proof state",
                 meta={"ticket": ctx.ticket_key,
                       "proof_md_path": str(proof_md),
                       "exists": proof_md.exists()})
        return TaskResult("ok", artifacts={"has_proof_md": False})
    return TaskResult("ok", artifacts={"has_proof_md": True})


@task("prove",
      preconditions=[status_is("proving")],
      postconditions=[file_exists("docs/proof.md")],
      timeout=PROOF_TIMEOUT)
def prove(ctx: TaskContext) -> TaskResult:
    ticket_dir = _ticket_dir(ctx)
    if not ticket_dir.is_dir():
        return TaskResult("failed", f"ticket dir missing: {ticket_dir}")
    workspace_root = Path(ctx.config["workspace"]["root"])
    proof_md_path = workspace_root / "PROOF.md"
    try:
        proof_md = proof_md_path.read_text() if proof_md_path.exists() else ""
    except OSError:
        proof_md = ""
    if not proof_md.strip():
        # Defensive: if PROOF.md vanished between enter_proving and prove,
        # write a NOT_APPLICABLE proof.md and exit ok rather than spinning.
        (ticket_dir / "docs" / "proof.md").write_text(
            "PROOF: NOT_APPLICABLE\n\nPROOF.md disappeared between gate "
            "evaluation and the prove step.\n"
        )
        return TaskResult("ok", artifacts={"skipped": True})
    prompt = _PROVE_PROMPT_TEMPLATE.format(proof_md=proof_md)
    log.emit("ticket_prove_started",
             f"Headless prove for {ctx.ticket_key}",
             meta={"ticket": ctx.ticket_key})
    sid, resume = _claim_session(ctx, "prove")
    result = run_claude_code(prompt, cwd=ticket_dir, timeout=PROOF_TIMEOUT,
                             session_id=sid, resume=resume)
    if result is None:
        if resume:
            _drop_session(ctx, "prove")
        return TaskResult("failed", "claude returned non-zero or empty")
    return TaskResult("ok")


@task("mark_ready",
      preconditions=[status_is("proving"),
                     file_exists("docs/proof.md")],
      on_success_status="pr_ready",
      timeout=900)
def mark_ready(ctx: TaskContext) -> TaskResult:
    """Final gate: proving → pr_ready after proof is on disk. Fires
    ticket_dev_complete. The skip-path version of the event (when there is
    no PROOF.md) fires inside enter_proving instead.

    Self-heals the worktree first so the gate reflects real work, not tool
    debris: strips ephemeral scratch (playwright artifacts, test output) and
    commits any real changes a prior stage left behind. Only genuinely
    uncommitted source (after that) blocks."""
    ticket_dir = _ticket_dir(ctx)
    _clean_workspace_scratch(ticket_dir)
    _commit_workspace_changes(ticket_dir, ctx.ticket_key or "",
                              message=f"chore: finalize {ctx.ticket_key} worktree for PR")
    dirty = _dirty_workspace_repos(ticket_dir)
    if dirty:
        return TaskResult("failed",
                          f"worktree has uncommitted changes in: {', '.join(dirty)}")
    _fire_ticket_dev_complete(ctx)
    return TaskResult("ok")


_PR_DESCRIPTION_PROMPT = """You are writing a per-repo pull request description for ticket {ticket_key}.

CONTEXT — full change manifest covering ALL repos this ticket touches (high-level summary; some parts may not apply to THIS repo):

{manifest}

THIS REPO is `{repo_name}`. Below is the unified diff of ONLY this repo's changes (other repos' diffs are not shown — write only about what this diff does).

DIFF:

{diff}

Write a 3-5 sentence high-level description of what changed in `{repo_name}` specifically. Focus on:
- What user-visible behavior or developer-facing capability changed (or is enabled by this repo's piece of the ticket)
- The general shape of the change (new endpoint, schema migration, refactor, config flip, etc.)
- Why this repo needed to change as part of the larger ticket — the integration role it plays

Do NOT:
- Walk through individual files or line numbers
- Repeat verbatim what other repos in this ticket are doing
- Use marketing language; be technical, direct, in plain prose
- Exceed 5 sentences

Output JSON only, no markdown fence:
{{"title": "{ticket_key}: <one-line summary specific to this repo's changes>", "description": "<3-5 sentence markdown description>"}}
"""


PR_DESCRIPTION_TIMEOUT = 900


@task("generate_pr_descriptions",
      preconditions=[status_is("pr_ready"),
                     file_exists("docs/change-manifest.md")],
      timeout=PR_DESCRIPTION_TIMEOUT)
def generate_pr_descriptions(ctx: TaskContext) -> TaskResult:
    """Generate per-repo PR titles + descriptions and stash on the ticket
    state under `pr_descriptions[<repo>] = {title, description, generated_at}`.

    Drives the modal at /api/tickets/{key}/pr-info AND the headless
    create_pr path in features.tickets:_create_pr, so manual and auto PRs
    both see the same per-repo prose. Skips repos whose worktree has no
    meaningful diff against the base branch. Re-runs are idempotent — only
    missing repos are regenerated, so partial state from a crash recovers
    cleanly."""
    ts = state.load_ticket(ctx.ticket_key or "") or {}
    slug = ts.get("slug", "")
    if not slug:
        return TaskResult("failed", "no slug")
    ws = ctx.config["workspace"]
    base_branch = ws.get("base_branch", "main")
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    manifest_path = ticket_dir / "docs" / "change-manifest.md"
    manifest = ""
    if manifest_path.exists():
        try:
            manifest = manifest_path.read_text()[:6000]
        except OSError:
            manifest = ""

    existing = dict(ts.get("pr_descriptions") or {})
    generated: list[str] = []
    skipped_empty: list[str] = []
    skipped_no_worktree: list[str] = []
    failed: list[str] = []

    for repo in get_repos(ctx.config):
        name = repo["name"]
        if name in existing:
            continue
        wt = ticket_worktree_path(ctx.config, slug, name)
        if not wt.is_dir():
            skipped_no_worktree.append(name)
            continue
        # Fetch base once so the diff and file count are against current origin,
        # not whatever the worktree was last seeded with.
        subprocess.run(["git", "fetch", "origin", base_branch],
                       cwd=str(wt), capture_output=True, timeout=60)
        diff_out = subprocess.run(
            ["git", "diff", f"origin/{base_branch}...HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=30,
        )
        diff_text = diff_out.stdout
        if not diff_text.strip():
            skipped_empty.append(name)
            continue
        files_changed_count = sum(
            1 for line in diff_text.splitlines() if line.startswith("diff --git")
        )
        current_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=10,
        ).stdout.strip() or ts.get("branch", "")
        if len(diff_text) > 12000:
            diff_text = diff_text[:12000] + "\n[diff truncated]"

        prompt = _PR_DESCRIPTION_PROMPT.format(
            ticket_key=ctx.ticket_key, repo_name=name,
            manifest=manifest or "(no change-manifest available)",
            diff=diff_text,
        )
        raw = run_haiku(prompt)
        parsed = extract_json(raw) if raw else None
        if not parsed or not isinstance(parsed, dict):
            failed.append(name)
            log.emit("pr_description_parse_failed",
                     f"{ctx.ticket_key}/{name}: could not parse haiku output",
                     meta={"ticket": ctx.ticket_key, "repo": name,
                           "raw": (raw or "")[:500]})
            continue

        title = parsed.get("title") or f"{ctx.ticket_key}: changes in {name}"
        description = parsed.get("description", "").strip()
        if not description:
            failed.append(name)
            continue
        existing[name] = {
            "title": title,
            "description": description,
            "branch": current_branch,
            "files_changed": files_changed_count,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        generated.append(name)

    if generated or failed:
        def _set(t):
            if t is None:
                return None
            t["pr_descriptions"] = existing
            return t
        state.update_ticket(ctx.ticket_key or "", _set)
        log.emit("ticket_pr_descriptions_generated",
                 f"{ctx.ticket_key}: generated={len(generated)} "
                 f"skipped_empty={len(skipped_empty)} failed={len(failed)}",
                 meta={"ticket": ctx.ticket_key,
                       "generated": generated,
                       "skipped_empty": skipped_empty,
                       "skipped_no_worktree": skipped_no_worktree,
                       "failed": failed})
    if failed and not generated:
        return TaskResult("failed", f"all repos failed: {failed}")
    return TaskResult("ok", artifacts={"generated": generated, "failed": failed})


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


CONFLICT_RESOLVE_TIMEOUT = 1200


@task("resolve_conflicts",
      preconditions=[status_is("in_review")],
      timeout=CONFLICT_RESOLVE_TIMEOUT)
def resolve_conflicts(ctx: TaskContext) -> TaskResult:
    from features import tickets as tix
    if not ctx.ticket_key:
        return TaskResult("failed", "ticket_key missing")
    ts = state.load_ticket(ctx.ticket_key)
    if ts is None:
        return TaskResult("failed", "ticket not found")
    ticket = {"key": ctx.ticket_key, "summary": ts.get("summary", ""),
              "description": ts.get("description", ""), "url": ts.get("url", "")}
    base_url = ctx.config.get("_base_url", "")
    prior_status = ts.get("status")
    prior_attempts = ts.get("conflict_resolution_attempts", 0)
    try:
        updated = tix._resolve_conflicts(ctx.config, ticket, ts, base_url)
    except Exception as e:
        log.emit("resolve_conflicts_error",
                 f"[{ctx.instance_key}] {ctx.ticket_key}: {type(e).__name__}: {e}",
                 meta={"ticket": ctx.ticket_key})
        return TaskResult("failed", f"{type(e).__name__}: {e}")
    state.save_ticket(ctx.ticket_key, updated)
    return TaskResult("ok", artifacts={
        "transitioned_to": updated.get("status"),
        "status_changed": updated.get("status") != prior_status,
        "attempts": updated.get("conflict_resolution_attempts", 0),
        "attempts_delta": updated.get("conflict_resolution_attempts", 0) - prior_attempts,
    })


@task("apply_note_reset",
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
        notes_dir = docs / "notes"
        notes_dir.mkdir(parents=True, exist_ok=True)
        (notes_dir / f"note-{now.replace(':', '-')}.md").write_text(f"# Note ({now})\n\n{note}\n")
    archive = docs / "archive" / now.replace(":", "-")
    moved = []
    for fname in ("change-manifest.md", "tri-review.md", "technical-plan.md",
                  "test-plan.md", "test-files-written.txt", "test-runs.md"):
        src = docs / fname
        if src.exists():
            archive.mkdir(parents=True, exist_ok=True)
            shutil.move(str(src), str(archive / fname))
            moved.append(fname)
    if moved:
        archived_to = str(archive)

    state.reset_ticket(ctx.ticket_key or "", target="new",
                       reason="note added; re-plan from scratch")
    return TaskResult("ok", artifacts={"archived_to": archived_to, "moved": moved})


@task("set_state",
      timeout=15)
def set_state(ctx: TaskContext) -> TaskResult:
    target = ctx.payload.get("target", "")
    if not target:
        return TaskResult("failed", "target state missing")
    fields: dict = {}
    if target == "done":
        fields["done_at"] = datetime.now(timezone.utc).isoformat()
    try:
        state.reset_ticket(ctx.ticket_key or "", target=target,
                           reason="manual set-state", **fields)
    except state.TicketStateError as e:
        return TaskResult("failed", str(e))
    return TaskResult("ok", artifacts={"transitioned_to": target})


@task("advance_ticket", timeout=30)
def advance_ticket(ctx: TaskContext) -> TaskResult:
    if not ctx.ticket_key:
        return TaskResult("ok", artifacts={"skipped": "no ticket_key"})
    from features import tickets as tix
    tix.advance_ticket(ctx.config, ctx.instance_key, ctx.ticket_key)
    return TaskResult("ok", artifacts={"advanced": ctx.ticket_key})


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
