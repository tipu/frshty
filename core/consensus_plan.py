"""Deterministic multi-model planning orchestrator (the Python replacement for
the `/ctp` markdown command).

frshty owns the mechanical orchestration here — baseline capture, a
byte-identical path-only prompt, the parallel fan-out to codex/agy/claude
with correct flags, per-model output sanity-gating, and assembling the change
manifest from a real git diff. Those are exactly the steps that failed silently
when a probabilistic Claude instance interpreted the markdown (PATH errors,
rate-limit banners stored as the plan, missing agy flags, unchecked
captures).

A headless Claude instance is invoked only for the judgment steps that code
cannot do: synthesizing the validated plans into docs/technical-plan.md while
implementing them into the worktrees, and writing the prose change manifest
plus the interactive change-explainer page grounded in the real diff.

The whole flow must fit inside the caller's task timeout (start_planning's
PLAN_TIMEOUT), which the worker enforces with a hard SIGKILL — so the fan-out
runs in parallel and each phase carries its own sub-budget.
"""
import concurrent.futures
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import core.log as log
from core.config import get_repos, ticket_worktree_path
from core.llm import (
    run_thinking,
    run_external_model,
    validate_model_output,
)


FANOUT_TIMEOUT = 600
BUILD_TIMEOUT = 900
MANIFEST_TIMEOUT = 240
EXPLAINER_TIMEOUT = 1200
MIN_TECHNICAL_PLAN_BYTES = 400


PLAN_DIRECTIVE = """You are producing a technical implementation plan for a ticket. Explore independently — do not rely on any summary; open the source files yourself.

OUTPUT CONTRACT (this overrides anything else):
- Output your technical plan to stdout as Markdown. Do NOT write, create, or edit any file.
- Do NOT implement code, run tests, or modify anything in the repository worktrees.
- Cite file:line evidence for every claim about existing code.

You are responsible for:
- Reading the requirement files under docs/ (e.g. docs/ticket.md, docs/comments.md for upstream discussion and reviewer/QA context that often explains why a ticket bounced back, docs/technical-plan.md if present, docs/pm-findings.md if present, and any docs/notes/*.md human guidance that overrides or refines the ticket).
- Exploring the repository worktree(s) in this directory yourself with grep/read/find.

Produce a plan with these sections:
1. Requirements summary and acceptance criteria.
2. Current state — endpoints/files involved, with file:line references.
3. Proposed solution — data flow, key logic, updated contracts/types.
4. Files to modify — a table of file, change type, description.
5. Test plan — unit, integration, and playwright/e2e tests per acceptance criterion.
6. Risks and tradeoffs.
"""


def _run_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")


def _capture_baselines(config: dict, slug: str) -> dict[str, tuple[Path, str]]:
    """Return {repo_name: (worktree_path, baseline_sha)} for each worktree that
    exists. A repo whose HEAD can't be read is skipped."""
    baselines: dict[str, tuple[Path, str]] = {}
    for repo in get_repos(config):
        wt = ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        rev = subprocess.run(["git", "-C", str(wt), "rev-parse", "HEAD"],
                             capture_output=True, text=True, timeout=30)
        if rev.returncode == 0 and rev.stdout.strip():
            baselines[repo["name"]] = (wt, rev.stdout.strip())
    return baselines


def _fan_out(prompt: str, ticket_dir: Path, run_dir: Path,
             include_dirs: list[str], timeout: int) -> dict[str, dict]:
    """Run the same prompt through codex, agy, and claude in parallel.
    Returns {model: {"text", "valid", "reason"}}."""

    def _claude() -> tuple[str | None, int | None]:
        text = run_thinking(prompt, cwd=ticket_dir, timeout=timeout)
        return text, (0 if text is not None else 1)

    def _codex() -> tuple[str | None, int | None]:
        last = run_dir / "codex-plan.md"
        return run_external_model(
            ["codex", "exec", "--skip-git-repo-check", "-o", str(last), prompt],
            fn_name="ctp_codex", model="codex", prompt=prompt,
            cwd=ticket_dir, timeout=timeout, last_message_file=last,
            transcript_file=run_dir / "codex-transcript.txt",
        )

    def _agy() -> tuple[str | None, int | None]:
        cmd = ["agy", "--dangerously-skip-permissions"]
        for d in include_dirs:
            cmd += ["--add-dir", d]
        cmd += ["-p", prompt]
        return run_external_model(
            cmd,
            fn_name="ctp_agy", model="agy", prompt=prompt,
            cwd=ticket_dir, timeout=timeout,
            transcript_file=run_dir / "agy-transcript.txt",
        )

    runners = {"claude": _claude, "codex": _codex, "agy": _agy}
    results: dict[str, dict] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(fn): name for name, fn in runners.items()}
        for fut in concurrent.futures.as_completed(futures):
            name = futures[fut]
            try:
                text, code = fut.result()
            except Exception as e:
                results[name] = {"text": None, "valid": False,
                                 "reason": f"{type(e).__name__}: {e}"}
                continue
            ok, reason = validate_model_output(text, exit_code=code)
            if ok:
                (run_dir / f"{name}-plan.md").write_text(text or "")
            results[name] = {"text": text, "valid": ok, "reason": reason}
    return results


def _synthesize_and_implement(ticket_dir: Path, plan_files: list[Path],
                              timeout: int) -> bool:
    listing = "\n".join(f"- {p}" for p in plan_files)
    prompt = (
        "You are the orchestrator producing the final technical plan and "
        "implementing it for this ticket.\n\n"
        f"You have {len(plan_files)} independent technical plans saved at:\n"
        f"{listing}\n\n"
        "Step 1 — Synthesize: Read all of those plan files plus the "
        "requirements under docs/ (including docs/comments.md for upstream "
        "reviewer/QA discussion, and any docs/notes/*.md human "
        "guidance, which is authoritative and overrides the ticket where they "
        "conflict). Reconcile them into one consensus plan; "
        "where they disagree on architecture, data model, or API, pick the "
        "better-evidenced option and note why. Write the result to "
        "docs/technical-plan.md. It MUST include a 'Test Plan' section listing "
        "unit, integration, and playwright tests for each acceptance "
        "criterion; translate each structured acceptance criterion's "
        "playwright steps into a concrete e2e test. If docs/pm-findings.md "
        "exists, read it and address each concern in docs/technical-plan.md; "
        "if a concern is invalid, explain why.\n\n"
        "Step 2 — Implement: Implement the consensus plan. All source changes "
        "go inside the repository worktree subdirectories of this ticket "
        "directory. Follow each project's conventions. Commit incrementally by "
        "logical unit inside each repo worktree."
    )
    run_thinking(prompt, cwd=ticket_dir, timeout=timeout)
    tp = ticket_dir / "docs" / "technical-plan.md"
    try:
        return tp.is_file() and len(tp.read_text().strip()) >= MIN_TECHNICAL_PLAN_BYTES
    except OSError:
        return False


def _assemble_diff(baselines: dict[str, tuple[Path, str]],
                   run_dir: Path) -> tuple[Path | None, list[str]]:
    """Build the change manifest's source-of-truth diff from real git output.
    Prefers committed history (baseline..HEAD); falls back to the working tree
    when the implement step left changes uncommitted."""
    sections: list[str] = []
    changed: list[str] = []
    for name, (wt, sha) in baselines.items():
        committed = subprocess.run(
            ["git", "-C", str(wt), "diff", f"{sha}..HEAD"],
            capture_output=True, text=True, timeout=120)
        diff = committed.stdout if committed.returncode == 0 else ""
        if not diff.strip():
            working = subprocess.run(
                ["git", "-C", str(wt), "diff", sha],
                capture_output=True, text=True, timeout=120)
            diff = working.stdout if working.returncode == 0 else ""
        if diff.strip():
            changed.append(name)
            sections.append(f"## repo: {name}\n\n```diff\n{diff}\n```\n")
    if not sections:
        return None, []
    patch = run_dir / "manifest-diff.patch"
    patch.write_text("\n".join(sections))
    return patch, changed


def _write_manifest(ticket_dir: Path, patch: Path, timeout: int) -> bool:
    prompt = (
        "Write docs/change-manifest.md at technical-product-owner altitude, "
        f"grounded in the ACTUAL diff at {patch} and the plan at "
        "docs/technical-plan.md. Use these sections: Capability delivered, "
        "Release-note framing, Problem and approach, What changed by area, New "
        "surfaces, Changed surfaces, Integration obligations, Tradeoffs "
        "accepted, What could break, What tests prove. Derive 'what changed' "
        "from the diff, not from the plan."
    )
    run_thinking(prompt, cwd=ticket_dir, timeout=timeout)
    cm = ticket_dir / "docs" / "change-manifest.md"
    return cm.is_file()


EXPLAINER_DIRECTIVE = """Build docs/change-explainer.html: a single self-contained interactive HTML page (inline CSS/JS only, zero external dependencies, must work opened from file://) that explains the code changes in the ACTUAL diff at {patch}. Read docs/ticket.md plus any follow-up context files next to it (docs/comments.md, docs/notes/*.md if present) as the source of truth for the goal; every blurb and role description must tie back to them, naming the specific requirement or acceptance criterion served — never generic.

DATA GATHERING (do all of this before writing any HTML):
1. Parse the patch into per-file unified diffs.
2. In the repository worktree subdirectories of this ticket directory, grep for every key changed/added function to get its REAL file:line and full signature, plus its call sites (what it calls, what calls it).
3. Capture the FULL source of every key changed file — do not truncate.
4. Embed everything as JS consts: TITLE, STAGES, CATS, FILES, FUNCS, TREE, DIFFS (path -> unified diff text), SRC (path -> full source). Render the page from these consts with plain JS DOM-building functions — no framework.

THEME: GitHub-dark via CSS variables: --bg:#0d1117 --panel:#161b22 --line:#30363d --text:#e6edf3 --dim:#8b949e --accent:#58a6ff --green:#3fb950 --purple:#bc8cff --teal:#39c5cf --pink:#f778ba --amber:#d29922. Monospace for code, paths, and signatures.

PAGE LAYOUT, single scrolling page top-to-bottom (no top-level tabs):
1. Header — page title, ticket key, one-line stat strip (files changed, +adds/-dels, repos).
2. Goal panel — what the ticket asks for and why, with acceptance criteria; fold in comments.md follow-up context.
3. Call-flow overview — one bordered group per scenario (e.g. startup / user action / data flow), each an indented call tree built with two helpers: f(id, children) for clickable function nodes (colored chip per pipeline stage, name + file:line tag) and p(label, note, children) for gray non-clickable plain steps (IPC hops, library calls, timers).
4. Pipeline strip — the STAGES as numbered colored blocks (id, title, color, one-line description); clicking a stage spotlights the matching file cards.
5. Category filter chips with counts, then file cards grouped by category: each card shows filename, repo, new/modified dot, a bar sized by lines added, +N/-N counts, and a one-sentence why-it-matters blurb tied to the ticket. Clicking a card opens its diff modal.

THREE OVERLAY MODALS (Esc or click-outside closes the topmost):
A. Diff modal — path header, colored unified diff (green added / red removed / blue hunk headers), why-it-matters footer.
B. Function detail modal — name, stage-colored role badge, signature rendered as code, file:line, a 'how it contributes' paragraph naming the ticket requirement/criterion it serves plus any comments.md context, 'calls ->' and '<- called by' pills that navigate directly to other function modals, and a View code button.
C. Code viewer modal — the file's FULL source from SRC, line numbers, syntax highlighting via a hand-rolled regex highlighter (keyword/type/string/comment/number/function-name colors, per-language keyword sets for the languages present, stateful handling of block comments and template literals), auto-scrolled to the function's line with that line highlighted.

QUALITY BAR: every function in TREE must exist in FUNCS with a real file:line you verified by grep; calls/calledBy pills must navigate; DIFFS and SRC must be complete, not samples. The reference for this style scored 'made really well' — match it.

Write only docs/change-explainer.html. Build it incrementally in several smaller writes (initial skeleton, then append each data const and section separately) rather than one single giant write. Do not modify code in the repository worktrees."""


def _write_explainer(ticket_dir: Path, patch: Path, timeout: int) -> bool:
    prompt = EXPLAINER_DIRECTIVE.format(patch=patch)
    run_thinking(prompt, cwd=ticket_dir, timeout=timeout)
    return (ticket_dir / "docs" / "change-explainer.html").is_file()


def run_consensus_plan(config: dict, ticket_dir: Path, slug: str, *,
                       ticket_key: str = "") -> tuple[bool, str]:
    """Plan via validated multi-model consensus, implement, and manifest.
    Returns (ok, reason). Leaves the scratch dir intact on failure for
    inspection; removes it on success."""
    run_dir = ticket_dir / ".tmp" / f"ctp-{_run_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (ticket_dir / "docs").mkdir(parents=True, exist_ok=True)

    baselines = _capture_baselines(config, slug)
    if not baselines:
        return False, "no repo worktrees found to plan against"

    prompt = PLAN_DIRECTIVE
    (run_dir / "composed-prompt.md").write_text(prompt)
    include_dirs = [str(wt) for wt, _ in baselines.values()]

    results = _fan_out(prompt, ticket_dir, run_dir, include_dirs, FANOUT_TIMEOUT)
    valid = [name for name, r in results.items() if r["valid"]]
    dropped = {name: r["reason"] for name, r in results.items() if not r["valid"]}
    log.emit("ctp_fanout_complete",
             f"[{ticket_key}] valid plans: {valid}; dropped: {dropped}",
             meta={"ticket": ticket_key, "valid": valid, "dropped": dropped})

    if len(valid) < 2:
        # Fan-out drops are availability/quality failures (non-zero exit,
        # empty, truncated) — never content disagreement, which is resolved
        # later in synthesis. So when the 2-model quorum can't be met because
        # external vendors were simply unreachable (e.g. a daily quota 429 or
        # a CLI timeout), don't deadlock the whole pipeline: fall back to a
        # claude-only plan as long as claude itself produced a valid one.
        # Loud log so the degraded run is visible. The full cross-check resumes
        # automatically on the next ticket once a second vendor is reachable.
        if "claude" in valid:
            log.emit("ctp_quorum_degraded",
                     f"[{ticket_key}] consensus quorum not met "
                     f"({len(valid)} valid: {valid}); other planners "
                     f"unavailable: {dropped}. Proceeding claude-only.",
                     meta={"ticket": ticket_key, "valid": valid,
                           "dropped": dropped, "degraded": True})
        else:
            return False, (f"consensus quorum not met and claude plan invalid: "
                           f"valid={valid}; dropped {dropped}")

    plan_files = [run_dir / f"{name}-plan.md" for name in valid]
    if not _synthesize_and_implement(ticket_dir, plan_files, BUILD_TIMEOUT):
        return False, "synthesis/implementation did not produce docs/technical-plan.md"

    patch, changed = _assemble_diff(baselines, run_dir)
    if patch is None:
        return False, "implementation produced no diff in any repo"

    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        manifest_fut = pool.submit(_write_manifest, ticket_dir, patch,
                                   MANIFEST_TIMEOUT)
        explainer_fut = pool.submit(_write_explainer, ticket_dir, patch,
                                    EXPLAINER_TIMEOUT)
        manifest_ok = manifest_fut.result()
        try:
            explainer_ok = explainer_fut.result()
        except Exception as e:
            explainer_ok = False
            log.emit("ctp_explainer_error",
                     f"[{ticket_key}] explainer step raised: {type(e).__name__}: {e}",
                     meta={"ticket": ticket_key})
    if not manifest_ok:
        return False, "manifest step did not produce docs/change-manifest.md"
    if not explainer_ok:
        log.emit("ctp_explainer_missing",
                 f"[{ticket_key}] explainer step did not produce "
                 f"docs/change-explainer.html; continuing without it",
                 meta={"ticket": ticket_key})

    log.emit("ctp_complete",
             f"[{ticket_key}] consensus plan implemented; repos changed: {changed}",
             meta={"ticket": ticket_key, "changed": changed, "planners": valid})
    try:
        shutil.rmtree(run_dir)
    except OSError:
        pass
    return True, f"planned via {valid}; changed {changed}"
