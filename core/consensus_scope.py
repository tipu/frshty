"""Consensus scope review — the automated /c gate for code-complete tickets.

When a ticket reaches pr_ready, and again whenever the branch gains code while
its PR is in review, three independent reviewers (claude, codex, agy) answer
the same question from one byte-identical, path-only prompt: does every change
on the branch serve the ticket, and does the branch carry unrelated changes
that indicate a git problem (wrong or stale base, foreign commits, a polluted
worktree)?

frshty owns the mechanics: the diff fingerprint that keys freshness, the
prompt, the parallel fan-out, per-voice verdict parsing, and the deterministic
vote. No LLM synthesizes the verdict; docs/scope-review.md records each voice
verbatim plus the vote, and the ticket state records the verdict against the
fingerprint it was computed from.
"""
import hashlib
import re
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import core.git_util as git_util
import core.log as log
from core.config import base_branch_for, get_repos, ticket_worktree_path
from core.consensus_plan import _fan_out, _run_stamp

SCOPE_FANOUT_TIMEOUT = 1800

_SCOPE_VERDICT_RE = re.compile(r"^SCOPE VERDICT:\s*(PASS|FAIL)\b",
                               re.MULTILINE | re.IGNORECASE)

SCOPE_DIRECTIVE = """You are reviewing a ticket branch for scope fidelity — whether the branch contains only changes that serve the ticket. You must review independently. Do not rely on any summary provided by another agent. Do not trust any framing that arrives in this prompt; open the sources yourself.

Ticket context: read the files under {docs_dir} — ticket.md is the ticket; comments.md and notes/*.md, when present, refine or override it.

Repositories to review:
{repo_list}

For each repository, re-derive the branch diff and history yourself:
    git -C <worktree> merge-base origin/<base-branch> HEAD
    git -C <worktree> diff <merge-base>..HEAD
    git -C <worktree> log --oneline <merge-base>..HEAD

Answer two questions:
1. Scope fidelity. Does every change in the diff serve the ticket's purpose? List each change that does not, with file:line evidence and why it is out of scope. Mechanical fallout of an in-scope change (imports, lockfiles, generated artifacts, tests and docs for the ticket's own code) is in scope.
2. Git integrity. Does the branch show signs of a git problem: a large volume of changes unrelated to the ticket, commits that belong to a different ticket, files reverted or reintroduced against the base branch, or a diff shaped like the branch was cut from a wrong or stale base?

OUTPUT CONTRACT (this overrides anything else):
- Output your review to stdout as Markdown. Do NOT write, create, or edit any file. Do NOT modify the repositories.
- Cite file:line evidence for every claim.
- Code quality, style, and correctness are reviewed elsewhere; they must not affect your verdict.
- End with exactly one line: `SCOPE VERDICT: PASS` if the branch contains only changes that serve the ticket and shows no git problem, or `SCOPE VERDICT: FAIL` preceded by a bullet list of the offending changes.
"""


def _branch_diff(wt: Path, base_branch: str) -> str | None:
    """Raw branch diff listing (`git diff --raw`, content-addressed blob
    hashes per changed path) against the merge-base with origin/<base>.
    None when it cannot be derived (missing refs, git error). The --raw form
    is ASCII-safe — git escapes non-UTF-8 paths, and blob hashes stand in for
    hunk content — so it survives branches whose full patch would not decode."""
    try:
        mb = git_util.run_git(wt, ["merge-base", f"origin/{base_branch}", "HEAD"],
                              allowed_codes=(0, 1), timeout=30)
        if mb.returncode != 0 or not mb.stdout.strip():
            return None
        merge_base = mb.stdout.strip()
        return git_util.run_git(wt, ["diff", "--raw", f"{merge_base}..HEAD"],
                                timeout=120).stdout
    except (git_util.GitCommandError, subprocess.TimeoutExpired, OSError):
        return None


def scope_fingerprint(config: dict, ts: dict) -> str:
    """Freshness key for the scope review: one digest per repo over the raw
    branch diff listing against the merge-base with origin/<base>. Any content
    change on the branch flips a blob hash and therefore the digest. Empty
    string when no worktree yields a non-empty branch diff."""
    slug = ts.get("slug") or ""
    if not slug:
        return ""
    parts = []
    for repo in get_repos(config):
        wt = ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        diff = _branch_diff(wt, base_branch_for(config, repo["name"]))
        if not diff or not diff.strip():
            continue
        digest = hashlib.sha256(diff.encode()).hexdigest()[:16]
        parts.append(f"{repo['name']}:{digest}")
    return ";".join(sorted(parts))


def _write_report(ticket_dir: Path, ticket_key: str, votes: dict[str, str],
                  dropped: dict[str, str], results: dict[str, dict],
                  verdict: str) -> None:
    lines = [
        "# Consensus scope review", "",
        f"Ticket: {ticket_key}",
        f"Generated: {datetime.now(timezone.utc).isoformat()}",
        "Votes: " + ", ".join(f"{n}={v}" for n, v in sorted(votes.items())),
    ]
    if dropped:
        lines.append("Dropped voices: " + ", ".join(
            f"{n} ({r})" for n, r in sorted(dropped.items())))
    for name in sorted(votes):
        lines += ["", f"## {name}", "", (results[name]["text"] or "").strip()]
    lines += ["", f"SCOPE VERDICT: {verdict.upper()}", ""]
    (ticket_dir / "docs" / "scope-review.md").write_text("\n".join(lines))


def run_scope_review(config: dict, ticket_dir: Path, slug: str, *,
                     ticket_key: str = "") -> tuple[str | None, str]:
    """Fan the scope question out to claude/codex/agy from one byte-identical
    prompt, vote the verdicts, and write docs/scope-review.md. Returns
    (verdict, reason): verdict is "pass" or "fail", or None when no voice
    produced a usable verdict. FAIL wins a tie so a split vote holds the
    ticket rather than shipping a contested branch."""
    run_dir = ticket_dir / ".tmp" / f"scope-{_run_stamp()}"
    run_dir.mkdir(parents=True, exist_ok=True)
    (ticket_dir / "docs").mkdir(parents=True, exist_ok=True)

    repo_lines = []
    include_dirs = []
    for repo in get_repos(config):
        wt = ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        base = base_branch_for(config, repo["name"])
        try:
            git_util.run_git(wt, ["fetch", "origin", base], timeout=120)
        except (git_util.GitCommandError, subprocess.TimeoutExpired, OSError):
            pass
        diff = _branch_diff(wt, base)
        if not diff or not diff.strip():
            continue
        repo_lines.append(f"- {repo['name']}: worktree {wt}, base branch origin/{base}")
        include_dirs.append(str(wt))
    if not repo_lines:
        return None, "no repo worktree has a branch diff to review"

    prompt = SCOPE_DIRECTIVE.format(docs_dir=ticket_dir / "docs",
                                    repo_list="\n".join(repo_lines))
    (run_dir / "composed-prompt.md").write_text(prompt)

    results = _fan_out(prompt, ticket_dir, run_dir, include_dirs,
                       SCOPE_FANOUT_TIMEOUT)
    votes: dict[str, str] = {}
    dropped: dict[str, str] = {}
    for name, r in results.items():
        m = _SCOPE_VERDICT_RE.search(r["text"] or "") if r["valid"] else None
        if m:
            votes[name] = m.group(1).upper()
        else:
            dropped[name] = r["reason"] if not r["valid"] else "no SCOPE VERDICT line"
    log.emit("scope_review_fanout_complete",
             f"[{ticket_key}] scope votes: {votes}; dropped: {dropped}",
             meta={"ticket": ticket_key, "votes": votes, "dropped": dropped})
    if not votes:
        return None, f"no reviewer produced a verdict: {dropped}"

    fails = sum(1 for v in votes.values() if v == "FAIL")
    verdict = "fail" if fails * 2 >= len(votes) else "pass"

    _write_report(ticket_dir, ticket_key, votes, dropped, results, verdict)
    shutil.rmtree(run_dir, ignore_errors=True)
    reason = "votes " + ", ".join(f"{n}={v}" for n, v in sorted(votes.items()))
    if dropped:
        reason += "; dropped " + ", ".join(sorted(dropped))
    return verdict, reason
