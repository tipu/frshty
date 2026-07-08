"""Background builder for the guided slide presentations shown on the review
and ticket detail pages.

Presentations are cached as presentation_cache.json next to the review/ticket
artifacts, with a sibling presentation_meta.json recording the head sha the
deck was built from ({"status": "ok"|"failed", "sha": ..., "ts": ...}).
Auto-triggers (post-review hook, in_review reconcile) compare that sha against
the current head and rebuild only when it moved; a failed build is not
retried until the sha changes, so a permanently-failing PR can never turn
into a rebuild loop. "building" is tracked only in memory — if the process
dies mid-build the meta file keeps its previous terminal state and the next
trigger simply rebuilds.

Auto-triggers are gated behind the `features.presentations` config flag;
the manual preprocess endpoints on the web layer work regardless.
"""
import json
import re
import subprocess
import threading
import time
from pathlib import Path

import core.config as cfg
import core.log as log
import core.state as state
from core.claude_runner import run_balanced, extract_json
from core.config import get_repos
from features.platforms import make_platform
from services import review_store


_inflight: set[tuple] = set()
_inflight_lock = threading.Lock()
_build_slots = threading.Semaphore(2)


def enabled(config: dict) -> bool:
    return bool((config.get("features") or {}).get("presentations"))


def build_presentation(ticket_goal: str, diff_text: str, worktree: Path | None) -> dict:
    if not diff_text:
        return {}
    tools = ["Read", "Glob", "Grep"] if worktree else None
    has_tools = worktree is not None
    grounding = (
        "You have read-only access to the PR's checked-out branch. Use Read/Grep/Glob to open the "
        "full files and the code around each change so your flow ordering and correctness checks are "
        "grounded in the real code, not just the diff.\n\n"
        if has_tools else ""
    )
    prompt = (
        "You are preparing a guided, slide-by-slide PRESENTATION that walks a reviewer through a pull "
        "request so they can understand it and validate its correctness quickly.\n\n"
        f"TICKET / GOAL:\n{ticket_goal or '(no ticket goal provided; infer the intent from the diff)'}\n\n"
        + grounding +
        "Produce these parts:\n"
        "1. problem: 2-4 plain-language sentences framing the problem this PR solves.\n"
        "2. approach: 1-2 sentences on the strategy the PR takes to solve it.\n"
        "3. steps: an ORDERED list that walks the change the way the code actually EXECUTES, beginning "
        "to end (entry point -> downstream calls -> data/state changes -> result). Break it into SMALL, "
        "digestible steps; each step covers one coherent chunk (a function, a block, one key change). "
        "Never dump a whole file. For each step provide:\n"
        "   - title: a short label for this step\n"
        "   - file: the file path\n"
        "   - code: ONLY the relevant lines, each prefixed 'NUM: ' with the real line number, kept short\n"
        "   - narrative: 2-4 sentences on what this code does and how it fits the end-to-end flow\n"
        "   - check: 'ok' if this chunk is correct and on-purpose, or 'concern' if anything looks wrong, "
        "risky, destructive, dead/unused, or unrelated to the goal\n"
        "   - check_note: one sentence justifying the check — confirm it is correct and relevant, or name "
        "the problem (errant, destructive, dead/benign, or does not advance the goal)\n"
        "4. verdict: an object with solves_problem (true/false), summary (3-5 sentences on whether the PR "
        "correctly and completely solves the goal with no errant, destructive, or dead code), and concerns "
        "(array of short strings, empty if none).\n\n"
        "Order steps by execution flow, NOT by file or diff order. Aim for 4-10 steps.\n\n"
        'Return ONLY valid JSON (no markdown fences): '
        '{"problem":"...","approach":"...","steps":[{"title":"...","file":"...","code":"NUM: ...","narrative":"...","check":"ok|concern","check_note":"..."}],'
        '"verdict":{"solves_problem":true,"summary":"...","concerns":["..."]}}\n\n'
        f"--- DIFF START ---\n{diff_text[:60000]}\n--- DIFF END ---"
    )
    output = run_balanced(prompt, worktree=worktree, tools=tools, timeout=900)
    if not output:
        return {}
    parsed = extract_json(output)
    if not isinstance(parsed, dict):
        return {}
    return parsed


def resolve_ticket_goal(config: dict, branch: str, repo: str, pr_id: int) -> str:
    m = re.search(r"[A-Za-z]+-\d+", branch or "")
    key = m.group().upper() if m else ""
    if key:
        t = state.load("tickets").get(key) or {}
        goal = _goal_from_ticket_state(t)
        if goal:
            return goal
    try:
        info = make_platform(config).get_pr_info(repo, pr_id) or {}
        return f"{info.get('title', '')}\n{info.get('description', '')}".strip()[:4000]
    except Exception:
        return ""


def _goal_from_ticket_state(ts: dict) -> str:
    parts = [ts.get("summary", ""), ts.get("description", "")]
    ac = (ts.get("acceptance_criteria_json") or {}).get("source_text", "")
    if ac:
        parts.append(ac)
    return "\n".join(p for p in parts if p).strip()[:4000]


def meta_path(cache_file: Path) -> Path:
    return cache_file.with_name("presentation_meta.json")


def read_meta(cache_file: Path) -> dict:
    try:
        data = json.loads(meta_path(cache_file).read_text())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_meta(cache_file: Path, status: str, sha: str) -> None:
    try:
        meta_path(cache_file).write_text(json.dumps(
            {"status": status, "sha": sha, "ts": time.time()}))
    except OSError:
        pass


def is_building(key: tuple) -> bool:
    with _inflight_lock:
        return key in _inflight


def ticket_cache_file(config: dict, ts: dict) -> Path | None:
    slug = ts.get("slug")
    if not slug:
        return None
    ws = config["workspace"]
    return ws["root"] / ws["tickets_dir"] / slug / "presentation_cache.json"


def ticket_head_sha(config: dict, ts: dict) -> str:
    """Combined HEAD sha across the ticket's repo worktrees — the freshness
    key for auto rebuilds. Every commit our pipeline pushes lands in these
    worktrees first, so a moved HEAD means the deck is stale."""
    slug = ts.get("slug")
    if not slug:
        return ""
    shas = []
    for repo in get_repos(config):
        wt = cfg.ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=15)
        if result.returncode == 0 and result.stdout.strip():
            shas.append(result.stdout.strip())
    return "-".join(shas)


def _spawn(key: tuple, cache_file: Path | None, sha: str, auto: bool, build_fn) -> str:
    if cache_file is None:
        return "unavailable"
    if auto:
        meta = read_meta(cache_file)
        if cache_file.exists():
            if not meta:
                _write_meta(cache_file, "ok", sha)
                return "cached"
            if meta.get("sha") == sha:
                return "cached"
        elif meta.get("status") == "failed" and meta.get("sha") == sha:
            return "failed"
    else:
        if cache_file.exists():
            return "cached"
    with _inflight_lock:
        if key in _inflight:
            return "in_progress"
        _inflight.add(key)

    def run():
        ok = False
        try:
            with _build_slots:
                data = build_fn()
            if data:
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                cache_file.write_text(json.dumps(data, indent=2))
                ok = True
        except Exception as e:
            try:
                log.emit("presentation_failed",
                         f"Failed to build presentation for {key}: {e}",
                         meta={"key": list(map(str, key))})
            except Exception:
                pass
        finally:
            _write_meta(cache_file, "ok" if ok else "failed", sha)
            with _inflight_lock:
                _inflight.discard(key)

    threading.Thread(target=run, daemon=True).start()
    return "started"


def spawn_review_build(config: dict, repo: str, pr_id: int,
                       head_sha: str = "", auto: bool = False) -> str:
    found = review_store.find_review(config["_state_dir"], repo, pr_id)
    if not found:
        return "unavailable"
    branch_dir, _comments, worktree = found
    cache_file = branch_dir / "presentation_cache.json"
    review_json = branch_dir / "review.json"
    try:
        review_data = json.loads(review_json.read_text()) if review_json.exists() else {}
    except (OSError, ValueError):
        review_data = {}
    branch = review_data.get("source_branch", "")

    def build():
        diff_path = branch_dir / "diff.txt"
        diff_text = diff_path.read_text() if diff_path.exists() else ""
        if not diff_text:
            platform = make_platform(config)
            review_store.populate_repo_cache(platform, config["_state_dir"], repo)
            diff_text = platform.get_pr_diff(repo, pr_id) or ""
        if not diff_text:
            return {}
        wt = worktree or _ensure_review_worktree(config, repo, pr_id, branch_dir, branch)
        goal = resolve_ticket_goal(config, branch, repo, pr_id)
        return build_presentation(goal, diff_text, wt)

    return _spawn(("review", repo, pr_id), cache_file, head_sha, auto, build)


def spawn_ticket_build(config: dict, ticket_key: str,
                       head_sha: str = "", auto: bool = False) -> str:
    ts = state.load("tickets").get(ticket_key) or {}
    cache_file = ticket_cache_file(config, ts)

    def build():
        diffs = _ticket_diffs(config, ts)
        diff_text = "\n".join(d for _, d in diffs if d)
        if not diff_text:
            return {}
        slug = ts.get("slug", "")
        wt = None
        for repo_name, _ in diffs:
            p = cfg.ticket_worktree_path(config, slug, repo_name)
            if p.is_dir():
                wt = p
                break
        goal = _goal_from_ticket_state(ts)
        return build_presentation(goal, diff_text, wt)

    return _spawn(("ticket", ticket_key), cache_file, head_sha, auto, build)


def _ticket_diffs(config: dict, ts: dict) -> list[tuple[str, str]]:
    prs = ts.get("prs") or []
    if prs:
        platform = make_platform(config)
        out = []
        for pr in prs:
            try:
                d = platform.get_pr_diff(pr["repo"], pr["id"]) or ""
            except Exception:
                d = ""
            out.append((pr["repo"], d))
        return out
    slug = ts.get("slug")
    if not slug:
        return []
    out = []
    for repo in get_repos(config):
        wt = cfg.ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        head = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "origin/HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=15)
        base_ref = (head.stdout.strip() if head.returncode == 0 and head.stdout.strip()
                    else f"origin/{cfg.base_branch_for(config, repo['name'])}")
        result = subprocess.run(
            ["git", "diff", f"{base_ref}...HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=30)
        if result.stdout.strip():
            out.append((repo["name"], result.stdout))
    return out


def _ensure_review_worktree(config: dict, repo: str, pr_id: int, branch_dir: Path, branch: str):
    wt = branch_dir / "worktree"
    if (wt / ".git").exists():
        return wt
    repos = get_repos(config)
    matching = [r for r in repos if r["name"] == repo]
    if branch and matching:
        repo_path = matching[0]["path"]
        wt.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "fetch", "origin", branch], cwd=str(repo_path), capture_output=True, timeout=60)
        subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=60)
        subprocess.run(["git", "worktree", "add", str(wt), branch], cwd=str(repo_path), capture_output=True, timeout=60)
        if (wt / ".git").exists():
            return wt
    platform = make_platform(config)
    review_store.populate_repo_cache(platform, config["_state_dir"], repo)
    fallback = config["_state_dir"] / "reviews" / repo / f"pr-{pr_id}" / "worktree"
    platform.ensure_pr_worktree(repo, pr_id, fallback)
    return fallback if (fallback / ".git").exists() else None


def reconcile_tickets(config: dict) -> None:
    """Per scan tick: rebuild the deck for any in_review ticket whose local
    HEAD moved since the last build. Covers both PR creation (no cache yet)
    and every subsequent commit. Silent when nothing changed."""
    if not enabled(config):
        return
    try:
        tickets = state.load("tickets")
    except Exception:
        return
    for key, ts in tickets.items():
        try:
            if ts.get("status") != "in_review" or not (ts.get("prs") or []):
                continue
            sha = ticket_head_sha(config, ts)
            if not sha:
                continue
            spawn_ticket_build(config, key, head_sha=sha, auto=True)
        except Exception as e:
            log.emit("presentation_reconcile_error",
                     f"{key}: {type(e).__name__}: {e}", meta={"ticket": key})
