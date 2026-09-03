"""Bespoke PR auto-review + auto-fix pipeline (features.pr_autofix).

Every newly created PR gets one cycle: claude and codex each review the full
diff, the two reviews are consolidated down to critical/high findings, and a
fix run resolves those findings directly on the PR branch (commit + push).
Pre-existing open PRs are baselined on the first poll and never touched.
GitHub-only. Enabled per instance via features.pr_autofix.
"""
import json
import threading
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import core.git_util as git_util
import core.log as log
import core.queue as q
import core.state as state
from core.claude_runner import run_agentic, run_balanced, run_claude_code, extract_json
from core.commit_message import COMMIT_SUBJECT_RULE, commit_subject
from core.config import base_branch_for, get_repos
from core.llm import READ_ONLY_TOOLS, WRITE_TOOLS, run_external_model
from features.platforms import make_platform

CLAUDE_REVIEW_TIMEOUT = 900
CODEX_REVIEW_TIMEOUT = 1200
CONSOLIDATE_TIMEOUT = 240
FIX_TIMEOUT = 1500
MAX_ATTEMPTS = 3
DIFF_CHAR_CAP = 400_000
ACTIONABLE = ("critical", "high")
SEEDED_KEY = "_seeded"

REVIEW_SCHEMA = (
    'OUTPUT FORMAT: Return a single JSON object (no markdown fences, no prose before or after) '
    'with this schema:\n'
    '{"summary":"one paragraph","findings":[{"severity":"critical"|"high"|"medium"|"low",'
    '"path":"file/path","line":123,"title":"short title","body":"what is wrong and why it matters"}]}\n'
    'SEVERITY RULES: critical = data loss, security hole, crash, or corruption that will happen. '
    'high = a real defect that produces wrong behavior for users. '
    'medium/low = everything else. Only report findings grounded in the diff. '
    'LINE RULES: line numbers refer to the NEW version of the file, derived from the @@ hunk headers.\n'
)

REVIEW_PROMPT = (
    "Review this pull request diff for defects. Your single concern: real problems that must be "
    "fixed before merge — logic errors, data loss, security holes, race conditions, broken error "
    "handling, contract breakage. Do not report style, naming, or preference issues.\n"
)

_worktree_locks: dict[str, threading.Lock] = {}
_worktree_locks_guard = threading.Lock()


def _worktree_lock(pr_key: str) -> threading.Lock:
    with _worktree_locks_guard:
        return _worktree_locks.setdefault(pr_key, threading.Lock())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def check(config: dict):
    instance_key = config["job"]["key"]
    platform = make_platform(config)
    if not hasattr(platform, "list_open_prs"):
        log.emit("pr_autofix_unsupported",
                 f"[{instance_key}] pr_autofix requires a platform with list_open_prs")
        return
    prs = platform.list_open_prs()
    st = state.load("pr_autofix")

    if not st.get(SEEDED_KEY):
        for pr in prs:
            st[f"{pr['repo']}/{pr['id']}"] = {
                "status": "baselined", "title": pr["title"], "url": pr["url"],
                "author": pr.get("author", ""), "seen_at": _now(),
            }
        st[SEEDED_KEY] = _now()
        state.save("pr_autofix", st)
        log.emit("pr_autofix_seeded",
                 f"baselined {len(prs)} pre-existing open PR(s); auto-review starts with the next new PR",
                 meta={"count": len(prs), "prs": [f"{p['repo']}#{p['id']}" for p in prs]})
        return

    active_keys = {SEEDED_KEY}
    for pr in prs:
        pr_key = f"{pr['repo']}/{pr['id']}"
        active_keys.add(pr_key)
        rec = st.get(pr_key)
        if rec is None:
            st[pr_key] = {
                "status": "queued", "title": pr["title"], "url": pr["url"],
                "author": pr.get("author", ""), "seen_at": _now(), "attempts": 0,
            }
            q.enqueue_job(instance_key, "pr_autofix_run", payload={"pr": pr})
            log.emit("pr_autofix_queued",
                     f"{pr['repo']}#{pr['id']}: new PR — queued claude+codex review",
                     links={"pr": pr["url"]},
                     meta={"repo": pr["repo"], "pr_id": pr["id"], "author": pr.get("author", "")})
        elif rec.get("status") == "error" and rec.get("attempts", 0) < MAX_ATTEMPTS:
            rec["status"] = "queued"
            q.enqueue_job(instance_key, "pr_autofix_run", payload={"pr": pr})
            log.emit("pr_autofix_requeued",
                     f"{pr['repo']}#{pr['id']}: retry {rec.get('attempts', 0)}/{MAX_ATTEMPTS} after error",
                     links={"pr": pr["url"]},
                     meta={"repo": pr["repo"], "pr_id": pr["id"], "attempts": rec.get("attempts", 0)})

    for stale_key in [k for k in st if k not in active_keys]:
        del st[stale_key]
    state.save("pr_autofix", st)


def _update_record(pr_key: str, **fields) -> None:
    st = state.load("pr_autofix")
    rec = st.setdefault(pr_key, {})
    rec.update(fields)
    state.save("pr_autofix", st)


def _review_prompt(pr: dict, diff_text: str, has_tools: bool) -> str:
    truncated = len(diff_text) > DIFF_CHAR_CAP
    body = diff_text[:DIFF_CHAR_CAP]
    parts = [
        REVIEW_PROMPT,
        f"PR: {pr['repo']}#{pr['id']} — {pr['title']}\nBranch: {pr.get('branch', '')}\n",
    ]
    if has_tools:
        parts.append(
            "The PR branch is checked out in your working directory. Open the files the diff "
            "touches and read the surrounding code before you judge a hunk. Do not answer in "
            "one turn.\n"
        )
    parts.append(f"DIFF:\n{body}\n")
    if truncated:
        parts.append(f"[diff truncated at {DIFF_CHAR_CAP} chars of {len(diff_text)}]\n")
    parts.append(REVIEW_SCHEMA)
    return "".join(parts)


def _claude_review(config: dict, prompt: str, worktree: Path | None) -> dict | None:
    model = (config.get("reviewer") or {}).get("model")
    if worktree:
        output = run_agentic(prompt, cwd=worktree, tools=READ_ONLY_TOOLS,
                             denied_tools=WRITE_TOOLS, model=model,
                             timeout=CLAUDE_REVIEW_TIMEOUT,
                             function_name="autofix_claude_review")
    else:
        output = run_balanced(prompt, model=model, timeout=CLAUDE_REVIEW_TIMEOUT)
    return extract_json(output) if output else None


def _codex_review(config: dict, prompt: str, worktree: Path, pr_key: str) -> dict | None:
    run_dir = config["_state_dir"] / "autofix" / "codex-runs" / pr_key.replace("/", "-")
    run_dir.mkdir(parents=True, exist_ok=True)
    last = run_dir / "autofix-last.md"
    try:
        text, exit_code = run_external_model(
            ["codex", "exec", "--skip-git-repo-check",
             "-c", 'model_reasoning_effort="medium"',
             "-o", str(last), "-"],
            fn_name="pr_autofix_codex", model="codex", prompt=prompt,
            cwd=worktree, timeout=CODEX_REVIEW_TIMEOUT,
            last_message_file=last,
            transcript_file=run_dir / "autofix-transcript.txt",
            stdin_text=prompt,
        )
    except OSError as e:
        log.emit("pr_autofix_codex_error", f"{pr_key}: codex could not run: {type(e).__name__}: {e}",
                 meta={"pr_key": pr_key})
        return None
    if exit_code != 0 or not text:
        return None
    return extract_json(text)


def _normalize_findings(data: dict | None, provider: str) -> list[dict]:
    if not data:
        return []
    findings = []
    for f in data.get("findings", []):
        if not isinstance(f, dict) or not f.get("body"):
            continue
        severity = str(f.get("severity", "")).lower()
        findings.append({
            "severity": severity,
            "path": f.get("path", ""),
            "line": f.get("line"),
            "title": f.get("title", ""),
            "body": f["body"],
            "providers": [provider],
        })
    return findings


def _consolidate(config: dict, pr: dict, by_provider: dict[str, list[dict]]) -> list[dict]:
    """Merge the two providers' findings and keep only critical/high.

    The LLM merge dedupes findings that describe the same defect. When the
    merge output is unusable, fall back to a mechanical (path, line) dedupe so
    a merge failure never drops a finding silently.
    """
    actionable = {p: [f for f in fs if f["severity"] in ACTIONABLE]
                  for p, fs in by_provider.items()}
    flat = [f for fs in actionable.values() for f in fs]
    if not flat:
        return []
    if sum(1 for fs in actionable.values() if fs) == 1:
        return flat

    prompt = (
        "Two independent reviewers produced findings on the same PR diff. "
        "Merge findings that describe the same underlying defect into one entry, "
        "keeping the clearer description and listing every provider that reported it. "
        "Do not drop, soften, or invent findings.\n\n"
        + "\n".join(f"{p.upper()} FINDINGS:\n{json.dumps(fs, indent=1)}\n"
                    for p, fs in actionable.items() if fs)
        + '\nReturn JSON: {"findings":[{"severity":"critical"|"high","path":"...","line":123,'
          '"title":"...","body":"...","providers":["claude","codex"]}]}'
    )
    output = run_balanced(prompt, timeout=CONSOLIDATE_TIMEOUT)
    merged = extract_json(output) if output else None
    result = []
    if merged and isinstance(merged.get("findings"), list):
        for f in merged["findings"]:
            if isinstance(f, dict) and f.get("body") and str(f.get("severity", "")).lower() in ACTIONABLE:
                f["severity"] = str(f["severity"]).lower()
                result.append(f)
    if result:
        return result

    log.emit("pr_autofix_merge_fallback",
             f"{pr['repo']}#{pr['id']}: LLM consolidation unusable; using mechanical dedupe",
             meta={"repo": pr["repo"], "pr_id": pr["id"]})
    deduped: dict[tuple, dict] = {}
    for f in flat:
        key = (f.get("path", ""), f.get("line"))
        if key in deduped:
            for p in f["providers"]:
                if p not in deduped[key]["providers"]:
                    deduped[key]["providers"].append(p)
        else:
            deduped[key] = f
    return list(deduped.values())


def _fix_tools(worktree: Path) -> list[str]:
    scope = str(worktree.resolve())
    return [
        f"Read(/{scope}/**)",
        "Grep",
        "Glob",
        f"Edit(/{scope}/**)",
        f"Write(/{scope}/**)",
        f"MultiEdit(/{scope}/**)",
        f"NotebookEdit(/{scope}/**)",
    ]


def _write_artifacts(config: dict, pr: dict, by_provider: dict, findings: list[dict]) -> None:
    slug = (pr.get("branch") or f"pr-{pr['id']}").replace("/", "-")
    out_dir = config["_state_dir"] / "autofix" / slug
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "reviews.json").write_text(json.dumps(by_provider, indent=1))
    (out_dir / "findings.json").write_text(json.dumps(findings, indent=1))


def run(config: dict, payload: dict) -> tuple[bool, str | None]:
    pr = payload["pr"]
    pr_key = f"{pr['repo']}/{pr['id']}"
    pr_ref = f"{pr['repo']}#{pr['id']}"
    links = {"pr": pr["url"], "detail": f"{config['_base_url']}/"}
    meta = {"repo": pr["repo"], "pr_id": pr["id"]}
    platform = make_platform(config)

    st = state.load("pr_autofix")
    attempts = st.get(pr_key, {}).get("attempts", 0) + 1
    _update_record(pr_key, status="reviewing", attempts=attempts, started_at=_now())

    def _fail(reason: str) -> tuple[bool, str]:
        log.emit("pr_autofix_error", f"{pr_ref}: {reason}", links=links,
                 meta={**meta, "reason": reason, "attempts": attempts})
        _update_record(pr_key, status="error", error=reason)
        return False, reason

    try:
        with _worktree_lock(pr_key):
            diff_text = platform.get_pr_diff(pr["repo"], pr["id"])
            if not diff_text:
                return _fail("empty diff")

            worktree = _ensure_worktree(config, pr)
            if not worktree:
                return _fail("could not create worktree")

            prompt = _review_prompt(pr, diff_text, has_tools=True)
            with ThreadPoolExecutor(max_workers=2) as pool:
                claude_fut = pool.submit(_claude_review, config, prompt, worktree)
                codex_fut = pool.submit(_codex_review, config, prompt, worktree, pr_key)
                by_provider = {
                    "claude": _normalize_findings(claude_fut.result(), "claude"),
                    "codex": _normalize_findings(codex_fut.result(), "codex"),
                }

            raw_claude = claude_fut.result()
            raw_codex = codex_fut.result()
            if raw_claude is None and raw_codex is None:
                return _fail("both providers returned no usable review")
            for provider, raw in (("claude", raw_claude), ("codex", raw_codex)):
                if raw is None:
                    log.emit("pr_autofix_provider_empty",
                             f"{pr_ref}: {provider} returned no usable review; consolidating without it",
                             links=links, meta={**meta, "provider": provider})

            findings = _consolidate(config, pr, by_provider)
            _write_artifacts(config, pr, by_provider, findings)
            counts = {p: len(fs) for p, fs in by_provider.items()}
            log.emit("pr_autofix_reviewed",
                     f"{pr_ref}: claude={counts['claude']} codex={counts['codex']} finding(s), "
                     f"{len(findings)} critical/high after consolidation",
                     links=links, meta={**meta, "counts": counts, "actionable": len(findings)})

            if not findings:
                _update_record(pr_key, status="clean", findings=[], finished_at=_now())
                log.emit("pr_autofix_clean", f"{pr_ref}: no critical/high findings — branch left untouched",
                         links=links, meta=meta)
                return True, None

            git_util.run_git(worktree, ["reset", "--hard", f"origin/{pr['branch']}"])
            git_util.run_git(worktree, ["clean", "-fd"])

            findings_list = "\n\n".join(
                f"[{i + 1}] ({f['severity']}) {f.get('path', '')}:{f.get('line', '')} — {f.get('title', '')}\n"
                f"{f['body']}"
                for i, f in enumerate(findings)
            )
            fix_prompt = (
                f"A consolidated code review of this PR produced {len(findings)} critical/high "
                f"finding(s). Resolve ALL of them with the smallest correct change. "
                f"Do not refactor beyond what a finding requires.\n\n{findings_list}\n\n"
                + COMMIT_SUBJECT_RULE
            )
            result = run_claude_code(fix_prompt, cwd=worktree, timeout=FIX_TIMEOUT,
                                     allowed_tools=_fix_tools(worktree))
            if result is None:
                return _fail("fix run failed")

            git_util.run_git(worktree, ["add", "-A"])
            staged = git_util.run_git(worktree, ["diff", "--cached", "--quiet"],
                                      allowed_codes=(0, 1)).returncode != 0
            if not staged:
                return _fail("fix run produced no changes")
            commit = git_util.commit_with_hooks(
                worktree,
                message=commit_subject(
                    worktree,
                    f"fix: resolve {len(findings)} critical/high review finding(s)",
                    findings_list,
                ),
                timeout=900,
            )
            if commit.returncode != 0:
                detail = (commit.stderr or commit.stdout or "").strip()[:200]
                return _fail(f"commit failed: {detail}")

            push = platform.push_branch(worktree, pr["branch"])
            if isinstance(push, dict) and not push.get("ok", True):
                return _fail(f"push failed: {str(push.get('error', ''))[:200]}")

            head = git_util.run_git(worktree, ["rev-parse", "HEAD"], timeout=10).stdout.strip()
        _update_record(pr_key, status="fixed", findings=findings,
                       fix_commit=head, finished_at=_now())
        log.emit("pr_autofix_fixed",
                 f"{pr_ref}: resolved {len(findings)} critical/high finding(s) on {pr['branch']} ({head[:10]})",
                 links=links, meta={**meta, "commit": head, "findings": len(findings)})
        return True, None
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        log.emit("pr_autofix_error", f"{pr_ref}: unexpected error — {reason[:120]}",
                 links=links,
                 meta={**meta, "reason": reason[:200], "traceback": traceback.format_exc()[-1500:]})
        _update_record(pr_key, status="error", error=reason[:200])
        return False, reason


def _ensure_worktree(config: dict, pr: dict) -> Path | None:
    state_dir = config["_state_dir"]
    branch_slug = pr["branch"].replace("/", "-")
    worktree_path = state_dir / "autofix_worktrees" / branch_slug

    matching = [r for r in get_repos(config) if r["name"] == pr["repo"]]
    if not matching:
        return None
    repo_path = matching[0]["path"]

    if (worktree_path / ".git").is_file():
        if not worktree_path.resolve().is_relative_to(state_dir.resolve()):
            return None
        git_util.run_git(worktree_path, ["fetch", "origin", pr["branch"]])
        git_util.run_git(worktree_path, ["reset", "--hard", f"origin/{pr['branch']}"])
        return worktree_path

    base_branch = base_branch_for(config, pr["repo"])
    return git_util.add_or_reuse_worktree(repo_path, worktree_path, pr["branch"], base_branch=base_branch)
