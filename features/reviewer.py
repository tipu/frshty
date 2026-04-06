import json
import re
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import core.log as log
import core.state as state
from core.claude_runner import run_sonnet, run_haiku, extract_json
from core.config import get_repos
from features.platforms import make_platform

PERSONA_SPEC = (
    "You are a spec reviewer. Your single concern: does this diff solve what the ticket or PR description asks for?\n\n"
    "Focus on:\n"
    "- Requirements coverage: every acceptance criterion in the description must be addressed\n"
    "- Missing functionality: what the description promises but the diff does not deliver\n"
    "- Scope creep: changes that go beyond what was asked (flag, don't block)\n"
    "- If the PR references a Jira ticket, check the diff against any acceptance criteria mentioned\n\n"
    "Do NOT review for code style, naming, performance, or maintainability. Those are other reviewers' jobs.\n"
    "If the diff fully satisfies the requirements, say so and approve.\n"
)

PERSONA_BREAKAGE = (
    "You are a production-breakage reviewer. Your single concern: will this diff break something in production?\n\n"
    "Focus on:\n"
    "- Logic errors: off-by-one, null/undefined access, wrong comparisons, incorrect status codes\n"
    "- Race conditions and data integrity: concurrent writes, missing transactions, partial updates\n"
    "- Error handling: try blocks that are too broad, swallowed exceptions, missing error paths\n"
    "- Backwards compatibility: API contract changes, migration issues, state transitions\n"
    "- Security: SQL injection, XSS, missing auth checks, secrets in code\n"
    "- Async/sync mismatch: blocking calls in async contexts, missing awaits\n"
    "- ORM misuse: N+1 queries, missing select_related/prefetch_related, tenant isolation bypass\n"
    "- Test coverage: are new code paths tested? Do tests assert meaningful behavior or just not-crash?\n\n"
    "Do NOT review for style, naming, or spec compliance. Those are other reviewers' jobs.\n"
    "If nothing will break, say so and approve.\n"
)

PERSONA_MAINTAINABILITY = (
    "You are a maintainability reviewer. Your single concern: would you regret merging this in 3 months?\n\n"
    "Focus on:\n"
    "- Unnecessary complexity: intermediate variables used once, wrapper functions with no logic, dead code\n"
    "- DRY violations: repeated logic that should have one home, copy-pasted code across files\n"
    "- Naming consistency: terms that drift from established vocabulary, variable names that lie\n"
    "- Architecture: logic in the wrong layer (validation in views instead of serializers, business logic in commands)\n"
    "- Commented-out code: flag every time, we have version control\n"
    "- AI-generated noise: boilerplate comments that describe what the code literally does\n"
    "- Convention violations: framework defaults overridden without reason\n"
    "- Pattern consistency: does this follow existing patterns in the codebase or introduce a new one?\n\n"
    "Do NOT review for production breakage or spec compliance. Those are other reviewers' jobs.\n"
    "Prefix minor issues with `nit:`. State blocking issues directly.\n"
)

PERSONAS = {"spec": PERSONA_SPEC, "breakage": PERSONA_BREAKAGE, "maintainability": PERSONA_MAINTAINABILITY}

JSON_OUTPUT_SCHEMA = (
    'OUTPUT FORMAT: Return a single JSON object (no markdown fences, no explanation) with this schema:\n'
    '{"verdict":"approved"|"changes_requested","author":"...","source_branch":"...","destination_branch":"...",'
    '"date":"YYYY-MM-DD","summary":"...","issues":[{"severity":"blocking"|"suggestion"|"question",'
    '"path":"file/path","line":123,"start_line":120,"body":"markdown description"}],'
    '"blocking_summary":["..."],"suggestions_summary":["..."],"questions_summary":["..."]}\n'
)

LINE_NUMBER_RULES = (
    "LINE NUMBER RULES: 'line' and 'start_line' must be the line number in the NEW version of the file. "
    "In the diff, hunk headers look like @@ -old,count +new,count @@. The +new number is where the new file lines start. "
    "For added lines (+), count from that starting number. For new files (@@ -0,0 +1,N @@), line 1 is the first + line. "
    "Do NOT count diff metadata lines (diff --git, index, ---, +++, @@) or the + prefix character itself. "
    "'line' is the most relevant line for the issue. 'start_line' is the first line of the relevant code block.\n"
)

BODY_RULES = (
    "BODY RULES: The 'body' field must NOT contain severity tags, bold markers, or line numbers. "
    "Severity is already in the 'severity' field. "
    "State the observed behavior factually in 1-2 sentences, then ask the author if it's intentional. "
    "Don't prescribe fixes. Don't explain why it's wrong. Let the question do the work.\n"
)


def check(config: dict):
    platform = make_platform(config)
    review_prs = platform.list_review_prs()
    if not review_prs:
        return

    review_state = state.load("reviews")
    base_url = config["_base_url"]

    for pr in review_prs:
        pr_key = f"{pr['repo']}/{pr['id']}"
        existing = review_state.get(pr_key, {})

        needs_review = False
        if not existing.get("reviewed"):
            needs_review = True
        elif existing.get("last_updated") != pr.get("updated_on"):
            needs_review = True

        if not needs_review:
            continue

        re_review = existing.get("reviewed", False)
        label = "Re-reviewing" if re_review else "Reviewing"
        log.emit("review_started", f"{label} PR #{pr['id']} in {pr['repo']}",
            links={"pr": pr["url"], "detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
            meta={"repo": pr["repo"], "pr_id": pr["id"], "re_review": re_review})

        result = review_pr(config, platform, pr)
        if result:
            review_state[pr_key] = {"reviewed": True, "branch": pr["branch"], "last_updated": pr.get("updated_on")}
            issues = result.get("issues", [])
            log.emit("review_complete", f"Review done: {result.get('verdict', 'unknown')}, {len(issues)} issues",
                links={"pr": pr["url"], "detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
                meta={"repo": pr["repo"], "pr_id": pr["id"], "verdict": result.get("verdict"), "issue_count": len(issues)})

            if issues:
                log.emit("review_comments_queued", f"{len(issues)} comments ready to submit",
                    links={"detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
                    meta={"repo": pr["repo"], "pr_id": pr["id"]})

    state.save("reviews", review_state)



def review_pr(config: dict, platform, pr: dict) -> dict | None:
    diff_text = platform.get_pr_diff(pr["repo"], pr["id"])
    if not diff_text:
        return None

    worktree = _ensure_review_worktree(config, pr)
    conventions = _load_conventions(config, pr["repo"])
    file_context = _read_changed_files(diff_text, worktree) if worktree else ""

    persona_results = _run_all_personas(pr, diff_text, conventions, file_context, worktree)
    successful = [(name, data) for name, data in persona_results if data is not None]
    if not successful:
        return None

    merged = _merge_reviews(successful)
    if merged.get("issues"):
        merged["issues"] = _validate_issues(merged["issues"], worktree)
        merged["issues"] = _simplify_all_issues(merged["issues"])
        merged["issues"] = _style_match_all(config, merged["issues"])

    branch_slug = pr["branch"].replace("/", "-") if pr.get("branch") else f"pr-{pr['id']}"
    review_dir = config["_state_dir"] / "reviews" / pr["repo"] / branch_slug
    review_dir.mkdir(parents=True, exist_ok=True)
    (review_dir / "review.json").write_text(json.dumps(merged, indent=2))

    queued = [
        {
            "pr_id": pr["id"], "repo": pr["repo"], "pr_url": pr["url"],
            "path": issue.get("path"), "line": issue.get("line"),
            "body": issue["body"], "severity": issue.get("severity", "suggestion"),
            "persona": issue.get("persona", ""), "status": "pending",
        }
        for issue in merged.get("issues", [])
    ]
    (review_dir / "queued_comments.json").write_text(json.dumps(queued, indent=2))

    return merged


def _build_persona_prompt(persona_text, pr, diff_text, conventions, file_context, has_tools):
    parts = [
        f"You are reviewing pull request #{pr['id']} in repository '{pr['repo']}' (branch: {pr['branch']}).\n",
        persona_text + "\n",
        JSON_OUTPUT_SCHEMA, LINE_NUMBER_RULES, BODY_RULES,
    ]
    if conventions:
        parts.append("Review against the project conventions provided. Only flag conventions that are explicitly stated in the conventions text. Do not infer or assume unwritten rules.\n")
    if has_tools:
        parts.append("You have read-only access to the repository. Use your tools to verify issues against the actual codebase when the diff alone is ambiguous.\n")
    if conventions:
        parts.append(f"--- PROJECT CONVENTIONS ---\n{conventions}\n--- END CONVENTIONS ---\n")
    if file_context:
        parts.append(f"--- CHANGED FILES ---\n{file_context}\n--- END CHANGED FILES ---\n")
    parts.append(f"--- DIFF START ---\n{diff_text}\n--- DIFF END ---")
    parts.append("\nIMPORTANT: Your entire response must be the JSON object and nothing else. No summary, no explanation, no markdown fences.")
    return "\n".join(parts)


def _run_single_persona(args):
    name, prompt, worktree = args
    tools = ["Read", "Glob", "Grep"] if worktree else None
    output = run_sonnet(prompt, worktree=worktree, tools=tools)
    if not output:
        return (name, None)
    data = extract_json(output)
    if data:
        for issue in data.get("issues", []):
            issue["persona"] = name
            issue["tool_assisted"] = worktree is not None
    return (name, data)


def _run_all_personas(pr, diff_text, conventions, file_context, worktree):
    tasks = []
    for persona_name, persona_text in PERSONAS.items():
        prompt = _build_persona_prompt(persona_text, pr, diff_text, conventions, file_context, worktree is not None)
        tasks.append((persona_name, prompt, worktree))

    with ThreadPoolExecutor(max_workers=3) as pool:
        results = list(pool.map(_run_single_persona, tasks))
    return results


def _merge_reviews(results: list[tuple[str, dict]]) -> dict:
    if len(results) == 1:
        name, data = results[0]
        for issue in data.get("issues", []):
            issue["agreed_by"] = [name]
        return data

    merge_input = json.dumps({name: data for name, data in results}, indent=2)
    merge_prompt = (
        "You are merging code review results from three reviewer personas that each looked at the same PR diff "
        "through a different lens. Below are the individual reviews as JSON.\n\n"
        "The personas:\n"
        "- spec: checked if the diff satisfies the ticket/PR requirements\n"
        "- breakage: checked if the diff will break in production\n"
        "- maintainability: checked if the diff will be regretted in 3 months\n\n"
        "Your task:\n"
        "1. Identify duplicate/overlapping findings across personas (same problem even if different wording or ±5 lines)\n"
        "2. Merge duplicates into single issues with an 'agreed_by' array listing which personas flagged it\n"
        "3. For merged issues, use the most detailed 'body' from the agreeing personas\n"
        "4. For merged issues, use the most severe severity rating\n"
        "5. Keep unique findings as-is with a single-element 'agreed_by'\n"
        "6. Preserve the 'persona' field from the source (for merged issues, use the persona whose body you kept)\n"
        "7. Verdict: use the most conservative (any 'changes_requested' wins)\n"
        "8. If any merged issue had tool_assisted=true, set it true on the merged issue\n"
        "9. Drop any finding where confidence is clearly below 70% (vague, speculative, or hedged language)\n\n"
        "Return a single JSON object (no markdown fences) with the same schema as the inputs plus 'agreed_by' on each issue.\n\n"
        f"--- REVIEWS ---\n{merge_input}\n--- END REVIEWS ---"
    )
    output = run_haiku(merge_prompt)
    if output:
        data = extract_json(output)
        if data:
            if "issues" not in data and len(data) == 1:
                data = next(iter(data.values()))
            if isinstance(data.get("issues"), list):
                return data

    all_issues = []
    for name, data in results:
        for issue in data.get("issues", []):
            issue["agreed_by"] = [name]
            all_issues.append(issue)
    base = results[0][1]
    base["issues"] = all_issues
    return base


VALIDATE_PROMPT = (
    "You are auditing a code review comment for correctness. Your job is to DEBUNK the comment if possible.\n\n"
    "Look for:\n"
    "- Guard clauses, early returns, or type checks that make the flagged issue impossible\n"
    "- Type narrowing (TypeScript/Python) that guarantees the variable is defined at the flagged line\n"
    "- Surrounding logic that already handles the concern\n"
    "- Initialization or assignment in a higher scope that the reviewer missed\n\n"
    "If the surrounding code clearly defeats the claim, it is a false positive.\n"
    "If you cannot determine from the provided context, say uncertain.\n"
    "Do not speculate about code you cannot see.\n\n"
    "Return ONLY a JSON object (no markdown fences):\n"
    '{"decision":"valid"|"false_positive"|"uncertain","reason":"one sentence"}\n'
)


def _read_function_context(worktree: Path, file_path: str, target_line: int) -> str:
    fp = worktree / file_path
    if not fp.is_file():
        return ""
    try:
        lines = fp.read_text().splitlines()
    except OSError:
        return ""
    start = max(0, target_line - 60)
    end = min(len(lines), target_line + 60)
    numbered = [f"{i+1}: {lines[i]}" for i in range(start, end)]
    return "\n".join(numbered)


def _validate_single(args):
    issue, worktree = args
    path = issue.get("path")
    line = issue.get("line")
    if not path or not line or not worktree:
        return issue

    context = _read_function_context(worktree, path, line)
    if not context:
        return issue

    prompt = (
        f"{VALIDATE_PROMPT}"
        f"REVIEW COMMENT (severity: {issue.get('severity', 'unknown')}):\n{issue['body']}\n\n"
        f"FILE: {path}, LINE: {line}\n\n"
        f"CODE CONTEXT:\n{context}\n"
    )
    output = run_sonnet(prompt, timeout=120)
    if not output:
        return issue

    data = extract_json(output)
    if not data:
        return issue

    decision = data.get("decision", "valid")
    if decision == "false_positive":
        reason = data.get("reason", "")
        log.emit("review_validation_dropped", f"Dropped: {path}:{line} — {reason}",
            meta={"path": path, "line": line, "body": issue["body"], "reason": reason})
        return None
    return issue


def _validate_issues(issues: list[dict], worktree: Path | None) -> list[dict]:
    if not worktree:
        return issues
    tasks = [(issue, worktree) for issue in issues]
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(_validate_single, tasks))
    return [r for r in results if r is not None]


def _simplify_body(body: str) -> str:
    output = run_haiku(
        "Rewrite this code review comment in 1-3 sentences. "
        "Speak directly to the code author. Be technical and direct, no fluff. "
        "Never use hyphens, em dashes, or bullet points. "
        "Wrap function names, variable names, class names, file paths, and code keywords in backticks (e.g. `myFunction`, `user_id`, `None`). "
        f"Return ONLY the rewritten text, nothing else.\n\n{body}"
    )
    return output if output else body


def _simplify_all_issues(issues: list[dict]) -> list[dict]:
    with ThreadPoolExecutor(max_workers=10) as pool:
        bodies = list(pool.map(lambda i: _simplify_body(i["body"]), issues))
    for issue, body in zip(issues, bodies):
        issue["body"] = body
    return issues


def _style_match(body: str, examples: str) -> str:
    if not examples:
        return body
    output = run_haiku(
        f"Rewrite this PR review comment to match this person's commenting style.\n\n"
        f"Style examples:\n{examples}\n\nComment to rewrite:\n{body}\n\n"
        f"Return ONLY the rewritten comment."
    )
    return output if output else body


def _style_match_all(config: dict, issues: list[dict]) -> list[dict]:
    history_path = config["_state_dir"] / "comment_history.jsonl"
    if not history_path.exists():
        return issues
    lines = history_path.read_text().strip().splitlines()[-20:]
    examples = "\n".join(lines)
    with ThreadPoolExecutor(max_workers=10) as pool:
        bodies = list(pool.map(lambda i: _style_match(i["body"], examples), issues))
    for issue, body in zip(issues, bodies):
        issue["body"] = body
    return issues


def _ensure_review_worktree(config, pr) -> Path | None:
    repos = get_repos(config)
    matching = [r for r in repos if r["name"] == pr["repo"]]
    if not matching:
        return None
    repo_path = matching[0]["path"]

    slug = pr["branch"].replace("/", "-")
    worktree_path = config["_state_dir"] / "reviews" / pr["repo"] / slug / "worktree"

    if (worktree_path / ".git").is_file():
        if not worktree_path.resolve().is_relative_to(config["_state_dir"].resolve()):
            return None
        subprocess.run(["git", "fetch", "origin", pr["branch"]], cwd=str(worktree_path), capture_output=True, timeout=60)
        subprocess.run(["git", "reset", "--hard", f"origin/{pr['branch']}"], cwd=str(worktree_path), capture_output=True, timeout=60)
        return worktree_path

    worktree_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "fetch", "origin", pr["branch"]], cwd=str(repo_path), capture_output=True, timeout=60)
    subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=60)
    result = subprocess.run(
        ["git", "worktree", "add", str(worktree_path), pr["branch"]],
        cwd=str(repo_path), capture_output=True, text=True, timeout=60,
    )
    return worktree_path if result.returncode == 0 else None


def _load_conventions(config, repo_name) -> str:
    ws = config["workspace"]
    root = ws["root"]
    parts = []
    for path in [root / "CLAUDE.md", root / ws.get("projects_dir", "") / repo_name / "CLAUDE.md"]:
        if path.is_file():
            try:
                parts.append(path.read_text())
            except OSError:
                pass
    return "\n\n".join(parts)


def _extract_changed_paths(diff_text: str) -> list[str]:
    return re.findall(r"diff --git a/.+ b/(.+)", diff_text)


def _read_changed_files(diff_text: str, worktree_path: Path) -> str:
    paths = _extract_changed_paths(diff_text)
    parts = []
    total = 0
    for p in paths:
        fp = worktree_path / p
        if not fp.is_file():
            continue
        try:
            size = fp.stat().st_size
            if size > 60_000:
                continue
            content = fp.read_text()
        except OSError:
            continue
        parts.append(f"--- FILE: {p} ---\n{content}")
        total += len(content)
        if total > 120_000:
            break
    return "\n\n".join(parts)
