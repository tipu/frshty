import json
import re
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import core.log as log
import core.state as state
import core.git_util as git_util
from core.claude_runner import run_agentic, run_balanced, run_haiku, extract_json
from core.llm import READ_ONLY_TOOLS, WRITE_TOOLS, run_external_model
from core.config import base_branch_for, get_repos
import features.presentation as presentation
from features.platforms import make_platform

PERSONA_SPEC = (
    "You are a spec reviewer. Your single concern: does this diff solve what the ticket or PR description asks for?\n\n"
    "Focus on:\n"
    "- Requirements coverage: every acceptance criterion in the description must be addressed\n"
    "- Missing functionality: what the description promises but the diff does not deliver\n"
    "- Scope creep: changes that go beyond what was asked. Flag the fact as a suggestion, then "
    "grade the change itself on its own consequence under the severity rules.\n"
    "- If the PR references a Jira ticket, check the diff against any acceptance criteria mentioned\n\n"
    "Do NOT go looking for code style, naming, performance, or maintainability. Those are other "
    "reviewers' jobs. Report a defect you happened to see while verifying a criterion.\n"
    "An empty issues list is a valid answer when every criterion is delivered and verified.\n\n"
    "HOW TO WORK:\n"
    "- Do not answer from the diff alone. Open the checkout and confirm every claim before you write it.\n"
    "- Read the ticket or PR description first and write out its acceptance criteria one by one.\n"
    "- For each criterion, search the repository for the code that satisfies it. A requirement can be met "
    "by code the diff does not touch, and a diff that looks complete can still miss a criterion.\n"
    "- Open the tests for the new behaviour. A criterion with no test is not delivered.\n"
    "- Take as many tool calls as you need. Many reads and greps, then the answer. A verified answer "
    "in twenty turns beats a guess in one.\n"
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
    "Do NOT go looking for style, naming, or spec compliance. Those are other reviewers' jobs.\n"
    "An empty issues list is a valid answer when you traced the changed paths and none of them break.\n\n"
    "HOW TO WORK:\n"
    "- Do not answer from the diff alone. Trace every finding in the checkout before you report it.\n"
    "- For each suspicious line, open the file and read the whole function, not the hunk.\n"
    "- Grep for the callers of every changed function and for the readers of every changed field or "
    "response key. Most breakage lives in the caller, not in the diff.\n"
    "- For permission, auth, and tenant code, open the base classes and the permission classes the "
    "changed view drops or keeps. Compare against a view that is known correct.\n"
    "- For migrations, read the models and the rows the migration touches, and follow every foreign "
    "key that points at the deleted or changed data.\n"
    "- Open the tests that cover the changed code paths. No test for a new path is a finding.\n"
    "- Take as many tool calls as you need. Report a finding only after you traced its failure path "
    "through real code. A verified answer in twenty turns beats a guess in one.\n"
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
    "Do NOT go hunting for production breakage or spec compliance. Those are other reviewers' jobs. "
    "But you open whole files to judge them, and the other reviewers cannot open all of them. When a "
    "file you opened contains a defect that meets a blocking rule below, report it at that severity. "
    "Staying in your lane is not a reason to file data loss as a nit.\n"
    "Prefix minor issues with `nit:`. A blocking finding still says plainly what breaks.\n\n"
    "HOW TO WORK:\n"
    "- Do not answer from the diff alone. Confirm every claim against the checkout.\n"
    "- Before you call something a new pattern, grep for the established pattern and name the file "
    "that establishes it.\n"
    "- Before you call something duplicated, find the other copy and name its file and line.\n"
    "- Before you call a name inconsistent, grep for the term across the repository and see which "
    "spelling the codebase already uses.\n"
    "- Take as many tool calls as you need. A verified answer in twenty turns beats a guess in one.\n"
)

PERSONAS = {"spec": PERSONA_SPEC, "breakage": PERSONA_BREAKAGE, "maintainability": PERSONA_MAINTAINABILITY}
REVIEW_RETRY_COOLDOWN_SECONDS = 60 * 60
REVIEW_MAX_CHANGED_LINES = 8000

REVIEW_TOOLS = READ_ONLY_TOOLS
REVIEW_DENIED_TOOLS = WRITE_TOOLS
REVIEW_PERSONA_TIMEOUT = 1200

JSON_OUTPUT_SCHEMA = (
    'OUTPUT FORMAT: Return a single JSON object (no markdown fences, no explanation) with this schema:\n'
    '{"verdict":"approved"|"changes_requested","author":"...","source_branch":"...","destination_branch":"...",'
    '"date":"YYYY-MM-DD","summary":"...","issues":[{"repo":"repository-name","severity":"blocking"|"suggestion"|"question",'
    '"path":"file/path","line":123,"start_line":120,"body":"markdown description"}],'
    '"blocking_summary":["..."],"suggestions_summary":["..."],"questions_summary":["..."]}\n'
)

SEVERITY_RULES = (
    "SEVERITY RULES: Severity describes the consequence of the defect, not the lane you were asked "
    "to review. Grade every finding on what it does to production data, to a user, or to a deploy, "
    "even when the defect sits at the edge of your focus list.\n"
    "Use \"blocking\" when any of these is true:\n"
    "- Data is lost, corrupted, or made unreachable. Stored bytes left with no row that names them, "
    "a row left with no bytes, a write silently discarded, a value overwritten by a concurrent request.\n"
    "- A lifecycle or state field is read and then acted on without a lock or a recheck, so two "
    "concurrent requests can both act on the stale value.\n"
    "- An acceptance criterion has no code path that delivers it end to end, including the schedule, "
    "job, or caller that must invoke it. A command nothing calls is not delivered.\n"
    "- A migration already applied on the base branch is edited in place, so an applied database and "
    "a fresh database end in different states.\n"
    "- An authenticated endpoint reads or allocates without a bound, so one request can exhaust the process.\n"
    "- Auth, tenant scoping, or a permission check is dropped, weakened, or bypassable.\n"
    "- The change breaks an existing caller, consumer, or stored contract with no compatibility path.\n"
    "A defect that raises no exception is not therefore a suggestion. Silent data loss is blocking. "
    "Grade on the worst outcome you traced, not on how loud it is.\n"
    "Use \"suggestion\" when the worst outcome you traced is cost, clutter, duplicated work, or "
    "future maintenance burden.\n"
    "Use \"question\" only when you used your tools, could not settle the fact, and that fact decides "
    "whether the finding exists. Not knowing where a caller lives is a thing to grep for, not a "
    "question. If you grep and find no caller, the missing caller is the finding.\n"
)

LINE_NUMBER_RULES = (
    "LINE NUMBER RULES: 'line' and 'start_line' must be the line number in the NEW version of the file. "
    "In the diff, hunk headers look like @@ -old,count +new,count @@. The +new number is where the new file lines start. "
    "For added lines (+), count from that starting number. For new files (@@ -0,0 +1,N @@), line 1 is the first + line. "
    "Do NOT count diff metadata lines (diff --git, index, ---, +++, @@) or the + prefix character itself. "
    "'line' is the most relevant line for the issue. 'start_line' is the first line of the relevant code block.\n"
)

HOUSE_VOICE = (
    "VOICE: Write the comment the way you would say it to the author at their desk.\n"
    "Two sentences, in this order:\n"
    "1. The ask, as a question: \"Can we <the change you want>?\" Say the change in plain words, "
    "not as an implementation plan. One question, and it is the first sentence.\n"
    "2. The reason: what the code does now and what that costs, joined by \"so\" or \"which\". "
    "Name the real trigger and the real consequence the user, the caller, or the ticket goal ends "
    "up with.\n"
    "Use plain nouns for the moving parts (\"the timer\", \"the composer\", \"the spoken "
    "offset\") instead of the symbol names, unless the author cannot find the code without the "
    "symbol. Contractions are fine. No code blocks, no bullet lists, no headings.\n"
    "Length: two sentences. A third only when the trigger needs its own sentence. Never more.\n"
    "Do not prescribe the patch, do not list steps, do not ask for a test, do not restate the code, "
    "do not add background.\n"
    "The question is how the finding is delivered, not a hedge. You verified the defect, so say what "
    "happens, not what might happen. \"Can\" for a path that only some inputs take is fine; "
    "\"might\", \"could\", and \"possibly\" about the defect itself are not.\n"
    "Example: \"Can we wait to reveal the text until the audio actually starts? Right now the timer "
    "starts as soon as text streams in, so the transcript gets ahead of ElevenLabs and misses the "
    "main goal of this ticket.\"\n"
    "Example: \"Can we treat an error as the end of the turn here? With voice enabled a failed "
    "response never becomes caught up, so the composer stays stuck in loading and the user can't "
    "retry.\"\n"
)

BODY_RULES = (
    "BODY RULES: The 'body' field must NOT contain severity tags, bold markers, or line numbers. "
    "Severity is already in the 'severity' field and the location is already in the 'path' and "
    "'line' fields.\n"
    + HOUSE_VOICE +
    "Every body opens with a question. That is the house voice, not the \"question\" severity. "
    "Grade severity by the SEVERITY RULES alone.\n"
    "Ask about a fact you could not settle only when you tried to settle it with your tools and "
    "failed. Then say what you checked and what is still unknown, and rate it \"question\".\n"
)

TOOL_USE_RULES = (
    "TOOL USE: You have a read-only checkout of this branch. Use it. The diff and the file excerpts "
    "below are the starting point, not the evidence. Before you report any issue, open the file, read "
    "the whole surrounding function, and grep for the callers of what changed. Do not answer in one "
    "turn: a real review is many reads and greps, then the JSON. If you write the JSON without having "
    "opened a single file, the review is wrong. When a claim resists verification, drop it or file it "
    "as a question that states what you checked.\n"
)

TICKET_TOOL_USE_RULES = (
    "TOOL USE: You have read-only checkouts of every repository in this ticket; each PR section below "
    "names its worktree path. Use them with those absolute paths. The diffs and file excerpts are the "
    "starting point, not the evidence. Before you report any issue, open the file, read the whole "
    "surrounding function, and grep for the callers of what changed, including callers in the other "
    "repositories of this ticket. Do not answer in one turn: a real review is many reads and greps, "
    "then the JSON. When a claim resists verification, drop it or file it as a question that states "
    "what you checked.\n"
)


def check(config: dict):
    platform = make_platform(config)
    review_prs = platform.list_pending_reviews_for_me()
    if not review_prs:
        return

    ticket_state = state.load("tickets")
    review_state = state.load("reviews")
    pending = state.load("reviews_pending")

    _track_pending_prs(pending, review_prs, ticket_state, review_state)
    _process_ready_tickets(config, pending)

    state.save("reviews_pending", pending)



def review_pr(config: dict, platform, pr: dict, ticket_context: str = "",
              prefetched_diff: str | None = None) -> dict | None:
    diff_text = prefetched_diff if prefetched_diff is not None else platform.get_pr_diff(pr["repo"], pr["id"])
    if not diff_text:
        return None

    worktree = _ensure_review_worktree(config, pr)
    review_dir = _review_dir(config, pr)
    diff_path = _stage_diff(review_dir, diff_text)
    conventions = _load_conventions(config, pr["repo"])

    prompts = {name: _build_persona_prompt(text, pr, diff_path,
                                           _extract_changed_paths(diff_text), conventions,
                                           worktree, ticket_context)
               for name, text in PERSONAS.items()}
    by_provider = _run_personas_for_providers(
        prompts, _review_providers(config), worktree=worktree or review_dir,
        add_dirs=[review_dir] if worktree else None,
        model=_reviewer_model(config), run_key=f"{pr['repo']}-{pr['id']}")

    issues_by_provider: dict[str, list[dict]] = {}
    shared_by_provider: dict[str, dict] = {}
    for provider, results in by_provider.items():
        successful = [(name, data) for name, data in results if data is not None]
        if not successful:
            log.emit("review_provider_empty",
                     f"{pr['repo']}#{pr['id']}: no valid {provider} persona output",
                     meta={"repo": pr["repo"], "pr_id": pr["id"], "provider": provider})
            continue
        merged = _merge_reviews(successful)
        if merged.get("issues"):
            merged["issues"] = _validate_issues(merged["issues"], worktree)
            merged["issues"] = _simplify_all_issues(merged["issues"])
        issues_by_provider[provider] = merged.get("issues", [])
        shared_by_provider[provider] = merged
        if provider != "claude":
            _write_review_artifacts(config, pr, dict(merged), diff_text, provider=provider)

    if not shared_by_provider:
        return None
    primary = dict(shared_by_provider.get("claude") or next(iter(shared_by_provider.values())))
    issues = _union_issues(issues_by_provider)
    blocking = [i for i in issues if i.get("severity") == "blocking"]
    only_other = sorted({p for i in blocking for p in i["found_by"]} - {"claude"})
    if blocking and only_other and not any("claude" in i["found_by"] for i in blocking):
        log.emit("review_provider_only_blocker",
                 f"{pr['repo']}#{pr['id']}: every blocking finding came from "
                 f"{', '.join(only_other)}, not claude",
                 meta={"repo": pr["repo"], "pr_id": pr["id"],
                       "providers": only_other, "blocking": len(blocking)})
    primary["issues"] = issues
    primary["verdict"] = "changes_requested" if blocking else "approved"
    _write_review_artifacts(config, pr, primary, diff_text, provider="claude")
    return primary


def _review_dir(config, pr) -> Path:
    branch_slug = pr["branch"].replace("/", "-") if pr.get("branch") else f"pr-{pr['id']}"
    review_dir = config["_state_dir"] / "reviews" / pr["repo"] / branch_slug
    review_dir.mkdir(parents=True, exist_ok=True)
    return review_dir


def _stage_diff(review_dir: Path, diff_text: str) -> Path:
    """Put the diff on disk so the reviewer fetches it with a tool call.

    A diff pasted into the prompt makes the model answer in one turn from the
    prompt alone. A diff it has to open is the first of many reads."""
    diff_path = review_dir / "diff.txt"
    diff_path.write_text(diff_text)
    return diff_path


def _write_review_artifacts(config, pr, merged: dict, diff_text: str,
                            provider: str = "claude") -> None:
    review_dir = _review_dir(config, pr)
    merged["pr_id"] = pr["id"]
    merged["pr_url"] = pr.get("url", "")
    merged["repo"] = pr["repo"]
    merged["provider"] = provider
    suffix = "" if provider == "claude" else f".{provider}"
    (review_dir / f"review{suffix}.json").write_text(json.dumps(merged, indent=2))
    (review_dir / "diff.txt").write_text(diff_text)

    queued = [
        {
            "pr_id": pr["id"], "repo": pr["repo"], "pr_url": pr.get("url", ""),
            "path": issue.get("path"), "line": issue.get("line"),
            "body": issue["body"], "severity": issue.get("severity", "suggestion"),
            "persona": issue.get("persona", ""), "status": "pending",
            "found_by": issue.get("found_by") or [issue.get("provider") or provider],
        }
        for issue in merged.get("issues", [])
    ]
    (review_dir / f"queued_comments{suffix}.json").write_text(json.dumps(queued, indent=2))


def _build_persona_prompt(persona_text, pr, diff_path, changed_paths, conventions,
                          worktree, ticket_context=""):
    parts = [
        f"You are reviewing pull request #{pr['id']} in repository '{pr['repo']}' (branch: {pr['branch']}).\n",
        persona_text + "\n",
        JSON_OUTPUT_SCHEMA, SEVERITY_RULES, LINE_NUMBER_RULES, BODY_RULES,
    ]
    if ticket_context:
        parts.append(
            "This PR is part of a larger ticket. The ticket goal and the full diffs of its "
            "sibling PRs (in other repositories) are provided below so you review this PR in "
            "the context of the whole change. A requirement implemented in a sibling PR is NOT "
            "missing from this PR. Flag cross-PR inconsistencies (mismatched API contracts, "
            "producer/consumer drift, a change here that breaks code changed in a sibling) as "
            "issues on this PR only when the fix belongs in this repository; otherwise raise "
            "them as questions. Only report issues anchored to files in THIS PR's diff.\n"
            f"--- TICKET CONTEXT ---\n{ticket_context}\n--- END TICKET CONTEXT ---\n")
    if conventions:
        parts.append("Review against the project conventions provided. Only flag conventions that are explicitly stated in the conventions text. Do not infer or assume unwritten rules.\n")
    if worktree:
        parts.append(TOOL_USE_RULES)
    if conventions:
        parts.append(f"--- PROJECT CONVENTIONS ---\n{conventions}\n--- END CONVENTIONS ---\n")
    if worktree:
        parts.append(f"The branch is checked out read-only at {worktree}. That checkout is your "
                     "working directory, so plain git commands and relative paths resolve inside it.\n")
    parts.append(f"--- DIFF ---\nThe complete diff of this PR is the file {diff_path}. "
                 "Read it with your tools and page through all of it; it is not pasted into "
                 "this prompt.\n--- END DIFF ---")
    if changed_paths:
        parts.append("--- FILES CHANGED ---\n" + "\n".join(changed_paths) + "\n--- END FILES CHANGED ---")
    parts.append("\nIMPORTANT: Explore first, then answer. Your FINAL message must be the JSON "
                 "object and nothing else. No summary, no explanation, no markdown fences.")
    return "\n".join(parts)


def _reviewer_model(config) -> str | None:
    return (config.get("reviewer") or {}).get("model")


def _review_providers(config) -> list[str]:
    return (config.get("reviewer") or {}).get("providers") or ["claude"]


def _max_changed_lines(config) -> int:
    value = (config.get("reviewer") or {}).get("max_changed_lines")
    if value is None:
        return REVIEW_MAX_CHANGED_LINES
    return int(value)


def _run_single_persona(args):
    name, prompt, cwd, model, add_dirs = args
    try:
        if cwd:
            output = run_agentic(prompt, cwd=cwd, add_dirs=add_dirs, tools=REVIEW_TOOLS,
                                 denied_tools=REVIEW_DENIED_TOOLS, model=model,
                                 timeout=REVIEW_PERSONA_TIMEOUT,
                                 function_name="review_persona")
        else:
            output = run_balanced(prompt, model=model)
    except subprocess.TimeoutExpired as e:
        log.emit("review_persona_timeout",
                 f"persona '{name}' timed out after {e.timeout}s",
                 meta={"persona": name, "worktree": str(cwd) if cwd else ""})
        return (name, None)
    if not output:
        return (name, None)
    data = extract_json(output)
    if data:
        for issue in data.get("issues", []):
            issue["persona"] = name
            issue["tool_assisted"] = cwd is not None
    return (name, data)


CODEX_REVIEW_TIMEOUT = 1200


def _run_codex_persona(args):
    name, prompt, cwd, ticket_key = args
    # Peer PRs are reviewed without a checkout, so cwd can be None. The run
    # directory still has to land somewhere or codex never gets asked at all.
    run_root = Path(cwd) if cwd else Path(tempfile.gettempdir()) / "frshty-codex-runs"
    run_dir = run_root / ".codex-runs" / ticket_key.replace("/", "-")
    run_dir.mkdir(parents=True, exist_ok=True)
    last = run_dir / f"{name}-last.md"
    try:
        text, exit_code = run_external_model(
            ["codex", "exec", "--skip-git-repo-check",
             "-c", 'model_reasoning_effort="medium"',
             "-o", str(last), "-"],
            fn_name="review_codex", model="codex", prompt=prompt,
            cwd=cwd, timeout=CODEX_REVIEW_TIMEOUT,
            last_message_file=last,
            transcript_file=run_dir / f"{name}-transcript.txt",
            stdin_text=prompt,
        )
    except subprocess.TimeoutExpired:
        log.emit("review_persona_timeout",
                 f"codex persona '{name}' timed out after {CODEX_REVIEW_TIMEOUT}s",
                 meta={"persona": name, "provider": "codex"})
        return (name, None)
    except OSError as e:
        log.emit("review_persona_error",
                 f"codex persona '{name}' could not run: {type(e).__name__}: {e}",
                 meta={"persona": name, "provider": "codex"})
        return (name, None)
    if exit_code != 0 or not text:
        return (name, None)
    data = extract_json(text)
    if data:
        for issue in data.get("issues", []):
            issue["persona"] = name
            issue["tool_assisted"] = True
    return (name, data)


def _run_personas_for_providers(prompts: dict, providers: list[str], *,
                                worktree, model, run_key: str,
                                add_dirs: list | None = None) -> dict[str, list]:
    """Run one set of persona prompts through every configured provider.

    Shared by review_pr and review_ticket. The experiment previously lived only
    in review_ticket, so reviewer.providers silently did nothing on the peer-PR
    path, which is where most reviews happen. Returns {provider: [(name, data)]}.
    """
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {}
        if "claude" in providers:
            futures["claude"] = [pool.submit(_run_single_persona, (name, prompt, worktree, model, add_dirs))
                                 for name, prompt in prompts.items()]
        if "codex" in providers:
            futures["codex"] = [pool.submit(_run_codex_persona, (name, prompt, worktree, run_key))
                                for name, prompt in prompts.items()]
        return {provider: [f.result() for f in futs] for provider, futs in futures.items()}


def _merge_reviews(results: list[tuple[str, dict]]) -> dict:
    if len(results) == 1:
        name, data = results[0]
        for issue in data.get("issues", []):
            issue["agreed_by"] = [name]
        return data

    merge_input = json.dumps({name: data for name, data in results}, indent=2)
    merge_prompt = (
        "You are merging code review results from three reviewer personas that each looked at the same change "
        "through a different lens. Below are the individual reviews as JSON.\n\n"
        "The personas:\n"
        "- spec: checked if the diff satisfies the ticket/PR requirements\n"
        "- breakage: checked if the diff will break in production\n"
        "- maintainability: checked if the diff will be regretted in 3 months\n\n"
        "Your task:\n"
        "1. Identify duplicate/overlapping findings across personas. Two findings are the same "
        "finding when they describe one defect, even when the wording differs, the line numbers sit "
        "up to five lines apart, or one persona describes the cause and another describes the "
        "symptom. Two symptoms of one root cause are one finding.\n"
        "2. Merge duplicates into single issues with an 'agreed_by' array listing which personas flagged it\n"
        "3. For merged issues, use the most detailed 'body' from the agreeing personas\n"
        "4. For merged issues, use the most severe severity rating\n"
        "5. Keep unique findings as-is with a single-element 'agreed_by'\n"
        "6. Preserve the 'persona' field from the source (for merged issues, use the persona whose body you kept)\n"
        "7. Verdict: use the most conservative (any 'changes_requested' wins)\n"
        "8. If any merged issue had tool_assisted=true, set it true on the merged issue\n"
        "9. Drop a finding only when it names no concrete defect at all: no file, no code path, nothing "
        "the author could act on. Hedged wording is not a reason to drop. Rewrite the hedge into a "
        "plain statement and keep the finding.\n"
        "10. Never drop a finding because only one persona reported it. Each persona looks through a "
        "different lens, so a single-persona finding is the normal case.\n"
        "11. Keep every 'body' whole and in the voice it arrived in. Each body opens with a question "
        "that asks for the change, then gives the reason. Keep both parts, do not shorten them, and "
        "do not rewrite the question into a statement.\n\n"
        "Return a single JSON object (no markdown fences) with the same schema as the inputs plus 'agreed_by' on each issue.\n\n"
        f"--- REVIEWS ---\n{merge_input}\n--- END REVIEWS ---"
    )
    output = run_balanced(merge_prompt, timeout=300)
    if output:
        data = extract_json(output)
        if data:
            if "issues" not in data and len(data) == 1:
                data = next(iter(data.values()))
            if isinstance(data.get("issues"), list):
                data["issues"] = _collapse_persona_duplicates(data["issues"])
                return data

    log.emit("review_merge_fallback",
             "persona merge model returned nothing; merging the persona lists in code",
             meta={"personas": [name for name, _ in results]})
    all_issues = []
    for name, data in results:
        for issue in data.get("issues", []):
            issue["agreed_by"] = [name]
            all_issues.append(issue)
    base = results[0][1]
    base["issues"] = _collapse_persona_duplicates(all_issues)
    return base


def _collapse_persona_duplicates(issues: list[dict]) -> list[dict]:
    """One comment per line when several personas report the same defect.

    Three personas read the same diff, so one defect arrives three times on one
    line. The merge model normally folds them together. When that call fails the
    persona lists were concatenated as they were, and the pull request got three
    comments stacked on one line. Collapse on the exact path and line, keep the
    most severe rating and the longest body, and name every persona in
    'agreed_by'. A finding with no path or no line never collapses: those are
    general comments and each one is its own remark."""
    out: list[dict] = []
    dropped: list[dict] = []
    for issue in issues:
        path = issue.get("path")
        line = issue.get("line")
        twin = next((o for o in out
                     if path and line
                     and o.get("path") == path and o.get("line") == line), None)
        if twin is None:
            out.append(issue)
            continue
        dropped.append({"path": path, "line": line, "persona": issue.get("persona", ""),
                        "body": issue.get("body", "")})
        twin["agreed_by"] = sorted(set(twin.get("agreed_by", []) + issue.get("agreed_by", [])))
        if issue.get("severity") == "blocking":
            twin["severity"] = "blocking"
        if len(issue.get("body", "")) > len(twin.get("body", "")):
            twin["body"] = issue["body"]
            twin["persona"] = issue.get("persona", twin.get("persona", ""))
        if issue.get("tool_assisted"):
            twin["tool_assisted"] = True
    if dropped:
        log.emit("review_duplicate_findings_collapsed",
                 f"{len(dropped)} finding(s) landed on a line another persona already flagged: "
                 + ", ".join(f"{d['path']}:{d['line']}" for d in dropped),
                 meta={"dropped": dropped, "kept": len(out)})
    return out


def _union_issues(per_provider: dict[str, list[dict]]) -> list[dict]:
    """One issue list from every provider that reviewed this PR.

    The A/B ran both providers and then posted only claude's list, so a defect
    only codex found reached nobody and the verdict was computed as if it did
    not exist. Near-duplicates collapse on path and line so two providers that
    found the same defect do not produce two comments, and the more severe of
    the two ratings wins. A finding never collapses against one from its own
    provider: two distinct defects a few lines apart in one review are two
    findings, and merging them would lose one."""
    out: list[dict] = []
    for provider, issues in per_provider.items():
        for issue in issues:
            twin = next((o for o in out
                         if provider not in o["found_by"]
                         and o.get("path") == issue.get("path")
                         and abs((o.get("line") or 0) - (issue.get("line") or 0)) <= 5), None)
            if twin is None:
                out.append({**issue, "provider": provider, "found_by": [provider]})
                continue
            twin["found_by"] = sorted(set(twin["found_by"] + [provider]))
            if issue.get("severity") == "blocking" and twin.get("severity") != "blocking":
                twin["severity"] = "blocking"
                twin["body"] = issue.get("body", twin.get("body", ""))
    return out


VALIDATE_PROMPT = (
    "You are auditing a code review comment for correctness. The comment is presumed correct. "
    "Overturn it only when the code below proves it wrong.\n\n"
    "A comment is a false positive only when one specific line in the context makes the reported "
    "failure impossible:\n"
    "- a guard clause, early return, or type check before the flagged line\n"
    "- type narrowing that guarantees the value is defined at the flagged line\n"
    "- surrounding logic that already handles the reported case\n"
    "- an initialization or assignment in a higher scope that the reviewer missed\n\n"
    "You must cite that line by its number from the context. No citation means no false positive.\n"
    "The context is a fixed window around the flagged line. Code outside it is not evidence. "
    "A concern you cannot check here is NOT a false positive: a missing caller, an untested path, a "
    "missing permission class, a migration effect, a design objection. Return 'valid' for those and "
    "let the author answer.\n"
    "A comment that describes the code correctly is 'valid' even when you would not have raised it, "
    "and even when it prescribes a fix you would write differently. You judge the claim, not the tone "
    "and not the severity. Every comment opens with a question to the author. That is the house "
    "style, not an admission of doubt; judge the claim the rest of the comment makes.\n\n"
    "Return ONLY a JSON object (no markdown fences):\n"
    '{"decision":"valid"|"false_positive"|"uncertain","defeating_line":<line number from the context, or null>,"reason":"one sentence"}\n'
)


def _read_function_context(worktree: Path, file_path: str, target_line: int) -> str:
    fp = worktree / file_path
    if not fp.is_file():
        return ""
    try:
        lines = fp.read_text().splitlines()
    except (OSError, UnicodeDecodeError):
        return ""
    start = max(0, target_line - 60)
    end = min(len(lines), target_line + 60)
    numbered = [f"{i+1}: {lines[i]}" for i in range(start, end)]
    return "\n".join(numbered)


def _cited_context_line(value, context: str) -> int | None:
    """The auditor may only drop an issue when it cites a line that is really in
    the context window it was shown. An uncited or invented line number means
    the claim is unproven, so the issue survives."""
    try:
        num = int(value)
    except (TypeError, ValueError):
        return None
    prefix = f"{num}: "
    for line in context.splitlines():
        if line.startswith(prefix):
            return num
    return None


def _validate_single(args):
    issue, worktree = args
    path = issue.get("path")
    line = issue.get("line")
    try:
        line = int(line) if line is not None else None
    except (TypeError, ValueError):
        line = None
    if not path or not line or not worktree:
        return issue

    context = _read_function_context(worktree, path, line)
    if not context:
        fp = worktree / path
        if fp.is_file():
            try:
                total = len(fp.read_text().splitlines())
            except (OSError, UnicodeDecodeError):
                total = 0
            if total and line > total:
                log.emit("review_line_out_of_range",
                         f"{path}:{line} is past the end of the file ({total} lines); "
                         "the comment skipped validation and would anchor nowhere",
                         meta={"path": path, "line": line, "file_lines": total,
                               "severity": issue.get("severity", ""), "body": issue["body"]})
        return issue

    prompt = (
        f"{VALIDATE_PROMPT}"
        f"REVIEW COMMENT (severity: {issue.get('severity', 'unknown')}):\n{issue['body']}\n\n"
        f"FILE: {path}, LINE: {line}\n\n"
        f"CODE CONTEXT:\n{context}\n"
    )
    output = run_balanced(prompt, timeout=120)
    if not output:
        return issue

    data = extract_json(output)
    if not data:
        return issue

    if data.get("decision") != "false_positive":
        return issue

    reason = data.get("reason", "")
    cited = _cited_context_line(data.get("defeating_line"), context)
    if cited is None:
        log.emit("review_validation_kept",
                 f"Kept {path}:{line}: false_positive claim cites no line in the context — {reason}",
                 meta={"path": path, "line": line, "body": issue["body"], "reason": reason,
                       "defeating_line": data.get("defeating_line")})
        return issue
    log.emit("review_validation_dropped", f"Dropped: {path}:{line} — {reason} (line {cited})",
        meta={"path": path, "line": line, "body": issue["body"], "reason": reason,
              "defeating_line": cited})
    return None


def _validate_issues(issues: list[dict], worktree: Path | None) -> list[dict]:
    if not worktree:
        return issues
    tasks = [(issue, worktree) for issue in issues]
    with ThreadPoolExecutor(max_workers=5) as pool:
        results = list(pool.map(_validate_single, tasks))
    return [r for r in results if r is not None]


REWRITE_INTRO = (
    "Rewrite this code review comment in the house voice below. Keep the defect and the reason it "
    "matters. Drop the implementation plan, the test request, and any sentence that only restates "
    "the code. Keep the specifics that make the finding real: the triggering input and the concrete "
    "consequence.\n\n"
)

REWRITE_EXAMPLE = (
    "\nBad: \"`usePacedDisplayMessages` starts revealing text solely from chat-stream state and "
    "never consumes the playback-start state emitted by `HeyGenAvatarStream`. Gate the reveal timer "
    "on the playback-start signal and add a test that the transcript stays hidden during TTS "
    "generation.\"\n"
    "Good: \"Can we wait to reveal the text until the audio actually starts? Right now the timer "
    "starts as soon as text streams in, so the transcript gets ahead of ElevenLabs and misses the "
    "main goal of this ticket.\"\n"
    "\nReturn ONLY the rewritten comment.\n"
)


def _simplify_body(body: str) -> str:
    output = run_balanced(REWRITE_INTRO + HOUSE_VOICE + REWRITE_EXAMPLE + f"\n{body}", timeout=120)
    return output if output else body


def _simplify_body_with_context(body: str, code_context: str | None, file_path: str, line_num: int) -> str:
    context_section = ""
    if code_context:
        context_section = f"\nCode context (line {line_num} in {file_path}):\n```\n{code_context}\n```\n"

    output = run_balanced(
        REWRITE_INTRO + HOUSE_VOICE + REWRITE_EXAMPLE
        + f"{context_section}\nReview comment to rewrite:\n{body}",
        timeout=120,
    )
    return output if output else body


def build_walkthrough_context(comment: dict, worktree: Path | None) -> dict:
    file_path = comment.get("path", "")
    line = int(comment.get("line", 0) or 0)

    if not worktree or not file_path or line <= 0:
        return {"explanation": comment.get("body", ""), "snippets": []}

    raw_context = _read_function_context(worktree, file_path, line)
    if not raw_context:
        return {"explanation": comment.get("body", ""), "snippets": []}

    severity = comment.get("severity", "suggestion")
    body = comment.get("body", "")
    prompt = (
        "You are helping a code reviewer deeply understand a review comment.\n\n"
        f"COMMENT:\nFile: {file_path}:{line}\nSeverity: {severity}\n\"{body}\"\n\n"
        f"CODE CONTEXT (±60 lines, line numbers on left):\n{raw_context}\n\n"
        "Tasks:\n"
        '1. Classify: "local" (style/naming, stays within one expression), "behavioral" (logic/control flow, needs full function), or "contract" (interface boundary, caller/callee mismatch)\n'
        "2. Identify ALL relevant code blocks in the provided context (may be multiple if the comment touches on multiple areas).\n"
        "3. For each block, extract it with role labels:\n"
        "   - 'flagged': the primary commented lines\n"
        "   - 'related': other relevant code in the same file that relates to the comment (e.g., another use of the same variable, another function handling the same case)\n"
        "4. For each block, write a specific 1-2 sentence explanation of how THIS BLOCK is relevant to the comment. Be thorough but concise.\n"
        "5. Write a 3-5 sentence overall explanation: what does this code do, why does the comment matter architecturally, what's the risk if not addressed. Do NOT prescribe a fix.\n\n"
        'Return ONLY valid JSON (no markdown): {"comment_type":"local|behavioral|contract","explanation":"overall explanation","snippets":[{"role":"flagged|related","code":"lines preserving NUM: text format","start_line":<int>,"explanation":"how this block relates to the comment"}]}'
    )

    result = run_haiku(prompt)
    if not result:
        return {"explanation": body, "snippets": []}

    parsed = extract_json(result)
    if not parsed:
        return {"explanation": body, "snippets": []}

    return {
        "explanation": parsed.get("explanation", body),
        "snippets": parsed.get("snippets", []),
    }


def _simplify_all_issues(issues: list[dict]) -> list[dict]:
    with ThreadPoolExecutor(max_workers=10) as pool:
        bodies = list(pool.map(lambda i: _simplify_body(i["body"]), issues))
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

    base_branch = base_branch_for(config, pr["repo"])
    return git_util.add_or_reuse_worktree(repo_path, worktree_path, pr["branch"], base_branch=base_branch)


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


def _changed_line_count(diff_text: str) -> int:
    return sum(1 for line in diff_text.splitlines()
               if line[:1] in ("+", "-") and not line.startswith(("+++", "---")))


def _extract_ticket_from_pr(pr: dict, ticket_state: dict) -> str | None:
    repo = pr.get("repo")
    pr_id = pr.get("id")
    if not repo or not pr_id:
        return None

    for ticket_key, ticket_data in ticket_state.items():
        prs = ticket_data.get("prs", [])
        for ticket_pr in prs:
            if ticket_pr.get("repo") == repo and ticket_pr.get("id") == pr_id:
                return ticket_key

    branch = pr.get("branch", "")
    if branch:
        match = re.search(r"(?i)\b([a-z]+-\d+)", branch)
        if match:
            return match.group(1).upper()

    return None


def _pr_needs_tracking(review_state: dict, pr: dict) -> bool:
    pr_key = f"{pr.get('repo')}/{pr.get('id')}"
    existing = review_state.get(pr_key, {})
    if existing.get("skipped"):
        return (pr.get("head_sha") or "") != (existing.get("skipped_head_sha") or "")
    if not existing.get("reviewed"):
        return True

    current_head = pr.get("head_sha") or ""
    previous_head = existing.get("last_head_sha") or ""
    if current_head and current_head != previous_head:
        return True

    return False


def _track_pending_prs(pending: dict, prs: list[dict], ticket_state: dict, review_state: dict) -> dict:
    now = time.time()
    for pr in prs:
        if not _pr_needs_tracking(review_state, pr):
            continue

        ticket = _extract_ticket_from_pr(pr, ticket_state)
        if ticket is None:
            ticket = "__no_ticket__"

        if ticket not in pending:
            pending[ticket] = {
                "tracked_at": now,
                "last_pr_at": now,
                "prs": [pr]
            }
        else:
            pr_key = (pr.get("repo"), pr.get("id"))
            existing_keys = {(p.get("repo"), p.get("id")) for p in pending[ticket]["prs"]}
            if pr_key not in existing_keys:
                pending[ticket]["prs"].append(pr)
                pending[ticket]["last_pr_at"] = now
                pending[ticket].pop("retry_after", None)

    return pending


def _process_ready_tickets(config: dict, pending: dict) -> dict:
    now = time.time()
    ready_tickets = []

    for ticket_key in list(pending.keys()):
        retry_after = pending[ticket_key].get("retry_after", 0)
        if retry_after and now < retry_after:
            continue
        last_pr_at = pending[ticket_key].get("last_pr_at", now)
        quiet_seconds = now - last_pr_at
        if quiet_seconds >= 900:
            ready_tickets.append(ticket_key)

    for ticket_key in ready_tickets:
        prs = pending[ticket_key].get("prs", [])
        if prs:
            failed_prs = review_ticket_prs(config, ticket_key, prs)
            if failed_prs:
                pending[ticket_key]["prs"] = failed_prs
                pending[ticket_key]["retry_after"] = now + REVIEW_RETRY_COOLDOWN_SECONDS
                pending[ticket_key]["last_attempt_at"] = now
                continue
        del pending[ticket_key]

    return pending


SIBLING_DIFF_CHAR_CAP = 60000


def _fetch_ticket_diffs(platform, prs: list[dict]) -> dict[str, str]:
    diffs = {}
    for pr in prs:
        try:
            diffs[f"{pr['repo']}/{pr['id']}"] = platform.get_pr_diff(pr["repo"], pr["id"]) or ""
        except Exception:
            diffs[f"{pr['repo']}/{pr['id']}"] = ""
    return diffs


def _oversized_prs(config: dict, ticket_key: str, prs: list[dict],
                   diffs: dict[str, str]) -> list[dict]:
    """The PRs an automatic review must not fire on. A ticket's PRs go into one
    prompt, so they pass or fail the cap together. __no_ticket__ PRs are
    unrelated, so each one is measured on its own."""
    cap = _max_changed_lines(config)
    if cap <= 0:
        return []
    if ticket_key == "__no_ticket__":
        return [p for p in prs
                if _changed_line_count(diffs.get(f"{p['repo']}/{p['id']}", "")) > cap]
    total = sum(_changed_line_count(diffs.get(f"{p['repo']}/{p['id']}", "")) for p in prs)
    return list(prs) if total > cap else []


def _record_oversized_skip(config: dict, ticket_key: str, prs: list[dict],
                           diffs: dict[str, str], review_state: dict) -> None:
    cap = _max_changed_lines(config)
    lines = sum(_changed_line_count(diffs.get(f"{p['repo']}/{p['id']}", "")) for p in prs)
    files = sum(len(_extract_changed_paths(diffs.get(f"{p['repo']}/{p['id']}", ""))) for p in prs)
    label = (ticket_key if ticket_key != "__no_ticket__"
             else ", ".join(f"{p['repo']}#{p['id']}" for p in prs))
    log.emit("review_skipped_too_large",
             f"{label}: {lines} changed lines in {files} files is over the {cap} line "
             "auto-review cap. The review did not run. Start it by hand to review it anyway.",
             links={"detail": f"{config['_base_url']}/reviews"},
             meta={"ticket": ticket_key, "changed_lines": lines, "files": files, "cap": cap,
                   "prs": [f"{p['repo']}/{p['id']}" for p in prs]})
    now = time.time()
    for pr in prs:
        pr_key = f"{pr['repo']}/{pr['id']}"
        entry = dict(review_state.get(pr_key, {}))
        entry.update({
            "skipped": "too_large",
            "skipped_head_sha": pr.get("head_sha", ""),
            "skipped_at": now,
            "branch": pr.get("branch", ""),
            "ticket": ticket_key,
        })
        review_state[pr_key] = entry


def _ticket_context_for(config, pr: dict, ticket_key: str, prs: list[dict],
                        diffs: dict[str, str]) -> str:
    goal = presentation.resolve_ticket_goal(config, pr.get("branch", ""), pr["repo"], pr["id"])
    parts = []
    if goal:
        label = ticket_key if ticket_key != "__no_ticket__" else "unknown"
        parts.append(f"Ticket {label}:\n{goal}\n")
    if ticket_key == "__no_ticket__":
        return "\n".join(parts)
    for sibling in prs:
        if sibling.get("repo") == pr.get("repo") and sibling.get("id") == pr.get("id"):
            continue
        d = diffs.get(f"{sibling['repo']}/{sibling['id']}", "")
        if len(d) > SIBLING_DIFF_CHAR_CAP:
            d = d[:SIBLING_DIFF_CHAR_CAP] + "\n... [diff truncated]"
        parts.append(
            f"=== Sibling PR #{sibling['id']} in '{sibling['repo']}' "
            f"(branch: {sibling.get('branch', '')}) ===\n{d or '[diff unavailable]'}\n")
    return "\n".join(parts)


def _build_ticket_persona_prompt(persona_text, ticket_key, goal, sections, has_tools):
    parts = [
        f"You are reviewing ALL the pull requests of ticket {ticket_key} together, as one change. "
        "The PRs may span multiple repositories; each section below names that PR's diff file and "
        "its checkout. Review the change as a whole: a requirement satisfied in any of the PRs is "
        "satisfied, and inconsistencies between PRs (mismatched API contracts, producer/consumer "
        "drift) are issues.\n",
        persona_text + "\n",
        JSON_OUTPUT_SCHEMA, SEVERITY_RULES, LINE_NUMBER_RULES, BODY_RULES,
        'Every issue MUST carry a "repo" field naming the repository it anchors to, and its "path" '
        "must be a file present in that repository's diff.\n",
    ]
    if goal:
        parts.append(f"--- TICKET GOAL ---\n{goal}\n--- END TICKET GOAL ---\n")
    if has_tools:
        parts.append(TICKET_TOOL_USE_RULES)
        parts.append("Your working directory is the parent of every checkout, so address the "
                     "checkouts and the diff files by the absolute paths given below and run git "
                     "as `git -C <checkout> ...`.\n")
    parts.append("No diff is pasted into this prompt. Read every diff file named below with your "
                 "tools before you report anything, and page through all of each one.\n")
    parts.extend(sections)
    parts.append("\nIMPORTANT: Explore first, then answer. Your FINAL message must be the JSON "
                 "object and nothing else. No summary, no explanation, no markdown fences.")
    return "\n".join(parts)


def _split_issues_by_pr(issues: list[dict], prs: list[dict], diffs: dict[str, str]) -> dict[str, list[dict]]:
    """Attribute each merged issue to one PR: by its 'repo' field when the path
    matches that repo's diff, else by unique path match across diffs. Issues
    that fit nowhere are dropped (logged) rather than posted to the wrong PR."""
    paths_by_key = {
        f"{p['repo']}/{p['id']}": set(_extract_changed_paths(diffs.get(f"{p['repo']}/{p['id']}", "")))
        for p in prs
    }
    key_by_repo = {p["repo"]: f"{p['repo']}/{p['id']}" for p in prs}
    out: dict[str, list[dict]] = {k: [] for k in paths_by_key}
    for issue in issues:
        path = issue.get("path") or ""
        key = key_by_repo.get(issue.get("repo") or "")
        if key and (not path or path in paths_by_key[key]):
            out[key].append(issue)
            continue
        matches = [k for k, ps in paths_by_key.items() if path and path in ps]
        if len(matches) == 1:
            out[matches[0]].append(issue)
            continue
        if key:
            out[key].append(issue)
            continue
        log.emit("review_issue_unattributed",
                 f"Dropped review issue that matched no PR: {path or '(no path)'}",
                 meta={"path": path, "repo": issue.get("repo", ""), "body": issue.get("body", "")[:200]})
    return out


def _reviewed_sibling_sections(config: dict, platform, ticket_key: str,
                               active_keys: set[str]) -> list[str]:
    """Context-only prompt sections for the ticket's already-reviewed open PRs.
    A PR reviewed in an earlier batch is not re-reviewed, but its diff rejoins
    the joint prompt so cross-PR inconsistencies with the PRs under review
    still surface."""
    review_state = state.load("reviews")
    sections = []
    for key, entry in review_state.items():
        if entry.get("ticket") != ticket_key or not entry.get("reviewed"):
            continue
        if key in active_keys:
            continue
        repo, _, pr_id_str = key.rpartition("/")
        try:
            pr_id = int(pr_id_str)
        except ValueError:
            continue
        try:
            info = platform.get_pr_info(repo, pr_id) or {}
        except Exception:
            continue
        if info.get("state") != "OPEN":
            continue
        diff = platform.get_pr_diff(repo, pr_id) or ""
        if not diff:
            continue
        diff_path = _stage_diff(
            _review_dir(config, {"repo": repo, "id": pr_id, "branch": entry.get("branch")}), diff)
        sections.append(
            f"=== ALREADY-REVIEWED PR #{pr_id} in repository '{repo}' (context only) ===\n"
            "This sibling PR of the same ticket was reviewed in an earlier batch. Do NOT "
            "raise issues against its files. Use it as context for the PRs under review: "
            "flag any inconsistency between them and this PR (mismatched API contracts, "
            "producer/consumer drift) as an issue on the PR under review.\n"
            f"diff file ({key}): {diff_path}\n"
            + "\n".join(_extract_changed_paths(diff)))
    return sections


def review_ticket(config: dict, ticket_key: str, prs: list[dict],
                  diffs: dict[str, str] | None = None) -> dict[str, dict | None]:
    """Single persona pass over the combined diffs of all the ticket's PRs.
    Returns {repo/id: per-PR merged review or None} and writes each PR's
    review artifacts, so the per-PR pages and comment queues work unchanged."""
    platform = make_platform(config)
    if diffs is None:
        diffs = _fetch_ticket_diffs(platform, prs)
    none_result: dict[str, dict | None] = {f"{p['repo']}/{p['id']}": None for p in prs}
    live = [p for p in prs if diffs.get(f"{p['repo']}/{p['id']}")]
    if not live:
        return none_result

    goal = presentation.resolve_ticket_goal(
        config, live[0].get("branch", ""), live[0]["repo"], live[0]["id"])
    worktrees, sections = {}, []
    for pr in live:
        key = f"{pr['repo']}/{pr['id']}"
        wt = _ensure_review_worktree(config, pr)
        worktrees[key] = wt
        conv = _load_conventions(config, pr["repo"])
        diff_path = _stage_diff(_review_dir(config, pr), diffs[key])
        sec = [f"=== PR #{pr['id']} in repository '{pr['repo']}' (branch: {pr.get('branch', '')}) ==="]
        if wt:
            sec.append(f"worktree (read-only checkout): {wt}")
        if conv:
            sec.append(f"--- CONVENTIONS ({pr['repo']}) ---\n{conv}\n--- END CONVENTIONS ---")
        sec.append(f"diff file ({key}): {diff_path}")
        sec.append(f"--- FILES CHANGED ({key}) ---\n"
                   + "\n".join(_extract_changed_paths(diffs[key]))
                   + f"\n--- END FILES CHANGED ({key}) ---")
        sections.append("\n".join(sec))

    has_tools = any(worktrees.values())
    cwd = config["_state_dir"] / "reviews"
    cwd.mkdir(parents=True, exist_ok=True)
    sections.extend(_reviewed_sibling_sections(config, platform, ticket_key,
                                               {f"{p['repo']}/{p['id']}" for p in live}))
    providers = _review_providers(config)
    prompts = {name: _build_ticket_persona_prompt(text, ticket_key, goal, sections, has_tools)
               for name, text in PERSONAS.items()}
    by_provider = _run_personas_for_providers(
        prompts, providers, worktree=cwd,
        model=_reviewer_model(config), run_key=ticket_key)

    results: dict[str, dict | None] = dict(none_result)
    by_pr: dict[str, dict[str, list[dict]]] = {f"{p['repo']}/{p['id']}": {} for p in live}
    shared_by_provider: dict[str, dict] = {}
    for provider, presults in by_provider.items():
        successful = [(name, data) for name, data in presults if data is not None]
        if not successful:
            log.emit("review_provider_empty",
                     f"{ticket_key}: no valid {provider} persona output",
                     meta={"ticket": ticket_key, "provider": provider})
            continue
        merged = _merge_reviews(successful)
        issues_by_key = _split_issues_by_pr(merged.get("issues", []), live, diffs)
        shared = {k: v for k, v in merged.items() if k != "issues"}
        shared_by_provider[provider] = shared
        for pr in live:
            key = f"{pr['repo']}/{pr['id']}"
            issues = issues_by_key.get(key, [])
            if issues:
                issues = _validate_issues(issues, worktrees[key])
                issues = _simplify_all_issues(issues)
            by_pr[key][provider] = issues
            if provider != "claude":
                _write_review_artifacts(config, pr, {
                    **shared,
                    "issues": issues,
                    "verdict": "changes_requested" if any(i.get("severity") == "blocking" for i in issues) else "approved",
                    "source_branch": pr.get("branch") or shared.get("source_branch", ""),
                }, diffs[key], provider=provider)

    if not shared_by_provider:
        return none_result
    primary = shared_by_provider.get("claude") or next(iter(shared_by_provider.values()))
    for pr in live:
        key = f"{pr['repo']}/{pr['id']}"
        if not by_pr[key]:
            continue
        issues = _union_issues(by_pr[key])
        blocking = [i for i in issues if i.get("severity") == "blocking"]
        only_other = sorted({p for i in blocking for p in i["found_by"]} - {"claude"})
        if blocking and only_other and not any("claude" in i["found_by"] for i in blocking):
            log.emit("review_provider_only_blocker",
                     f"{key}: every blocking finding came from {', '.join(only_other)}, not claude",
                     meta={"repo": pr["repo"], "pr_id": pr["id"], "ticket": ticket_key,
                           "providers": only_other, "blocking": len(blocking)})
        results[key] = {
            **primary,
            "issues": issues,
            "verdict": "changes_requested" if blocking else "approved",
            "source_branch": pr.get("branch") or primary.get("source_branch", ""),
        }
        _write_review_artifacts(config, pr, results[key], diffs[key], provider="claude")
    if all(v is None for v in results.values()):
        return none_result
    return results


def review_ticket_prs(config: dict, ticket_key: str, prs: list[dict],
                      auto: bool = True) -> list[dict]:
    platform = make_platform(config)
    review_state = state.load("reviews")
    base_url = config["_base_url"]
    failed_prs: list[dict] = []

    diffs = _fetch_ticket_diffs(platform, prs)
    if auto:
        oversized = _oversized_prs(config, ticket_key, prs, diffs)
        if oversized:
            _record_oversized_skip(config, ticket_key, oversized, diffs, review_state)
            state.save("reviews", review_state)
            oversized_keys = {(p["repo"], p["id"]) for p in oversized}
            prs = [p for p in prs if (p["repo"], p["id"]) not in oversized_keys]
            if not prs:
                return []

    if ticket_key == "__no_ticket__":
        results = {}
        for pr in prs:
            pr_key = f"{pr['repo']}/{pr['id']}"
            re_review = review_state.get(pr_key, {}).get("reviewed", False)
            label = "Re-reviewing" if re_review else "Reviewing"
            log.emit("review_started", f"{label} PR #{pr['id']} in {pr['repo']}",
                links={"pr": pr["url"], "detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
                meta={"repo": pr["repo"], "pr_id": pr["id"], "ticket": ticket_key, "re_review": re_review})
            ticket_context = _ticket_context_for(config, pr, ticket_key, prs, diffs)
            results[pr_key] = review_pr(config, platform, pr, ticket_context=ticket_context,
                                        prefetched_diff=diffs.get(pr_key, ""))
    else:
        log.emit("review_started",
            f"Reviewing ticket {ticket_key}: {len(prs)} PR(s) as one change",
            links={"detail": f"{base_url}/reviews/{prs[0]['repo']}/{prs[0]['id']}"},
            meta={"ticket": ticket_key, "prs": [f"{p['repo']}/{p['id']}" for p in prs]})
        results = review_ticket(config, ticket_key, prs, diffs=diffs)

    for pr in prs:
        pr_key = f"{pr['repo']}/{pr['id']}"
        head_sha = pr.get("head_sha", "")
        result = results.get(pr_key)
        if result:
            review_state[pr_key] = {
                "reviewed": True,
                "branch": pr["branch"],
                "ticket": ticket_key,
                "reviewed_at": time.time(),
                "last_updated": pr.get("updated_on"),
                "last_head_sha": head_sha,
            }
            state.save("reviews", review_state)
            if presentation.enabled(config):
                try:
                    presentation.spawn_review_build(config, pr["repo"], pr["id"],
                                                    head_sha=head_sha, auto=True)
                except Exception as e:
                    log.emit("presentation_trigger_failed",
                             f"{pr['repo']}#{pr['id']}: {type(e).__name__}: {e}",
                             meta={"repo": pr["repo"], "pr_id": pr["id"]})
            issues = result.get("issues", [])
            log.emit("review_complete", f"{pr['repo']}#{pr['id']}: Review done — {result.get('verdict', 'unknown')}, {len(issues)} issues",
                links={"pr": pr["url"], "detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
                meta={"repo": pr["repo"], "pr_id": pr["id"], "ticket": ticket_key, "verdict": result.get("verdict"), "issue_count": len(issues)})

            if issues:
                log.emit("review_comments_queued", f"{pr['repo']}#{pr['id']}: {len(issues)} comments ready to submit",
                    links={"detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
                    meta={"repo": pr["repo"], "pr_id": pr["id"], "ticket": ticket_key})
        else:
            log.emit("review_failed", f"Review failed for {pr['repo']}#{pr['id']} (cooldown before retry)",
                links={"pr": pr["url"], "detail": f"{base_url}/reviews/{pr['repo']}/{pr['id']}"},
                meta={"repo": pr["repo"], "pr_id": pr["id"], "ticket": ticket_key})
            failed_prs.append(pr)

    return failed_prs
