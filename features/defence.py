"""Substantiate a claim about a branch before the claim is offered as a reply.

A drafted reply is only worth as much as the evidence behind it. features/tickets
drafts replies to reviewer comments with a single model call and no verification,
so the author re-checks every one by hand. This module turns a claim into a named
test, runs that test twice inside one scratch checkout, and accepts the claim only
when the test passes with the branch's source and fails without it.

The reverse-patch differential is what makes the check honest. The obvious
alternative — run at HEAD, then run at origin/<base> — accepts tests that pass for
the wrong reason. A test can observe the revision instead of the behaviour
(`git rev-parse HEAD != origin/main` passes that gate and measures nothing), and a
test file added on the branch fails at base because it is absent rather than
because the behaviour is missing. Reversing only the source hunks inside a single
checkout holds the commit sha, the paths and the dependencies identical across
both runs, so the source change is the only thing that differs.

The model never emits a command. It names a test that already exists in the tree.
frshty builds the command from the repo's own runner. That is what keeps arbitrary
shell out of the loop.
"""
import json
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, asdict
from pathlib import Path

import core.log as log
from core.claude_runner import run_balanced, extract_json
from core.deps import relink_shared_venv
from core.llm import run_external_model


SUBSTANTIATED = "SUBSTANTIATED"
REFUTED = "REFUTED"
INCONCLUSIVE = "INCONCLUSIVE"
PENDING = "PENDING"

TEST_TIMEOUT = 600
PROPOSE_TIMEOUT = 180
JUDGE_TIMEOUT = 300

_TEST_PATH_PATTERNS = (
    re.compile(r"(^|/)tests?/"),
    re.compile(r"(^|/)__tests__/"),
    re.compile(r"(^|/)test_[^/]+\.py$"),
    re.compile(r"_test\.py$"),
    re.compile(r"\.(test|spec)\.[jt]sx?$"),
)


@dataclass
class DefenceResult:
    claim: str
    verdict: str = INCONCLUSIVE
    reason: str = ""
    test_id: str = ""
    with_change_exit: int | None = None
    without_change_exit: int | None = None
    with_change_output: str = ""
    without_change_output: str = ""
    judge: str = ""
    judge_verdict: str = ""
    head_sha: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def is_test_path(rel_path: str) -> bool:
    """True when a repo-relative path holds tests rather than source."""
    return any(p.search(rel_path) for p in _TEST_PATH_PATTERNS)


def detect_runner(worktree: Path, test_file: str, test_name: str) -> list[str] | None:
    """Build the command that runs one named test, using the repo's own runner.

    Returns None when no runner can be identified. The command is built here and
    never by a model, so a proposal cannot smuggle in arbitrary shell.
    """
    if test_file.endswith(".py"):
        if (worktree / "Pipfile").exists():
            return ["pipenv", "run", "pytest", f"{test_file}::{test_name}", "-x", "-q"]
        venv_python = worktree / ".venv" / "bin" / "python"
        interpreter = str(venv_python) if venv_python.exists() else sys.executable
        return [interpreter, "-m", "pytest", f"{test_file}::{test_name}", "-x", "-q"]
    if re.search(r"\.[jt]sx?$", test_file):
        pkg = worktree / "package.json"
        if not pkg.exists():
            return None
        try:
            scripts = json.loads(pkg.read_text()).get("scripts", {}) or {}
        except (OSError, ValueError):
            scripts = {}
        blob = " ".join(str(v) for v in scripts.values())
        if "vitest" in blob:
            return ["npx", "vitest", "run", test_file, "-t", test_name]
        if "jest" in blob:
            return ["npx", "jest", test_file, "-t", test_name]
        return None
    return None


def source_diff(worktree: Path, base_branch: str, head: str = "HEAD") -> str:
    """The branch's diff against its base, with test files excluded.

    Excluding tests is deliberate. The differential run must keep the branch's
    test in place while removing the source it exercises; reversing the test
    itself would delete the probe along with the behaviour.

    `head` is pinned by the caller so the diff, the checkouts and the recorded sha
    all describe one commit. Resolving HEAD separately per command lets a commit
    landing mid-run leave the three disagreeing.
    """
    merged = subprocess.run(
        ["git", "merge-base", f"origin/{base_branch}", head],
        cwd=str(worktree), capture_output=True, text=True, timeout=60)
    merge_base = (merged.stdout or "").strip()
    if merged.returncode != 0 or not merge_base:
        return ""
    listed = subprocess.run(
        ["git", "diff", "--name-only", merge_base, head],
        cwd=str(worktree), capture_output=True, text=True, timeout=60)
    if listed.returncode != 0:
        return ""
    sources = [n for n in listed.stdout.splitlines() if n.strip() and not is_test_path(n.strip())]
    if not sources:
        return ""
    produced = subprocess.run(
        ["git", "diff", "--binary", "--full-index", merge_base, head, "--", *sources],
        cwd=str(worktree), capture_output=True, text=True, timeout=120)
    return produced.stdout if produced.returncode == 0 else ""


_NOT_AN_ASSERTION = (
    "ERROR collecting", "INTERNALERROR", "ImportError", "ModuleNotFoundError",
    "no tests ran", "No test files found", "usage:", "unrecognized arguments",
    "SyntaxError", "fatal:",
)


def _is_assertion_failure(exit_code: int | None, output: str) -> tuple[bool, str]:
    """Whether the second run failed because the assertion failed.

    Any non-zero exit used to count as evidence. It should not. A timeout, an
    import error, a collection error, pytest's exit 4 for a usage mistake and its
    exit 5 for "nothing collected" all produce a non-zero code without the test
    ever deciding anything. Only exit 1 means the test ran and its assertion
    failed, and even then a collection error can masquerade as one.
    """
    if exit_code is None:
        return False, "the run without the change did not finish, so it proves nothing"
    if exit_code == 0:
        return False, ("the test passes with the source change reversed, so it does not "
                       "measure the change")
    if exit_code != 1:
        return False, (f"the run without the change exited {exit_code}, which is a runner "
                       "error rather than a failed assertion")
    for marker in _NOT_AN_ASSERTION:
        if marker in output:
            return False, (f"the run without the change reported '{marker}', which is a "
                           "setup failure rather than a failed assertion")
    return True, ""


def _run_test(cmd: list[str], cwd: Path) -> tuple[int | None, str]:
    try:
        r = subprocess.run(cmd, cwd=str(cwd), capture_output=True, text=True,
                           timeout=TEST_TIMEOUT)
    except subprocess.TimeoutExpired:
        return None, "timed out"
    except OSError as e:
        return None, f"{type(e).__name__}: {e}"
    return r.returncode, ((r.stdout or "") + (r.stderr or ""))[-4000:]


_PROPOSE_PROMPT = """A reviewer asked a question about a pull request. The author's answer is the CLAIM below.
Your job is to name ONE test, already present in this branch, whose result decides whether the claim is true.

The test must exercise the behaviour the claim describes. It must not merely assert that a file changed,
that a revision differs, or that something imports cleanly.

If no existing test decides the claim, say so. Do not invent a test that does not exist.

CLAIM:
{claim}

BRANCH DIFF (source files only):
{diff}

TEST FILES PRESENT IN THIS BRANCH:
{test_files}

Reply with JSON only:
{{"can_prove": true, "test_file": "<repo-relative path>", "test_name": "<exact test function or it() name>", "why": "<one sentence>"}}
or
{{"can_prove": false, "reason": "<one sentence>"}}
"""


_JUDGE_PROMPT = """You are judging whether captured test output substantiates a claim. Be adversarial.

You are given the claim, the branch diff, and the output of the SAME named test run twice: once with the
branch's source changes in place, and once with those source changes reversed inside the same checkout.
The commit, the paths and the dependencies were identical in both runs.

Decide whether that evidence establishes the claim.

Answer REFUTED if the evidence contradicts the claim.
Answer INCONCLUSIVE if the test does not measure what the claim says, or the output is empty or unrelated.
Answer SUBSTANTIATED only when the output shows the claimed behaviour present with the change and absent without it.
Uncertainty is INCONCLUSIVE, never SUBSTANTIATED.

CLAIM:
{claim}

BRANCH DIFF:
{diff}

TEST RUN WITH THE CHANGE (exit {with_exit}):
{with_output}

TEST RUN WITH THE CHANGE REVERSED (exit {without_exit}):
{without_output}

Reply with JSON only:
{{"verdict": "SUBSTANTIATED|REFUTED|INCONCLUSIVE", "reason": "<one sentence>"}}
"""


def _list_test_files(worktree: Path, limit: int = 400) -> list[str]:
    out = subprocess.run(["git", "ls-files"], cwd=str(worktree),
                         capture_output=True, text=True, timeout=60).stdout
    return [p for p in out.splitlines() if p and is_test_path(p)][:limit]


def _propose_test(claim: str, diff: str, test_files: list[str]) -> dict | None:
    raw = run_balanced(_PROPOSE_PROMPT.format(
        claim=claim, diff=diff[:12000], test_files="\n".join(test_files)),
        timeout=PROPOSE_TIMEOUT)
    if not raw:
        return None
    parsed = extract_json(raw)
    return parsed if isinstance(parsed, dict) else None


def _judge_blind(config: dict, claim: str, diff: str, result: DefenceResult,
                 worktree: Path) -> tuple[str, str, str]:
    """Ask an independent model whether the captured output establishes the claim.

    The judge never sees the proposing model's rationale. It sees the claim, the
    diff and the two captured outputs. A different vendor is used when one is
    configured, because two calls to the same provider are not an independent
    second opinion.
    """
    prompt = _JUDGE_PROMPT.format(
        claim=claim, diff=diff[:8000],
        with_exit=result.with_change_exit, with_output=result.with_change_output,
        without_exit=result.without_change_exit, without_output=result.without_change_output,
    )
    text, _ = run_external_model(
        ["codex", "exec", "--skip-git-repo-check", prompt],
        fn_name="defence_judge", model="codex", prompt=prompt,
        cwd=worktree, timeout=JUDGE_TIMEOUT,
    )
    judge = "codex"
    if not text:
        text = run_balanced(prompt, timeout=JUDGE_TIMEOUT) or ""
        judge = "claude"
    parsed = extract_json(text) if text else None
    if not isinstance(parsed, dict):
        return judge, INCONCLUSIVE, "judge returned no parseable verdict"
    verdict = str(parsed.get("verdict", "")).upper()
    if verdict not in (SUBSTANTIATED, REFUTED, INCONCLUSIVE):
        verdict = INCONCLUSIVE
    return judge, verdict, str(parsed.get("reason", ""))[:300]


def substantiate(config: dict, claim: str, repo_name: str, worktree: Path,
                 base_branch: str) -> DefenceResult:
    """Decide whether a claim about this branch is supported by a test.

    Runs one named test in two separate checkouts of the same commit: one as the
    branch stands, one with the branch's source hunks reversed. A claim is
    SUBSTANTIATED only when the test passes with the change, fails by assertion
    without it, and an independent judge accepts the captured output.

    Both checkouts sit at the same pinned commit, so a test that reads the
    revision instead of the behaviour passes in both and proves nothing. They are
    separate directories, so a test cannot leave itself a marker in the first run
    and read it in the second: sharing one directory made that trick produce the
    pass-then-fail pattern for any claim at all.
    """
    result = DefenceResult(claim=claim)
    head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(worktree),
                          capture_output=True, text=True, timeout=30)
    result.head_sha = (head.stdout or "").strip()
    if head.returncode != 0 or not result.head_sha:
        result.reason = "could not resolve the branch head"
        return result

    diff = source_diff(worktree, base_branch, result.head_sha)
    if not diff.strip():
        result.reason = "branch has no source changes against its base"
        return result

    proposal = _propose_test(claim, diff, _list_test_files(worktree))
    if not proposal or not proposal.get("can_prove"):
        result.reason = (proposal or {}).get("reason", "no existing test decides this claim")[:300]
        return result

    test_file = str(proposal.get("test_file", "")).strip().lstrip("/")
    test_name = str(proposal.get("test_name", "")).strip()
    if not test_file or not test_name or not (worktree / test_file).is_file():
        result.reason = f"proposed test does not exist in the branch: {test_file}::{test_name}"
        return result
    if not is_test_path(test_file):
        result.reason = f"proposed probe is not a test file: {test_file}"
        return result
    result.test_id = f"{test_file}::{test_name}"

    scratch = Path(tempfile.mkdtemp(prefix="frshty-defence-"))
    with_dir, without_dir = scratch / "with", scratch / "without"
    try:
        for target in (with_dir, without_dir):
            add = subprocess.run(
                ["git", "worktree", "add", "--detach", str(target), result.head_sha],
                cwd=str(worktree), capture_output=True, text=True, timeout=180)
            if add.returncode != 0:
                result.reason = f"could not create scratch checkout: {(add.stderr or '').strip()[:200]}"
                return result
            relink_shared_venv(config, repo_name, target)

        patch = scratch / "source.patch"
        patch.write_text(diff)
        rev = subprocess.run(["git", "apply", "-R", str(patch)], cwd=str(without_dir),
                             capture_output=True, text=True, timeout=120)
        if rev.returncode != 0:
            result.reason = f"could not reverse the source change: {(rev.stderr or '').strip()[:200]}"
            return result

        cmd = detect_runner(with_dir, test_file, test_name)
        if not cmd:
            result.reason = f"no known test runner for {test_file}"
            return result

        result.with_change_exit, result.with_change_output = _run_test(cmd, with_dir)
        if result.with_change_exit != 0:
            result.reason = "the proposed test does not pass on this branch"
            return result

        without_cmd = detect_runner(without_dir, test_file, test_name)
        result.without_change_exit, result.without_change_output = _run_test(
            without_cmd or cmd, without_dir)
    finally:
        for target in (with_dir, without_dir):
            subprocess.run(["git", "worktree", "remove", "--force", str(target)],
                           cwd=str(worktree), capture_output=True, timeout=120)
        shutil.rmtree(scratch, ignore_errors=True)

    ok, why = _is_assertion_failure(result.without_change_exit, result.without_change_output)
    if not ok:
        result.reason = why
        return result

    judge, verdict, reason = _judge_blind(config, claim, diff, result, worktree)
    result.judge = judge
    result.judge_verdict = verdict
    result.reason = reason
    result.verdict = verdict if verdict in (SUBSTANTIATED, REFUTED) else INCONCLUSIVE
    log.emit("defence_result",
             f"{repo_name}: {result.verdict} for {result.test_id or 'no test'}",
             meta={"repo": repo_name, "verdict": result.verdict, "judge": judge,
                   "test_id": result.test_id})
    return result


def render(result: DefenceResult) -> str:
    """The evidence block appended to a drafted reply.

    The header is machine-readable so a caller can gate on it without parsing
    prose, which is the failure the click-indicator proof.md demonstrated.
    """
    lines = [
        f"VERDICT: {result.verdict}",
        f"CLAIM: {result.claim}",
    ]
    if result.test_id:
        lines.append(f"PROOF: {result.test_id} with_change={result.with_change_exit} "
                     f"without_change={result.without_change_exit}")
    if result.judge:
        lines.append(f"JUDGE: {result.judge}={result.judge_verdict}")
    if result.head_sha:
        lines.append(f"SHA: {result.head_sha}")
    if result.reason:
        lines.append(f"NOTE: {result.reason}")
    return "\n".join(lines)
