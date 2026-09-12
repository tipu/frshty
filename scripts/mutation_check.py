"""Break each gate that lets a ticket reach done, and require the suite to go red.

A passing suite is evidence only when it can fail. This repository holds 43k
lines of tests and features/defence.py already applies that standard to a
drafted reply: a claim is accepted only when a named test passes with the
branch's source hunks and fails with them reverted. The same standard was never
applied to the gates themselves, so a gate whose test has quietly stopped
measuring it reads exactly like a gate that works.

This harness closes that. Every entry in MUTATIONS removes one gate from a
throwaway copy of the tree and names the test that must fail as a result. A
mutation that survives means the named test is not measuring the gate. The run
fails on the first survivor, so the suite cannot go green while a gate is
unguarded.

The patch is a single exact string replacement, and the anchor must occur
exactly once in the file. An anchor that stopped matching after a refactor
would otherwise mutate nothing and count as a kill. The tree is hashed before
and after the edit as a second, independent check on the same failure.

A test that crashes is not a kill. pytest records an unhandled TypeError the
same way it records a decided assertion, so a mutation that makes the target
test explode before it reaches its assertions would otherwise read as evidence.
Only a failure whose message is a decided assertion counts. That is the rule
features/defence.py already applies to the evidence behind a drafted reply.

Only the target test's own file is run per mutant. The whole suite takes two
and a half minutes, and running it once per mutation would put the harness out
of reach of the loop it exists to guard. The control pass runs every distinct
target file first and requires it green, so a mutant's red is attributable to
the mutation.

Usage:
    python3 scripts/mutation_check.py
    python3 scripts/mutation_check.py --only push_gate_tests_ignored
    python3 scripts/mutation_check.py --table alternative_table.json
"""
import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
IGNORE = shutil.ignore_patterns(
    ".git", "__pycache__", "*.pyc", ".venv", "venv", "node_modules",
    ".pytest_cache", ".mypy_cache", ".ruff_cache")
SUITE_TIMEOUT = 900
DECIDED_FAILURE = ("assert", "AssertionError", "Failed")

MUTATIONS = [
    {
        "label": "commit_gate_attribution_blind",
        "gate": "the commit gate refuses a message that credits the agent",
        "target": "tests/features/test_work_commit_gate.py::TestGateCommit"
                  "::test_denies_a_message_that_is_nothing_but_attribution",
        "path": "services/work_launch.py",
        "old": '        found = pattern.search(text or "")',
        "new": "        found = None",
    },
    {
        "label": "commit_gate_shared_checkout_open",
        "gate": "the commit gate refuses a commit into a shared checkout",
        "target": "tests/features/test_work_worktree_gates.py::TestCommitGateRepository"
                  "::test_denies_a_shared_checkout_commit_for_a_task_that_owns_no_worktree",
        "path": "services/work_launch.py",
        "old": '    for found in _parse_git_all(command, "commit"):',
        "new": "    for found in []:",
    },
    {
        "label": "commit_gate_rewrite_bypasses_the_repo_test",
        "gate": "a message the commit gate corrected is still tested for the shared checkout",
        "target": "tests/features/test_work_worktree_gates.py::TestCommitGateRepository"
                  "::test_a_stripped_message_does_not_carry_a_commit_into_the_shared_checkout",
        "path": "services/work_launch.py",
        "old": '        reason = f"agent attribution removed ({label}): {match}"',
        "new": '        reason = f"agent attribution removed ({label}): {match}"\n'
               '        return {"decision": "allow", "reason": reason, "command": rewritten}',
    },
    {
        "label": "write_gate_shared_checkout_open",
        "gate": "the write gate refuses an edit inside a shared checkout",
        "target": "tests/features/test_work_worktree_gates.py::TestWriteGate"
                  "::test_denies_an_edit_in_a_shared_checkout_and_allows_it_in_the_worktree",
        "path": "scripts/work_hook.py",
        "old": "        repo_path = work_worktree.shared_checkout(directory)\n"
               "        if not repo_path:\n"
               '            return ""',
        "new": "        repo_path = work_worktree.shared_checkout(directory)\n"
               "        if True:\n"
               '            return ""',
    },
    {
        "label": "push_gate_lint_ignored",
        "gate": "the push gate refuses a push whose lint failed",
        "target": "tests/features/test_work_push_gate.py::TestGatePush"
                  "::test_lint_failure_denies_before_tests",
        "path": "services/work_launch.py",
        "old": '        return {"decision": "deny",\n'
               '                "reason": _deny_reason(\n'
               '                    "lint", f"pre-commit ({lint[\'status\']}, exit "',
        "new": '        return {"decision": "allow",\n'
               '                "reason": _deny_reason(\n'
               '                    "lint", f"pre-commit ({lint[\'status\']}, exit "',
    },
    {
        "label": "push_gate_tests_ignored",
        "gate": "the push gate refuses a push whose test suite failed",
        "target": "tests/features/test_work_push_gate.py::TestGatePush::test_test_failure_denies",
        "path": "services/work_launch.py",
        "old": '    if tests["result"] not in ("pass", "no_runner"):',
        "new": "    if False:",
    },
    {
        "label": "push_gate_untestable_repo_passes",
        "gate": "a Python repository with no local venv cannot report a passing suite",
        "target": "tests/features/test_work_push_gate.py::TestGatePush"
                  "::test_no_local_venv_sentinel_denies",
        "path": "services/work_launch.py",
        "old": '        return {"result": "fail", "cmd": "(no local venv)", "exit_code": -1,',
        "new": '        return {"result": "pass", "cmd": "(no local venv)", "exit_code": -1,',
    },
    {
        "label": "proof_gate_always_proving",
        "gate": "a ticket with no PROOF.md skips proving instead of entering it",
        "target": "tests/features/test_proof_md_gate.py::TestEnterProvingTarget"
                  "::test_routes_to_pr_ready_when_proof_absent",
        "path": "core/tasks/tickets.py",
        "old": '    return "proving" if result.artifacts.get("has_proof_md") else "pr_ready"',
        "new": '    return "proving"',
    },
    {
        "label": "proof_gate_dirty_worktree_ignored",
        "gate": "enter_proving refuses a worktree with uncommitted changes",
        "target": "tests/features/test_proof_md_gate.py::TestWorktreeGuard"
                  "::test_dirty_worktree_blocks_enter_proving",
        "path": "core/tasks/tickets.py",
        "old": "    dirty = _dirty_workspace_repos(ticket_dir)\n"
               "    if dirty:\n"
               '        return TaskResult("failed",\n'
               "                          f\"worktree has uncommitted changes in: {', '.join(dirty)}\")\n"
               '    workspace_root = Path(ctx.config["workspace"]["root"])',
        "new": "    dirty = _dirty_workspace_repos(ticket_dir)\n"
               "    if False:\n"
               '        return TaskResult("failed",\n'
               "                          f\"worktree has uncommitted changes in: {', '.join(dirty)}\")\n"
               '    workspace_root = Path(ctx.config["workspace"]["root"])',
    },
    {
        "label": "scope_gate_pr_ready_open",
        "gate": "a pr_ready ticket holds its PR until the scope review passes",
        "target": "tests/features/test_scope_review.py::TestPrReadyScopeGate"
                  "::test_pending_enqueues_review_and_holds_pr",
        "path": "features/ticket_states.py",
        "old": '        if scope in ("pending", "fail"):',
        "new": "        if False:",
    },
    {
        "label": "scope_gate_merge_open",
        "gate": "a failed scope review blocks the auto-merge",
        "target": "tests/features/test_scope_review.py::TestInReviewScopeGate"
                  "::test_fail_blocks_auto_merge",
        "path": "features/ticket_states.py",
        "old": '            and scope in ("disabled", "pass")):',
        "new": "            and True):",
    },
    {
        "label": "scope_gate_scheduler_open",
        "gate": "a scheduled PR is held until the scope review passes",
        "target": "tests/features/test_scope_review.py::TestScheduledCreatePrScopeGate"
                  "::test_held_when_review_not_passed",
        "path": "core/scheduler.py",
        "old": '    if _scope_review_state(config, ts) not in ("disabled", "pass"):',
        "new": "    if False:",
    },
    {
        "label": "stale_green_merges",
        "gate": "a stalled check run clears ci_passed so the merge is not fed a stale green",
        "target": "tests/features/test_ci_passed_freshness.py::TestAutoMergeIsNotFedStaleGreen"
                  "::test_a_stalled_check_run_clears_it_and_blocks_the_merge",
        "path": "features/ticket_states.py",
        "old": '            ts.pop("checks_started_at", None)\n'
               '            ts.pop("ci_passed", None)',
        "new": '            ts.pop("checks_started_at", None)',
    },
    {
        "label": "hard_block_retried",
        "gate": "a hard block escapes the retry-loop exemption and blocks the ticket",
        "target": "tests/core/test_hard_block.py::TestHardBlockEscapesTheRetryExemption"
                  "::test_a_hard_block_blocks_even_for_a_retry_loop_task",
        "path": "core/tasks/registry.py",
        "old": '    if ctx.task in _RETRY_LOOP_TASKS and not getattr(result, "hard_block", False):',
        "new": "    if ctx.task in _RETRY_LOOP_TASKS:",
    },
    {
        "label": "sweep_closes_a_live_ticket",
        "gate": "the sweep closes a ticket only when the ticket source says it is finished",
        "target": "tests/features/test_sweep_terminality.py::TestSweepAsksTheSource"
                  "::test_pr_ready_is_not_closed_when_upstream_wants_rework",
        "path": "features/tickets.py",
        "old": "        if upstream is None or not _is_terminal_upstream(config, upstream):",
        "new": "        if False:",
    },
    {
        "label": "defence_accepts_a_runner_error",
        "gate": "only a failed assertion counts as evidence for a claim",
        "target": "tests/features/test_defence.py::TestOnlyAnAssertionFailureCounts"
                  "::test_pytest_exit_5_nothing_collected_is_not_evidence",
        "path": "features/defence.py",
        "old": "    if exit_code != 1:",
        "new": "    if False:",
    },
    {
        "label": "defence_reverses_the_test_too",
        "gate": "the reversed patch excludes test files so the probe survives the reversal",
        "target": "tests/features/test_defence.py::TestSourceSelection"
                  "::test_test_files_are_excluded_from_the_reversed_patch",
        "path": "features/defence.py",
        "old": "    sources = [n for n in listed.stdout.splitlines() "
               "if n.strip() and not is_test_path(n.strip())]",
        "new": "    sources = [n for n in listed.stdout.splitlines() if n.strip()]",
    },
]


def copy_repo(dst: Path) -> None:
    shutil.copytree(REPO, dst, ignore=IGNORE, symlinks=True)


def hash_tree(root: Path) -> dict:
    out = {}
    for path in sorted(root.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts:
            out[str(path.relative_to(root))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return out


def apply_patch(root: Path, mutation: dict) -> str:
    """Replace the anchor, or return why the replacement could not be made.

    The path is resolved inside the copy and refused if it lands anywhere else.
    An absolute path discards the copy's root when joined, and a path with `..`
    walks out of it, so either would edit the real checkout and leave it edited
    while the copy stayed clean and the run reported nothing applied.
    """
    if os.path.isabs(mutation["path"]):
        return f"{mutation['path']} is not a repository-relative path"
    target = (root / mutation["path"]).resolve()
    if not target.is_relative_to(root.resolve()):
        return f"{mutation['path']} points outside the copied tree"
    if not target.is_file():
        return f"{mutation['path']} does not exist"
    source = target.read_text()
    found = source.count(mutation["old"])
    if found == 0:
        return f"the anchor is not present in {mutation['path']}"
    if found > 1:
        return f"the anchor occurs {found} times in {mutation['path']}"
    if mutation["old"] == mutation["new"]:
        return "the replacement is identical to the anchor"
    target.write_text(source.replace(mutation["old"], mutation["new"]))
    return ""


def split_target(target: str) -> tuple[str, str, str]:
    """A pytest node id as (file, junit classname, junit test name)."""
    parts = target.split("::")
    test_file = parts[0]
    module = test_file[:-3].replace("/", ".") if test_file.endswith(".py") else test_file
    classname = ".".join([module, *parts[1:-1]])
    return test_file, classname, parts[-1]


def run_file(root: Path, test_file: str) -> dict:
    """Run one test file inside `root` and return {(classname, name): status}.

    The report is written to a fresh path outside `root`, and that path is
    removed before the run. A report inside the tree would be copied into every
    mutant, and a mutant whose interpreter dies before pytest can write would
    then be judged on the previous run's verdicts: a mutation that never ran
    would read as a kill.
    """
    handle, path = tempfile.mkstemp(prefix="frshty-mutation-report-", suffix=".xml")
    os.close(handle)
    report = Path(path)
    report.unlink()
    env = {**os.environ, "PYTHONPATH": str(root), "PYTHONDONTWRITEBYTECODE": "1"}
    env.pop("PYTEST_ADDOPTS", None)
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", test_file, "-q", "--tb=no",
             "-p", "no:cacheprovider", f"--junit-xml={report}"],
            cwd=str(root), capture_output=True, text=True, timeout=SUITE_TIMEOUT, env=env)
        if not report.is_file():
            raise RuntimeError(f"pytest produced no report: rc={proc.returncode} "
                               f"{(proc.stdout + proc.stderr)[-2000:]}")
        try:
            parsed = ET.parse(report).getroot()
        except ET.ParseError as exc:
            raise RuntimeError(f"pytest produced an unreadable report: {exc}") from exc
    finally:
        report.unlink(missing_ok=True)
    results = {}
    for case in parsed.iter("testcase"):
        status, detail = "PASS", ""
        for child in case:
            if child.tag == "failure":
                detail = child.get("message") or ""
                status = "FAIL" if detail.startswith(DECIDED_FAILURE) else "CRASH"
            elif child.tag == "error":
                status, detail = "ERROR", child.get("message") or ""
            elif child.tag == "skipped":
                status = "SKIP"
        results[(case.get("classname", ""), case.get("name", ""))] = (status, detail)
    return results


def check_mutation(root: Path, mutation: dict, known: dict) -> str:
    """Run one mutant. Returns the failure line, or "" when the gate was killed."""
    label = mutation["label"]
    test_file, classname, name = split_target(mutation["target"])
    if (classname, name) not in known.get(test_file, {}):
        return f"UNKNOWN TARGET TEST: {label} -> {mutation['target']}"
    copy_repo(root)
    before = hash_tree(root)
    problem = apply_patch(root, mutation)
    if problem:
        return f"PATCH DID NOT APPLY: {label} ({problem})"
    if hash_tree(root) == before:
        return f"PATCH DID NOT APPLY: {label} (the tree is unchanged)"
    try:
        results = run_file(root, test_file)
    except (RuntimeError, subprocess.TimeoutExpired) as exc:
        return f"MUTANT SUITE DID NOT RUN: {label}: {exc}"
    outcome = results.get((classname, name))
    if outcome is None:
        return f"TARGET TEST DID NOT RUN: {label} -> {mutation['target']}"
    status, detail = outcome
    if status == "ERROR":
        return (f"NOT A KILL: {label} -> {mutation['target']} errored in setup or "
                f"teardown rather than deciding: {detail[:200]}")
    if status == "CRASH":
        return (f"NOT A KILL: {label} -> {mutation['target']} raised rather than "
                f"failing an assertion: {detail[:200]}")
    if status != "FAIL":
        return f"MUTATION SURVIVED: {label} ({mutation['target']} still {status})"
    collateral = [f"{c}::{n}" for (c, n), (s, _) in results.items()
                  if s not in ("PASS", "SKIP") and (c, n) != (classname, name)]
    extra = f" (also failed: {len(collateral)})" if collateral else ""
    print(f"killed: {label} -> {name}{extra}")
    return ""


REQUIRED_KEYS = ("label", "target", "path", "old", "new")


def load_table(path: str | None) -> list[dict]:
    if not path:
        return MUTATIONS
    table = json.loads(Path(path).read_text())
    if not isinstance(table, list) or not table:
        raise SystemExit(f"{path} does not hold a non-empty list of mutations")
    for index, row in enumerate(table):
        if not isinstance(row, dict):
            raise SystemExit(f"{path}: entry {index} is not an object")
        missing = [k for k in REQUIRED_KEYS if not isinstance(row.get(k), str)]
        if missing:
            raise SystemExit(f"{path}: entry {index} is missing {missing}")
    return table


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--only", default="",
                        help="comma-separated labels to run instead of the whole table")
    parser.add_argument("--table", default="",
                        help="JSON file holding a mutation table to use instead of the built-in one")
    args = parser.parse_args()

    mutations = load_table(args.table or None)
    if args.only:
        wanted = [s.strip() for s in args.only.split(",") if s.strip()]
        if not wanted:
            raise SystemExit("--only names no mutation")
        unknown = [w for w in wanted if not any(m["label"] == w for m in mutations)]
        if unknown:
            raise SystemExit(f"no such mutation: {unknown}")
        mutations = [m for m in mutations if m["label"] in wanted]
    if not mutations:
        raise SystemExit("there are no mutations to run")

    work = Path(tempfile.mkdtemp(prefix="frshty-mutation-"))
    failures = []
    try:
        known = {}
        control_root = work / "control"
        copy_repo(control_root)
        files = []
        for mutation in mutations:
            test_file = split_target(mutation["target"])[0]
            if test_file not in files:
                files.append(test_file)
        for test_file in files:
            try:
                results = run_file(control_root, test_file)
            except (RuntimeError, subprocess.TimeoutExpired) as exc:
                print(f"CONTROL RUN IS NOT GREEN: {test_file} did not run: {exc}")
                failures.append(test_file)
                continue
            red = sorted(f"{c}::{n}" for (c, n), (s, _) in results.items()
                         if s != "PASS" and s != "SKIP")
            if red:
                print(f"CONTROL RUN IS NOT GREEN: {test_file}: {red}")
                failures.append(test_file)
                continue
            known[test_file] = results
            print(f"control: {test_file} {len(results)} tests, none red")
        if failures:
            print(f"MUTATION CHECK FAILED: the control run is not green in {failures}")
            return 1

        for index, mutation in enumerate(mutations):
            root = work / "mutants" / f"{index:03d}"
            problem = check_mutation(root, mutation, known)
            if problem:
                print(problem)
                failures.append(mutation["label"])
            shutil.rmtree(root, ignore_errors=True)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    if failures:
        print(f"MUTATION CHECK FAILED: {failures}")
        return 1
    print(f"all {len(mutations)} gates went red when removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
