"""Git must go through core.git_util.run_git, not raw subprocess.

subprocess.run returns an object whose .stdout is empty both when a command had
nothing to report and when it failed. Reading only .stdout turned failures into
facts repeatedly in this codebase: a failed `git diff` became "no changes" and
marked a ticket merged, a failed `git status` became "clean" and skipped
committing real work, a failed `git rev-list` became "no commits to lose" and
hard-reset the branch, and a failed `git diff` told the Submit PR modal a repo
had nothing to push.

Analysing whether a given call checks its return code is unreliable, so this
does the simple thing instead: new literal `subprocess.run(["git", ...])` calls
are rejected outside the seam. ALLOWED is the backlog. It may shrink. Adding to
it needs a reason.
"""
import ast
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
SKIP_DIRS = {".venv", "node_modules", ".git", ".claude", ".tmp", "tests", "__pycache__"}

# Files still calling git directly, with the count at the time the rule landed.
# Lower a number when you migrate a call. Do not raise one.
ALLOWED = {
    # The seam itself: raw git belongs here and nowhere else, so this ceiling
    # tracks the seam as it absorbs calls from the files below. Every other
    # number may only go down.
    "core/git_util.py": 18,
    "features/tickets.py": 33,
    "web/tickets.py": 9,
    "features/defence.py": 8,
    "core/tasks/tickets.py": 7,
    "features/own_prs.py": 6,
    "features/presentation.py": 6,
    "core/consensus_plan.py": 3,
    "web/reviews.py": 3,
    "core/branch_sync.py": 2,
    "features/platforms.py": 2,
    "features/reviewer.py": 2,
}


def _is_git_subprocess(node: ast.AST) -> bool:
    """A subprocess.run / check_output / Popen whose first argv element is "git"."""
    if not isinstance(node, ast.Call):
        return False
    f = node.func
    if not (isinstance(f, ast.Attribute) and f.attr in ("run", "check_output", "Popen", "call")):
        return False
    if not (isinstance(f.value, ast.Name) and f.value.id == "subprocess"):
        return False
    if not node.args:
        return False
    first = node.args[0]
    if isinstance(first, ast.List) and first.elts:
        head = first.elts[0]
        return isinstance(head, ast.Constant) and head.value == "git"
    return False


def _count_git_calls(path: pathlib.Path) -> int:
    try:
        tree = ast.parse(path.read_text())
    except (SyntaxError, UnicodeDecodeError):
        return 0
    return sum(1 for n in ast.walk(tree) if _is_git_subprocess(n))


def _scan() -> dict[str, int]:
    found = {}
    for p in sorted(ROOT.rglob("*.py")):
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        n = _count_git_calls(p)
        if n:
            found[str(p.relative_to(ROOT))] = n
    return found


class TestGitCallsGoThroughTheSeam:
    def test_the_rule_can_actually_see_a_git_call(self, tmp_path):
        """A check that reports success must be able to report failure."""
        bad = tmp_path / "bad.py"
        bad.write_text('import subprocess\nsubprocess.run(["git", "status"], cwd=".")\n')
        assert _count_git_calls(bad) == 1
        good = tmp_path / "good.py"
        good.write_text('run_git(wt, ["status", "--porcelain"])\n')
        assert _count_git_calls(good) == 0

    def test_no_new_file_calls_git_directly(self):
        for path, n in sorted(_scan().items()):
            assert path in ALLOWED, (
                f"{path} calls git through subprocess directly ({n} call(s)). Use "
                f"core.git_util.run_git, which raises on a status you did not allow, "
                f"instead of reading .stdout from a command that may have failed.")

    def test_no_file_grew_more_direct_git_calls(self):
        found = _scan()
        for path, allowed in sorted(ALLOWED.items()):
            actual = found.get(path, 0)
            assert actual <= allowed, (
                f"{path} now makes {actual} direct git calls, up from {allowed}. "
                f"Route the new one through core.git_util.run_git.")

    def test_the_backlog_numbers_are_honest(self):
        """A stale ceiling hides progress and lets a call sneak back in."""
        found = _scan()
        stale = {p: (allowed, found.get(p, 0)) for p, allowed in ALLOWED.items()
                 if found.get(p, 0) < allowed}
        assert not stale, (
            "ALLOWED is above the real count, so it no longer constrains anything. "
            f"Lower these to the actual number: {stale}")
