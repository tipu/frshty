"""Git helpers shared across the ticket pipeline.

The key concern here is `commit_with_hooks`: frshty makes intermediate commits
inside per-ticket worktrees (e.g. after fix_review_findings applies an LLM
patch, or after write_tests scaffolds tests). Those worktrees inherit
pre-commit hooks from the parent repo's `.git/hooks/pre-commit`, which on
this codebase resolves to a shell script that exec's
`<venv>/bin/python -mpre_commit hook-impl …`. When the venv path inside the
hook doesn't exist and `pre-commit` isn't on the subprocess PATH, the hook
prints `pre-commit not found. Did you forget to activate your virtualenv?`
and exits 1 — which fails the commit and traps the worktree in a dirty state.

Strategy: locate the per-repo pre-commit binary (typically
`<repo>/.venv/bin/pre-commit`), run `pre-commit run` ourselves so the
auto-fixing hooks (ruff --fix, ruff-format) actually run and re-stage their
output, then `git commit` normally so the git-driven hook re-runs and
no-ops. If a hook surfaces real lint errors that auto-fix can't resolve,
the second `git commit` re-runs the same hooks and fails, surfacing the
lint error to the caller.

There is no bypass. A repo that configures pre-commit but whose binary cannot
be located returns exit 127 with an explanatory stderr. It used to commit with
--no-verify instead, which let a ticket advance as though the hooks had passed;
windows-rpa-client committed that way during DEV-635 and was reported clean.
Because that flag was appended outside the config check, it also suppressed
native git hooks in repos with no pre-commit config at all.
"""
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import core.log as log


PRE_COMMIT_TIMEOUT = 600


def _worktree_holding_branch(repo_path: Path, branch: str) -> Path | None:
    """Return the path of the existing worktree that currently has `branch`
    checked out, or None. Parses `git worktree list --porcelain`."""
    result = subprocess.run(
        ["git", "worktree", "list", "--porcelain"],
        cwd=str(repo_path), capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        return None
    current = None
    for line in result.stdout.splitlines():
        if line.startswith("worktree "):
            current = line[len("worktree "):].strip()
        elif line.startswith("branch ") and current:
            ref = line[len("branch "):].strip()
            if ref == f"refs/heads/{branch}":
                return Path(current)
    return None


def add_or_reuse_worktree(repo_path: Path, worktree_path: Path, branch: str,
                          base_branch: str = "main", timeout: int = 60) -> Path | None:
    """Create a worktree for `branch` at `worktree_path`, or reuse the worktree
    that already has it checked out.

    `git worktree add` exits non-zero when `branch` is already checked out by
    another worktree (typically a per-ticket workspace under projects/).
    Resolution: if the holder is the canonical repo checkout (`repo_path`
    itself), free it by checking out `base_branch` there and retry the add;
    otherwise reuse the holder worktree directly. Returns the usable worktree
    path, or None if it could not be resolved."""
    worktree_path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["git", "fetch", "origin", branch], cwd=str(repo_path), capture_output=True, timeout=timeout)
    subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=timeout)
    result = subprocess.run(
        ["git", "worktree", "add", str(worktree_path), branch],
        cwd=str(repo_path), capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode == 0:
        return worktree_path

    holder = _worktree_holding_branch(repo_path, branch)
    if holder is None:
        return None
    if holder.resolve() == Path(repo_path).resolve():
        freed = subprocess.run(
            ["git", "checkout", base_branch],
            cwd=str(repo_path), capture_output=True, text=True, timeout=timeout,
        )
        if freed.returncode != 0:
            return None
        subprocess.run(["git", "worktree", "prune"], cwd=str(repo_path), capture_output=True, timeout=timeout)
        retry = subprocess.run(
            ["git", "worktree", "add", str(worktree_path), branch],
            cwd=str(repo_path), capture_output=True, text=True, timeout=timeout,
        )
        return worktree_path if retry.returncode == 0 else None
    return holder


def pre_commit_candidates(repo_dir: Path) -> list[Path]:
    """Every path `_find_pre_commit` would consider, in preference order.

    Exposed so a caller can watch all of them. Watching only the one that
    resolves today misses a repair that plants a higher-priority runner, because
    that changes which path resolves rather than the contents of the old one.
    The PATH fallback is included for the same reason: it is the runner that
    actually executes when none of the fixed locations has one."""
    fixed = [
        repo_dir / ".venv" / "bin" / "pre-commit",
        Path.home() / ".local" / "bin" / "pre-commit",
        Path("/usr/local/bin/pre-commit"),
        Path("/usr/bin/pre-commit"),
    ]
    located = shutil.which("pre-commit")
    if located and Path(located) not in fixed:
        fixed.append(Path(located))
    return fixed


def _find_pre_commit(repo_dir: Path) -> Path | None:
    """Return the path to a usable pre-commit binary, or None.

    Preference order: per-repo `.venv/bin/pre-commit`, then `~/.local/bin`,
    then `/usr/local/bin` / `/usr/bin`, then anything on PATH."""
    for c in pre_commit_candidates(repo_dir):
        try:
            if c.is_file() and os.access(c, os.X_OK):
                return c
        except OSError:
            continue
    return None


def _hook_env(repo_dir: Path, pre_commit: Path | None = None) -> dict[str, str]:
    """Build an env where the chosen pre-commit binary is on PATH.

    The git-driven hook script re-resolves `pre-commit` from PATH. _find_pre_commit
    falls back to ~/.local/bin and PATH, so putting only the repo's .venv/bin here
    meant the hook could fail to find the very binary we had just selected and
    report "pre-commit not found. Did you forget to activate your virtualenv?".
    """
    env = {**os.environ}
    for candidate in (pre_commit.parent if pre_commit else None,
                      repo_dir / ".venv" / "bin"):
        if candidate and candidate.is_dir():
            env["PATH"] = f"{candidate}{os.pathsep}{env.get('PATH', '')}"
    return env


@dataclass
class CommitOutcome:
    """What happened when we tried to commit, and the evidence for it.

    Every non-zero result used to look identical to the caller, and the useful
    text — the diagnostics from the explicit `pre-commit run` passes — was logged
    and thrown away, so only the later `git commit` output survived. Anything
    deciding what to do next was reading the wrong thing.
    """
    status: str          # committed | hook_failed | tooling_failed | git_failed
    phase: str           # locate_runner | hook_pass_1 | hook_pass_2 | git_commit
    repo: str
    exit_code: int
    output: str
    before_head: str
    after_head: str

    @property
    def ok(self) -> bool:
        return self.status == "committed"


# Scoped to the hook runner. A bare "permission denied" or "command not found"
# appears in ordinary test output too, and classifying a genuine test failure as
# an environment problem blocks work that an agent could have fixed.
_ENVIRONMENT_MARKERS = (
    "pre-commit not found", "pre-commit: not found", "did you forget to activate",
    "no such file or directory: 'pre-commit'", "pre-commit: command not found",
    "hook script", "unable to find pre-commit",
)
_DEPENDENCY_MARKERS = (
    "unknown import symbol", "modulenotfounderror", "no module named",
    "could not be resolved", "cannot find implementation or library stub",
    "is not a known attribute of module", "importerror",
    # TypeScript's form of the same question. Left out originally, so it reached
    # the repair agent, which can satisfy it with a fabricated .d.ts.
    "cannot find module", "ts2307", "cannot find name",
)


_ANSI = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def strip_ansi(text: str) -> str:
    """Hook output with the colour taken out.

    Tools decide to colour from the environment, not from whether anyone is
    reading. ruff writes `\\x1b[1m\\x1b[91mE501 `, and the escape sits directly
    against the code, so `\\bE501\\b` finds no word boundary and the diagnostic
    goes unrecognised. The classifier then blocks a ticket it knows how to fix.
    """
    return _ANSI.sub("", text or "")


def triage_commit_failure(status: str, output: str) -> str:
    """Classify a failed commit deterministically, before asking any model.

    Returns environment | dependency | git | ambiguous. Only "ambiguous" is worth
    a model call, and only "ambiguous" may lead to editing code. The three real
    failures behind this — a type error in a generated test, a symbol missing
    from a published sibling package, and a missing pre-commit binary — all
    arrived through one code path and all got the same answer: "edit something".
    Two of those three are not fixable by editing this repository.
    """
    text = strip_ansi(output or "").lower()
    if status == "git_failed":
        return "git"
    if status == "tooling_failed" or any(m in text for m in _ENVIRONMENT_MARKERS):
        return "environment"
    if any(m in text for m in _DEPENDENCY_MARKERS):
        return "dependency"
    return "ambiguous"


def commit_outcome(repo_dir: Path, message: str | None = None,
                   extra_commit_args: list[str] | None = None,
                   timeout: int = 120) -> CommitOutcome:
    """Stage and commit, reporting which phase failed and keeping its output."""
    repo = repo_dir.name

    def _head() -> str:
        got = subprocess.run(["git", "rev-parse", "HEAD"], cwd=str(repo_dir),
                             capture_output=True, text=True, timeout=30)
        return (got.stdout or "").strip() if got.returncode == 0 else ""

    before = _head()
    config = repo_dir / ".pre-commit-config.yaml"
    pc = _find_pre_commit(repo_dir) if config.is_file() else None
    env = _hook_env(repo_dir, pc)

    if config.is_file() and pc is None:
        return CommitOutcome("tooling_failed", "locate_runner", repo, 127,
                             "pre-commit is configured for this repository but no "
                             "binary could be located in .venv/bin or on PATH",
                             before, before)

    if pc is not None:
        for attempt in (1, 2):
            run = subprocess.run([str(pc), "run"], cwd=str(repo_dir),
                                 capture_output=True, text=True, env=env,
                                 timeout=PRE_COMMIT_TIMEOUT)
            subprocess.run(["git", "add", "-A"], cwd=str(repo_dir),
                           capture_output=True, timeout=30)
            if run.returncode == 0:
                break
            if attempt == 2:
                # Running git commit now would only repeat the same checks and
                # replace these diagnostics with a terser failure.
                return CommitOutcome("hook_failed", f"hook_pass_{attempt}", repo,
                                     run.returncode,
                                     ((run.stdout or "") + (run.stderr or "")).strip(),
                                     before, before)

    args = ["git", "commit", *(extra_commit_args or [])]
    if message is not None:
        args.extend(["-m", message])
    done = subprocess.run(args, cwd=str(repo_dir), capture_output=True,
                          text=True, timeout=timeout, env=env)
    if done.returncode != 0:
        return CommitOutcome("git_failed", "git_commit", repo, done.returncode,
                             ((done.stdout or "") + (done.stderr or "")).strip(),
                             before, before)
    return CommitOutcome("committed", "git_commit", repo, 0,
                         (done.stdout or "").strip(), before, _head())


def commit_with_hooks(repo_dir: Path,
                      message: str | None = None,
                      extra_commit_args: list[str] | None = None,
                      check: bool = False,
                      timeout: int = 120) -> subprocess.CompletedProcess:
    """Stage all changes and commit `repo_dir`, running pre-commit hooks
    manually first when possible.

    `message`: passed via `-m`. Omit for paths that use `--no-edit` (merge
    finalize).
    `extra_commit_args`: e.g. `["--no-edit"]` for merge commits.
    `check`: forward to subprocess.run; True raises on non-zero exit.

    Never bypasses verification. A repo that configures pre-commit but whose
    binary cannot be located returns exit 127 with an explanatory stderr, so the
    caller sees a tooling failure instead of an unverified commit. A repo with no
    pre-commit config commits normally, leaving any native git hooks in force.

    This is `commit_outcome` flattened into a CompletedProcess for callers that
    only read a return code. It used to run the hooks itself and then run
    `git commit` regardless of what they said, so a repo whose hooks fail but
    which installs no native git hook committed and reported success."""
    outcome = commit_outcome(repo_dir, message=message,
                             extra_commit_args=extra_commit_args, timeout=timeout)
    args = ["git", "commit", *(extra_commit_args or [])]
    if message is not None:
        args.extend(["-m", message])

    if outcome.ok:
        return subprocess.CompletedProcess(args, 0, outcome.output, "")

    if outcome.status == "tooling_failed":
        log.emit(
            "git_pre_commit_unavailable",
            f"{repo_dir.name}: pre-commit config present but no binary found "
            f"in the repo .venv or on PATH; refusing to commit unverified",
            meta={"repo": repo_dir.name},
        )
    elif outcome.status == "hook_failed":
        log.emit(
            "git_pre_commit_unresolved",
            f"{repo_dir.name}: pre-commit reported unresolved issues after "
            f"retry; refusing to commit",
            meta={"repo": repo_dir.name, "output_tail": (outcome.output or "")[-2000:]},
        )
    code = outcome.exit_code or 1
    if check:
        raise subprocess.CalledProcessError(code, args, output="", stderr=outcome.output)
    return subprocess.CompletedProcess(args, code, "", outcome.output)


def lint_files(repo_dir: Path, files: list[str],
               timeout: int = PRE_COMMIT_TIMEOUT) -> dict:
    """Run the repo's pre-commit hooks over `files` without committing.

    The work-layer push gate's lint step: same runner discovery and env as
    `commit_outcome`, scoped to the files a push introduces. A repo with no
    pre-commit config passes trivially, matching `commit_with_hooks`. A repo
    that configures pre-commit but whose binary cannot be located reports
    tooling_failed rather than passing unverified. A non-zero exit reports
    hook_failed even when the hooks auto-fixed the files, because a fix the
    hook just wrote is not in the commits being pushed.

    Returns {"status": pass|hook_failed|tooling_failed|no_config,
    "exit_code": int, "output": str}."""
    config = repo_dir / ".pre-commit-config.yaml"
    if not config.is_file():
        return {"status": "no_config", "exit_code": 0, "output": ""}
    pc = _find_pre_commit(repo_dir)
    if pc is None:
        return {"status": "tooling_failed", "exit_code": 127,
                "output": "pre-commit is configured for this repository but no "
                          "binary could be located in .venv/bin or on PATH"}
    present = [f for f in files if (repo_dir / f).is_file()]
    if not present:
        return {"status": "pass", "exit_code": 0, "output": "no files to lint"}
    try:
        run = subprocess.run([str(pc), "run", "--files", *present],
                             cwd=str(repo_dir), capture_output=True, text=True,
                             env=_hook_env(repo_dir, pc), timeout=timeout)
    except subprocess.TimeoutExpired:
        return {"status": "hook_failed", "exit_code": -1,
                "output": f"pre-commit run timed out after {timeout}s"}
    output = strip_ansi((run.stdout or "") + (run.stderr or "")).strip()
    return {"status": "pass" if run.returncode == 0 else "hook_failed",
            "exit_code": run.returncode, "output": output[-4000:]}


def git_common_dir(repo_dir: Path) -> str:
    """Absolute path of the repo's shared git directory. Two checkouts that
    return the same value share one config file, so they cannot hold two
    different identities."""
    r = subprocess.run(["git", "rev-parse", "--path-format=absolute",
                        "--git-common-dir"],
                       cwd=str(repo_dir), capture_output=True, text=True, timeout=30)
    return r.stdout.strip() if r.returncode == 0 else ""


def linked_worktrees(repo_dir: Path) -> list[Path]:
    """Every checkout attached to this repo, the canonical one included.

    Worktree-local config (`extensions.worktreeConfig`) outranks the shared
    config, so a repo-level identity is not proof about a worktree until the
    worktree itself is asked."""
    r = subprocess.run(["git", "worktree", "list", "--porcelain"],
                       cwd=str(repo_dir), capture_output=True, text=True, timeout=30)
    if r.returncode != 0:
        return [repo_dir]
    paths = [Path(line[len("worktree "):]) for line in r.stdout.splitlines()
             if line.startswith("worktree ")]
    return paths or [repo_dir]


def effective_identity(repo_dir: Path) -> tuple[str, str]:
    """The author and committer git would actually stamp on a commit here.

    Reads `git var`, not the config, because ambient GIT_AUTHOR_* /
    GIT_COMMITTER_* variables and worktree-local config outrank anything we
    write. Returns ("", "") when git cannot answer."""
    out = []
    for var in ("GIT_AUTHOR_IDENT", "GIT_COMMITTER_IDENT"):
        r = subprocess.run(["git", "var", var], cwd=str(repo_dir),
                           capture_output=True, text=True, timeout=30)
        ident = r.stdout.strip() if r.returncode == 0 else ""
        out.append(ident.rsplit(">", 1)[0] + ">" if ">" in ident else "")
    return out[0], out[1]


def configure_repo_identity(repo_dir: Path, name: str,
                            email: str) -> tuple[bool, str]:
    """Make `repo_dir` commit as `name <email>`, and prove that it will.

    Written to the repo's local config rather than passed per command: frshty
    is not the only thing that commits in these worktrees. The headless agent
    and the interactive tmux panes run their own `git commit`, and neither
    inherits flags or env we set around our own subprocesses. Repo-local
    config is the one place all of them read. Linked worktrees share the
    canonical checkout's config, so setting it once covers every ticket
    worktree, existing and future.

    Returns (ok, detail). `ok` means git itself now reports this identity for
    both author and committer."""
    want = f"{name} <{email}>"
    for key, value in (("user.name", name), ("user.email", email)):
        prior = subprocess.run(["git", "config", "--local", "--get-all", key],
                               cwd=str(repo_dir), capture_output=True, text=True,
                               timeout=30).stdout.strip()
        r = subprocess.run(["git", "config", "--local", "--replace-all", key, value],
                           cwd=str(repo_dir), capture_output=True, text=True, timeout=30)
        if r.returncode != 0:
            return False, f"could not set {key}: {(r.stderr or '').strip()[:200]}"
        if prior and prior != value:
            log.emit("git_identity_replaced",
                     f"{repo_dir.name}: {key} was {prior!r}, now {value!r}",
                     meta={"repo": repo_dir.name, "key": key,
                           "was": prior, "now": value})

    for checkout in linked_worktrees(repo_dir):
        if not checkout.is_dir():
            continue
        author, committer = effective_identity(checkout)
        if not author or not committer:
            return False, f"git var could not report an identity in {checkout}"
        if author != want or committer != want:
            return False, (f"{checkout} still commits as author {author!r} "
                           f"committer {committer!r}; something outranks the "
                           f"repo config (worktree config or "
                           f"GIT_AUTHOR_*/GIT_COMMITTER_* env)")
    return True, want


class GitCommandError(RuntimeError):
    """A git command exited with a status the caller did not allow."""

    def __init__(self, args, result):
        self.args_run, self.result = args, result
        super().__init__(
            f"git {' '.join(args)} exited {result.returncode}: "
            f"{((result.stderr or '') + (result.stdout or '')).strip()[:300]}")


def run_git(cwd, args: list[str], *, allowed_codes=(0,), timeout: int = 60):
    """Run a git command and refuse to hand back output from a failed one.

    subprocess.run returns an object whose .stdout is empty both when the
    command had nothing to report and when it failed. Every caller that read
    only .stdout therefore turned a failure into a fact: a failed `git diff`
    became "no changes" and marked a ticket merged, a failed `git status`
    became "clean" and skipped committing real work, a failed `git rev-list`
    became "no commits to lose" and hard-reset the branch.

    Anything outside `allowed_codes` raises. Commands whose non-zero status is
    information rather than failure — `diff --quiet`, `merge-base --is-ancestor`
    — pass the codes they expect instead of being silently tolerated.
    """
    result = subprocess.run(["git", *args], cwd=str(cwd), capture_output=True,
                            text=True, timeout=timeout)
    if result.returncode not in allowed_codes:
        raise GitCommandError(args, result)
    return result


def is_dirty(worktree) -> bool:
    """Whether the worktree has tracked or untracked changes. Raises if unknown.

    Deliberately not returning False on failure: the callers use this to decide
    whether destroying the working tree is safe."""
    return bool(run_git(worktree, ["status", "--porcelain"], timeout=30).stdout.strip())


def refresh_worktree_onto_base(worktree, base_branch: str) -> dict:
    """Bring a ticket worktree up to date with its base without destroying work.

    A plain `reset --hard origin/<base>` is correct only when the branch has no
    commits of its own. When it does — a replan, a note-reset, or any re-entry
    into planning after implementation — the reset deletes them, and unpushed
    commits survive only in the reflog. Merge instead in that case, which
    achieves the same fresh base and keeps the work.

    A reset is only safe when there is nothing to lose, so this refuses to touch
    a worktree with uncommitted or untracked changes, and refuses to act at all
    on a state it could not read. Every git call here is checked: a failure that
    silently produced empty output is what made the original reset destructive.

    Returns {"result": reset|merged|merge_failed|no_base|dirty|unknown, "ahead": int}.
    """
    wt = str(worktree)
    fetch = subprocess.run(["git", "fetch", "origin", base_branch],
                           cwd=wt, capture_output=True, text=True, timeout=60)
    if fetch.returncode != 0:
        return {"result": "no_base", "ahead": 0, "error": (fetch.stderr or "").strip()[:200]}

    try:
        raw = run_git(wt, ["rev-list", "--count", f"origin/{base_branch}..HEAD"],
                      timeout=30).stdout.strip()
        if not raw.isdigit():
            return {"result": "unknown", "ahead": 0, "error": "unreadable commit count"}
        ahead = int(raw)
        dirty = is_dirty(wt)
    except GitCommandError as e:
        return {"result": "unknown", "ahead": 0, "error": str(e)[:200]}

    if ahead == 0:
        if dirty:
            # reset --hard plus clean -fd would delete tracked edits and untracked
            # files alike. "No commits ahead" says nothing about the working tree.
            return {"result": "dirty", "ahead": 0,
                    "error": "worktree has uncommitted changes; refusing to reset"}
        try:
            run_git(wt, ["reset", "--hard", f"origin/{base_branch}"])
            run_git(wt, ["clean", "-fd"])
        except GitCommandError as e:
            return {"result": "unknown", "ahead": 0, "error": str(e)[:200]}
        return {"result": "reset", "ahead": 0}

    merged = subprocess.run(["git", "merge", f"origin/{base_branch}", "--no-edit"],
                            cwd=wt, capture_output=True, text=True, timeout=120)
    if merged.returncode != 0:
        aborted = subprocess.run(["git", "merge", "--abort"], cwd=wt,
                                 capture_output=True, text=True, timeout=60)
        detail = (merged.stderr or merged.stdout or "").strip()[:200]
        if aborted.returncode != 0:
            detail = f"{detail} (merge --abort also failed; worktree left mid-merge)"
        return {"result": "merge_failed", "ahead": ahead, "error": detail}
    return {"result": "merged", "ahead": ahead}
