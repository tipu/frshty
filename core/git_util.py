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
the second `git commit` re-runs the same hooks and fails — surfacing the
lint error to the caller rather than silently bypassing it with --no-verify.

Fallback: only when no pre-commit binary can be located does this helper
fall back to `git commit --no-verify`. That keeps frshty progressing on
repos that have a `.pre-commit-config.yaml` but no installed venv.
"""
import os
import shutil
import subprocess
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


def _find_pre_commit(repo_dir: Path) -> Path | None:
    """Return the path to a usable pre-commit binary, or None.

    Preference order: per-repo `.venv/bin/pre-commit`, then `~/.local/bin`,
    then `/usr/local/bin` / `/usr/bin`, then anything on PATH."""
    candidates = [
        repo_dir / ".venv" / "bin" / "pre-commit",
        Path.home() / ".local" / "bin" / "pre-commit",
        Path("/usr/local/bin/pre-commit"),
        Path("/usr/bin/pre-commit"),
    ]
    for c in candidates:
        try:
            if c.is_file() and os.access(c, os.X_OK):
                return c
        except OSError:
            continue
    located = shutil.which("pre-commit")
    return Path(located) if located else None


def _hook_env(repo_dir: Path) -> dict[str, str]:
    """Build an env where the repo's `.venv/bin` is on PATH so any git-driven
    hooks that exec `pre-commit` from PATH find the per-repo binary."""
    env = {**os.environ}
    venv_bin = repo_dir / ".venv" / "bin"
    if venv_bin.is_dir():
        env["PATH"] = f"{venv_bin}:{env.get('PATH', '')}"
    return env


def _run_pre_commit(repo_dir: Path, pc: Path, env: dict[str, str]) -> None:
    """Run `pre-commit run` and re-stage anything it auto-fixed. Idempotent
    second pass catches the typical "files were modified, please re-run"
    exit code from the first invocation."""
    for attempt in range(2):
        run = subprocess.run(
            [str(pc), "run"],
            cwd=str(repo_dir), capture_output=True, text=True,
            env=env, timeout=PRE_COMMIT_TIMEOUT,
        )
        subprocess.run(["git", "add", "-A"], cwd=str(repo_dir),
                       capture_output=True, timeout=30)
        if run.returncode == 0:
            return
        if attempt == 1:
            log.emit(
                "git_pre_commit_unresolved",
                f"{repo_dir.name}: pre-commit reported unresolved issues "
                f"after retry; subsequent git commit will surface them",
                meta={"repo": repo_dir.name,
                      "stdout_tail": (run.stdout or "")[-2000:],
                      "stderr_tail": (run.stderr or "")[-1000:]},
            )


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

    Falls back to `git commit --no-verify` only when no pre-commit binary
    can be located (e.g. repo has a hook script installed but no venv with
    pre-commit). When pre-commit IS available, the helper commits without
    --no-verify so real lint failures surface as CalledProcessError /
    non-zero returncode rather than being silently bypassed."""
    args = ["git", "commit"]
    extras = list(extra_commit_args or [])

    config = repo_dir / ".pre-commit-config.yaml"
    pc = _find_pre_commit(repo_dir) if config.is_file() else None

    if pc is None:
        if config.is_file():
            log.emit(
                "git_pre_commit_unavailable",
                f"{repo_dir.name}: pre-commit config present but binary not "
                f"located in repo .venv or PATH; committing with --no-verify",
                meta={"repo": repo_dir.name},
            )
        args.append("--no-verify")
        args.extend(extras)
        if message is not None:
            args.extend(["-m", message])
        return subprocess.run(
            args, cwd=str(repo_dir), capture_output=True, text=True,
            check=check, timeout=timeout,
        )

    # Run the auto-fixers ourselves so their modifications get re-staged
    # before the git-driven hook re-runs the same checks. The env carries
    # `<repo>/.venv/bin` on PATH so the git-driven hook script's
    # `command -v pre-commit` fallback resolves to the same binary.
    env = _hook_env(repo_dir)
    _run_pre_commit(repo_dir, pc, env)
    args.extend(extras)
    if message is not None:
        args.extend(["-m", message])
    return subprocess.run(
        args, cwd=str(repo_dir), capture_output=True, text=True,
        check=check, timeout=timeout, env=env,
    )


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
