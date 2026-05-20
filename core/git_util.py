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
