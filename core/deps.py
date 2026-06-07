"""Shared dependency stores for ticket worktrees.

Every ticket worktree of the same repo used to materialize its own full
pipenv venv (pipenv names venvs by hashing the project path) and its own
node_modules download, so disk usage scaled linearly with concurrent
tickets. This module makes installs share storage instead:

pipenv — one venv per (repo, lockfile-hash) lives in
`<workspace.root>/.venv-store/<repo>-<sha256(Pipfile.lock)[:8]>`. The
runner builds it once (serialized with fcntl.flock so concurrent tickets
with the same lockfile can't corrupt a half-built venv) and then symlinks
`<worktree>/.venv` at it. pipenv resolves a project-local `.venv` before
its path-hash naming, so every later invocation — runner dep_commands,
agent-run `pipenv install`/`pipenv run`, and the test runner's
`.venv/bin/pytest` preference — lands in the shared venv with no per-call
env plumbing. A ticket that changes the lockfile hashes to a new name and
gets a fresh isolated venv automatically. The store lives under
workspace.root because that path is bind-mounted identically into the
runner containers (container HOME is overlay and dies on rebuild).

pnpm — `export_pnpm_store` points `npm_config_store_dir` at
`<workspace.root>/.pnpm-store` for the whole runner process; agent runs
and test runs inherit it via os.environ, and because the store shares a
filesystem with the worktrees pnpm hardlinks packages instead of copying.

cleanup — `gc_dep_stores` (driven by the dep_store_gc cron task) deletes
store venvs untouched for `max_age_days`; ensure/relink refresh a venv's
mtime on every use. Deleting a venv a live ticket still links to is
self-healing: the next ensure rebuilds it.
"""
import fcntl
import hashlib
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path

import core.log as log

BUILD_TIMEOUT = 1800
DEP_TIMEOUT = 300
GC_INTERVAL_S = 86400
DEFAULT_MAX_AGE_DAYS = 14


def venv_store_dir(config: dict) -> Path:
    return config["workspace"]["root"] / ".venv-store"


def pnpm_store_dir(config: dict) -> Path:
    return config["workspace"]["root"] / ".pnpm-store"


def shared_venv_name(repo_name: str, repo_dir: Path) -> str | None:
    """Deterministic shared-venv name for a worktree: `<repo>-<hash8>` of
    Pipfile.lock (falls back to Pipfile when the repo has no lockfile).
    Returns None for repos without a Pipfile — non-pipenv repos never get
    a shared venv."""
    for fname in ("Pipfile.lock", "Pipfile"):
        f = repo_dir / fname
        if f.is_file():
            digest = hashlib.sha256(f.read_bytes()).hexdigest()[:8]
            return f"{repo_name}-{digest}"
    return None


def _venv_ready(venv: Path) -> bool:
    return (venv / "bin" / "python").exists()


def _link(wt_path: Path, venv: Path) -> None:
    """Point `<worktree>/.venv` at the shared venv. A real (non-symlink)
    .venv directory is left alone — it belongs to the repo or the agent."""
    link = wt_path / ".venv"
    try:
        if link.is_symlink():
            if link.readlink() == venv:
                return
            link.unlink()
        elif link.exists():
            return
        link.symlink_to(venv)
    except OSError as e:
        log.emit("dep_venv_link_failed",
                 f"Failed to link {link} -> {venv}: {e}")


def ensure_shared_venv(config: dict, repo_name: str, wt_path: Path,
                       dep_cmd: str | None = None) -> bool:
    """Build the shared venv for this worktree's lockfile if it doesn't
    exist yet, then symlink `<worktree>/.venv` at it. The build runs
    `dep_cmd` (default `pipenv install --dev`) with PIPENV_CUSTOM_VENV_NAME
    and WORKON_HOME forcing the venv into the store; fcntl.flock on a
    per-name lockfile serializes concurrent first builds. Returns True when
    the venv is ready and linked."""
    name = shared_venv_name(repo_name, wt_path)
    if name is None:
        return False
    store = venv_store_dir(config)
    store.mkdir(parents=True, exist_ok=True)
    venv = store / name

    link = wt_path / ".venv"
    if link.is_symlink() and link.readlink() != venv:
        link.unlink(missing_ok=True)

    lock_path = store / f".{name}.lock"
    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            if not _venv_ready(venv):
                if venv.exists():
                    shutil.rmtree(venv, ignore_errors=True)
                env = {**os.environ,
                       "PIPENV_CUSTOM_VENV_NAME": name,
                       "WORKON_HOME": str(store),
                       "PIPENV_IGNORE_VIRTUALENVS": "1"}
                argv = shlex.split(dep_cmd or "pipenv install --dev")
                try:
                    result = subprocess.run(
                        argv, cwd=str(wt_path), capture_output=True,
                        text=True, timeout=BUILD_TIMEOUT, env=env)
                except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
                    shutil.rmtree(venv, ignore_errors=True)
                    log.emit("dep_install_failed",
                             f"Shared venv build failed for {name}: {type(e).__name__}: {e}",
                             meta={"repo": repo_name, "venv": name})
                    return False
                if result.returncode != 0 or not _venv_ready(venv):
                    shutil.rmtree(venv, ignore_errors=True)
                    log.emit("dep_install_failed",
                             f"Shared venv build failed for {name}: rc={result.returncode}",
                             meta={"repo": repo_name, "venv": name,
                                   "stderr": (result.stderr or "")[-500:]})
                    return False
            os.utime(venv)
        finally:
            fcntl.flock(lf, fcntl.LOCK_UN)
    _link(wt_path, venv)
    return True


def relink_shared_venv(config: dict, repo_name: str, wt_path: Path) -> None:
    """Restore the `.venv` symlink after sync paths that run `git clean -fd`
    (which deletes untracked symlinks). No-op when the shared venv for the
    worktree's current lockfile hasn't been built yet — the next
    ensure_shared_venv builds it."""
    name = shared_venv_name(repo_name, wt_path)
    if name is None:
        return
    venv = venv_store_dir(config) / name
    if _venv_ready(venv):
        os.utime(venv)
        _link(wt_path, venv)


def run_dep_command(config: dict, repo_name: str, wt_path: Path, cmd: str) -> bool:
    """Run one configured dep_commands entry. pipenv commands for repos with
    a Pipfile route through the shared venv store; everything else runs
    unchanged with the historical fire-and-forget semantics."""
    argv = shlex.split(cmd)
    if argv and argv[0] == "pipenv" and shared_venv_name(repo_name, wt_path):
        return ensure_shared_venv(config, repo_name, wt_path, dep_cmd=cmd)
    try:
        result = subprocess.run(argv, cwd=str(wt_path), capture_output=True,
                                timeout=DEP_TIMEOUT)
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False
    return result.returncode == 0


def export_pnpm_store(config: dict) -> None:
    """Point pnpm's content-addressable store at a path that is shared by
    every ticket worktree, survives container rebuilds, and sits on the
    same filesystem as the worktrees so pnpm hardlinks instead of copying.
    setdefault keeps any operator-provided store untouched."""
    os.environ.setdefault("npm_config_store_dir", str(pnpm_store_dir(config)))


def gc_dep_stores(config: dict, max_age_days: int = DEFAULT_MAX_AGE_DAYS) -> dict:
    """Delete shared venvs untouched for `max_age_days`. Self-throttled to
    one sweep per GC_INTERVAL_S via a stamp file so the cron tick can route
    this unconditionally. Takes the per-venv flock before deleting so a
    concurrent ensure_shared_venv never sees a half-deleted venv. Ends with
    a best-effort `pnpm store prune`."""
    store = venv_store_dir(config)
    deleted: list[str] = []
    if not store.is_dir():
        return {"deleted": deleted}
    now = time.time()
    stamp = store / ".gc-stamp"
    if stamp.exists() and now - stamp.stat().st_mtime < GC_INTERVAL_S:
        return {"deleted": deleted, "throttled": True}
    stamp.touch()
    cutoff = now - max_age_days * 86400
    for venv in sorted(store.iterdir()):
        try:
            if venv.name.startswith(".") or not venv.is_dir():
                continue
            if venv.stat().st_mtime >= cutoff:
                continue
            with open(store / f".{venv.name}.lock", "w") as lf:
                fcntl.flock(lf, fcntl.LOCK_EX)
                try:
                    if venv.is_dir() and venv.stat().st_mtime < cutoff:
                        shutil.rmtree(venv, ignore_errors=True)
                        deleted.append(venv.name)
                finally:
                    fcntl.flock(lf, fcntl.LOCK_UN)
        except OSError:
            continue
    pstore = pnpm_store_dir(config)
    if pstore.is_dir():
        try:
            subprocess.run(["pnpm", "store", "prune"], capture_output=True,
                           timeout=600,
                           env={**os.environ, "npm_config_store_dir": str(pstore)})
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
            pass
    if deleted:
        log.emit("dep_store_gc", f"Deleted {len(deleted)} stale shared venv(s)",
                 meta={"deleted": deleted})
    return {"deleted": deleted}
