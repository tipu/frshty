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

readiness — a venv is ready only when its READY_MARKER exists. The build
writes the marker last, so a build killed part-way (a container reload)
leaves no marker and the next ensure rebuilds it. A venv built before the
marker existed rebuilds once.

review re-runs — `install_worktree_deps` with REVIEW_KEY skips an entry
when its fingerprint matches the last success in this worktree. The
fingerprint covers the command text, the content of every absolute file
path in the command, the git blob ids of the tracked dependency manifests
(DEP_MANIFESTS), and the tracked-file status. The status catches a step
that writes a tracked file: `git reset --hard` reverts that file, the
status no longer matches the stamp, and the step runs again. pipenv entries
are never skipped, because their venv lives in the shared store, which GC
empties. The stamp sits in the worktree's git dir, so it never shows in
`git status` and goes away with the worktree.
"""
import fcntl
import hashlib
import json
import os
import shlex
import shutil
import subprocess
import time
from pathlib import Path

import core.git_util as git_util
import core.log as log

BUILD_TIMEOUT = 1800
DEP_TIMEOUT = 300
GC_INTERVAL_S = 86400
DEFAULT_MAX_AGE_DAYS = 14
READY_MARKER = ".frshty-ready"
REVIEW_KEY = "review_dep_commands"
DEP_STAMP = "frshty-dep-stamp.json"
DEP_MANIFESTS = ("package.json", "pnpm-lock.yaml", "pnpm-workspace.yaml", ".npmrc",
                 "package-lock.json", "yarn.lock", "Pipfile", "Pipfile.lock",
                 "pyproject.toml", "uv.lock", "requirements*.txt", "*.prisma")


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
    return (venv / READY_MARKER).is_file() and (venv / "bin" / "python").exists()


def _clear_worktree_venv(wt_path: Path) -> None:
    """Remove `<worktree>/.venv` whether it is a stale symlink or a real
    directory left by the clone/setup, so pipenv redirects into the store
    instead of resolving a project-local venv."""
    link = wt_path / ".venv"
    if link.is_symlink():
        link.unlink(missing_ok=True)
    elif link.is_dir():
        shutil.rmtree(link, ignore_errors=True)
    elif link.exists():
        link.unlink(missing_ok=True)


def _link(wt_path: Path, venv: Path) -> None:
    """Point `<worktree>/.venv` at the shared venv, replacing whatever is
    there now (a wrong-target symlink or a real `.venv` from the clone) so
    pipenv resolves the shared store rather than a project-local venv."""
    link = wt_path / ".venv"
    try:
        if link.is_symlink() and link.readlink() == venv:
            return
        _clear_worktree_venv(wt_path)
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

    lock_path = store / f".{name}.lock"
    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        try:
            if not _venv_ready(venv):
                if venv.exists():
                    shutil.rmtree(venv, ignore_errors=True)
                _clear_worktree_venv(wt_path)
                env = {**os.environ,
                       "PIPENV_CUSTOM_VENV_NAME": name,
                       "WORKON_HOME": str(store),
                       "PIPENV_IGNORE_VIRTUALENVS": "1",
                       "PIPENV_DONT_LOAD_ENV": "1"}
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
                if result.returncode != 0 or not (venv / "bin" / "python").exists():
                    shutil.rmtree(venv, ignore_errors=True)
                    log.emit("dep_install_failed",
                             f"Shared venv build failed for {name}: rc={result.returncode}",
                             meta={"repo": repo_name, "venv": name,
                                   "stderr": (result.stderr or "")[-500:]})
                    return False
                (venv / READY_MARKER).touch()
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


def run_dep_command(config: dict, repo_name: str, wt_path: Path, cmd: str) -> str | None:
    """Run one configured dep_commands entry. pipenv commands for repos with
    a Pipfile route through the shared venv store; everything else runs
    unchanged with the historical fire-and-forget semantics. Return None on
    success, or the reason the command failed.

    A pipenv command in a worktree with no Pipfile is refused. pipenv has
    nothing to install there, so all it does is write a skeleton Pipfile, and
    that file then sits untracked in the worktree for a later commit to pick
    up. A live django-api migrated to uv and deleted its Pipfile while
    `dep_commands` still said `pipenv install --dev`; the skeleton reached the
    DEV-743 branch twice, as 2ad039d and eacb51e.

    `shared_venv_name` is not the test for this, because it falls back to
    Pipfile.lock and answers for a worktree that has only the lockfile.
    """
    argv = shlex.split(cmd)
    if argv and argv[0] == "pipenv":
        if not (wt_path / "Pipfile").is_file():
            log.emit("dep_command_skipped",
                     f"{repo_name}: skipped `{cmd}` — the worktree has no Pipfile, "
                     f"so pipenv would only write a skeleton one",
                     meta={"repo": repo_name, "cmd": cmd, "worktree": str(wt_path)})
            return "the worktree has no Pipfile"
        if ensure_shared_venv(config, repo_name, wt_path, dep_cmd=cmd):
            return None
        return "the shared venv build failed"
    try:
        result = subprocess.run(argv, cwd=str(wt_path), capture_output=True,
                                text=True, errors="replace", timeout=DEP_TIMEOUT)
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
        return f"{type(e).__name__}: {e}"
    if result.returncode == 0:
        return None
    return failure_reason(result)


def failure_reason(result: subprocess.CompletedProcess) -> str:
    """`rc=<code>: ` and the last 500 characters of stderr and of stdout.
    pnpm writes its ERR_PNPM line to stdout and only warnings to stderr, so
    stderr alone can hold no reason at all."""
    tails = [f"{name}: {text.strip()[-500:]}"
             for name, text in (("stderr", result.stderr), ("stdout", result.stdout))
             if text and text.strip()]
    return f"rc={result.returncode}: " + " | ".join(tails)


def _git_output(wt_path: Path, argv: list[str]) -> str | None:
    result = git_util.run_git_status(wt_path, argv, timeout=30)
    if result.returncode != 0:
        return None
    return result.stdout


def _worktree_state(wt_path: Path) -> str | None:
    """`git ls-files -s` of the tracked dependency manifests (mode, blob id
    and path of each) and `git status` of the tracked files. None when git
    fails, so every entry runs."""
    manifests = _git_output(wt_path, ["ls-files", "-s", "--",
                                      *[f":(glob)**/{p}" for p in DEP_MANIFESTS]])
    status = _git_output(wt_path, ["status", "--porcelain", "--untracked-files=no"])
    if manifests is None or status is None:
        return None
    return manifests + "\0" + status


def _absolute_paths(cmd: str) -> list[str]:
    """Every absolute path in a shell command: a word, the value after
    `--opt=`, and the words of a quoted script such as `sh -c '...'`."""
    lexer = shlex.shlex(cmd, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        tokens = list(lexer)
    except ValueError:
        return []
    paths = []
    for token in tokens:
        value = token.split("=", 1)[1] if "=" in token and not token.startswith("/") else token
        if value.startswith("/"):
            paths.append(value)
        if token != cmd and any(c.isspace() or c in ";&|()<>" for c in token) \
                and not all(c in ";&|()<>" for c in token):
            paths.extend(_absolute_paths(token))
    return paths


def dep_fingerprint(cmd: str, state: str) -> str:
    """sha256 of the command text, the content of each absolute file path in
    the command (a pinned CLI or a recipe script), and the worktree state."""
    h = hashlib.sha256(cmd.encode() + b"\0")
    for token in _absolute_paths(cmd):
        path = Path(token)
        if path.is_file():
            try:
                h.update(token.encode() + b"\0" + path.read_bytes() + b"\0")
            except OSError:
                h.update(token.encode() + b"\0unreadable\0")
    h.update(state.encode())
    return h.hexdigest()


def _cacheable(cmd: str) -> bool:
    argv = shlex.split(cmd)
    return bool(argv) and argv[0] != "pipenv"


def _read_stamp(stamp: Path) -> dict:
    try:
        data = json.loads(stamp.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def install_worktree_deps(config: dict, repo_name: str, wt_path: Path,
                          key: str = "dep_commands") -> None:
    """Relink the shared venv and run every `workspace.<key>` entry that
    matches `repo_name` in `wt_path`. A failed command is logged, not raised,
    so the caller still gets its worktree.

    For REVIEW_KEY, an entry whose fingerprint matches its last success in
    this worktree is skipped. Only successes are stamped, so a failed entry
    runs again on the next review."""
    relink_shared_venv(config, repo_name, wt_path)
    entries = [dep for dep in (config.get("workspace") or {}).get(key, []) or []
               if dep.get("match") == repo_name]
    if not entries:
        return
    stamp = None
    state = None
    if key == REVIEW_KEY:
        git_dir = (_git_output(wt_path, ["rev-parse", "--absolute-git-dir"]) or "").strip()
        state = _worktree_state(wt_path)
        if git_dir and state is not None:
            stamp = Path(git_dir) / DEP_STAMP
    done = _read_stamp(stamp) if stamp else {}
    succeeded = []
    for dep in entries:
        cmd = dep["cmd"]
        if stamp and state is not None and _cacheable(cmd) \
                and done.get(cmd) == dep_fingerprint(cmd, state):
            succeeded.append(cmd)
            continue
        done.pop(cmd, None)
        reason = run_dep_command(config, repo_name, wt_path, cmd)
        if reason:
            log.emit("worktree_dep_command_failed",
                     f"{repo_name}: `{cmd}` failed in {wt_path}: {reason}",
                     meta={"repo": repo_name, "cmd": cmd, "worktree": str(wt_path),
                           "reason": reason})
        elif _cacheable(cmd):
            succeeded.append(cmd)
    if not stamp:
        return
    after = _worktree_state(wt_path)
    done = {cmd: dep_fingerprint(cmd, after) for cmd in succeeded} if after is not None else {}
    try:
        stamp.write_text(json.dumps(done))
    except OSError as e:
        log.emit("dep_stamp_write_failed", f"Failed to write {stamp}: {e}",
                 meta={"repo": repo_name, "worktree": str(wt_path)})


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
    a best-effort `pnpm store prune`, except when a review install runs
    `--offline`: prune deletes every package that no current node_modules
    links to, and an offline review install of a later PR needs those
    packages in the store."""
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
    offline_reviews = any("--offline" in (dep.get("cmd") or "")
                          for dep in (config.get("workspace") or {}).get(REVIEW_KEY, []) or [])
    if pstore.is_dir() and not offline_reviews:
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
