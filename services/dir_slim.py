"""Delete the bulky files that finished tasks and tickets leave behind.

A task worktree or a ticket directory keeps its dependency trees and its
recorded videos long after the work on it ends. `sweep` deletes every
directory named in SLIM_DIRS or starting with a prefix in SLIM_DIR_PREFIXES,
and every file whose suffix is in SLIM_SUFFIXES,
once the work has been finished for SLIM_AFTER_DAYS. The worktree and the
ticket directory stay. An install or a build makes them again.

A task worktree qualifies when every work item that ran in it is closed or
archived, was last updated SLIM_AFTER_DAYS ago, and has no live session. A
ticket directory qualifies when its ticket has a status in TICKET_DONE_STATUSES
and no file outside SLIM_DIRS in it changed for SLIM_AFTER_DAYS.

A path that git tracks is never deleted, and neither is a directory that holds
a git tree. A symlink is never followed and never deleted, so a .venv that
links into the shared venv store stays. Nothing under
.git is read. A slimmed worktree holds no media any more, so work_worktree.gc
can then remove it.

`sweep` runs from the work board scan loop at most once per SWEEP_INTERVAL_S."""
import os
import shutil
import threading
import time
from pathlib import Path

import core.config as core_config
import core.db as db
import core.git_util as git_util
import core.log as log
import core.runtime as runtime
import core.terminal as terminal
from services import disk_usage, work_store, work_worktree

SLIM_AFTER_DAYS = 5
SWEEP_INTERVAL_S = 3600
SLIM_DIRS = frozenset({"node_modules", "venv", ".venv", ".next", "cdk.out",
                       "__pycache__"})
SLIM_DIR_PREFIXES = (".webpack_cache",)
SLIM_SUFFIXES = frozenset({".mp4", ".webm"})
TICKET_DONE_STATUSES = ("merged", "done", "ignored")

_lock = threading.Lock()
_last_sweep = 0.0


def _enclosing_repo(top: Path) -> tuple[bool, Path | None]:
    """Whether git could tell, and the git tree that holds `top` or None."""
    found = git_util.run_git_status(top, ["rev-parse", "--show-toplevel"], timeout=30)
    if found.returncode == 0 and found.stdout.strip():
        return True, Path(found.stdout.strip())
    return "not a git repository" in (found.stderr or ""), None


def _raise(error: OSError) -> None:
    raise error


def _holds_repo(path: Path) -> bool:
    """Whether a git tree lives anywhere under `path`. True when a directory
    cannot be read, so a failure keeps the path."""
    for _, dirs, files in os.walk(path, onerror=_raise):
        if ".git" in dirs or ".git" in files:
            return True
    return False


def _confined(top: Path, base: Path) -> bool:
    """Whether `top` lies under `base` with no symlink on the way down."""
    try:
        relative = top.relative_to(base)
    except ValueError:
        return False
    if not relative.parts or ".." in relative.parts:
        return False
    return os.path.realpath(top) == os.path.join(os.path.realpath(base), *relative.parts)


def _scan(top: Path, base: Path) -> tuple[list[tuple[Path, Path | None]], float]:
    """The slim targets under `top`, each with the git tree that holds it or
    None, and the newest mtime of a file outside SLIM_DIRS. Nothing when a
    symlink lies between `base` and `top`, or git cannot tell whether a tree
    holds `top`."""
    targets: list[tuple[Path, Path | None]] = []
    newest = 0.0
    if not _confined(top, base) or not top.is_dir():
        return targets, newest
    known, outer = _enclosing_repo(top)
    if not known:
        return targets, newest
    stack: list[tuple[Path, Path | None]] = [(top, outer)]
    while stack:
        directory, repo = stack.pop()
        try:
            entries = list(os.scandir(directory))
        except OSError:
            continue
        if any(e.name == ".git" for e in entries):
            repo = directory
        for entry in entries:
            if entry.name == ".git" or entry.is_symlink():
                continue
            path = Path(entry.path)
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                continue
            if is_dir:
                if (entry.name in SLIM_DIRS
                        or entry.name.startswith(SLIM_DIR_PREFIXES)):
                    targets.append((path, repo))
                else:
                    stack.append((path, repo))
                continue
            try:
                newest = max(newest, entry.stat(follow_symlinks=False).st_mtime)
            except OSError:
                pass
            if os.path.splitext(entry.name)[1].lower() in SLIM_SUFFIXES:
                targets.append((path, repo))
    return targets, newest


def _tracked(path: Path, repo: Path | None) -> bool:
    """Whether git tracks the path or a file under it. True when git cannot
    tell, so a failure keeps the path."""
    if repo is None:
        return False
    listed = git_util.run_git_status(
        path.parent, ["--literal-pathspecs", "ls-files", "-z", "--", path.name],
        timeout=60)
    return listed.returncode != 0 or bool(listed.stdout)


def slim(top: Path, base: Path) -> dict:
    """Delete the untracked slim targets under `top`. Returns the paths
    deleted, the bytes they held, and the errors met."""
    targets, _ = _scan(top, base)
    return _delete(targets)


def _delete(targets: list[tuple[Path, Path | None]]) -> dict:
    removed: list[str] = []
    errors: list[str] = []
    freed = 0
    for path, repo in targets:
        if _tracked(path, repo):
            continue
        try:
            if path.is_dir() and not path.is_symlink() and _holds_repo(path):
                continue
        except OSError:
            continue
        size, _ = disk_usage._measure(path)
        try:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink()
        except OSError as e:
            errors.append(f"{path}: {type(e).__name__}: {e}")
            continue
        removed.append(str(path))
        freed += size
    return {"removed": removed, "bytes": freed, "errors": errors}


def _idle_long_enough(item: dict, now: float) -> bool:
    updated = disk_usage._updated_ts(item)
    return (not disk_usage._in_use(item) and updated > 0
            and now - updated >= SLIM_AFTER_DAYS * 86400)


def _task_ready(row: dict, now: float) -> bool:
    for item_id in work_worktree._items_using(row):
        item = db.query_one(
            "SELECT id, state, archived_at, updated_at FROM work_items WHERE id = ?",
            (item_id,))
        if item is None or not _idle_long_enough(dict(item), now):
            return False
        if terminal.session_healthy(f"work-{item_id}").get("alive"):
            return False
    return True


def slim_tasks(now: float) -> list[dict]:
    """Slim every task worktree frshty created whose work is finished."""
    results = []
    rows = db.query_all(
        "SELECT * FROM work_worktrees WHERE removed_at IS NULL AND origin = 'created' "
        "ORDER BY id")
    for raw in rows:
        row = dict(raw)
        if not os.path.isdir(row["path"]) or not _task_ready(row, now):
            continue
        with work_store.launch_lock:
            if not _task_ready(row, now):
                continue
            result = slim(Path(row["path"]), core_config.TASK_WORKTREE_ROOT)
        results.append({"path": row["path"], **result})
    return results


def slim_tickets(instance_key: str, config: dict, now: float) -> list[dict]:
    """Slim every ticket directory of one instance whose ticket is done."""
    ws = config.get("workspace") or {}
    if not ws.get("root"):
        return []
    tickets_root = Path(ws["root"]) / str(ws.get("tickets_dir") or "tickets")
    marks = ",".join("?" * len(TICKET_DONE_STATUSES))
    rows = db.query_all(
        f"SELECT slug FROM tickets WHERE instance_key = ? AND status IN ({marks}) "
        "AND slug IS NOT NULL AND slug != ''",
        (instance_key, *TICKET_DONE_STATUSES))
    results = []
    for row in rows:
        ticket_dir = tickets_root / row["slug"]
        if (ticket_dir.parent != tickets_root or ticket_dir.is_symlink()
                or not ticket_dir.is_dir()):
            continue
        targets, newest = _scan(ticket_dir, tickets_root)
        if not targets or now - newest < SLIM_AFTER_DAYS * 86400:
            continue
        results.append({"path": str(ticket_dir), **_delete(targets)})
    return results


def sweep(now: float | None = None, force: bool = False) -> list[dict]:
    """Slim every finished task worktree and done ticket directory. Runs at
    most once per SWEEP_INTERVAL_S unless `force` is set. Returns one result
    per directory slimmed."""
    global _last_sweep
    now = time.time() if now is None else now
    with _lock:
        if not force and now - _last_sweep < SWEEP_INTERVAL_S:
            return []
        _last_sweep = now
        results = slim_tasks(now)
        instances = runtime.instances()
        for reg in instances.all() if instances else []:
            if (reg.config.get("features") or {}).get("tickets"):
                results += slim_tickets(reg.instance_key, reg.config, now)
    results = [r for r in results if r["removed"] or r["errors"]]
    for result in results:
        if result["errors"]:
            log.emit("dir_slim_error",
                     f"could not delete {len(result['errors'])} path(s) in "
                     f"{result['path']}: {result['errors'][0][:200]}",
                     meta={"path": result["path"], "errors": result["errors"][:20]})
    removed = [r for r in results if r["removed"]]
    if removed:
        freed = sum(r["bytes"] for r in removed)
        log.emit("dir_slim",
                 f"freed {disk_usage.human(freed)} of dependencies and videos in "
                 f"{len(removed)} finished task or ticket folder(s)",
                 meta={"bytes": freed,
                       "paths": {r["path"]: r["removed"][:20] for r in removed}})
    return results
