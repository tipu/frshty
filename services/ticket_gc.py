"""Remove the ticket folders whose work ended.

Each ticket and each task run in a tickets root gets a folder there that
holds a git worktree of every repository, with its dependencies and build
output. One folder holds 1 to 20 GB. dir_slim deletes only the dependency
trees and videos, so the folder itself stayed on disk for good.

`sweep` reclaims a folder of the tickets root of each instance with the
tickets feature in two cases.

- Its work ended. The folder of a ticket whose status is in ENDED_STATUSES
  and whose row was last updated ENDED_AFTER_DAYS ago, or a work-NNNN folder
  whose work item is closed or archived, was last updated ENDED_AFTER_DAYS
  ago and has no live session. This pass runs at most once per
  SWEEP_INTERVAL_S.
- It is an orphan. No ticket row of any instance names the folder, and no
  work item of this database owns it, and no file in it changed for
  ORPHAN_AFTER_DAYS. This pass runs at most once per ORPHAN_SWEEP_INTERVAL_S,
  once a day.

A folder that a ticket in another state names, a folder whose name starts
with a dot, a symlink, and a folder that a work run of an unfinished work
item uses are never touched.

A reclaim first removes every git tree in the folder, deepest first. A linked
worktree goes through git_util.release_worktree with a salvage label, so its
uncommitted files and its HEAD stay reachable under
refs/frshty-salvage/tickets/<folder>/<path> of its repository, and the branch
stays, so the ticket pipeline can add the worktree again. A standalone
clone is deleted only when worktree_loss finds nothing to lose. A git tree
that cannot be removed keeps the whole folder. Then the rest is deleted. A
ticket folder keeps its docs/ directory and its top-level files, because
/tickets, releases and the PR pipeline read them after the ticket ends.
Every other folder is deleted whole.

`sweep` runs from the work board scan loop."""
import os
import re
import shutil
import threading
import time
from pathlib import Path

import core.db as db
import core.git_util as git_util
import core.log as log
import core.runtime as runtime
import core.terminal as terminal
from services import disk_usage, work_store

ENDED_AFTER_DAYS = 1
ORPHAN_AFTER_DAYS = 7
SWEEP_INTERVAL_S = 3600
ORPHAN_SWEEP_INTERVAL_S = 86400
ENDED_STATUSES = ("merged", "done", "ignored")
TICKET_KEEP = "docs"
SKIP_WALK = frozenset({".git", "node_modules"})
WORK_FOLDER = re.compile(r"work-(\d+)(?:-|$)")

_lock = threading.Lock()
_last_sweep = 0.0
_last_orphan_sweep = 0.0


def _confined(folder: Path, base: Path) -> bool:
    """Whether `folder` is a direct child of `base` with no symlink on the way."""
    return (folder.parent == base and not folder.is_symlink()
            and os.path.realpath(folder) == os.path.join(os.path.realpath(base), folder.name))


def _git_trees(folder: Path) -> list[Path]:
    """Every directory under `folder` that holds a .git entry, deepest first.
    Symlinks are not followed. None of .git and node_modules is entered."""
    trees = []
    for root, dirs, files in os.walk(folder, onerror=lambda e: None):
        if ".git" in dirs or ".git" in files:
            trees.append(Path(root))
        dirs[:] = [d for d in dirs if d not in SKIP_WALK
                   and not os.path.islink(os.path.join(root, d))]
    return sorted(trees, key=lambda p: len(p.parts), reverse=True)


def _main_checkout(tree: Path) -> str:
    """The directory to run `git worktree` in for a linked worktree, "" when
    git cannot name the repository."""
    _, common = git_util.git_dirs(tree)
    if not common:
        return ""
    return os.path.dirname(common) if os.path.basename(common) == ".git" else common


def _clone_loss(clone: Path) -> str:
    """What deleting a standalone clone would lose, "" when nothing: a local
    change, a commit of any ref that no remote branch holds, such as another
    branch, a stash or a salvage ref, or a linked worktree."""
    loss = git_util.worktree_loss(clone)
    if loss:
        return loss
    local = git_util.run_git_status(clone, ["rev-list", "--max-count=1", "--all",
                                            "--not", "--remotes"], timeout=120)
    if local.returncode != 0:
        return f"git rev-list failed: {local.stderr.strip()[:200]}"
    if local.stdout.strip():
        return "a ref holds a commit that no remote branch holds"
    linked = git_util.worktree_entries(clone)
    if linked is None:
        return "git worktree list failed"
    if linked:
        return "the clone has a linked worktree"
    return ""


def _release(folder: Path, tree: Path, still_idle) -> str:
    """Remove one git tree of `folder`. "" when it is gone, else the reason
    it stays."""
    if (tree / ".git").is_dir():
        loss = _clone_loss(tree)
        if loss:
            return f"a clone that would lose this: {loss}"
        if not still_idle():
            return "used during the sweep"
        shutil.rmtree(tree)
        return ""
    repo = _main_checkout(tree)
    if not repo:
        return "git cannot name the repository of the worktree"
    label = f"tickets/{folder.name}/{tree.relative_to(folder).as_posix()}"
    return git_util.release_worktree(repo, tree, still_idle=still_idle, salvage_label=label)


def _busy(folder: Path) -> str:
    """What still works in `folder`, "" when nothing: a ticket terminal of a
    ticket the folder belongs to, or a run of an unfinished work item or of
    one with a live session."""
    for row in db.query_all("SELECT DISTINCT ticket_key FROM tickets WHERE slug = ?",
                            (folder.name,)):
        if terminal.session_healthy(row["ticket_key"]).get("alive"):
            return f"the terminal of {row['ticket_key']} is open"
    inside = str(folder).rstrip(os.sep) + os.sep
    rows = db.query_all(
        "SELECT DISTINCT r.work_item_id, r.cwd, i.state, i.archived_at FROM work_runs r "
        "JOIN work_items i ON i.id = r.work_item_id WHERE r.cwd = ? OR r.cwd LIKE ?",
        (str(folder), inside + "%"))
    for row in rows:
        if row["cwd"] != str(folder) and not (row["cwd"] or "").startswith(inside):
            continue
        if disk_usage._in_use(dict(row)):
            return f"work item {row['work_item_id']} is not finished"
        if terminal.session_healthy(f"work-{row['work_item_id']}").get("alive"):
            return f"work item {row['work_item_id']} has a live session"
    return ""


def release_trees(folder: Path) -> str:
    """Remove every git tree of `folder`. "" when all are gone, else the
    reason the first one that stays gives. Each removal asks _busy again
    after its checks, so a terminal or a run that starts during the sweep
    keeps the tree."""
    still_idle = lambda: not _busy(folder)
    for tree in _git_trees(folder):
        reason = _release(folder, tree, still_idle)
        if reason:
            return f"{tree}: {reason}"
    return ""


def delete_rest(folder: Path, keep_docs: bool) -> None:
    """Delete `folder`, or, when `keep_docs` is set, every directory in it
    but docs/."""
    if keep_docs:
        for entry in os.scandir(folder):
            if entry.name == TICKET_KEEP or not entry.is_dir(follow_symlinks=False):
                continue
            shutil.rmtree(entry.path)
    else:
        shutil.rmtree(folder)


def _work_item_ended(item_id: int, now: float) -> bool | None:
    """Whether the work item ended ENDED_AFTER_DAYS ago with no live session,
    None when this database holds no such item."""
    item = db.query_one(
        "SELECT id, state, archived_at, updated_at FROM work_items WHERE id = ?", (item_id,))
    if item is None:
        return None
    item = dict(item)
    if disk_usage._in_use(item):
        return False
    if now - disk_usage._updated_ts(item) < ENDED_AFTER_DAYS * 86400:
        return False
    return not terminal.session_healthy(f"work-{item_id}").get("alive")


def _ticket_ended(row: dict, now: float) -> bool:
    return (row["status"] in ENDED_STATUSES
            and now - disk_usage._updated_ts(row) >= ENDED_AFTER_DAYS * 86400)


def _verdict(name: str, folder: Path, instance_key: str, now: float,
             orphans: bool) -> tuple[str, bool] | None:
    """("ended" or "orphan", keep_docs) for a folder to reclaim, None to leave
    it. Only an orphan reads the disk."""
    rows = [dict(r) for r in db.query_all(
        "SELECT instance_key, status, updated_at FROM tickets WHERE slug = ?", (name,))]
    if rows:
        own = [r for r in rows if r["instance_key"] == instance_key]
        if own and all(_ticket_ended(r, now) for r in rows):
            return "ended", True
        return None
    match = WORK_FOLDER.match(name)
    ended = _work_item_ended(int(match.group(1)), now) if match else None
    if ended:
        return "ended", False
    if ended is False or not orphans:
        return None
    _, newest = disk_usage._measure(folder)
    if now - newest < ORPHAN_AFTER_DAYS * 86400:
        return None
    return "orphan", False


def sweep_tickets(instance_key: str, config: dict, now: float, orphans: bool) -> list[dict]:
    """Reclaim every folder of one instance's tickets root whose work ended,
    and every orphan when `orphans` is set."""
    ws = config.get("workspace") or {}
    if not (config.get("features") or {}).get("tickets") or not ws.get("root"):
        return []
    base = Path(ws["root"]) / str(ws.get("tickets_dir") or "tickets")
    if base.is_symlink() or not base.is_dir():
        return []
    results = []
    for name in sorted(os.listdir(base)):
        folder = base / name
        if name.startswith(".") or not folder.is_dir() or not _confined(folder, base):
            continue
        verdict = _verdict(name, folder, instance_key, now, orphans)
        if verdict is None:
            continue
        before, _ = disk_usage._measure(folder)
        kept = ""
        try:
            with work_store.launch_lock:
                if _busy(folder):
                    continue
                if verdict[0] == "ended" and _verdict(name, folder, instance_key,
                                                      now, False) != verdict:
                    continue
                kept = release_trees(folder)
            if not kept:
                delete_rest(folder, keep_docs=verdict[1])
        except OSError as e:
            kept = f"{type(e).__name__}: {e}"[:300]
        after, _ = disk_usage._measure(folder) if folder.exists() else (0, 0.0)
        results.append({"path": str(folder), "bytes": max(before - after, 0),
                        "kept": kept, "why": verdict[0]})
    return results


def sweep(now: float | None = None, force: bool = False) -> list[dict]:
    """Reclaim the ended and orphan folders of every tickets root. The ended
    pass runs at most once per SWEEP_INTERVAL_S and the orphan pass at most
    once per ORPHAN_SWEEP_INTERVAL_S unless `force` is set. Returns one result
    per folder reclaimed or kept."""
    global _last_sweep, _last_orphan_sweep
    now = time.time() if now is None else now
    with _lock:
        if not force and now - _last_sweep < SWEEP_INTERVAL_S:
            return []
        _last_sweep = now
        orphans = force or now - _last_orphan_sweep >= ORPHAN_SWEEP_INTERVAL_S
        if orphans:
            _last_orphan_sweep = now
        results = []
        instances = runtime.instances()
        for reg in instances.all() if instances else []:
            results += sweep_tickets(reg.instance_key, reg.config, now, orphans)
    gone = [r for r in results if not r["kept"]]
    kept = [r for r in results if r["kept"]]
    if gone:
        freed = sum(r["bytes"] for r in gone)
        log.emit("ticket_gc",
                 f"freed {disk_usage.human(freed)} in {len(gone)} ended or orphan "
                 "ticket folder(s)",
                 meta={"bytes": freed, "paths": {r["path"]: r["why"] for r in gone}})
    if kept:
        log.emit("ticket_gc_kept",
                 f"kept {len(kept)} ended or orphan ticket folder(s) that frshty "
                 "could not remove",
                 meta={"kept": {r["path"]: r["kept"] for r in kept}})
    return results
