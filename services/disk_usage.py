"""The disk frshty holds under its state root, and what to clear first.

`scan` measures every entry of frshty_root(). It splits the worktrees and
artifacts folders into one unit per task folder and the orphaned-worktrees
folder into one unit per entry, because those are the units an operator
clears. A unit is a suggestion when no unfinished task uses it, nothing in
it changed for IDLE_DAYS, and it holds more than MIN_SUGGEST_BYTES. A task
folder named work-<id> belongs to that task, and so does every task whose
recorded worktree lies inside it. Suggestions come largest first. Each
worktree suggestion that holds a git tree counts the uncommitted files in it,
so the operator sees what a removal would lose.

Nothing here deletes anything. The automatic sweeps in work_worktree.gc and
work_artifacts.gc_artifacts stay the only code that removes a folder.

`refresh` runs `scan` at most once per SCAN_INTERVAL_S from the work board
scan loop. The usage is high when frshty holds WARN_BYTES or more, or when
the filesystem has less than WARN_FREE_PERCENT free. A disk_usage_high event
goes to the feed when the usage becomes high and names the largest
suggestions, and a disk_usage_ok event when it is no longer high."""
import os
import re
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.git_util as git_util
import core.log as log
import core.state as state
from core.paths import frshty_root
from services import work_store

IDLE_DAYS = 3
MIN_SUGGEST_BYTES = 100 * 1024 ** 2
WARN_BYTES = 20 * 1024 ** 3
WARN_FREE_PERCENT = 10
SCAN_INTERVAL_S = 3600
MAX_SUGGESTIONS = 15
EVENT_SUGGESTIONS = 3
SPLIT_DIRS = ("artifacts", "orphaned-worktrees")
WORKTREES_DIR = "worktrees"
ITEM_DIR = re.compile(r"^work-(\d+)")
_STATE_MODULE = "disk_usage"

_lock = threading.Lock()
_report: dict | None = None


def _measure(path: Path) -> tuple[int, float]:
    """The bytes a path holds on disk and the newest mtime inside it.
    Symlinks are counted as links and never followed."""
    try:
        top = path.lstat()
    except OSError:
        return 0, 0.0
    size = top.st_blocks * 512
    newest = top.st_mtime
    if not path.is_dir() or path.is_symlink():
        return size, newest
    for root, dirs, files in os.walk(path, onerror=lambda e: None):
        for name in dirs + files:
            try:
                st = os.lstat(os.path.join(root, name))
            except OSError:
                continue
            size += st.st_blocks * 512
            newest = max(newest, st.st_mtime)
    return size, newest


def _units(base: Path) -> list[tuple[str, Path]]:
    """(kind, path) for every unit under the state root."""
    units: list[tuple[str, Path]] = []
    try:
        entries = sorted(base.iterdir())
    except OSError:
        return units
    for entry in entries:
        split: list[tuple[str, Path]] | None = None
        try:
            if entry.name == WORKTREES_DIR and entry.is_dir() and not entry.is_symlink():
                split = [("worktree", p)
                         for repo in sorted(p for p in entry.iterdir()
                                            if p.is_dir() and not p.is_symlink())
                         for p in sorted(repo.iterdir())]
            elif entry.name in SPLIT_DIRS and entry.is_dir() and not entry.is_symlink():
                kind = "artifact" if entry.name == "artifacts" else "orphan"
                split = [(kind, p) for p in sorted(entry.iterdir())]
        except OSError:
            split = None
        units += split if split is not None else [("other", entry)]
    return units


def _items_for(kind: str, path: Path) -> list[dict]:
    """The tasks that use a unit, each with its state and last update."""
    ids: set[int] = set()
    match = ITEM_DIR.match(path.name)
    if match and kind in ("worktree", "artifact"):
        ids.add(int(match.group(1)))
    if kind != "other":
        inside = str(path).rstrip(os.sep) + os.sep
        ids |= {r["work_item_id"] for r in db.query_all(
            "SELECT work_item_id, path FROM work_worktrees WHERE path LIKE ?",
            (inside + "%",)) if (r["path"] or "").startswith(inside)}
        ids |= {r["work_item_id"] for r in db.query_all(
            "SELECT DISTINCT work_item_id, cwd FROM work_runs WHERE cwd = ? OR cwd LIKE ?",
            (str(path), inside + "%"))
            if r["cwd"] == str(path) or (r["cwd"] or "").startswith(inside)}
    items = []
    for item_id in sorted(ids):
        row = db.query_one(
            "SELECT id, state, archived_at, updated_at FROM work_items WHERE id = ?",
            (item_id,))
        if row is not None:
            items.append(dict(row))
    return items


def _in_use(item: dict) -> bool:
    return not item["archived_at"] and item["state"] not in work_store.CLOSED_STATES


def _updated_ts(item: dict) -> float:
    try:
        when = datetime.fromisoformat((item["updated_at"] or "").strip())
    except ValueError:
        return 0.0
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return when.timestamp()


def _git_trees(path: Path) -> list[Path] | None:
    """The git trees of a worktree unit, None when the unit cannot be read."""
    try:
        if (path / ".git").exists():
            return [path]
        return [p for p in sorted(path.iterdir()) if (p / ".git").exists()]
    except OSError:
        return None


def _uncommitted(trees: list[Path]) -> int | None:
    """Uncommitted files across git trees, None when git cannot read one."""
    total = 0
    for tree in trees:
        try:
            total += len(git_util.dirty_paths(tree))
        except Exception:
            return None
    return total


def scan(now: float | None = None, base: Path | None = None) -> dict:
    now = time.time() if now is None else now
    base = base or frshty_root()
    units = []
    for kind, path in _units(base):
        size, newest = _measure(path)
        items = _items_for(kind, path)
        last_used = max([newest] + [_updated_ts(i) for i in items])
        units.append({"kind": kind, "path": str(path), "name": path.name, "bytes": size,
                      "last_used": datetime.fromtimestamp(last_used, timezone.utc).isoformat(),
                      "idle_days": max(0, int((now - last_used) // 86400)),
                      "items": [{"id": i["id"], "state": i["state"]} for i in items],
                      "in_use": any(_in_use(i) for i in items)})
    total = sum(u["bytes"] for u in units)
    candidates = sorted((u for u in units if u["kind"] != "other" and not u["in_use"]
                         and u["idle_days"] >= IDLE_DAYS and u["bytes"] > MIN_SUGGEST_BYTES),
                        key=lambda u: (-u["bytes"], u["path"]))
    suggestions = candidates[:MAX_SUGGESTIONS]
    for unit in suggestions:
        if unit["kind"] != "worktree" or not Path(unit["path"]).is_dir():
            continue
        trees = _git_trees(Path(unit["path"]))
        if trees is None or trees:
            unit["uncommitted"] = None if trees is None else _uncommitted(trees)
    try:
        fs = shutil.disk_usage(base)
        disk = {"total": fs.total, "free": fs.free,
                "free_percent": round(100 * fs.free / fs.total, 1) if fs.total else 0.0}
    except OSError:
        disk = None
    reasons = []
    if total >= WARN_BYTES:
        reasons.append(f"frshty holds {human(total)}, over {human(WARN_BYTES)}")
    if disk and disk["free_percent"] < WARN_FREE_PERCENT:
        reasons.append(f"the disk has {disk['free_percent']}% free, under {WARN_FREE_PERCENT}%")
    by_kind: dict[str, int] = {}
    for unit in units:
        by_kind[unit["kind"]] = by_kind.get(unit["kind"], 0) + unit["bytes"]
    return {"scanned_at": datetime.fromtimestamp(now, timezone.utc).isoformat(),
            "root": str(base), "total_bytes": total, "by_kind": by_kind, "disk": disk,
            "high": bool(reasons), "reasons": reasons, "idle_days": IDLE_DAYS,
            "min_bytes": MIN_SUGGEST_BYTES,
            "reclaimable_bytes": sum(u["bytes"] for u in candidates),
            "reclaimable_count": len(candidates), "suggestions": suggestions}


def human(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.0f} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def _report_change(report: dict) -> None:
    prior = state.load(_STATE_MODULE) or {}
    if report["high"] and not prior.get("high"):
        top = "; ".join(f"{u['kind']} {u['name']} {human(u['bytes'])} idle {u['idle_days']} d"
                        for u in report["suggestions"][:EVENT_SUGGESTIONS])
        log.emit("disk_usage_high",
                 f"disk usage is high: {', '.join(report['reasons'])}. "
                 f"{report['reclaimable_count']} idle folder(s) hold "
                 f"{human(report['reclaimable_bytes'])}"
                 + (f"; clear first: {top}" if top else ""),
                 links={"tasks": "/tasks"},
                 meta={"reasons": report["reasons"], "total_bytes": report["total_bytes"],
                       "suggestions": [u["path"] for u in report["suggestions"]]})
    elif not report["high"] and prior.get("high"):
        log.emit("disk_usage_ok",
                 f"disk usage is no longer high: frshty holds {human(report['total_bytes'])}",
                 links={"tasks": "/tasks"}, meta={"total_bytes": report["total_bytes"]})
    if bool(prior.get("high")) != report["high"]:
        state.save(_STATE_MODULE, {"high": report["high"]})


def refresh(force: bool = False, now: float | None = None) -> dict:
    """The latest report. Scans again when the last scan is SCAN_INTERVAL_S
    old, or when `force` is set."""
    global _report
    now = time.time() if now is None else now
    with _lock:
        if (not force and _report is not None
                and now - datetime.fromisoformat(_report["scanned_at"]).timestamp()
                < SCAN_INTERVAL_S):
            return _report
        _report = scan(now)
        _report_change(_report)
        return _report


def latest() -> dict:
    """The cached report, scanned now when no scan ran yet."""
    return _report if _report is not None else refresh()
