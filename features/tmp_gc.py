"""Remove the stale files that pile up in an instance container's /tmp.

The container's /tmp is a RAM-backed tmpfs (scripts/instance.py TMP_MOUNT),
so every byte in it counts against the container's memory limit. Two kinds of
file fill it:

- pytest runs: pytest-of-<user>/pytest-<n> holds every tmp_path of one test
  session. One session of this repository writes about 400 MB.
- native libraries that a bundled CLI extracts on every start, named
  .<16 hex>-<8 hex>.so. Each copy is about 5 MB and no exit removes it.

`sweep` removes a pytest run whose directory did not change for STALE_S and a
library file older than STALE_S. It keeps the run that pytest-current points
to, a run whose .lock names a live pid, and every path that a live process
has as its cwd, holds open or maps. It removes nothing when it cannot read
the /proc entry of a live process.
It touches nothing else in /tmp: claude-1000, cc-socks, frshty-xauth and
.X11-unix match neither pattern.

The cron fan-out routes the sweep to every instance. It runs at most once per
SWEEP_INTERVAL_S. Run it by hand:

    FRSHTY_ROOT=<state dir> python -m features.tmp_gc <config.toml> [--dry-run]
"""
import argparse
import json
import os
import re
import shutil
import time
from pathlib import Path

import core.config as core_config
import core.log as log
import core.state as state

TMP_DIR = "/tmp"
STALE_S = 6 * 3600
SWEEP_INTERVAL_S = 3600
STAMP = ".tmp-gc-stamp"
PYTEST_ROOT = re.compile(r"^pytest-of-[^/]+$")
PYTEST_RUN = re.compile(r"^pytest-\d+$")
EXTRACTED_SO = re.compile(r"^\.[0-9a-f]{16}-[0-9a-f]{8}\.so$")


class ProcUnreadable(Exception):
    pass


def _live_paths(proc: str = "/proc") -> set[str]:
    """Every path that a live process uses: its cwd, each open file and each
    mapped file. A process that exits mid-scan adds nothing. Raises
    ProcUnreadable when /proc or the entry of a live process cannot be read,
    so a sweep never treats an unknown process as one that uses nothing."""
    paths: set[str] = set()
    try:
        pids = [p for p in os.listdir(proc) if p.isdigit()]
    except OSError as e:
        raise ProcUnreadable(f"{proc}: {e}") from e
    for pid in pids:
        base = os.path.join(proc, pid)
        for read in (_cwd, _fds, _maps):
            try:
                paths.update(read(base))
            except (FileNotFoundError, ProcessLookupError):
                pass
            except OSError as e:
                if os.path.exists(base):
                    raise ProcUnreadable(f"{base}: {e}") from e
    return paths


def _cwd(base: str) -> list[str]:
    return [os.readlink(os.path.join(base, "cwd"))]


def _fds(base: str) -> list[str]:
    out = []
    for fd in os.listdir(os.path.join(base, "fd")):
        try:
            out.append(os.readlink(os.path.join(base, "fd", fd)))
        except FileNotFoundError:
            pass
    return out


def _maps(base: str) -> list[str]:
    out = []
    with open(os.path.join(base, "maps")) as f:
        for line in f:
            parts = line.split(None, 5)
            if len(parts) == 6 and parts[5].startswith("/"):
                out.append(parts[5].rstrip("\n"))
    return out


def _locked_by_live_pid(run: str) -> bool:
    """pytest writes its pid into <run>/.lock while the session runs and
    removes the file when the session ends."""
    try:
        pid = int(Path(run, ".lock").read_text().strip())
    except (OSError, ValueError):
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _in_use(path: str, live: set[str]) -> bool:
    prefix = path + os.sep
    return any(p == path or p.startswith(prefix) for p in live)


def _candidates(root: str, cutoff: float) -> list[str]:
    """The stale pytest runs and extracted libraries directly under root."""
    out: list[str] = []
    try:
        entries = list(os.scandir(root))
    except OSError:
        return out
    for entry in entries:
        if EXTRACTED_SO.match(entry.name) and entry.is_file(follow_symlinks=False):
            if entry.stat(follow_symlinks=False).st_mtime < cutoff:
                out.append(entry.path)
        elif PYTEST_ROOT.match(entry.name) and entry.is_dir(follow_symlinks=False):
            current = os.path.realpath(os.path.join(entry.path, "pytest-current"))
            try:
                runs = list(os.scandir(entry.path))
            except OSError:
                continue
            for run in runs:
                if not (PYTEST_RUN.match(run.name) and run.is_dir(follow_symlinks=False)):
                    continue
                if os.path.realpath(run.path) == current or _locked_by_live_pid(run.path):
                    continue
                if run.stat(follow_symlinks=False).st_mtime < cutoff:
                    out.append(run.path)
    return sorted(out)


def sweep(config: dict, now: float | None = None, force: bool = False,
          dry_run: bool = False, root: str = TMP_DIR, proc: str = "/proc") -> dict:
    """Remove every stale pytest run and extracted library in root that no
    live process uses.

    A dry run removes nothing and reports what a sweep would remove.
    Returns {"removed": [...], "kept": {path: reason}, "freed_bytes": n}, or
    {"throttled": True} when the last sweep ran less than SWEEP_INTERVAL_S ago."""
    now = time.time() if now is None else now
    state_dir = Path(config["_state_dir"])
    stamp = state_dir / STAMP
    if not (force or dry_run) and stamp.exists() and now - stamp.stat().st_mtime < SWEEP_INTERVAL_S:
        return {"throttled": True}
    if not (force or dry_run):
        state_dir.mkdir(parents=True, exist_ok=True)
        stamp.touch()
    candidates = _candidates(root, now - STALE_S)
    removed: list[str] = []
    kept: dict[str, str] = {}
    freed = 0
    key = config["job"]["key"]
    for path in candidates:
        size = _size(path)
        try:
            live = _live_paths(proc)
        except ProcUnreadable as e:
            if not dry_run:
                log.emit("tmp_gc_failed", f"Stopped the /tmp sweep: cannot read {e}",
                         meta={"removed": removed}, job=key, instance_key=key)
            return {"removed": removed, "kept": kept, "freed_bytes": freed,
                    "stopped": str(e)}
        if _in_use(path, live):
            kept[path] = "in use by a live process"
            continue
        if not dry_run:
            try:
                if os.path.isdir(path):
                    shutil.rmtree(path)
                else:
                    os.remove(path)
            except OSError as e:
                kept[path] = str(e)
                continue
        removed.append(path)
        freed += size
    if removed and not dry_run:
        log.emit("tmp_gc", f"Removed {len(removed)} stale /tmp entr(ies), {freed // (1 << 20)} MB",
                 meta={"removed": removed, "freed_bytes": freed}, job=key, instance_key=key)
    errors = {p: r for p, r in kept.items() if r != "in use by a live process"}
    if errors and not dry_run:
        log.emit("tmp_gc_failed", f"Could not remove {len(errors)} stale /tmp entr(ies)",
                 meta={"kept": errors}, job=key, instance_key=key)
    return {"removed": removed, "kept": kept, "freed_bytes": freed}


def _size(path: str) -> int:
    if not os.path.isdir(path):
        try:
            return os.lstat(path).st_size
        except OSError:
            return 0
    total = 0
    for dirpath, _, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(dirpath, name)).st_size
            except OSError:
                pass
    return total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m features.tmp_gc")
    parser.add_argument("config")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    config = core_config.load_config(args.config)
    state.init(config["job"]["key"])
    print(json.dumps(sweep(config, force=True, dry_run=args.dry_run), indent=1))
