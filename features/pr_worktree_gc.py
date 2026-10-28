"""Remove the PR worktrees that frshty no longer uses.

own_prs keeps one worktree per PR branch under <state>/pr_worktrees, and
pr_autofix one under <state>/autofix_worktrees. frshty locks each one with
WORKTREE_LOCK_REASON, so a `git worktree remove` from outside is refused, and
nothing in frshty removed them when the PR closed. They pile up: one PR
worktree holds about 1.6 GB.

`sweep` removes each of those worktrees that carries frshty's lock and was
not used for IDLE_DAYS, through git_util.release_worktree, which keeps a
worktree with a local change or a HEAD that no remote branch holds. Each
feature's _ensure_worktree adds the worktree again when its PR needs it, so
a removed worktree of a PR that is still open costs one checkout.

The cron fan-out routes the sweep to every instance with review_prs or
pr_autofix. It runs at most once per SWEEP_INTERVAL_S. Run it by hand for an
instance that cannot boot:

    FRSHTY_ROOT=<state dir> python -m features.pr_worktree_gc <config.toml> [--dry-run] [--only FILE]

--only limits the sweep to the worktree paths that FILE lists, one per line.
"""
import argparse
import json
import os
import time
from pathlib import Path

import core.config as core_config
import core.git_util as git_util
import core.log as log
import core.state as state

IDLE_DAYS = 3
SWEEP_INTERVAL_S = 3600
WORKTREE_DIRS = ("pr_worktrees", "autofix_worktrees")
STAMP = ".pr-worktree-gc-stamp"


def _last_used(worktree: str) -> float | None:
    """The newest change to the worktree's HEAD or index. A reset, a commit
    and a checkout each write one of them. None when git cannot name the
    worktree's admin directory."""
    git_dir, _ = git_util.git_dirs(worktree)
    if not git_dir:
        return None
    times = [os.stat(p).st_mtime for p in (os.path.join(git_dir, n) for n in
                                           ("index", "HEAD", os.path.join("logs", "HEAD")))
             if os.path.exists(p)]
    return max(times) if times else None


def _idle(worktree: str, cutoff: float) -> bool:
    used = _last_used(worktree)
    return used is not None and used < cutoff


def sweep(config: dict, now: float | None = None, force: bool = False,
          dry_run: bool = False, only: set[str] | None = None) -> dict:
    """Remove every idle PR worktree of this instance that loses nothing.

    A dry run removes nothing and reports what a sweep would remove and keep.
    `only`, when given, limits the sweep to those worktree paths.
    Returns {"removed": [...], "kept": {path: reason}}, or {"throttled": True}
    when the last sweep ran less than SWEEP_INTERVAL_S ago."""
    now = time.time() if now is None else now
    state_dir = Path(config["_state_dir"])
    stamp = state_dir / STAMP
    if not (force or dry_run) and stamp.exists() and now - stamp.stat().st_mtime < SWEEP_INTERVAL_S:
        return {"throttled": True}
    if not (force or dry_run):
        state_dir.mkdir(parents=True, exist_ok=True)
        stamp.touch()
    roots = [os.path.realpath(state_dir / d) for d in WORKTREE_DIRS]
    cutoff = now - IDLE_DAYS * 86400
    removed: list[str] = []
    kept: dict[str, str] = {}
    for repo in core_config.get_repos(config):
        blocks = git_util.worktree_entries(repo["path"])
        if blocks is None:
            kept[str(repo["path"])] = "git worktree list failed"
            continue
        for lines in blocks:
            if not lines[0].startswith("worktree "):
                continue
            path = lines[0][len("worktree "):].strip()
            real = os.path.realpath(path)
            if not any(real.startswith(r + os.sep) for r in roots):
                continue
            if only is not None and path not in only:
                continue
            if f"locked {git_util.WORKTREE_LOCK_REASON}" not in lines or not os.path.isdir(path):
                continue
            used = _last_used(path)
            if used is None:
                kept[path] = "admin directory not found"
                continue
            if not _idle(path, cutoff):
                continue
            if dry_run:
                reason = git_util.worktree_loss(path)
            else:
                reason = git_util.release_worktree(repo["path"], path,
                                                   still_idle=lambda p=path: _idle(p, cutoff))
            if reason:
                kept[path] = reason
            else:
                removed.append(path)
    key = config["job"]["key"]
    if removed and not dry_run:
        log.emit("pr_worktree_gc", f"Removed {len(removed)} idle PR worktree(s)",
                 meta={"removed": removed}, job=key, instance_key=key)
    if kept and not dry_run:
        log.emit("pr_worktree_gc_kept",
                 f"Kept {len(kept)} idle PR worktree(s) that frshty could not remove",
                 meta={"kept": kept}, job=key, instance_key=key)
    return {"removed": removed, "kept": kept}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m features.pr_worktree_gc")
    parser.add_argument("config")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only")
    args = parser.parse_args()
    paths = None
    if args.only:
        paths = {ln.strip() for ln in Path(args.only).read_text().splitlines() if ln.strip()}
    config = core_config.load_config(args.config)
    state.init(config["job"]["key"])
    print(json.dumps(sweep(config, force=True, dry_run=args.dry_run, only=paths), indent=1))
