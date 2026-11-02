"""Remove the PR review worktrees whose pull request is merged or closed.

reviewer._ensure_review_worktree checks each reviewed PR branch out at
<state>/reviews/<repo>/<branch>/worktree, and nothing removed one when the PR
closed. One review worktree holds several GB.

`sweep` skips a worktree used in the last IDLE_S, because a review of a new
PR on the same branch resets the worktree before it writes the PR's id. It
asks the platform for the state of each PR that a review folder records in
review*.json or queued_comments*.json. When every one of them is merged or
closed, it removes the worktree through git_util.release_worktree, which runs
`git worktree remove` and keeps a worktree with a local change or a HEAD that
no remote branch holds. It removes only the worktree directory: review.json,
queued_comments*.json and diff.txt stay, because /reviews and
services.review_store read them.

The cron fan-out routes the sweep to every instance with review_prs. It runs
at most once per SWEEP_INTERVAL_S. Run it by hand for an instance that cannot
boot:

    FRSHTY_ROOT=<state dir> python -m features.review_worktree_gc <config.toml> [--dry-run] [--only FILE]

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
from features import pr_worktree_gc
from features.platforms import make_platform

IDLE_S = 86400
SWEEP_INTERVAL_S = 3600
CLOSED_STATES = ("MERGED", "CLOSED", "DECLINED", "SUPERSEDED")
STAMP = ".review-worktree-gc-stamp"


def _pr_ids(review_dir: Path) -> set[int] | None:
    """Every PR id that a review folder's review*.json and queued comments
    record. A branch name can return in a later PR, so one folder can hold the
    files of several PRs. None when a file cannot be read, because a review
    can be writing it."""
    ids = set()
    for f in review_dir.glob("review*.json"):
        try:
            data = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if isinstance(data, dict) and data.get("pr_id"):
            ids.add(data["pr_id"])
    for f in review_dir.glob("queued_comments*.json"):
        try:
            comments = json.loads(f.read_text())
        except (json.JSONDecodeError, OSError):
            return None
        if isinstance(comments, list):
            ids.update(c["pr_id"] for c in comments if isinstance(c, dict) and c.get("pr_id"))
    return ids


def sweep(config: dict, now: float | None = None, force: bool = False,
          dry_run: bool = False, only: set[str] | None = None) -> dict:
    """Remove every review worktree of this instance whose PR is merged or
    closed and that loses nothing.

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
    cutoff = now - IDLE_S
    platform = None
    removed: list[str] = []
    kept: dict[str, str] = {}
    for repo in core_config.get_repos(config):
        root = os.path.realpath(state_dir / "reviews" / repo["name"])
        blocks = git_util.worktree_entries(repo["path"])
        if blocks is None:
            kept[str(repo["path"])] = "git worktree list failed"
            continue
        for lines in blocks:
            if not lines[0].startswith("worktree "):
                continue
            path = lines[0][len("worktree "):].strip()
            real = Path(os.path.realpath(path))
            if real.name != "worktree" or os.path.dirname(real.parent) != root:
                continue
            if only is not None and path not in only:
                continue
            if f"locked {git_util.WORKTREE_LOCK_REASON}" not in lines or not os.path.isdir(path):
                continue
            if pr_worktree_gc._last_used(path) is None:
                kept[path] = "admin directory not found"
                continue
            if not pr_worktree_gc._idle(path, cutoff):
                continue
            pr_ids = _pr_ids(real.parent)
            if pr_ids is None:
                kept[path] = "a review file cannot be read"
                continue
            if not pr_ids:
                kept[path] = "no review file names the PR"
                continue
            if platform is None:
                platform = make_platform(config)
            try:
                pr_states = [platform.get_pr_state(repo["name"], i) for i in sorted(pr_ids)]
            except Exception as e:
                kept[path] = f"PR state lookup failed: {str(e)[:200]}"
                continue
            if not all(s in CLOSED_STATES for s in pr_states):
                continue
            if dry_run:
                reason = git_util.worktree_loss(path)
            else:
                reason = git_util.release_worktree(
                    repo["path"], path, still_idle=lambda p=path: pr_worktree_gc._idle(p, cutoff))
            if reason:
                kept[path] = reason
            else:
                removed.append(path)
    key = config["job"]["key"]
    if removed and not dry_run:
        log.emit("review_worktree_gc", f"Removed {len(removed)} review worktree(s) of closed PRs",
                 meta={"removed": removed}, job=key, instance_key=key)
    if kept and not dry_run:
        log.emit("review_worktree_gc_kept",
                 f"Kept {len(kept)} review worktree(s) that frshty could not remove",
                 meta={"kept": kept}, job=key, instance_key=key)
    return {"removed": removed, "kept": kept}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="python -m features.review_worktree_gc")
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
