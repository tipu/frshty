"""Shared "keep a PR branch current with its base" routine.

Used by both the own-PR poll loop (features/own_prs.py) and the ticket
pipeline (features/tickets.py) so behavior is identical. The caller owns
worktree creation (lazily, via ensure_worktree) and all logging; this
function owns the git decision-making and the per-branch sync state it
reads/writes on the passed-in `st` dict.
"""
import subprocess
from pathlib import Path

import core.blocked as blocked_attempts
import core.git_util as git_util

MAX_BASE_SYNC_ATTEMPTS = 2
DIRTY_WORKTREE_ERROR = "worktree has uncommitted changes"


def ls_remote_sha(repo_path, base_branch: str) -> str:
    result = subprocess.run(["git", "ls-remote", "origin", base_branch],
                            cwd=str(repo_path), capture_output=True, text=True, timeout=30)
    if result.returncode != 0 or not result.stdout.strip():
        return ""
    return result.stdout.split()[0]


def blocked_by_dirty_worktree(st: dict) -> bool:
    return (st.get("last_base_error") or "").startswith(DIRTY_WORKTREE_ERROR)


def worktree_is_clean(worktree) -> bool:
    if not worktree or not Path(worktree).is_dir():
        return False
    try:
        return not git_util.run_git(worktree, ["status", "--porcelain"], timeout=30).stdout.strip()
    except git_util.GitCommandError:
        return False


def branch_head(repo_path, branch: str) -> str:
    return ls_remote_sha(repo_path, f"refs/heads/{branch}") if branch else ""


def blocker_cleared(st: dict, repo_path, branch: str, worktree_fn) -> bool:
    """Whether the blocker that capped this sync can have gone away.

    A dirty worktree clears when the worktree is clean. Every other blocker
    (a merge conflict, a refused push, a foreign author) clears only when
    the PR branch head moves. A new commit on the base does not clear a
    blocker, so it buys no new attempt."""
    if blocked_by_dirty_worktree(st):
        return worktree_is_clean(worktree_fn())
    head = branch_head(repo_path, branch)
    return bool(head) and head != st.get("base_sync_head")


def _block(st: dict, attempts: int, result: str, error: str, repo_path, branch: str,
           resource: str) -> None:
    """Record one blocked attempt on st and in the durable record."""
    head = branch_head(repo_path, branch)
    st["base_sync_attempts"] = attempts
    st["last_base_error"] = error
    st["base_sync_head"] = head
    blocked_attempts.record(resource, blocked_attempts.SYNC_BASE,
                            blocked_attempts.blocker_of(result, error), error, head)


def _unblock(st: dict, resource: str) -> None:
    st["base_synced"] = True
    st["base_sync_attempts"] = 0
    st.pop("last_base_error", None)
    st.pop("base_sync_head", None)
    blocked_attempts.clear(resource, blocked_attempts.SYNC_BASE)


def _count_failure(st: dict, attempts: int, result: str, error: str, repo_path, branch: str,
                   resource: str) -> dict:
    """Spend one attempt on a sync that could not start.

    Callers re-queue the sync while an attempt is left, so a failure that
    spends none re-queues it for ever and starves every later stage."""
    _block(st, attempts + 1, result, error, repo_path, branch, resource)
    return {"result": result, "error": error, "attempts": attempts + 1,
            "capped": attempts + 1 >= MAX_BASE_SYNC_ATTEMPTS}


def sync_branch_with_base(platform, repo_path, base_branch: str, branch: str,
                          st: dict, ensure_worktree, ticket_key: str = "",
                          resource: str = "") -> dict:
    """Merge base into branch when base has moved ahead, then push.

    st keys (read/written): base_sync_sha, base_synced, base_sync_attempts,
    last_base_error, base_sync_head. A move of base clears base_synced but
    keeps the attempts: only a success or a cleared blocker resets them.
    resource names the PR ("repo#id") in the durable blocked record; an
    empty resource writes no record. ensure_worktree is a zero-arg callable
    returning the worktree Path (or None) — only invoked once base is known to have moved,
    so the common no-op tick costs a single ls-remote.

    Returns {"result": one of skip|capped|no_base|ls_remote_failed|
    no_worktree|dirty_worktree|fetch_failed|uptodate|merge_failed|push_failed|
    synced, ...}.
    """
    if not base_branch or not repo_path:
        return {"result": "no_base"}
    base_sha = ls_remote_sha(repo_path, base_branch)
    if not base_sha:
        return {"result": "ls_remote_failed"}
    if st.get("base_sync_sha") != base_sha:
        st["base_sync_sha"] = base_sha
        st.pop("base_synced", None)
    if st.get("base_synced"):
        return {"result": "skip"}
    attempts = st.get("base_sync_attempts", 0)
    worktree = None
    if attempts >= MAX_BASE_SYNC_ATTEMPTS:
        if blocked_by_dirty_worktree(st):
            worktree = ensure_worktree()
        if not blocker_cleared(st, repo_path, branch, lambda: worktree):
            return {"result": "capped", "attempts": attempts}
        attempts = st["base_sync_attempts"] = 0
        st.pop("last_base_error", None)

    worktree = worktree or ensure_worktree()
    if not worktree:
        return _count_failure(st, attempts, "no_worktree", "no worktree for the PR branch",
                              repo_path, branch, resource)
    fetch = subprocess.run(["git", "fetch", "origin", base_branch, branch],
                           cwd=str(worktree), capture_output=True, text=True, timeout=60)
    if fetch.returncode != 0:
        return _count_failure(st, attempts, "fetch_failed", (fetch.stderr or "").strip()[:200],
                              repo_path, branch, resource)
    try:
        behind = git_util.run_git(
            worktree, ["rev-list", "--count", f"origin/{branch}..origin/{base_branch}"],
            timeout=10).stdout.strip()
    except git_util.GitCommandError as e:
        return _count_failure(st, attempts, "fetch_failed", str(e)[:200], repo_path, branch, resource)
    if behind == "0":
        _unblock(st, resource)
        return {"result": "uptodate"}

    # A failed `git status` returns empty stdout, which reads as a clean tree and
    # walks straight past the dirty_worktree guard below into a merge and a push.
    try:
        dirty = git_util.run_git(worktree, ["status", "--porcelain"], timeout=30).stdout.strip()
    except git_util.GitCommandError as e:
        return {"result": "dirty_worktree", "error": f"could not read worktree state: {e}"[:200],
                "attempts": st.get("base_sync_attempts", 0), "capped": False}
    if dirty:
        files = ", ".join(ln.strip().split(maxsplit=1)[-1] for ln in dirty.splitlines()[:5])
        error = f"{DIRTY_WORKTREE_ERROR} ({files}); merge would overwrite them"
        _block(st, MAX_BASE_SYNC_ATTEMPTS, "dirty_worktree", error, repo_path, branch, resource)
        return {"result": "dirty_worktree", "error": error,
                "attempts": MAX_BASE_SYNC_ATTEMPTS, "capped": True}

    merged = platform.merge_base(worktree, base_branch, prev_error=st.get("last_base_error"),
                                 ticket_key=ticket_key)
    if not merged.get("ok"):
        return _count_failure(st, attempts, "merge_failed", merged.get("error", ""),
                              repo_path, branch, resource)

    return _push(platform, worktree, branch, base_branch, st, attempts, repo_path, resource)


def _push(platform, worktree, branch: str, base_branch: str, st: dict, attempts: int,
          repo_path, resource: str) -> dict:
    pushed = platform.push_branch(worktree, branch)
    if not pushed.get("ok"):
        return _count_failure(st, attempts, "push_failed", pushed.get("error", ""),
                              repo_path, branch, resource)
    _unblock(st, resource)
    return {"result": "synced", "base": base_branch}
