"""Shared "keep a PR branch current with its base" routine.

Used by both the own-PR poll loop (features/own_prs.py) and the ticket
pipeline (features/tickets.py) so behavior is identical. The caller owns
worktree creation (lazily, via ensure_worktree) and all logging; this
function owns the git decision-making and the per-branch sync state it
reads/writes on the passed-in `st` dict.
"""
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import core.blocked as blocked_attempts
import core.git_util as git_util
from core.ticket_status import BASE_SYNC

MAX_BASE_SYNC_ATTEMPTS = BASE_SYNC.budget
DIRTY_WORKTREE_ERROR = "worktree has uncommitted changes"
TRANSIENT_RESULTS = ("no_worktree", "fetch_failed")
BLOCK_LAPSE_HOURS = 24


def ls_remote_sha(repo_path, base_branch: str) -> str:
    result = subprocess.run(["git", "ls-remote", "origin", base_branch],
                            cwd=str(repo_path), capture_output=True, text=True, timeout=30)
    if result.returncode != 0 or not result.stdout.strip():
        return ""
    return result.stdout.split()[0]


def remote_heads(repo_path) -> dict[str, str]:
    """Every branch head on origin, by branch name, from one ls-remote.

    The own-PR poll used to ask origin once per PR branch, and each call can
    wait 30 s on a slow remote. One listing per repository per poll answers
    every PR of that repository. An empty dict reads as "unknown" for every
    branch, the same answer a failed per-branch ls-remote gave."""
    try:
        result = git_util.run_git(repo_path, ["ls-remote", "--heads", "origin"], timeout=30)
    except git_util.GitCommandError:
        return {}
    heads = {}
    for line in result.stdout.splitlines():
        sha, _, ref = line.partition("\t")
        if ref.startswith("refs/heads/"):
            heads[ref.removeprefix("refs/heads/")] = sha
    return heads


def _remote_sha(repo_path, branch: str, heads: dict | None) -> str:
    if heads is None:
        return ls_remote_sha(repo_path, f"refs/heads/{branch}")
    return heads.get(branch, "")


def blocked_by_dirty_worktree(st: dict) -> bool:
    return (st.get("last_base_error") or "").startswith(DIRTY_WORKTREE_ERROR)


def worktree_is_clean(worktree) -> bool:
    if not worktree or not Path(worktree).is_dir():
        return False
    try:
        return not git_util.run_git(worktree, ["status", "--porcelain"], timeout=30).stdout.strip()
    except git_util.GitCommandError:
        return False


def parked(repo_path, branch: str, base_branch: str, st: dict, heads: dict | None = None) -> bool:
    """True while a push the identity guard refused would be refused again.

    The refusal depends on the commits of the PR branch and on its base, not
    on the base head, so a moved base must not wake the sync. A new head on
    the PR branch or a new base branch for the PR does."""
    park = st.get("base_sync_parked")
    if not park:
        return False
    if park.get("base") == base_branch and _remote_sha(repo_path, branch, heads) == park.get("head"):
        return True
    st.pop("base_sync_parked", None)
    return False


def branch_head(repo_path, branch: str) -> str:
    return ls_remote_sha(repo_path, f"refs/heads/{branch}") if branch else ""


def _lapsed(blocked_at) -> bool:
    try:
        return datetime.now(timezone.utc) - datetime.fromisoformat(blocked_at) >= timedelta(
            hours=BLOCK_LAPSE_HOURS)
    except (TypeError, ValueError):
        return True


def blocker_cleared(st: dict, repo_path, branch: str, base_sha: str, worktree_fn,
                    heads: dict | None = None) -> bool:
    """Whether the blocker that capped this sync can have gone away.

    A dirty worktree clears when the worktree is clean. A sync that could
    not start (no worktree, a failed fetch) is not about the branch, so a
    new base commit buys it one more try. Every other blocker (a merge
    conflict, a refused push, a foreign author) clears when the PR branch
    head moves, and a new base commit does not clear it. Any block lapses
    after BLOCK_LAPSE_HOURS, so a cause that cleared unseen costs one try a
    day, not a PR that never syncs again."""
    if _lapsed(st.get("base_sync_blocked_at")):
        return True
    if blocked_by_dirty_worktree(st):
        return worktree_is_clean(worktree_fn())
    if st.get("base_sync_result") in TRANSIENT_RESULTS and st.get("base_sync_blocked_base") != base_sha:
        return True
    head = _remote_sha(repo_path, branch, heads) if branch else ""
    return bool(head) and head != st.get("base_sync_head")


def _block(st: dict, attempts: int, result: str, error: str, repo_path, branch: str,
           resource: str, heads: dict | None = None) -> None:
    """Record one blocked attempt on st and in the durable record."""
    head = _remote_sha(repo_path, branch, heads) if branch else ""
    st["base_sync_attempts"] = attempts
    st["last_base_error"] = error
    st["base_sync_head"] = head
    st["base_sync_result"] = result
    st["base_sync_blocked_base"] = st.get("base_sync_sha", "")
    st["base_sync_blocked_at"] = datetime.now(timezone.utc).isoformat()
    blocked_attempts.record(resource, blocked_attempts.SYNC_BASE,
                            blocked_attempts.blocker_of(result, error), error, head)


def _unblock(st: dict, resource: str) -> None:
    st["base_synced"] = True
    st["base_sync_attempts"] = 0
    st.pop("last_base_error", None)
    for k in ("base_sync_head", "base_sync_result", "base_sync_blocked_base", "base_sync_blocked_at"):
        st.pop(k, None)
    blocked_attempts.clear(resource, blocked_attempts.SYNC_BASE)


def _count_failure(st: dict, attempts: int, result: str, error: str, repo_path, branch: str,
                   resource: str, heads: dict | None = None) -> dict:
    """Spend one attempt on a sync that could not start.

    Callers re-queue the sync while an attempt is left, so a failure that
    spends none re-queues it for ever and starves every later stage."""
    _block(st, attempts + 1, result, error, repo_path, branch, resource, heads)
    return {"result": result, "error": error, "attempts": attempts + 1,
            "capped": not BASE_SYNC.may_continue(attempts + 1)}


def sync_branch_with_base(platform, repo_path, base_branch: str, branch: str,
                          st: dict, ensure_worktree, ticket_key: str = "",
                          resource: str = "", heads: dict | None = None) -> dict:
    """Merge base into branch when base has moved ahead, then push.

    st keys (read/written): base_sync_sha, base_synced, base_sync_attempts,
    last_base_error, base_sync_head, base_sync_result, base_sync_blocked_base,
    base_sync_blocked_at. A move of base clears base_synced but
    keeps the attempts: only a success or a cleared blocker resets them.
    resource names the PR ("repo#id") in the durable blocked record; an
    empty resource writes no record. ensure_worktree is a zero-arg callable
    returning the worktree Path (or None) — only invoked once base is known to have moved,
    so the common no-op tick costs a single ls-remote. heads is a
    remote_heads listing that the caller shares across the PRs of one
    repository; with it the tick costs no remote call, and the fetch is
    skipped when the local origin refs already hold the listed base and
    branch heads.

    Returns {"result": one of parked|skip|capped|no_base|ls_remote_failed|
    no_worktree|dirty_worktree|fetch_failed|uptodate|merge_failed|push_failed|
    synced, ...}.
    """
    if not base_branch or not repo_path:
        return {"result": "no_base"}
    if parked(repo_path, branch, base_branch, st, heads):
        return {"result": "parked"}
    base_sha = ls_remote_sha(repo_path, base_branch) if heads is None else heads.get(base_branch, "")
    if not base_sha:
        return {"result": "ls_remote_failed"}
    if st.get("base_sync_sha") != base_sha:
        st["base_sync_sha"] = base_sha
        st.pop("base_synced", None)
    attempts = st.get("base_sync_attempts", 0)
    exit_reason = BASE_SYNC.exit_reason(attempts, converged=bool(st.get("base_synced")))
    if exit_reason == "converged":
        return {"result": "skip"}
    worktree = None
    if exit_reason == "budget_spent":
        if blocked_by_dirty_worktree(st):
            worktree = ensure_worktree()
        if not blocker_cleared(st, repo_path, branch, base_sha, lambda: worktree, heads):
            return {"result": "capped", "attempts": attempts}
        attempts = st["base_sync_attempts"] = 0
        st.pop("last_base_error", None)

    worktree = worktree or ensure_worktree()
    if not worktree:
        return _count_failure(st, attempts, "no_worktree", "no worktree for the PR branch",
                              repo_path, branch, resource, heads)
    if (heads is None or git_util.remote_ref_sha(worktree, base_branch) != base_sha
            or git_util.remote_ref_sha(worktree, branch) != heads.get(branch, "")):
        fetch = subprocess.run(["git", "fetch", "origin", base_branch, branch],
                               cwd=str(worktree), capture_output=True, text=True, timeout=60)
        if fetch.returncode != 0:
            return _count_failure(st, attempts, "fetch_failed", (fetch.stderr or "").strip()[:200],
                                  repo_path, branch, resource, heads)
    try:
        behind = git_util.run_git(
            worktree, ["rev-list", "--count", f"origin/{branch}..origin/{base_branch}"],
            timeout=10).stdout.strip()
    except git_util.GitCommandError as e:
        return _count_failure(st, attempts, "fetch_failed", str(e)[:200], repo_path, branch, resource, heads)
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
        _block(st, MAX_BASE_SYNC_ATTEMPTS, "dirty_worktree", error, repo_path, branch, resource, heads)
        return {"result": "dirty_worktree", "error": error,
                "attempts": MAX_BASE_SYNC_ATTEMPTS, "capped": True}

    merged = platform.merge_base(worktree, base_branch, prev_error=st.get("last_base_error"),
                                 ticket_key=ticket_key)
    if not merged.get("ok"):
        return _count_failure(st, attempts, "merge_failed", merged.get("error", ""),
                              repo_path, branch, resource, heads)

    return _push(platform, worktree, branch, base_branch, st, attempts, repo_path, resource,
                 heads)


def _push(platform, worktree, branch: str, base_branch: str, st: dict, attempts: int,
          repo_path, resource: str, heads: dict | None = None) -> dict:
    pushed = platform.push_branch(worktree, branch)
    if not pushed.get("ok"):
        if pushed.get("identity_blocked"):
            head = branch_head(repo_path, branch)
            if head:
                st["base_sync_parked"] = {"head": head, "base": base_branch}
        return _count_failure(st, attempts, "push_failed", pushed.get("error", ""),
                              repo_path, branch, resource, heads)
    _unblock(st, resource)
    return {"result": "synced", "base": base_branch}
