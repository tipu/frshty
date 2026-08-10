"""Shared "keep a PR branch current with its base" routine.

Used by both the own-PR poll loop (features/own_prs.py) and the ticket
pipeline (features/tickets.py) so behavior is identical. The caller owns
worktree creation (lazily, via ensure_worktree) and all logging; this
function owns the git decision-making and the per-branch sync state it
reads/writes on the passed-in `st` dict.
"""
import subprocess

MAX_BASE_SYNC_ATTEMPTS = 2


def ls_remote_sha(repo_path, base_branch: str) -> str:
    result = subprocess.run(["git", "ls-remote", "origin", base_branch],
                            cwd=str(repo_path), capture_output=True, text=True, timeout=30)
    if result.returncode != 0 or not result.stdout.strip():
        return ""
    return result.stdout.split()[0]


def sync_branch_with_base(platform, repo_path, base_branch: str, branch: str,
                          st: dict, ensure_worktree) -> dict:
    """Merge base into branch when base has moved ahead, then push.

    st keys (read/written): base_sync_sha, base_synced, base_sync_attempts,
    last_base_error. ensure_worktree is a zero-arg callable returning the
    worktree Path (or None) — only invoked once base is known to have moved,
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
        st["base_sync_attempts"] = 0
        st.pop("base_synced", None)
        st.pop("last_base_error", None)
    if st.get("base_synced"):
        return {"result": "skip"}
    attempts = st.get("base_sync_attempts", 0)
    if attempts >= MAX_BASE_SYNC_ATTEMPTS:
        return {"result": "capped", "attempts": attempts}

    worktree = ensure_worktree()
    if not worktree:
        return {"result": "no_worktree"}
    fetch = subprocess.run(["git", "fetch", "origin", base_branch],
                           cwd=str(worktree), capture_output=True, text=True, timeout=60)
    if fetch.returncode != 0:
        return {"result": "fetch_failed"}
    behind = subprocess.run(["git", "rev-list", "--count", f"HEAD..origin/{base_branch}"],
                            cwd=str(worktree), capture_output=True, text=True, timeout=10).stdout.strip()
    if behind == "0":
        st["base_synced"] = True
        return {"result": "uptodate"}

    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=str(worktree),
                           capture_output=True, text=True, timeout=30).stdout.strip()
    if dirty:
        files = ", ".join(ln.strip().split(maxsplit=1)[-1] for ln in dirty.splitlines()[:5])
        error = f"worktree has uncommitted changes ({files}); merge would overwrite them"
        st["base_sync_attempts"] = MAX_BASE_SYNC_ATTEMPTS
        st["last_base_error"] = error
        return {"result": "dirty_worktree", "error": error,
                "attempts": MAX_BASE_SYNC_ATTEMPTS, "capped": True}

    merged = platform.merge_base(worktree, base_branch, prev_error=st.get("last_base_error"))
    if not merged.get("ok"):
        st["base_sync_attempts"] = attempts + 1
        st["last_base_error"] = merged.get("error", "")
        return {"result": "merge_failed", "error": merged.get("error", ""),
                "attempts": attempts + 1, "capped": attempts + 1 >= MAX_BASE_SYNC_ATTEMPTS}

    pushed = platform.push_branch(worktree, branch)
    if not pushed.get("ok"):
        st["base_sync_attempts"] = attempts + 1
        return {"result": "push_failed", "error": pushed.get("error", ""), "attempts": attempts + 1}

    st["base_synced"] = True
    return {"result": "synced", "base": base_branch}
