"""Task worktrees for work-board runs.

A code task must not run in a shared checkout. Other agents hold uncommitted
work there, `git add -A` takes it, and no agent may clean the tree because
`git checkout`, `git stash` and `git reset` are forbidden in a checkout other
sessions are using.

`plan` decides where one launch runs. It writes nothing, so a launch that
cannot resolve is still refused before the work item is claimed. `ensure`
materializes the plan once the item id exists. `gc` reclaims a worktree only
when it can prove nothing is left in it.
"""
import os
import re
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path

import core.config as core_config
import core.db as db
import core.deps as deps
import core.git_util as git_util
import core.log as log
import core.runtime as runtime
import core.terminal as terminal
from services import work_store


GIT_TIMEOUT = 120
BRANCH_CANDIDATES = 9
KEEP_FINISHED_DAYS = 7


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _git(cwd, args: list[str], timeout: int = GIT_TIMEOUT):
    """git through the seam, for the calls whose exit status is the answer."""
    return git_util.run_git_status(cwd, args, timeout=timeout)


def instance_config(key: str) -> dict | None:
    """The loaded config of one project, None when that project has none."""
    instances = runtime.instances()
    entry = instances.get(key) if instances else None
    return entry.config if entry is not None else None


def branch_name(config: dict | None, item_id: int, objective: str) -> str:
    """The branch one task works on, built the way the ticket pipeline builds a
    ticket branch: the id, then the first seven kebab words of the objective."""
    slug = re.sub(r"[^a-z0-9]+", "-", (objective or "").lower()).strip("-")
    words = [w for w in slug.split("-") if w][:7]
    tail = "-".join(words)
    name = f"work-{item_id}" + (f"-{tail}" if tail else "")
    prefix = ((config or {}).get("workspace") or {}).get("branch_prefix", "")
    return f"{prefix}/{name}" if prefix else name


def base_branch_for(config: dict | None, repo_path: str, repo_name: str) -> str:
    """The branch a task worktree starts from.

    A project with a config states it. A project with none is asked for its
    origin/HEAD, and `main` is the last resort."""
    if config is not None:
        return core_config.base_branch_for(config, repo_name)
    got = _git(repo_path, ["symbolic-ref", "--short", "refs/remotes/origin/HEAD"],
               timeout=30)
    head = got.stdout.strip() if got.returncode == 0 else ""
    return head.split("/", 1)[1] if head.startswith("origin/") else "main"


def repo_common_dir(repo_path: str) -> str:
    _, common = git_util.git_dirs(repo_path)
    return common


def repo_root(directory: str) -> str:
    """The canonical checkout of the repository `directory` belongs to, "" when
    it is not in one.

    A file is answered for by the directory holding it, so a caller that knows
    only the path an agent tried to write does not have to work that out.

    Resolved from --git-common-dir rather than from --show-toplevel, because
    --show-toplevel inside a linked worktree names the worktree, not the
    repository it belongs to."""
    if directory and not os.path.isdir(directory):
        directory = os.path.dirname(directory)
    _, common = git_util.git_dirs(directory)
    if not common:
        return ""
    if os.path.basename(common) == ".git":
        return os.path.dirname(common)
    return common


# ---------------------------------------------------------------- table


def _row(row) -> dict | None:
    return dict(row) if row is not None else None


def for_item(item_id: int) -> dict | None:
    """The live worktree row of one task, None when it has none."""
    return _row(db.query_one(
        "SELECT * FROM work_worktrees WHERE work_item_id = ? AND removed_at IS NULL "
        "ORDER BY id DESC LIMIT 1", (item_id,)))


def for_item_repo(item_id: int, common_dir: str) -> dict | None:
    return _row(db.query_one(
        "SELECT * FROM work_worktrees WHERE work_item_id = ? AND repo_common_dir = ? "
        "AND removed_at IS NULL", (item_id, common_dir)))


def _record(item_id: int, spec: dict, path: str, branch: str, origin: str) -> dict:
    now = _now()
    with db.tx() as c:
        c.execute(
            "INSERT OR REPLACE INTO work_worktrees(work_item_id, project_key, "
            "repo_name, repo_path, repo_common_dir, path, branch, base_branch, "
            "origin, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (item_id, spec.get("project_key", ""), spec["repo_name"],
             spec["repo_path"], spec["repo_common_dir"], path, branch,
             spec["base_branch"], origin, now))
    return for_item_repo(item_id, spec["repo_common_dir"]) or {}


# ---------------------------------------------------------------- plan


def _tickets_root(config: dict) -> Path | None:
    ws = config.get("workspace") or {}
    root = ws.get("root")
    return Path(root) / str(ws.get("tickets_dir") or "tickets") if root else None


def _repos_of(config: dict) -> list[dict]:
    """The repositories one project holds, [] when its tree cannot be read.

    core.config.get_repos scans the projects directory, and that directory can
    be missing: an unmounted disk or a renamed folder would otherwise raise
    out of every launch."""
    try:
        return core_config.get_repos(config)
    except (OSError, KeyError):
        return []


def _ticket_dir_is_materialized(config: dict, slug: str) -> bool:
    """Whether at least one repository of a ticket directory holds a worktree."""
    for repo in _repos_of(config):
        path = core_config.ticket_worktree_path(config, slug, repo["name"])
        if path.is_dir() and git_util.is_worktree(path):
            return True
    return False


def _ticket_dir_for_cwd(cwd: str, contexts: list[str]) -> bool:
    """Whether `cwd` is a ticket directory of a selected project that holds at
    least one worktree."""
    resolved = Path(os.path.abspath(cwd))
    for key in contexts:
        config = instance_config(key)
        if config is None:
            continue
        root = _tickets_root(config)
        if root is None or resolved.parent != Path(os.path.abspath(root)):
            continue
        if _ticket_dir_is_materialized(config, resolved.name):
            return True
    return False


def _ticket_dir_from_objective(objective: str, contexts: list[str]) -> str:
    """The ticket directory a selected project's ticket key names in the
    objective, "" when the objective names none or the ticket has no worktree.

    The ticket keys come from the tickets table, so no pattern has to guess
    what a key looks like, and a key that belongs to another project is never
    matched."""
    words = set(re.findall(r"[A-Za-z][A-Za-z0-9]*-\d+", objective or ""))
    if not words:
        return ""
    for key in contexts:
        config = instance_config(key)
        if config is None:
            continue
        root = _tickets_root(config)
        if root is None:
            continue
        rows = db.query_all(
            "SELECT ticket_key, slug FROM tickets WHERE instance_key = ? "
            "AND slug IS NOT NULL AND slug != ''",
            (key,))
        for row in rows:
            if row["ticket_key"] not in words:
                continue
            if _ticket_dir_is_materialized(config, row["slug"]):
                return str(Path(root) / row["slug"])
    return ""


def _repo_entry(config: dict | None, repo_path: str, repo_name: str,
                project_key: str) -> dict:
    return {"repo_path": os.path.abspath(repo_path), "repo_name": repo_name,
            "project_key": project_key,
            "repo_common_dir": repo_common_dir(repo_path),
            "base_branch": base_branch_for(config, repo_path, repo_name)}


def _project_repos(key: str, entries: list[dict]) -> list[tuple[dict | None, str, str]]:
    """(config, path, name) of every repository one selected project holds.

    A project with a loaded config lists its repositories there. A project
    that has none — frshty and algotrader2 are configured on the board only —
    is one repository when its root is itself a checkout."""
    config = instance_config(key)
    if config is not None:
        return [(config, str(r["path"]), r["name"]) for r in _repos_of(config)]
    entry = next((e for e in entries if e["key"] == key), None)
    root = entry["root"] if entry else ""
    if root and repo_root(root) == os.path.abspath(root):
        return [(None, os.path.abspath(root), os.path.basename(str(root).rstrip("/")))]
    return []


def project_for_repo(root: str, contexts: list[str],
                     entries: list[dict]) -> tuple[dict | None, str]:
    """The (config, project key) of the selected project that owns `root`.

    A task can select several projects, so the repository being written to is
    what says which project's base branch and dependency commands apply."""
    want = repo_common_dir(root)
    for key in contexts:
        for config, path, _ in _project_repos(key, entries):
            if repo_common_dir(path) == want:
                return config, key
    return None, ""


def _resolve_repo(caller_cwd: str, contexts: list[str], entries: list[dict],
                  repo_pick: str) -> dict | None:
    """The single repository a task works in, None when none resolves.

    A project can hold many repositories, and one worktree per repository is
    far too much disk for a board that holds more tasks than the ticket
    pipeline holds tickets, so a task that resolves no single repository runs
    where it runs today."""
    if caller_cwd:
        root = repo_root(caller_cwd)
        if root:
            config, key = project_for_repo(root, contexts, entries)
            return _repo_entry(config, root, os.path.basename(root), key)
    candidates: list[tuple[dict | None, str, str, str]] = []
    for key in contexts:
        for config, path, name in _project_repos(key, entries):
            if repo_pick and name != repo_pick:
                continue
            candidates.append((config, path, name, key))
    if len(candidates) != 1:
        return None
    config, path, name, key = candidates[0]
    return _repo_entry(config, path, name, key)


def plan(objective: str, caller_cwd: str, cwd: str, contexts: list[str],
         entries: list[dict], repo_pick: str = "", no_worktree: bool = False,
         item_id: int | None = None) -> dict:
    """Where one launch runs. Writes nothing.

    The first rule that matches wins:

    R1 the task already has a worktree that still exists on disk
    R2 the resolved directory is already a worktree
    R3 the resolved directory is a ticket directory holding a worktree
    R4 the caller named no directory and the objective names a ticket key of a
       selected project whose ticket worktree exists
    R5 a single repository resolves, so a task worktree is created
    R6 nothing resolves, so the launch runs where it runs today
    """
    if item_id is not None:
        row = for_item(item_id)
        if row and os.path.isdir(row["path"]):
            return {"rule": "R1", "cwd": row["path"], "create": False, "row": row}
    if cwd and git_util.is_worktree(cwd):
        return {"rule": "R2", "cwd": cwd, "create": False}
    if cwd and _ticket_dir_for_cwd(cwd, contexts):
        return {"rule": "R3", "cwd": cwd, "create": False}
    if not caller_cwd:
        ticket_dir = _ticket_dir_from_objective(objective, contexts)
        if ticket_dir:
            return {"rule": "R4", "cwd": ticket_dir, "create": False}
    if no_worktree:
        return {"rule": "R6", "cwd": cwd, "create": False}
    repo = _resolve_repo(caller_cwd, contexts, entries, repo_pick)
    if repo and repo["repo_common_dir"]:
        return {"rule": "R5", "cwd": cwd, "create": True, **repo}
    return {"rule": "R6", "cwd": cwd, "create": False}


# ---------------------------------------------------------------- ensure


_ensure_lock = threading.RLock()


def _classify_holder(repo_path: str, branch: str) -> tuple[str, Path | None]:
    """What holds `branch`: free, worktree, canonical or prunable."""
    holder = git_util.worktree_holding_branch(Path(repo_path), branch)
    if holder is None:
        return "free", None
    if os.path.realpath(holder) == os.path.realpath(repo_path):
        return "canonical", holder
    if holder.is_dir() and git_util.is_worktree(holder):
        return "worktree", holder
    return "prunable", holder


def _pick_branch(repo_path: str, base: str) -> tuple[str, str, Path | None]:
    """A branch name this task can use, and what already holds it.

    Returns (branch, state, holder). A holder that is a live worktree is this
    task's own worktree, because the branch name carries the item id, so it is
    reused. A branch the canonical checkout holds cannot be freed, because
    freeing it means running `git checkout` in a shared checkout, so the next
    candidate is tried instead. A fixed single fallback is not enough: a
    create interrupted before the row was written would pick the same name
    again, fail the add, and drop the task back to the shared checkout."""
    for n in range(1, BRANCH_CANDIDATES + 1):
        branch = base if n == 1 else f"{base}-{n}"
        state, holder = _classify_holder(repo_path, branch)
        if state == "prunable":
            _git(repo_path, ["worktree", "prune"])
            state, holder = _classify_holder(repo_path, branch)
        if state in ("free", "worktree"):
            return branch, state, holder
    return "", "exhausted", None


def _install_deps(config: dict | None, repo_name: str, path: Path) -> None:
    if config is None:
        return
    try:
        deps.relink_shared_venv(config, repo_name, path)
        for dep in (config.get("workspace") or {}).get("dep_commands", []) or []:
            if dep.get("match") == repo_name:
                deps.run_dep_command(config, repo_name, path, dep["cmd"])
    except Exception as e:
        log.emit("work_worktree_deps_failed",
                 f"{repo_name}: {type(e).__name__}: {e}",
                 meta={"repo": repo_name, "path": str(path)})


def ensure(item_id: int, spec: dict, objective: str = "") -> dict:
    """Materialize the worktree `plan` chose. Returns the row, or {} when the
    worktree could not be made.

    Idempotent on (work item, repository). A launch never fails because a
    worktree could not be created: the caller falls back to the directory it
    already had. Callers hold work_store.launch_lock, so gc cannot remove the
    directory between this returning it and tmux opening it."""
    common = spec.get("repo_common_dir") or ""
    repo_path = spec.get("repo_path") or ""
    if not common or not repo_path:
        return {}
    with _ensure_lock:
        row = for_item_repo(item_id, common)
        if row and os.path.isdir(row["path"]):
            return row
        config = instance_config(spec.get("project_key") or "")
        path = core_config.task_worktree_path(
            config, spec.get("project_key") or "", item_id, spec["repo_name"])
        base = branch_name(config, item_id, objective)
        branch, state, holder = _pick_branch(repo_path, base)
        if state == "exhausted":
            log.emit("work_worktree_failed",
                     f"work item {item_id}: no free branch name for "
                     f"{spec['repo_name']}; the task stays in {repo_path}")
            return {}
        if state == "worktree" and holder is not None:
            _install_deps(config, spec["repo_name"], holder)
            return _record(item_id, spec, str(holder), branch, "reused_holder")
        base_branch = spec["base_branch"]
        _git(repo_path, ["fetch", "origin", base_branch])
        _git(repo_path, ["worktree", "prune"])
        for start in (f"origin/{base_branch}", base_branch, "HEAD"):
            made = _git(repo_path, ["branch", branch, start])
            if made.returncode == 0:
                break
        path.parent.mkdir(parents=True, exist_ok=True)
        added = _git(repo_path, ["worktree", "add", str(path), branch])
        if added.returncode != 0:
            log.emit("work_worktree_failed",
                     f"work item {item_id}: worktree add failed for "
                     f"{spec['repo_name']}: {(added.stderr or '').strip()[:300]}")
            return {}
        _install_deps(config, spec["repo_name"], path)
        return _record(item_id, spec, str(path), branch, "created")


def ensure_for_repo(item_id: int, repo_dir: str, objective: str = "",
                    entries: list[dict] | None = None) -> dict:
    """Materialize a worktree for the repository `repo_dir` belongs to.

    The write and commit gates know the file or directory a session tried to
    change, and that names the repository even when the task selected a
    project holding several. Returns the row, or {} when none could be made."""
    root = repo_root(repo_dir)
    if not root:
        return {}
    item = db.query_one("SELECT contexts, objective FROM work_items WHERE id = ?",
                        (item_id,))
    contexts = [c for c in ((item["contexts"] if item else "") or "").split(",") if c]
    config, project_key = project_for_repo(root, contexts, entries or [])
    spec = _repo_entry(config, root, os.path.basename(root), project_key)
    return ensure(item_id, spec, objective or (item["objective"] if item else ""))


def shared_checkout(directory: str) -> str:
    """The canonical checkout a write into `directory` lands in, "" when the
    write is not into one.

    The test is the worktree path, never the repository id. A canonical
    checkout and every worktree of it share one --git-common-dir, so a rule
    keyed on the repository id would let a task edit the shared checkout the
    moment it owned any worktree of that repository."""
    git_dir, common = git_util.git_dirs(directory)
    if not git_dir or git_dir != common:
        return ""
    return repo_root(directory)


def session_item(session_id: str) -> dict | None:
    """The work item behind one agent session, None when there is none."""
    return _row(db.query_one(
        "SELECT i.id, i.objective, i.contexts, i.worktree_opt_out "
        "FROM work_runs r JOIN work_items i ON i.id = r.work_item_id "
        "WHERE r.session_id = ?", (session_id,)))


def last_row(item_id: int) -> dict | None:
    """The newest worktree row of one task, removed rows included."""
    return _row(db.query_one(
        "SELECT * FROM work_worktrees WHERE work_item_id = ? ORDER BY id DESC LIMIT 1",
        (item_id,)))


def rebuild(item_id: int, objective: str = "", path: str = "") -> dict:
    """Make the worktree a launch resolved exist again.

    The sweep can remove a directory between a launch resolving it and the
    launch opening it, and the launch must not answer that by dropping the
    agent into the shared checkout. The row still names the repository, so the
    tree is rebuilt on a branch of that repository instead. The row at `path`
    is read first, because a task can hold a worktree of more than one
    repository and the path names the one that went missing; reading the
    task's newest row instead would rebuild the wrong repository and hand the
    launch the wrong directory. `path` also covers the task that reuses
    another task's worktree and lost the race before it had a row of its own.

    Returns the row, or {} when nothing records what the directory was."""
    row = None
    if path:
        row = _row(db.query_one(
            "SELECT * FROM work_worktrees WHERE path = ? ORDER BY id DESC LIMIT 1",
            (path,)))
    if row is None:
        row = last_row(item_id)
    if row is None:
        return {}
    spec = {"project_key": row["project_key"], "repo_name": row["repo_name"],
            "repo_path": row["repo_path"],
            "repo_common_dir": row["repo_common_dir"],
            "base_branch": row["base_branch"]}
    return ensure(item_id, spec, objective)


def adopt_path(item_id: int, path: str) -> dict:
    """Record that one task works in a worktree another task made.

    A follow-up inherits its source's directory and matches R2, so it creates
    nothing and would own nothing. Recording it makes the follow-up a holder
    of the directory: `gc` then holds the directory to the follow-up's own
    state, and the follow-up's own follow-up finds it. `origin` is not
    `created`, so `gc` never removes this row's worktree on its account."""
    if not path:
        return {}
    source = db.query_one(
        "SELECT * FROM work_worktrees WHERE path = ? AND removed_at IS NULL "
        "ORDER BY id DESC LIMIT 1", (path,))
    if source is None:
        return {}
    existing = for_item_repo(item_id, source["repo_common_dir"])
    if existing is not None:
        return existing
    spec = {"project_key": source["project_key"], "repo_name": source["repo_name"],
            "repo_path": source["repo_path"],
            "repo_common_dir": source["repo_common_dir"],
            "base_branch": source["base_branch"]}
    return _record(item_id, spec, path, source["branch"], "reused_worktree")


def adopt_run(item_id: int, path: str) -> None:
    """Move the task's live run into the directory it now works in."""
    run = db.query_one(
        "SELECT session_id FROM work_runs WHERE work_item_id = ? ORDER BY id DESC LIMIT 1",
        (item_id,))
    if run:
        work_store.record_run_cwd(run["session_id"], path)


# ---------------------------------------------------------------- context


def context_block(row: dict) -> str:
    """What the agent is told about the worktree it starts in."""
    return (
        "\n\n## Working tree\n\n"
        f"You are in a git worktree for this task: {row['path']}\n"
        f"The branch is {row['branch']}. It starts from {row['base_branch']}.\n"
        "Commit and push on this branch. Open the pull request from it.\n"
        "Do not detach HEAD. Work on this branch, or the cleanup sweep cannot "
        "see your commits.\n\n"
        f"The shared checkout of this repository is {row['repo_path']}. Other "
        "agents hold uncommitted work there. Do not edit it. Do not run git "
        "checkout, git stash, git reset or git clean in it. Read it if you "
        "need to.\n"
    )


# ---------------------------------------------------------------- cleanup


def _finished_long_enough(item: dict, now: datetime) -> bool:
    if item["archived_at"]:
        return True
    if item["state"] not in work_store.FINISHED_STATES:
        return False
    updated = (item["updated_at"] or "").strip()
    try:
        when = datetime.fromisoformat(updated)
    except ValueError:
        return False
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    return now - when >= timedelta(days=KEEP_FINISHED_DAYS)


def _items_using(row: dict) -> list[int]:
    """Every work item that still needs this directory, the row's own included.

    A follow-up inherits its parent's directory and matches R2, so it runs in
    the parent's worktree and records no row of its own. Reading only the row
    would let the sweep delete a tree a follow-up is still working in, and
    reading only unfinished follow-ups would delete one that finished a minute
    ago and is waiting to be acknowledged. Every item that ever ran there is
    held to the same conditions as the owner."""
    ids = {row["work_item_id"]}
    ids |= {r["work_item_id"] for r in db.query_all(
        "SELECT work_item_id FROM work_worktrees WHERE path = ? AND id != ? "
        "AND removed_at IS NULL", (row["path"], row["id"]))}
    inside = row["path"].rstrip(os.sep) + os.sep
    # The LIKE narrows the scan; the comparison decides, because a path may
    # hold a LIKE wildcard and a run may sit in a subdirectory of the
    # worktree rather than at its root.
    ids |= {r["work_item_id"] for r in db.query_all(
        "SELECT DISTINCT work_item_id, cwd FROM work_runs WHERE cwd = ? OR cwd LIKE ?",
        (row["path"], row["path"] + "%"))
        if r["cwd"] == row["path"] or (r["cwd"] or "").startswith(inside)}
    return sorted(ids)


def _ahead_of_base(row: dict) -> int | None:
    """Commits the worktree's HEAD carries that its base does not, None when no
    base branch resolves."""
    for base in (f"origin/{row['base_branch']}", row["base_branch"]):
        ahead = _git(row["path"], ["rev-list", "--count", f"{base}..HEAD"], timeout=60)
        counted = ahead.stdout.strip()
        if ahead.returncode == 0 and counted.isdigit():
            return int(counted)
    return None


def _keep_reason(row: dict, now: datetime) -> str:
    """Why a worktree must not be removed, "" when it can be.

    Every test here reads the worktree itself. A plain `status --porcelain`
    hides gitignored files, and `worktree remove` deletes them, so the status
    call asks for ignored and untracked files and overrides a repository that
    configures status.showUntrackedFiles=no. A detached HEAD leaves the
    recorded branch clean, so a commit made while detached would be
    unreachable after removal; the HEAD is compared to the recorded branch
    instead. `gc` fetches the base before this runs, because a branch whose
    pull request was merged still looks ahead of a base ref that was last
    updated when the worktree was made."""
    for item_id in _items_using(row):
        item = db.query_one(
            "SELECT id, state, archived_at, updated_at FROM work_items WHERE id = ?",
            (item_id,))
        if item is None:
            return f"work item {item_id} is gone"
        if not _finished_long_enough(dict(item), now):
            return f"work item {item_id} is not finished long enough"
        if terminal.session_healthy(f"work-{item_id}").get("alive"):
            return f"work item {item_id} still has a live session"
    status = _git(row["path"], ["-c", "status.showUntrackedFiles=all", "status",
                                "--porcelain", "--ignored"], timeout=60)
    if status.returncode != 0:
        return f"status failed: {(status.stderr or '').strip()[:200]}"
    if status.stdout.strip():
        return "the worktree holds uncommitted or ignored files"
    head = _git(row["path"], ["rev-parse", "--abbrev-ref", "HEAD"], timeout=30)
    if head.returncode != 0 or head.stdout.strip() != row["branch"]:
        return f"HEAD is {(head.stdout or '').strip() or 'unreadable'}, not {row['branch']}"
    ahead = _ahead_of_base(row)
    if ahead is None:
        return "no base branch to compare against"
    if ahead:
        return f"the branch is {ahead} commit(s) ahead of {row['base_branch']}"
    return ""


def _mark_removed(row: dict) -> None:
    with db.tx() as c:
        c.execute("UPDATE work_worktrees SET removed_at = ? WHERE id = ?",
                  (_now(), row["id"]))


def _remove(row: dict) -> bool:
    removed = _git(row["repo_path"], ["worktree", "remove", row["path"]])
    if removed.returncode != 0:
        log.emit("work_worktree_remove_failed",
                 f"work item {row['work_item_id']}: {(removed.stderr or '').strip()[:200]}")
        return False
    _git(row["repo_path"], ["worktree", "prune"])
    _git(row["repo_path"], ["branch", "-d", row["branch"]])
    _mark_removed(row)
    return True


def gc(now: datetime | None = None) -> list[dict]:
    """Reclaim the task worktrees nothing needs any more.

    Only worktrees frshty created are ever removed: a ticket worktree or a
    worktree that arrived from somewhere else is left alone. A worktree that
    fails any one condition is kept and left for the operator, and the board
    shows it on the task it belongs to. Returns the worktrees removed."""
    now = now or datetime.now(timezone.utc)
    removed: list[dict] = []
    rows = db.query_all(
        "SELECT * FROM work_worktrees WHERE removed_at IS NULL AND origin = 'created' "
        "ORDER BY id")
    for raw in rows:
        row = dict(raw)
        if not os.path.isdir(row["path"]):
            with work_store.launch_lock:
                _git(row["repo_path"], ["worktree", "prune"])
                _mark_removed(row)
            removed.append({"id": row["id"], "path": row["path"],
                            "result": "already gone"})
            continue
        item = db.query_one(
            "SELECT id, state, archived_at, updated_at FROM work_items WHERE id = ?",
            (row["work_item_id"],))
        if item is None or not _finished_long_enough(dict(item), now):
            continue
        # The base moves while a task waits: a branch whose pull request was
        # merged still looks ahead of the ref this repository last fetched.
        # Outside the lock, because a fetch talks to the network and every
        # launch would wait behind it.
        _git(row["repo_path"], ["fetch", "origin", row["base_branch"]], timeout=60)
        with work_store.launch_lock:
            if not os.path.isdir(row["path"]):
                continue
            if _keep_reason(row, now):
                continue
            if _remove(row):
                removed.append({"id": row["id"], "path": row["path"],
                                "result": "removed"})
    return removed
