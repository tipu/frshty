"""Run each finished task's live check once, a day after its agent named it.

At least 25 frshty-board tasks reopened a fix that looked done when the agent
stopped and did not hold a day later: 12 on LSP, 7 on the deepseek voice, 5 on
client names. The agent prints a `LIVE_CHECK:` command when its change is
deployed (`work_store.record_live_check` stores it). This module runs that
command once, after LIVE_CHECK_DELAY_HOURS, and reopens the task when it
exits non-zero, so a fix that did not hold comes back to the board without
the operator finding it by hand.

The command runs without a shell. It comes from the agent of a task on the
operator's own board, and `shlex.split` keeps a stray `;` or `$(...)` in it
from becoming a second command. A check that needs a pipe says so with an
explicit `bash -c '...'`.
"""
import shlex
import subprocess
from datetime import datetime, timedelta, timezone

import core.config as core_config
import core.db as db
import core.log as log
from services import work_store

LIVE_CHECK_DELAY_HOURS = 24
LIVE_CHECK_TIMEOUT_SECONDS = 120
LIVE_CHECK_INTERVAL_MINUTES = 60
LIVE_CHECKS_PER_RUN = 5
OUTPUT_TAIL_LINES = 20
OUTPUT_MAX_CHARS = 2000
LIVE_CHECK_BOARD_KEY = "frshty"


def enabled(config: dict | None) -> bool:
    """Whether this instance runs live checks and asks its agents for one.

    `[live_check] enabled` decides. Without it only the frshty board runs
    them, because the reopened fixes were all there."""
    config = config or {}
    setting = (config.get("live_check") or {}).get("enabled")
    if setting is not None:
        return bool(setting)
    return (config.get("job") or {}).get("key") == LIVE_CHECK_BOARD_KEY


def _checkout(config: dict) -> str:
    """The shared checkout a check runs in: the instance's first repository
    that exists. The task worktree is usually gone by the time a day passed."""
    for repo in core_config.get_repos(config):
        path = repo["path"]
        if path.is_dir():
            return str(path)
    return ""


def _tail(text: str) -> str:
    lines = (text or "").strip().splitlines()[-OUTPUT_TAIL_LINES:]
    return "\n".join(lines)[-OUTPUT_MAX_CHARS:]


def _execute(command: str, cwd: str) -> tuple[int | None, str]:
    """Run one check. The exit code is None when the command never ran to an
    exit: bad quoting, a missing program, or the timeout."""
    try:
        argv = shlex.split(command)
    except ValueError as e:
        return None, f"the command does not parse: {e}"
    if not argv:
        return None, "the command is empty"
    try:
        proc = subprocess.run(argv, cwd=cwd, capture_output=True, text=True,
                              timeout=LIVE_CHECK_TIMEOUT_SECONDS)
    except subprocess.TimeoutExpired as e:
        out = (e.stdout or "") if isinstance(e.stdout, str) else ""
        return None, _tail(out + f"\ntimed out after {LIVE_CHECK_TIMEOUT_SECONDS} s")
    except OSError as e:
        return None, f"{type(e).__name__}: {e}"
    return proc.returncode, _tail((proc.stdout or "") + "\n" + (proc.stderr or ""))


def _due(now: datetime) -> list[dict]:
    cutoff = (now - timedelta(hours=LIVE_CHECK_DELAY_HOURS)).isoformat()
    return db.query_all(
        "SELECT id, live_check, contexts FROM work_items WHERE live_check != '' "
        "AND live_check_ran_at IS NULL AND live_check_set_at <= ? "
        f"AND state IN {work_store.FINISHED_STATES_SQL} "
        "ORDER BY live_check_set_at", (cutoff,))


def _other_project(contexts: str, board_key: str) -> str:
    """The project a task selected when it is not this board's own, "" when
    the check belongs here. A task that selects no project is the board's
    own work. The check runs in this board's checkout, so a check written
    for another project's repository would judge the wrong tree."""
    keys = [k.strip() for k in (contexts or "").split(",")
            if k.strip() and k.strip() not in work_store.NON_PROJECT_CONTEXTS]
    if not keys or board_key in keys:
        return ""
    return keys[0]


def _claim(item_id: int, command: str, now: str) -> bool:
    """Mark the check as run before it runs, so a second instance in the same
    process or an overlapping job never runs it again."""
    with db.tx() as c:
        return bool(c.execute(
            "UPDATE work_items SET live_check_ran_at = ? WHERE id = ? "
            "AND live_check = ? AND live_check_ran_at IS NULL",
            (now, item_id, command)).rowcount)


def _reopen_message(command: str, code: int | None, output: str) -> str:
    exit_text = "no exit code" if code is None else f"exit code {code}"
    return (f"The live check failed a day after deploy: `{command}` ended with "
            f"{exit_text}. Last output:\n{output or '(none)'}")


def run_due(config: dict, now: datetime | None = None) -> list[dict]:
    """Run every due live check once and reopen the tasks whose check fails."""
    now = now or datetime.now(timezone.utc)
    cwd = _checkout(config)
    board_key = (config.get("job") or {}).get("key") or ""
    results = []
    due = [r for r in _due(now) if not _other_project(r["contexts"], board_key)]
    for row in due[:LIVE_CHECKS_PER_RUN]:
        item_id, command = row["id"], row["live_check"]
        stamp = now.isoformat()
        if not _claim(item_id, command, stamp):
            continue
        if cwd:
            code, output = _execute(command, cwd)
        else:
            code, output = None, "the instance has no repository checkout to run the check in"
        passed = code == 0
        with db.tx() as c:
            c.execute("UPDATE work_items SET live_check_exit = ?, live_check_output = ? "
                      "WHERE id = ?", (code, output, item_id))
            c.execute(
                "INSERT INTO work_events(work_item_id, kind, payload, created_at) "
                "VALUES (?, 'live_check', ?, ?)",
                (item_id, db.dump_json({"command": command, "exit": code,
                                        "output": output, "passed": passed}), stamp))
        if passed:
            log.emit("work_live_check_passed",
                     f"work item {item_id}: the live check `{command}` passed")
        else:
            message = _reopen_message(command, code, output)
            reopened = work_store.apply_action(item_id, "reopen")
            if reopened.get("error"):
                log.emit("work_live_check_error",
                         f"work item {item_id}: the live check failed and the "
                         f"reopen was refused: {reopened['error']}")
            else:
                with db.tx() as c:
                    c.execute("UPDATE work_items SET stop_reason = ?, live_check = '' "
                              "WHERE id = ?", (message[:OUTPUT_MAX_CHARS + 500], item_id))
            log.emit("work_live_check_failed", f"work item {item_id}: {message}")
        results.append({"id": item_id, "command": command, "exit": code,
                        "passed": passed})
    return results
