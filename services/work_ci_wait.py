"""Wait on the checks of a pull request for an agent, and resume it with the result.

The usage review of work 303 found frshty-board agents spending 28 of 65
tool-hours on CI watches, gh polls and sleep. core/sleep_gate now refuses
those waits, and the agent ends its turn with a line
`waiting on PR #N checks` instead. maybe_autocontinue parks such a turn in
waiting_external and writes a ci_wait event. This sweep, run on every scan of
the work debrief loop, reads the checks of that pull request and, once they
pass or fail, sends the result into the agent's pane, so the session goes on
without spending a turn per poll.

The checks are read with GitHubPlatform.get_pr_checks, under the gh account
of the project the run was launched for, in the repository the run's working
directory pushes to. A wait that sees no checks for CI_WAIT_NO_CHECKS_MINUTES,
or no result for CI_WAIT_MAX_MINUTES, resumes the agent with that fact rather
than holding it forever. A ci_wait_done event closes the wait; it is written
only after the message reached the pane, so a failed send is tried again on
the next scan.
"""
import re
import subprocess
from datetime import datetime, timezone

import core.db as db
import core.git_util as git_util
import core.log as log
from features.platforms import GitHubPlatform
from features.pr_ci import ci_summary, failing_check_names
from services import work_launch, work_store

CI_WAIT_NO_CHECKS_MINUTES = 10
CI_WAIT_MAX_MINUTES = 120
_REMOTE_SLUG = re.compile(r"[:/]([^/:]+)/([^/]+?)(?:\.git)?/?$")

RESULT_PROMPT = (
    work_store.BOARD_PROMPT_MARKER + " The board watched the checks of PR #{pr}. Result: {result}. "
    "Continue the task from here. To wait on checks again, end your turn "
    "with the line `waiting on PR #{pr} checks`."
)


def _repo_slug(cwd: str) -> str:
    """owner/name of the repository `cwd` pushes to, "" when it has none."""
    if not cwd:
        return ""
    try:
        out = git_util.run_git(cwd, ["remote", "get-url", "origin"], timeout=15)
    except (git_util.GitCommandError, OSError, subprocess.SubprocessError):
        return ""
    match = _REMOTE_SLUG.search(out.stdout.strip())
    return f"{match.group(1)}/{match.group(2)}" if match else ""


def _platform(env_key: str, slug: str) -> GitHubPlatform:
    """A GitHub reader for `slug` under the gh account of the run's project."""
    config = (work_launch._instance_config(env_key) if env_key
              else work_launch.personal_config()) or {}
    account = (config.get("github") or {}).get("account") or ""
    return GitHubPlatform({"github": {"repo": slug, "account": account}, "workspace": {}})


def _minutes_since(stamp: str, now: datetime) -> float:
    return (now - datetime.fromisoformat(stamp)).total_seconds() / 60


def _result(row: dict, now: datetime) -> str:
    """The result to give the agent, "" while the wait goes on."""
    slug = _repo_slug(row["cwd"])
    age = _minutes_since(row["waited_at"], now)
    if not slug:
        return ("the board could not read the GitHub repository of "
                f"{row['cwd'] or 'this run'}, so it did not watch the checks")
    checks = _platform(row["env_key"] or "", slug).get_pr_checks(slug, row["pr"])
    summary = ci_summary(checks)
    if summary == "passing":
        return "every check passed"
    if summary == "failing":
        return "these checks failed: " + ", ".join(failing_check_names(checks))
    if summary == "none":
        if age >= CI_WAIT_NO_CHECKS_MINUTES:
            return f"no checks were reported in {CI_WAIT_NO_CHECKS_MINUTES} minutes"
        return ""
    if checks is None:
        log.emit("work_ci_wait_read_failed",
                 f"work item {row['item_id']}: the checks of {slug}#{row['pr']} "
                 "could not be read; the next scan tries again")
    if age >= CI_WAIT_MAX_MINUTES:
        return f"the checks were still {summary} after {CI_WAIT_MAX_MINUTES} minutes"
    return ""


def sweep_ci_waits(now: datetime | None = None) -> list[dict]:
    """Resume every agent whose pull request checks finished."""
    now_dt = now or datetime.now(timezone.utc)
    rows = db.query_all(
        "SELECT i.id AS item_id, r.id AS run_id, r.tmux_key, r.cwd, r.env_key, "
        "e.id AS event_id, e.payload, e.created_at AS waited_at "
        "FROM work_items i JOIN work_runs r ON r.id = "
        "(SELECT r2.id FROM work_runs r2 WHERE r2.work_item_id = i.id ORDER BY r2.id DESC LIMIT 1) "
        "JOIN work_events e ON e.id = (SELECT MAX(e2.id) FROM work_events e2 "
        "WHERE e2.work_run_id = r.id AND e2.kind = 'ci_wait') "
        "WHERE i.state = 'waiting_external' AND NOT EXISTS (SELECT 1 FROM work_events d "
        "WHERE d.work_run_id = r.id AND d.kind = 'ci_wait_done' AND d.id > e.id)")
    actions: list[dict] = []
    for row in rows:
        row["pr"] = int(db.load_json(row, "payload").get("pr") or 0)
        result = _result(row, now_dt)
        if not result:
            continue
        if not work_store.tmux_send(row["tmux_key"],
                                    RESULT_PROMPT.format(pr=row["pr"], result=result)):
            log.emit("work_ci_wait_send_failed",
                     f"work item {row['item_id']}: the result of PR #{row['pr']} "
                     "did not reach the agent pane; the next scan tries again")
            continue
        stamp = work_store._now()
        with db.tx() as c:
            resumed = c.execute(
                "UPDATE work_items SET state = 'agent_working', snoozed_until = NULL, "
                "stop_reason = '', updated_at = ? WHERE id = ? AND state = 'waiting_external'",
                (stamp, row["item_id"])).rowcount
            if resumed:
                c.execute("UPDATE work_runs SET status = 'running' WHERE id = ?",
                          (row["run_id"],))
            c.execute(
                "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at) "
                "VALUES (?, ?, 'ci_wait_done', ?, ?)",
                (row["item_id"], row["run_id"],
                 db.dump_json({"pr": row["pr"], "result": result}), stamp))
        actions.append({"id": row["item_id"], "pr": row["pr"], "result": result})
    return actions
