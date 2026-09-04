import os
import re
import shlex
import subprocess
import threading
import time
import uuid
from pathlib import Path

import core.config as core_config
import core.db as db
import core.git_util as git_util
import core.log as log
import core.runtime as runtime
import core.terminal as terminal
from core.tasks.tickets import (
    TEST_RUN_TIMEOUT, _NO_LOCAL_PY_VENV_SENTINEL, _detect_runner, _run_repo_tests,
)
from services import work_artifacts, work_store, work_tags, work_worktree


def personal_config() -> dict | None:
    instances = runtime.instances()
    if not instances:
        return None
    entry = instances.get("personal")
    if not entry:
        return None
    return entry.config


SLACK_INT_DIR = os.path.expanduser("~/Documents/dev/slack_int")


def _instance_config(key: str) -> dict | None:
    """The config of one project, None when that project is not loaded."""
    instances = runtime.instances()
    entry = instances.get(key) if instances else None
    return entry.config if entry is not None else None


def _instance_env_config(key: str, agent: str) -> dict | None:
    """The config of one project when that project pins its own environment
    for `agent`, None when it does not.

    A project pins the environment of an agent when its llm block names a
    binary, a configuration directory or environment overrides for that agent.
    Every other project runs the operator's default account. The test is per
    agent, because a project that pins codex says nothing about the claude
    account a claude task must use."""
    config = _instance_config(key)
    if config is None:
        return None
    agent_cfg = ((config.get("llm") or {}).get(agent)) or {}
    if agent_cfg.get("bin") or agent_cfg.get("config_dir") or agent_cfg.get("env"):
        return config
    return None


def agent_config(contexts: list[str], default_config: dict,
                 agent: str = "claude") -> dict:
    """The project whose environment the launched agent runs with.

    A task that selects one project with its own environment for this agent
    runs as the account that project is configured with, and the returned key
    names that project. A task that selects no such project runs the
    operator's default account under the empty key. Two selected projects with
    two environments have no single right answer, and running as a third
    account would be wrong, so the launch is refused."""
    picked = []
    for key in dict.fromkeys(contexts):
        cfg = _instance_env_config(key, agent)
        if cfg is not None:
            picked.append((key, cfg))
    if not picked:
        return {"key": "", "config": default_config}
    if len(picked) > 1:
        names = ", ".join(k for k, _ in picked)
        return {"error": f"projects {names} each pin their own {agent} environment; "
                         "select one of them"}
    return {"key": picked[0][0], "config": picked[0][1]}


def project_entries() -> list[dict]:
    """Every project a task can select, with the repositories it holds.

    The repository list comes from core.config.get_repos, not from
    workspace.repos, because a project configured with projects_dir names no
    repositories there and would report none."""
    instances = runtime.instances()
    entries = []
    for key in sorted(instances.keys() if instances else []):
        cfg = instances.get(key).config
        ws = cfg.get("workspace") or {}
        repos = [r["name"] for r in work_worktree._repos_of(cfg)]
        entries.append({"key": key, "root": str(ws.get("root", "")), "repos": repos})
    extras = {
        "frshty": os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "clarivis": os.path.expanduser("~/Documents/dev/clarivis"),
        "algotrader2": os.path.expanduser("~/Documents/dev/algotrader2/implementation"),
    }
    for key, root in extras.items():
        if not any(e["key"] == key for e in entries) and os.path.isdir(root):
            entries.append({"key": key, "root": root, "repos": []})
    return sorted(entries, key=lambda e: e["key"])


def link_map() -> dict:
    repos: dict[str, dict] = {}
    instances = runtime.instances()
    for key in sorted(instances.keys() if instances else []):
        cfg = instances.get(key).config
        platform = (cfg.get("job") or {}).get("platform")
        ws = cfg.get("workspace") or {}
        if platform == "github":
            gh_repo = (cfg.get("github") or {}).get("repo") or []
            for full in ([gh_repo] if isinstance(gh_repo, str) else gh_repo):
                name = full.split("/")[-1]
                repos.setdefault(name, {
                    "pr": f"https://github.com/{full}/pull/{{n}}",
                    "commit": f"https://github.com/{full}/commit/{{sha}}",
                })
        elif platform == "bitbucket":
            org = (cfg.get("bitbucket") or {}).get("org")
            if not org:
                continue
            names = []
            for r in ws.get("repos") or []:
                names.append(r.get("name", "") if isinstance(r, dict) else str(r))
            pdir, root = ws.get("projects_dir"), ws.get("root")
            if pdir and root:
                p = os.path.join(str(root), pdir)
                if os.path.isdir(p):
                    names += [d for d in os.listdir(p)
                              if os.path.isdir(os.path.join(p, d)) and not d.startswith(".")]
            for name in names:
                if not name:
                    continue
                repos.setdefault(name, {
                    "pr": f"https://bitbucket.org/{org}/{name}/pull-requests/{{n}}",
                    "commit": f"https://bitbucket.org/{org}/{name}/commits/{{sha}}",
                })
    return {"repos": repos}


def slack_available() -> bool:
    return os.path.isdir(os.path.join(SLACK_INT_DIR, "messages"))


def read_system_prompt(runs: list[dict]) -> str:
    """The launch context seeded into the work session via
    --append-system-prompt, read back from the file the first launch wrote.
    Empty string when no run has a launch file (pre-feature items, or a
    cleaned launch directory)."""
    for run in runs:
        path = os.path.join(terminal.LAUNCH_CONTEXT_DIR, f"{run['session_id']}.md")
        try:
            with open(path) as f:
                return f.read()
        except OSError:
            continue
    return ""


GUIDANCE_FILE = "CLAUDE.md"


def _repo_dirs(config: dict) -> list[str]:
    """Every repo directory of one project, from the explicit repo list or
    from the scan of the projects directory, the two layouts a workspace
    config can use."""
    ws = config.get("workspace") or {}
    root = str(ws.get("root") or "")
    if not root:
        return []
    names = ws.get("repos") or []
    if names:
        dirs = []
        for entry in names:
            name = entry.get("name", "") if isinstance(entry, dict) else str(entry)
            if name:
                dirs.append(os.path.join(root, name))
        return dirs
    projects_dir = ws.get("projects_dir")
    if not projects_dir:
        return []
    base = os.path.join(root, str(projects_dir))
    if not os.path.isdir(base):
        return []
    exclude = set(ws.get("exclude") or [])
    return [os.path.join(base, name) for name in sorted(os.listdir(base))
            if name not in exclude
            and os.path.exists(os.path.join(base, name, ".git"))]


def _guidance_files(key: str, root: str) -> list[str]:
    """Every CLAUDE.md that holds the rules of one project: the file at the
    workspace root and the file in each repo under it.

    An agent only reads the file in its own working directory. A task that
    names a project can start in another directory, and it can name more than
    one project, so the paths have to be written into the launch context."""
    paths = []
    root_file = os.path.abspath(os.path.join(str(root), GUIDANCE_FILE))
    if os.path.isfile(root_file):
        paths.append(root_file)
    instances = runtime.instances()
    entry = instances.get(key) if instances else None
    if entry is None:
        return paths
    for repo_dir in _repo_dirs(entry.config):
        path = os.path.abspath(os.path.join(repo_dir, GUIDANCE_FILE))
        if os.path.isfile(path) and path not in paths:
            paths.append(path)
    return paths


def _context_block(contexts: list[str], slack: bool) -> str:
    lines = []
    guidance: list[str] = []
    by_key = {e["key"]: e for e in project_entries()}
    for key in contexts:
        e = by_key.get(key)
        if not e:
            continue
        repos = ", ".join(e["repos"])
        suffix = f", repos: {repos}" if repos else ""
        files = [f for f in _guidance_files(e["key"], e["root"]) if f not in guidance]
        guidance += files
        if files:
            suffix += f", rules: {', '.join(files)}"
        lines.append(f"- Project {e['key']}: workspace {e['root']}{suffix}")
    if slack:
        lines.append(
            f"- Slack: a local capture archive at {SLACK_INT_DIR}/messages/<workspace>/messages.jsonl "
            "holds each org's messages. Pick the workspace directory matching this project and "
            "read the recent tail for context on what people are saying and waiting on.")
    if not lines:
        return ""
    block = "\n\n## Context sources\n\n" + "\n".join(lines) + "\n"
    if guidance:
        block += ("\nRead every file listed as rules above before you do anything else. "
                  "Those files hold the rules of the project. Follow them for this task.\n")
    return block


def _source_block(source_item_id: int) -> str:
    item = db.query_one(
        "SELECT id, objective, summary, current_checkpoint FROM work_items WHERE id = ?",
        (source_item_id,))
    if not item:
        return ""
    outcome = (item["summary"] or item["current_checkpoint"] or "(no summary recorded)").strip()
    artifacts = db.query_all(
        "SELECT path, note FROM work_artifacts WHERE work_item_id = ? ORDER BY id",
        (source_item_id,))
    lines = [f"This job follows completed work item {item['id']}: {item['objective']}",
             "", "Outcome of that job:", outcome[:2000]]
    if artifacts:
        lines.append("")
        lines.append("Artifacts that job produced:")
        for a in artifacts:
            lines.append(f"- {a['path']}" + (f" - {a['note']}" if a["note"] else ""))
    return "\n\n## Previous work item\n\n" + "\n".join(lines) + "\n"


def _reviewer_cmd(agent: str, config: dict) -> tuple[str, str]:
    """The other model's name and the command line that runs it once.

    Only the config directory variable is carried, and an env override of it
    wins over config_dir, the precedence core.terminal already uses. The pane
    environment drops CLAUDE_CONFIG_DIR and CODEX_HOME, so a bare command
    would authenticate as the operator's default account, but the rest of an
    instance's env overrides may hold secrets and must stay out of the
    prompt. Both commands read the question from stdin."""
    llm = (config or {}).get("llm") or {}
    if agent == "codex":
        cfg = llm.get("claude") or {}
        var, tail = "CLAUDE_CONFIG_DIR", "--dangerously-skip-permissions -p"
        default_bin = "claude"
    else:
        cfg = llm.get("codex") or {}
        var = "CODEX_HOME"
        tail = "exec --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check -"
        default_bin = "codex"
    env = {str(k): str(v) for k, v in (cfg.get("env") or {}).items()}
    config_dir = env.get(var) or cfg.get("config_dir")
    prefix = ""
    if config_dir:
        prefix = f"{var}={shlex.quote(os.path.expanduser(str(config_dir)))} "
    bin_name = shlex.quote(str(cfg.get("bin", default_bin)))
    return ("claude" if agent == "codex" else "codex"), f"{prefix}{bin_name} {tail}"


def _cross_check_block(agent: str, config: dict) -> str:
    """Tell the working agent to have the other model check its work.

    A claude session is checked by codex. A codex session is checked by
    claude. The reviewer is asked for high and critical defects only. A
    style note must never hold up the report."""
    other, cmd = _reviewer_cmd(agent, config)
    return (
        f"Double check your analysis and your code with {other} before you report the "
        f"work done. Write your question to a file. Give {other} the claim you make and "
        f"the files you changed. Ask {other} to report only high severity and critical "
        "defects: a wrong result, a crash, data loss, a security hole, a broken contract "
        "between caller and callee, or any behavior that differs from the stated "
        f"objective. Tell {other} to skip every nit that does not change behavior, "
        "including style, naming, formatting, comment wording, and test coverage "
        f"suggestions. Tell {other} to give the severity and the concrete failure case "
        f"for each finding. Run `{cmd}` from the "
        "working directory with that file on stdin. Fix every finding you agree with. "
        "State every finding you rejected and the reason. "
    )


SLACK_LABEL = "slack_int"


def _resolve_launch(objective: str, cwd: str, contexts: list[str], agent: str,
                    source_item_id: int | None, repo_pick: str = "",
                    no_worktree: bool = False, item_id: int | None = None) -> dict:
    """Validate one launch and resolve everything it needs to start.

    An approval launches a work item that already exists, so every check that
    can refuse a launch has to run before the item is claimed. This returns
    the resolved arguments, or {"error": ...} and nothing has changed.

    The worktree is planned here and materialized in `_start`. Planning writes
    nothing, so a launch that cannot resolve still leaves the board as it
    was, and materializing needs a work item id that does not exist yet on
    this path."""
    objective = (objective or "").strip()
    contexts = [c for c in (contexts or []) if isinstance(c, str)]
    agent = (agent or "claude").strip().lower()
    if not objective:
        return {"error": "empty objective"}
    if agent not in terminal.AGENTS:
        return {"error": f"unknown agent: {agent}"}
    config = personal_config()
    if config is None:
        return {"error": "personal instance not loaded; work layer is read-only"}
    source_block = ""
    if source_item_id is not None:
        source_block = _source_block(source_item_id)
        if not source_block:
            return {"error": f"unknown source work item: {source_item_id}"}
    caller_cwd = (cwd or "").strip()
    cwd = caller_cwd
    entries = project_entries()
    if not cwd and len(contexts) == 1:
        entry = next((e for e in entries if e["key"] == contexts[0]), None)
        if entry and os.path.isdir(entry["root"]):
            cwd = entry["root"]
    cwd = cwd or str(config["workspace"]["root"])
    if not os.path.isdir(cwd):
        return {"error": f"cwd does not exist: {cwd}"}
    env = agent_config(contexts, config, agent)
    if "error" in env:
        return env
    worktree = work_worktree.plan(objective, caller_cwd, cwd, contexts, entries,
                                  repo_pick=repo_pick, no_worktree=no_worktree,
                                  item_id=item_id)
    return {"objective": objective, "contexts": contexts, "agent": agent,
            "config": config, "env_key": env["key"], "env_config": env["config"],
            "source_block": source_block, "cwd": worktree["cwd"],
            "worktree": worktree}


def launch(objective: str, cwd: str = "", contexts: list[str] | None = None,
           slack: bool = False, source_item_id: int | None = None,
           agent: str = "claude", brief: str = "", repo: str = "",
           no_worktree: bool = False) -> dict:
    plan = _resolve_launch(objective, cwd, contexts or [], agent, source_item_id,
                           repo_pick=repo, no_worktree=no_worktree)
    if "error" in plan:
        return plan
    objective, contexts = plan["objective"], plan["contexts"]
    label_list = contexts + ([SLACK_LABEL] if slack else [])
    labels = ",".join(label_list)
    tags = work_tags.derive_tags(objective, label_list,
                                 [e["key"] for e in project_entries()])
    item_id = work_store.create_item(objective, instance_key="personal", contexts=labels,
                                     source_item_id=source_item_id, tags=",".join(tags),
                                     worktree_opt_out=no_worktree)
    return _start(item_id, plan, slack, brief, tags)


def launch_proposed(item_id: int, agent: str = "claude") -> dict:
    """Start the agent on a proposal the operator approved.

    The item is already on the board with its objective, project labels,
    working directory and brief, so approval only has to resolve the launch,
    claim the row and start the session. A resolve that fails leaves the
    proposal where it was; a claim that loses a race reports it."""
    item = db.query_one(
        "SELECT id, state, objective, contexts, tags, source_item_id, launch_cwd, "
        "launch_brief, worktree_opt_out FROM work_items WHERE id = ?", (item_id,))
    if not item:
        return {"error": "unknown work item"}
    if item["state"] != work_store.PROPOSED_STATE:
        return {"error": f"work item {item_id} is not awaiting approval "
                         f"(state: {item['state']})"}
    labels = [c for c in (item["contexts"] or "").split(",") if c]
    slack = SLACK_LABEL in labels
    plan = _resolve_launch(item["objective"], item["launch_cwd"],
                           [c for c in labels if c != SLACK_LABEL], agent,
                           item["source_item_id"],
                           no_worktree=bool(item["worktree_opt_out"]),
                           item_id=item_id)
    if "error" in plan:
        return plan
    if not work_store.claim_proposal(item_id):
        return {"error": f"work item {item_id} is no longer awaiting approval"}
    # A claimed proposal that produced no run never reached an agent, so it
    # goes back on the board for the operator to approve again. The test is
    # the run, not how _start ended: it can raise before add_run, on an
    # artifact or prompt failure, and it can also raise after the agent is
    # already running, when the kickoff thread or the tagging call fails.
    # Releasing on the second would show a live agent as waiting for approval.
    # A launch that did create a run is already recorded as failed_stale by
    # mark_launch_failed, which is what every other launch leaves behind.
    try:
        result = _start(item_id, plan, slack, item["launch_brief"] or "",
                        work_tags.split_tags(item["tags"]))
    except Exception:
        if not work_store.has_run(item_id):
            work_store.release_proposal(item_id)
        raise
    if not work_store.has_run(item_id):
        work_store.release_proposal(item_id)
    return result


def _materialize(item_id: int, plan: dict) -> tuple[str, dict]:
    """The directory a run starts in, and the worktree row behind it.

    Runs outside work_store.launch_lock, because it fetches and installs
    dependencies. A create that fails is recorded and the run starts in the
    directory the plan already had, so a launch never fails because a worktree
    could not be made; the write gate and the commit gate then keep that run
    from writing into a shared checkout."""
    worktree = plan.get("worktree") or {}
    cwd = worktree.get("cwd") or plan["cwd"]
    if worktree.get("row"):
        return cwd, worktree["row"]
    if not worktree.get("create"):
        # A follow-up runs in its source's worktree under R2 and creates
        # nothing. Recording it makes this task a holder of the directory too,
        # so the sweep holds the directory to this task's state as well.
        return cwd, work_worktree.adopt_path(item_id, cwd)
    row = work_worktree.ensure(item_id, worktree, plan["objective"])
    return (row["path"], row) if row else (cwd, {})


def _start(item_id: int, plan: dict, slack: bool, brief: str,
           tags: list[str]) -> dict:
    objective, contexts, agent = plan["objective"], plan["contexts"], plan["agent"]
    config, source_block = plan["config"], plan["source_block"]
    env_config = plan.get("env_config") or config
    session_id = str(uuid.uuid4())
    artifact_dir = work_artifacts.item_dir(item_id)
    tmux_key = f"work-{item_id}"
    # Materializing runs outside the lock, because it fetches from the remote
    # and installs dependencies, and holding the global launch lock across
    # either would stall every other launch, resume and write gate. It is safe
    # there: materializing records a work_worktrees row for this item, this
    # item is not finished, and gc keeps any worktree an unfinished item
    # holds.
    cwd, worktree_row = _materialize(item_id, plan)
    # Checking the directory and opening the pane are one critical section.
    # gc() takes the same lock, so without it the sweep can remove the
    # directory between the check and tmux opening it, and a tmux session
    # whose -c directory is gone does not fail: it starts in $HOME, where
    # every relative path the agent writes would land.
    with work_store.launch_lock:
        if not os.path.isdir(cwd):
            # The sweep removed it between materializing and here. Rebuilding
            # costs a fetch inside the lock, and that is the right trade: the
            # alternative is a launch that fails, or one that falls back into
            # the shared checkout this whole path exists to keep it out of.
            rebuilt = work_worktree.rebuild(item_id, objective, cwd)
            if rebuilt:
                cwd, worktree_row = rebuilt["path"], rebuilt
        if not os.path.isdir(cwd):
            log.emit("work_launch_failed",
                     f"work item {item_id}: working directory {cwd} is gone")
            return {"error": f"cwd does not exist: {cwd}", "item_id": item_id}
        run_id = work_store.add_run(
            item_id, session_id, tmux_key, cwd, provider=agent, env_recorded=True,
            env_key=plan.get("env_key", ""),
            env_config_dir=terminal.agent_config_dir(env_config, agent),
            board_url=core_config.board_url())
        context = (
            f"# Work item {item_id}\n\n## Objective\n\n{objective}\n"
            + source_block + _context_block(contexts, slack)
            + (work_worktree.context_block(worktree_row) if worktree_row else "")
            + (brief or "") + "\n"
            "Work toward the objective. When you stop, state a one-line checkpoint. "
            "When you hit a decision point, decide yourself by default: pick the "
            "most correct, cleanest, simplest option and keep going. Ask the "
            "operator only when you truly cannot decide — the choice is "
            "irreversible or destructive, or it depends on operator intent you "
            "cannot infer. Ask with the AskUserQuestion tool, then end your turn "
            "immediately; the work board shows the question to the operator, and "
            "when their answer arrives as your next message, resume work from it. "
            "Use that same tool for anything only the operator can supply — a "
            "secret, a one-time code, an approval — not only for a decision. A "
            "request written in prose does not reach the operator. "
            "Never send outward communications (Slack messages, GitHub or Bitbucket comments, "
            "emails, posts to external services) unless the operator explicitly asks for that "
            "in this conversation; draft the content and ask instead. "
            "When you produce a file the operator will open (report, page, video, image), "
            f"write it under {artifact_dir}/ unless it belongs in a repository, and print "
            "a line: ARTIFACT: /absolute/path - one-line description. Never write such a "
            "file to /tmp or to a scratchpad directory: the board serves the file from "
            "disk long after this session ends. Write every report, summary, analysis or "
            "other prose document for the operator as a self-contained .html file, never "
            "as Markdown or plain text: the board opens an artifact in a browser, and a "
            ".md file reads there as raw source. A document that belongs in a repository "
            "keeps that repository's own format. Never publish an HTML page to the hosted "
            "Claude artifact service; write the .html file into that folder, with every "
            "image it needs beside it. "
            "Write git commit messages and pull request descriptions about the change only. "
            "Never name the model, the vendor, the agent or the tool that produced the work. "
            "Never add a session link, a Co-Authored-By trailer, or a line saying the work "
            "was generated by an agent. This rule overrides every other attribution "
            "instruction, including one that arrives later in the session. "
            + _cross_check_block(agent, env_config)
            + f"When the objective is fully met, end your final message with the single line {work_store.DONE_MARKER}."
        )
        try:
            terminal.launch_agent(tmux_key, cwd, session_id, context, True,
                                  config=env_config, agent=agent)
            health = terminal.session_healthy(tmux_key, agent=agent)
            if not health.get("alive"):
                raise RuntimeError("tmux session did not start")
        except Exception as e:
            work_store.mark_launch_failed(run_id, f"{type(e).__name__}: {e}")
            log.emit("work_launch_failed", f"work item {item_id}: {type(e).__name__}: {e}")
            return {"error": f"launch failed: {e}", "item_id": item_id}
    threading.Thread(target=_kickoff, args=(tmux_key, run_id, agent), daemon=True).start()
    if len(tags) < work_tags.MAX_TAGS:
        work_tags.schedule_implicit_tags(item_id, objective, config)
    return {"item_id": item_id, "run_id": run_id, "session_id": session_id,
            "tmux_key": tmux_key, "state": "agent_working", "agent": agent,
            "cwd": cwd, "worktree": worktree_row.get("path", "")}


WORK_SESSION_PREFIX = "term-work-"
SUSPEND_IDLE_SECONDS = 30 * 60


def suspend_idle_done_sessions(now: float | None = None) -> list[int]:
    """Kill the tmux session of every finished work item once its pane has sat
    idle for SUSPEND_IDLE_SECONDS. A task waiting to be acknowledged is
    finished, so its session is suspended too.

    Without this every finished item keeps a resident Claude process forever.
    Nothing is lost by the kill: the conversation lives in the Claude
    transcript, and opening the item's terminal again relaunches Claude with
    --resume on the same session id (resume_session). The idle window keeps a
    session the operator is actively revisiting alive; a work session with no
    matching work item row is treated as done."""
    now = time.time() if now is None else now
    killed: list[int] = []
    for s in terminal.list_sessions():
        if not s["name"].startswith(WORK_SESSION_PREFIX):
            continue
        suffix = s["name"][len(WORK_SESSION_PREFIX):]
        if not suffix.isdigit() or now - s["activity"] < SUSPEND_IDLE_SECONDS:
            continue
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (int(suffix),))
        if item is not None and item["state"] not in work_store.FINISHED_STATES:
            continue
        terminal.kill_terminal(f"work-{suffix}")
        killed.append(int(suffix))
    if killed:
        log.emit("work_sessions_suspended",
                 f"suspended {len(killed)} done work session(s): {sorted(killed)}")
    return killed


def resume_session(item_id: int) -> bool:
    """Bring a suspended work session back to its old state: recreate the tmux
    session in the run's cwd and relaunch Claude with --resume on the item's
    original session id.

    The run records the project whose environment the launch used and the
    configuration directory it resolved to. The recorded directory is the
    authority, so a resume finds the same conversation after the project
    config changed. A project that no longer loads refuses the resume, because
    every other account starts a different conversation, and a config that
    stopped loading is a state the operator can repair. A run that recorded no
    environment at all started before the columns existed, and it keeps the
    live config the way it always did.

    The directory is resolved again, because a resume does not go through
    `_start` and would otherwise put an agent straight back into the shared
    checkout a pre-feature run recorded. A worktree the sweep removed is
    rebuilt on the same branch instead of being lost.

    No-op while the agent is still running, checked under launch_lock so two
    concurrent terminal connects cannot both relaunch it. A surviving tmux
    pane with no agent is respawned with the resume command."""
    run = db.query_one(
        "SELECT session_id, cwd, provider, agent_session_id, env_recorded, env_key, "
        "env_config_dir FROM work_runs WHERE work_item_id = ? ORDER BY id DESC LIMIT 1",
        (item_id,))
    if not run:
        return False
    config = personal_config()
    if config is None:
        return False
    env_config = config
    if run["env_recorded"]:
        base = config
        if run["env_key"]:
            base = _instance_config(run["env_key"])
            if base is None:
                log.emit("work_agent_env_missing",
                         f"work item {item_id}: project {run['env_key']} no longer "
                         "loads, so the environment of the launch cannot be rebuilt; "
                         "the session is not resumed")
                return False
        env_config = terminal.with_config_dir(base, run["provider"],
                                              run["env_config_dir"])
    item = db.query_one(
        "SELECT objective, contexts, worktree_opt_out FROM work_items WHERE id = ?",
        (item_id,))
    recorded = run["cwd"] if run["cwd"] and os.path.isdir(run["cwd"]) else ""
    contexts = [c for c in ((item["contexts"] if item else "") or "").split(",")
                if c and c != SLACK_LABEL]
    key = f"work-{item_id}"
    if terminal.session_healthy(key, agent=run["provider"])["agent_running"]:
        return True
    # Resolved and materialized outside the lock, for the reason _start does
    # the same: a fetch and a dependency install must not stall every other
    # launch. The row materializing records belongs to this item, and gc keeps
    # a worktree whose holder still has a live session or is not finished.
    cwd = recorded
    if item is not None:
        worktree = work_worktree.plan(
            item["objective"], recorded, recorded, contexts, project_entries(),
            no_worktree=bool(item["worktree_opt_out"]), item_id=item_id)
        cwd = worktree["cwd"]
        if worktree.get("create"):
            row = work_worktree.ensure(item_id, worktree, item["objective"])
            if row:
                cwd = row["path"]
    # Re-check and launch are one critical section: the sweep must not remove
    # the directory between the check and tmux opening it, because a tmux
    # session whose -c directory is gone starts in $HOME instead of failing.
    with work_store.launch_lock:
        if terminal.session_healthy(key, agent=run["provider"])["agent_running"]:
            return True
        if not cwd or not os.path.isdir(cwd):
            rebuilt = work_worktree.rebuild(
                item_id, item["objective"] if item else "", cwd)
            if rebuilt:
                cwd = rebuilt["path"]
        if not os.path.isdir(cwd):
            # A task that ever had a worktree is never resumed in the workspace
            # root. The root of a project is the shared checkout, or holds it,
            # and putting a resumed agent there is the failure this whole
            # feature exists to prevent. The operator repairs it instead.
            if work_worktree.last_row(item_id) is not None:
                log.emit("work_resume_failed",
                         f"work item {item_id}: its worktree is gone and could not "
                         "be rebuilt; the session is not resumed rather than "
                         "resumed in a shared checkout")
                return False
            cwd = str(config["workspace"]["root"])
        if not os.path.isdir(cwd):
            log.emit("work_resume_failed",
                     f"work item {item_id}: neither the recorded directory nor "
                     f"the workspace root {cwd} exists; the session is not resumed")
            return False
        if cwd != run["cwd"]:
            work_store.record_run_cwd(run["session_id"], cwd)
        terminal.launch_agent(key, cwd, run["session_id"], "", False, config=env_config,
                              agent=run["provider"],
                              agent_session_id=run["agent_session_id"])
    return True


def launch_followup(source_item_id: int, objective: str, cwd: str = "",
                    contexts: list[str] | None = None, slack: bool | None = None,
                    agent: str = "") -> dict:
    """Launch a task that continues a finished task.

    A caller that names the projects, the Slack archive, the working directory
    or the agent gets exactly those. A caller that omits them inherits them
    from the source task, so a follow-up typed in one box runs where its
    source ran. The inherited directory is the one the source run started in,
    which is the resolved directory, not the project the operator picked. An
    inherited directory that no longer exists is dropped, so a follow-up never
    fails on a directory the caller did not name. Passing an empty context
    list is a choice, not an omission: it launches with no project context."""
    source = db.query_one(
        "SELECT i.id, i.state, i.contexts, i.launch_cwd, "
        "(SELECT provider FROM work_runs r WHERE r.work_item_id = i.id "
        "ORDER BY r.id DESC LIMIT 1) AS last_provider, "
        "(SELECT cwd FROM work_runs r WHERE r.work_item_id = i.id "
        "ORDER BY r.id DESC LIMIT 1) AS last_cwd "
        "FROM work_items i WHERE i.id = ?", (source_item_id,))
    if not source:
        return {"error": f"unknown source work item: {source_item_id}"}
    if source["state"] not in work_store.FINISHED_STATES:
        return {"error": f"source work item {source_item_id} is not done (state: {source['state']})"}
    labels = [c for c in (source["contexts"] or "").split(",") if c]
    source_contexts = [c for c in labels if c != SLACK_LABEL]
    if contexts is None:
        contexts = source_contexts
        if slack is None:
            slack = SLACK_LABEL in labels
    # The directory is inherited on its own, not only when the projects were
    # omitted too. The follow-up box on the task page always sends the
    # projects, seeded from the source task, so a rule that keyed on them cut
    # every follow-up launched from the board off from its source's worktree.
    # A follow-up that changes the projects is a different piece of work, and
    # the source's worktree would be the wrong repository for it.
    if not cwd and set(contexts) == set(source_contexts):
        # The source's worktree row is read ahead of the recorded directory,
        # so the chain holds even when the run row was never moved to the
        # worktree a gate handed out mid-session.
        source_worktree = work_worktree.for_item(source_item_id) or {}
        inherited_cwd = (source_worktree.get("path") or source["last_cwd"]
                         or source["launch_cwd"] or "")
        if os.path.isdir(inherited_cwd):
            cwd = inherited_cwd
    return launch(objective, cwd=cwd, contexts=contexts, slack=bool(slack),
                  source_item_id=source_item_id,
                  agent=agent or source["last_provider"] or "claude")


PUSH_GATE_TEST_TIMEOUT = TEST_RUN_TIMEOUT // 3
_GATE_TAIL = 1500
_SHELL_SEPARATORS = ("&&", "||", ";", "|", "&", "(", ")", "\n")
_GIT_TWO_ARG_FLAGS = ("-c", "--exec-path", "--git-dir", "--work-tree", "--namespace")


def _segment_git(tokens: list[str], verb: str) -> str | None:
    """The -C directory of a `git <verb>` in one pipeline segment, "" when the
    command has no -C, or None when the segment does not run `git <verb>`."""
    i = 0
    while i < len(tokens) and "=" in tokens[i] and not tokens[i].startswith("-"):
        i += 1
    if i >= len(tokens) or os.path.basename(tokens[i]) != "git":
        return None
    i += 1
    chdir = ""
    while i < len(tokens):
        tok = tokens[i]
        if tok == "-C":
            chdir = tokens[i + 1] if i + 1 < len(tokens) else ""
            i += 2
        elif tok in _GIT_TWO_ARG_FLAGS:
            i += 2
        elif tok.startswith("-"):
            i += 1
        else:
            return chdir if tok == verb else None
    return None


_HEREDOC_RE = re.compile(r"<<-?\s*(['\"]?)([A-Za-z_][A-Za-z0-9_]*)\1")


def _strip_heredocs(command: str) -> str:
    """`command` without the body of any heredoc it feeds a program.

    A heredoc body is data, not a command. Left in place, a command that
    writes a file about git reads as a git invocation, and the commit gate
    then blocks a command that runs no commit."""
    lines = command.split("\n")
    kept: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        kept.append(line)
        markers = [m.group(2) for m in _HEREDOC_RE.finditer(line)]
        i += 1
        for marker in markers:
            while i < len(lines) and lines[i].strip() != marker:
                i += 1
            i += 1
    return "\n".join(kept)


def _tokenize(command: str) -> list[str] | None:
    """Shell tokens of `command`, or None when shlex cannot tokenize it."""
    lex = shlex.shlex(command.replace("\n", " ; "), posix=True,
                      punctuation_chars=True)
    lex.whitespace_split = True
    try:
        return list(lex)
    except ValueError:
        return None


def _compose_chdir(chdir: str, found: str) -> str:
    """The directory a segment runs in, from the `cd` that preceded it and the
    `git -C` it carries.

    `-C` used to discard the `cd`, so `cd /shared/repo && git -C src commit`
    reported `src` alone. That resolves against the session's own directory
    and makes a gate inspect the wrong repository."""
    if not found:
        return chdir
    if os.path.isabs(found) or not chdir:
        return found
    return os.path.normpath(os.path.join(chdir, found))


def _parse_git_all(command: str, verb: str) -> list[dict]:
    """Every `git <verb>` invocation in a shell command line.

    Returns one {"chdir": dir-or-""} per segment that runs the verb, in order.
    Walks shell tokens segment by segment so a quoted string, a grep pattern,
    or `git commit -m push` does not match; tracks a preceding `cd <dir>` and a
    `git -C <dir>` so a gate checks the repository the command actually
    targets. Returning every match matters: stopping at the first one let a
    second `git commit` later in the same command line through unchecked. A
    command shlex cannot tokenize falls back to a word-boundary match inside
    one segment."""
    command = _strip_heredocs(command)
    tokens = _tokenize(command)
    if tokens is None:
        if re.search(rf"(?:^|[|;&])[^|;&]*\bgit\b[^|;&]*\b{verb}\b", command):
            return [{"chdir": ""}]
        return []
    chdir = ""
    found_all: list[dict] = []
    segment: list[str] = []
    for tok in tokens + ["\n"]:
        if tok in _SHELL_SEPARATORS:
            found = _segment_git(segment, verb)
            if found is not None:
                found_all.append({"chdir": _compose_chdir(chdir, found)})
            elif len(segment) >= 2 and segment[0] == "cd":
                chdir = _compose_chdir(chdir, segment[1])
            segment = []
        else:
            segment.append(tok)
    return found_all


def _parse_git(command: str, verb: str) -> dict | None:
    """The first `git <verb>` invocation in a shell command line, or None."""
    found = _parse_git_all(command, verb)
    return found[0] if found else None


def parse_push(command: str) -> dict | None:
    """Detect a `git push` invocation in a shell command line."""
    return _parse_git(command, "push")


def parse_commit(command: str) -> dict | None:
    """Detect a `git commit` invocation in a shell command line."""
    return _parse_git(command, "commit")


def _repo_root(start_dir: str) -> Path | None:
    try:
        top = git_util.run_git(start_dir, ["rev-parse", "--show-toplevel"],
                               timeout=30).stdout.strip()
    except (git_util.GitCommandError, subprocess.TimeoutExpired, OSError):
        return None
    return Path(top) if top else None


def _outgoing_files(repo: Path) -> list[str] | None:
    """Files changed between the remote base and HEAD, or None when no remote
    base exists to diff against. Base preference: the branch upstream, then
    origin/HEAD."""
    base = ""
    for probe in (["rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}"],
                  ["symbolic-ref", "--short", "refs/remotes/origin/HEAD"]):
        try:
            base = git_util.run_git(repo, probe, timeout=30).stdout.strip()
        except (git_util.GitCommandError, subprocess.TimeoutExpired, OSError):
            base = ""
        if base:
            break
    if not base:
        return None
    try:
        out = git_util.run_git(repo, ["diff", "--name-only", f"{base}...HEAD"],
                               timeout=30).stdout
    except (git_util.GitCommandError, subprocess.TimeoutExpired, OSError):
        return None
    return [ln.strip() for ln in out.splitlines() if ln.strip()]


def _gate_tests(repo: Path) -> dict:
    runner = _detect_runner(repo)
    if runner is None:
        return {"result": "no_runner", "cmd": "", "exit_code": 0, "tail": ""}
    cmd, env = runner
    if cmd and cmd[0] == _NO_LOCAL_PY_VENV_SENTINEL:
        return {"result": "fail", "cmd": "(no local venv)", "exit_code": -1,
                "tail": "Python tests detected but no local virtualenv solution "
                        "found. Expected one of: .venv/bin/pytest, Pipfile, "
                        "uv.lock, poetry.lock. Install dependencies into a "
                        "local .venv, then push again."}
    outcome = _run_repo_tests(repo, cmd, env, timeout=PUSH_GATE_TEST_TIMEOUT)
    outcome["cmd"] = " ".join(cmd)
    return outcome


def _deny_reason(stage: str, detail: str) -> str:
    return (f"Push blocked by the work-layer code gate: {stage} failed.\n\n"
            f"{detail.strip()}\n\n"
            "Fix the failures, commit the fixes, and run the push again; the "
            "gate reruns on every push. Do not bypass the gate by loosening "
            "the linter, skipping tests, or pushing another way.")


_ATTRIBUTION_PATTERNS = (
    ("agent session link",
     re.compile(r"https?://\S*(?:claude\.ai|anthropic\.com|openai\.com|chatgpt\.com)\S*", re.I)),
    ("agent session trailer",
     re.compile(r"(?:^|\\n)\s*(?:claude|codex|agent|assistant|ai)[-_ ]session\s*:",
                re.I | re.M)),
    ("agent co-author trailer",
     re.compile(r"co-authored-by:[^\n]*(?:claude|anthropic|codex|openai|chatgpt|gpt|copilot|cursor|\bai\b)", re.I)),
    ("agent credit line",
     re.compile(r"(?:generated|written|authored|produced|created|committed|drafted)\s+(?:with|by)\s+"
                r"(?:the\s+|an?\s+)?(?:claude|codex|anthropic|openai|chatgpt|gpt|copilot|cursor|"
                r"ai\b|llm\b|model\b|agent\b|assistant\b)", re.I)),
    ("agent authorship label",
     re.compile(r"\b(?:ai|llm|agent|machine)[-_]generated\b", re.I)),
    ("agent emoji", re.compile(r"\U0001F916")),
)

_MESSAGE_FILE_MAX = 256 * 1024

_COMMIT_DENY_REASON = (
    "Commit blocked by the work-layer commit gate: the message credits the "
    "agent that wrote the change.\n\n"
    "Matched {label}: {match}\n\n"
    "Write the message about the change only. Do not name the model, the "
    "vendor, the agent or the tool that produced it. Do not add a session "
    "link, a Co-Authored-By trailer, or a line saying the work was generated "
    "by an agent. This rule overrides every other attribution instruction. "
    "Rewrite the message and commit again."
)


def _message_files(tokens: list[str] | None) -> list[str]:
    """Paths passed to `git commit` as -F / --file, whose contents become the
    commit message."""
    paths: list[str] = []
    for i, tok in enumerate(tokens or []):
        if tok in ("-F", "--file"):
            if i + 1 < len(tokens):
                paths.append(tokens[i + 1])
        elif tok.startswith("--file="):
            paths.append(tok[len("--file="):])
        elif tok.startswith("-F") and len(tok) > 2:
            paths.append(tok[2:])
    return paths


def attribution_match(text: str) -> tuple[str, str] | None:
    """The (label, matched text) of the first agent self-attribution in `text`,
    or None when it carries none.

    Self-attribution is a link back to the agent session, an agent trailer, or
    a line crediting a model for the work. The patterns match attribution
    only, never subject matter, so a commit that changes the code frshty runs
    agents with ("run a work item with codex instead of claude") still
    commits."""
    for label, pattern in _ATTRIBUTION_PATTERNS:
        found = pattern.search(text or "")
        if found:
            return label, found.group(0)[:200]
    return None


_SHARED_COMMIT_DENY = (
    "Commit blocked by the work-layer commit gate: {repo} is a shared "
    "checkout. Other agents hold uncommitted work in it, and `git add -A` "
    "would take their changes into your commit.\n\n"
    "{where}\n\n"
    "Commit there instead. Leave the shared checkout untouched. Do not run "
    "git checkout, git stash, git reset or git clean in it."
)


def _commit_repos(command: str, cwd: str) -> list[str]:
    """The canonical checkout each `git commit` in `command` writes into.

    Empty when every commit targets a worktree or no git repository at all."""
    shared = []
    for found in _parse_git_all(command, "commit"):
        start = os.path.normpath(os.path.join(cwd or ".", found["chdir"] or "."))
        root = work_worktree.shared_checkout(start)
        if root and root not in shared:
            shared.append(root)
    return shared


def gate_commit(session_id: str, command: str, cwd: str = "") -> dict:
    """Deny a work-session `git commit` that credits the agent or that writes
    into a shared checkout.

    The message reaches git as a -m argument, a heredoc inside one, or a file
    named by -F, so the gate reads the command line and any such file. The
    operator wants no agent attribution in the history; the launch context
    says so, and a system reminder issued later in a session can still tell
    the agent to append a session link, so the rule is enforced here as well.

    The shared-checkout test is not keyed on the task already owning a
    worktree. A task that resolved none owns nothing, and a rule keyed on
    ownership would let exactly that task commit into the shared tree. A deny
    carries `need_worktree`, the repository the caller has to obtain a
    worktree of, so the message can name a real directory.
    Returns {"decision": "allow"|"deny", "reason": str}."""
    if parse_commit(command) is None:
        return {"decision": "allow", "reason": "not a commit"}
    text = command
    for path in _message_files(_tokenize(command)):
        message_file = Path(os.path.join(cwd or ".", path))
        try:
            if not message_file.is_file() or message_file.stat().st_size > _MESSAGE_FILE_MAX:
                continue
            text += "\n" + message_file.read_text(errors="replace")
        except OSError:
            continue
    found = attribution_match(text)
    if found:
        label, match = found
        work_store.record_gate(session_id, "commit_gate", "fail",
                               {"command": command[:300], "label": label, "match": match})
        return {"decision": "deny",
                "reason": _COMMIT_DENY_REASON.format(label=label, match=match)}
    item = work_worktree.session_item(session_id)
    if item is None or item["worktree_opt_out"]:
        return {"decision": "allow", "reason": "no agent attribution"}
    shared = _commit_repos(command, cwd)
    if not shared:
        return {"decision": "allow", "reason": "no agent attribution"}
    work_store.record_gate(session_id, "commit_gate", "fail",
                           {"command": command[:300], "shared": shared})
    # Keyed on the repository that was denied. A task can hold a worktree of
    # more than one repository, and naming the newest would send the agent to
    # commit one repository's changes into another repository's tree.
    row = work_worktree.for_item_repo(
        item["id"], work_worktree.repo_common_dir(shared[0]))
    where = (f"A worktree for this task is ready at {row['path']} on branch "
             f"{row['branch']}." if row and os.path.isdir(row["path"])
             else "Ask the work board for a worktree of this repository.")
    return {"decision": "deny", "need_worktree": shared[0], "item_id": item["id"],
            "reason": _SHARED_COMMIT_DENY.format(repo=shared[0], where=where)}


def gate_push(session_id: str, command: str, cwd: str) -> dict:
    """Lint and test every repository a work-session push targets.

    Returns {"decision": "allow"|"deny", "reason": str} and records the
    outcome as a push_gate event on the work item. Lint runs first and a lint
    failure denies before the test suite runs, so the fix loop stays short.
    A command whose repository cannot be resolved is allowed but recorded,
    because denying on a parse failure would wedge pushes the gate cannot
    even check. A command line that runs more than one push is gated once per
    push; the first denial wins."""
    pushes = _parse_git_all(command, "push")
    if not pushes:
        return {"decision": "allow", "reason": "not a push"}
    result = {"decision": "allow", "reason": "lint and tests passed"}
    for push in pushes:
        start_dir = os.path.normpath(os.path.join(cwd or ".", push["chdir"] or "."))
        result = _gate_one_push(session_id, command, start_dir)
        if result["decision"] == "deny":
            return result
    return result


def _gate_one_push(session_id: str, command: str, start_dir: str) -> dict:
    repo = _repo_root(start_dir)
    payload = {"command": command[:300], "repo": repo.name if repo else ""}
    if repo is None:
        payload["note"] = f"no git repository resolved from {start_dir}"
        work_store.record_gate(session_id, "push_gate", "skipped", payload)
        return {"decision": "allow", "reason": "no repository resolved"}
    files = _outgoing_files(repo)
    if files is None:
        lint = {"status": "skipped", "exit_code": 0,
                "output": "no remote base to diff against; lint skipped"}
    else:
        lint = git_util.lint_files(repo, files)
    payload["lint"] = {**lint, "output": lint["output"][-_GATE_TAIL:]}
    if lint["status"] not in ("pass", "no_config", "skipped"):
        work_store.record_gate(session_id, "push_gate", "fail", payload)
        return {"decision": "deny",
                "reason": _deny_reason(
                    "lint", f"pre-commit ({lint['status']}, exit "
                    f"{lint['exit_code']}):\n{lint['output'][-_GATE_TAIL:]}")}
    tests = _gate_tests(repo)
    payload["tests"] = {**tests, "tail": (tests.get("tail") or "")[-_GATE_TAIL:]}
    if tests["result"] not in ("pass", "no_runner"):
        work_store.record_gate(session_id, "push_gate", "fail", payload)
        return {"decision": "deny",
                "reason": _deny_reason(
                    "the test suite", f"{tests.get('cmd') or 'tests'} "
                    f"({tests['result']}, exit {tests.get('exit_code')}):\n"
                    f"{(tests.get('tail') or '')[-_GATE_TAIL:]}")}
    work_store.record_gate(session_id, "push_gate", "pass", payload)
    return {"decision": "allow", "reason": "lint and tests passed"}


TRUST_PROMPT_TRIES = 8
TRUST_PROMPT_INTERVAL = 2


def _answer_trust_prompt(tmux_key: str) -> bool:
    """Clear the codex directory-trust question before the readiness check.

    Codex holds the pane on the question while its process is already up, so
    the readiness check would call the run healthy and return with the
    question still on screen."""
    for _ in range(TRUST_PROMPT_TRIES):
        time.sleep(TRUST_PROMPT_INTERVAL)
        if terminal.answer_codex_trust(tmux_key):
            return True
    return False


def _kickoff(tmux_key: str, run_id: int, agent: str = "claude"):
    """Confirm the agent CLI came up, and give Claude its first prompt.

    Claude is seeded through --append-system-prompt, so it sits idle until a
    prompt arrives. Codex takes the same context as its first prompt on the
    command line, so it is already working and needs no kickoff message."""
    try:
        if agent == "codex":
            _answer_trust_prompt(tmux_key)
        for _ in range(30):
            time.sleep(3)
            if terminal.session_healthy(tmux_key, agent=agent).get("agent_running"):
                time.sleep(4)
                if agent != "claude":
                    return
                if work_store.tmux_send(tmux_key, "Begin the objective from your system prompt now."):
                    return
                break
        work_store.mark_launch_failed(
            run_id, f"kickoff never delivered: {agent} did not start in the pane")
    except Exception as e:
        work_store.mark_launch_failed(run_id, f"kickoff error: {type(e).__name__}: {e}")
