import os
import re
import shlex
import subprocess
import threading
import time
import uuid
from pathlib import Path

import core.db as db
import core.git_util as git_util
import core.log as log
import core.runtime as runtime
import core.terminal as terminal
from core.tasks.tickets import (
    TEST_RUN_TIMEOUT, _NO_LOCAL_PY_VENV_SENTINEL, _detect_runner, _run_repo_tests,
)
from services import work_store, work_tags


def personal_config() -> dict | None:
    instances = runtime.instances()
    if not instances:
        return None
    entry = instances.get("personal")
    if not entry:
        return None
    return entry.config


SLACK_INT_DIR = os.path.expanduser("~/Documents/dev/slack_int")


def project_entries() -> list[dict]:
    instances = runtime.instances()
    entries = []
    for key in sorted(instances.keys() if instances else []):
        cfg = instances.get(key).config
        ws = cfg.get("workspace") or {}
        repos = [r.get("name", "") for r in ws.get("repos") or [] if isinstance(r, dict)]
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


def _context_block(contexts: list[str], slack: bool) -> str:
    lines = []
    by_key = {e["key"]: e for e in project_entries()}
    for key in contexts:
        e = by_key.get(key)
        if not e:
            continue
        repos = ", ".join(e["repos"])
        suffix = f", repos: {repos}" if repos else ""
        lines.append(f"- Project {e['key']}: workspace {e['root']}{suffix}")
    if slack:
        lines.append(
            f"- Slack: a local capture archive at {SLACK_INT_DIR}/messages/<workspace>/messages.jsonl "
            "holds each org's messages. Pick the workspace directory matching this project and "
            "read the recent tail for context on what people are saying and waiting on.")
    if not lines:
        return ""
    return "\n\n## Context sources\n\n" + "\n".join(lines) + "\n"


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


def launch(objective: str, cwd: str = "", contexts: list[str] | None = None,
           slack: bool = False, source_item_id: int | None = None,
           agent: str = "claude") -> dict:
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
    cwd = (cwd or "").strip()
    if not cwd and len(contexts) == 1:
        entry = next((e for e in project_entries() if e["key"] == contexts[0]), None)
        if entry and os.path.isdir(entry["root"]):
            cwd = entry["root"]
    cwd = cwd or str(config["workspace"]["root"])
    if not os.path.isdir(cwd):
        return {"error": f"cwd does not exist: {cwd}"}
    label_list = contexts + (["slack_int"] if slack else [])
    labels = ",".join(label_list)
    tags = work_tags.derive_tags(objective, label_list,
                                 [e["key"] for e in project_entries()])
    item_id = work_store.create_item(objective, instance_key="personal", contexts=labels,
                                     source_item_id=source_item_id, tags=",".join(tags))
    session_id = str(uuid.uuid4())
    tmux_key = f"work-{item_id}"
    run_id = work_store.add_run(item_id, session_id, tmux_key, cwd, provider=agent)
    context = (
        f"# Work item {item_id}\n\n## Objective\n\n{objective}\n"
        + source_block + _context_block(contexts, slack) + "\n"
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
        "print a line: ARTIFACT: /absolute/path - one-line description. "
        f"When the objective is fully met, end your final message with the single line {work_store.DONE_MARKER}."
    )
    try:
        with work_store.launch_lock:
            terminal.launch_agent(tmux_key, cwd, session_id, context, True, config=config,
                                  agent=agent)
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
            "tmux_key": tmux_key, "state": "agent_working", "agent": agent}


WORK_SESSION_PREFIX = "term-work-"
SUSPEND_IDLE_SECONDS = 30 * 60


def suspend_idle_done_sessions(now: float | None = None) -> list[int]:
    """Kill the tmux session of every done work item once its pane has sat
    idle for SUSPEND_IDLE_SECONDS.

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
        if item is not None and item["state"] != "done":
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

    No-op while the tmux session still exists, checked under launch_lock so
    two concurrent terminal connects cannot both type the resume command into
    the pane."""
    run = db.query_one(
        "SELECT session_id, cwd, provider, agent_session_id FROM work_runs "
        "WHERE work_item_id = ? ORDER BY id DESC LIMIT 1", (item_id,))
    if not run:
        return False
    config = personal_config()
    if config is None:
        return False
    cwd = run["cwd"] if run["cwd"] and os.path.isdir(run["cwd"]) else str(config["workspace"]["root"])
    key = f"work-{item_id}"
    with work_store.launch_lock:
        if terminal.session_healthy(key, agent=run["provider"])["alive"]:
            return True
        terminal.launch_agent(key, cwd, run["session_id"], "", False, config=config,
                              agent=run["provider"],
                              agent_session_id=run["agent_session_id"])
    return True


def launch_followup(source_item_id: int, objective: str, cwd: str = "",
                    contexts: list[str] | None = None, slack: bool = False,
                    agent: str = "claude") -> dict:
    source = db.query_one("SELECT id, state FROM work_items WHERE id = ?", (source_item_id,))
    if not source:
        return {"error": f"unknown source work item: {source_item_id}"}
    if source["state"] != "done":
        return {"error": f"source work item {source_item_id} is not done (state: {source['state']})"}
    return launch(objective, cwd=cwd, contexts=contexts, slack=slack,
                  source_item_id=source_item_id, agent=agent)


PUSH_GATE_TEST_TIMEOUT = TEST_RUN_TIMEOUT // 3
_GATE_TAIL = 1500
_SHELL_SEPARATORS = ("&&", "||", ";", "|", "&", "(", ")", "\n")
_GIT_TWO_ARG_FLAGS = ("-c", "--exec-path", "--git-dir", "--work-tree", "--namespace")


def _segment_push(tokens: list[str]) -> str | None:
    """The -C directory of a `git push` in one pipeline segment, "" when the
    push has no -C, or None when the segment is not a push."""
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
            return chdir if tok == "push" else None
    return None


def parse_push(command: str) -> dict | None:
    """Detect a `git push` invocation in a shell command line.

    Returns {"chdir": dir-or-""} for the pushing segment, or None when the
    command does not push. Walks shell tokens segment by segment so a quoted
    string, a grep pattern, or `git commit -m push` does not match; tracks a
    preceding `cd <dir>` and a `git -C <dir>` so the gate checks the
    repository the push actually targets. A command shlex cannot tokenize
    falls back to a word-boundary match inside one segment."""
    lex = shlex.shlex(command.replace("\n", " ; "), posix=True,
                      punctuation_chars=True)
    lex.whitespace_split = True
    try:
        tokens = list(lex)
    except ValueError:
        if re.search(r"(?:^|[|;&])[^|;&]*\bgit\b[^|;&]*\bpush\b", command):
            return {"chdir": ""}
        return None
    chdir = ""
    segment: list[str] = []
    for tok in tokens + ["\n"]:
        if tok in _SHELL_SEPARATORS:
            found = _segment_push(segment)
            if found is not None:
                return {"chdir": found or chdir}
            if len(segment) >= 2 and segment[0] == "cd":
                chdir = segment[1]
            segment = []
        else:
            segment.append(tok)
    return None


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


def gate_push(session_id: str, command: str, cwd: str) -> dict:
    """Lint and test the repository a work-session push targets.

    Returns {"decision": "allow"|"deny", "reason": str} and records the
    outcome as a push_gate event on the work item. Lint runs first and a lint
    failure denies before the test suite runs, so the fix loop stays short.
    A command whose repository cannot be resolved is allowed but recorded,
    because denying on a parse failure would wedge pushes the gate cannot
    even check."""
    push = parse_push(command)
    if push is None:
        return {"decision": "allow", "reason": "not a push"}
    start_dir = os.path.normpath(os.path.join(cwd or ".", push["chdir"] or "."))
    repo = _repo_root(start_dir)
    payload = {"command": command[:300], "repo": repo.name if repo else ""}
    if repo is None:
        payload["note"] = f"no git repository resolved from {start_dir}"
        work_store.record_push_gate(session_id, "skipped", payload)
        return {"decision": "allow", "reason": "no repository resolved"}
    files = _outgoing_files(repo)
    if files is None:
        lint = {"status": "skipped", "exit_code": 0,
                "output": "no remote base to diff against; lint skipped"}
    else:
        lint = git_util.lint_files(repo, files)
    payload["lint"] = {**lint, "output": lint["output"][-_GATE_TAIL:]}
    if lint["status"] not in ("pass", "no_config", "skipped"):
        work_store.record_push_gate(session_id, "fail", payload)
        return {"decision": "deny",
                "reason": _deny_reason(
                    "lint", f"pre-commit ({lint['status']}, exit "
                    f"{lint['exit_code']}):\n{lint['output'][-_GATE_TAIL:]}")}
    tests = _gate_tests(repo)
    payload["tests"] = {**tests, "tail": (tests.get("tail") or "")[-_GATE_TAIL:]}
    if tests["result"] not in ("pass", "no_runner"):
        work_store.record_push_gate(session_id, "fail", payload)
        return {"decision": "deny",
                "reason": _deny_reason(
                    "the test suite", f"{tests.get('cmd') or 'tests'} "
                    f"({tests['result']}, exit {tests.get('exit_code')}):\n"
                    f"{(tests.get('tail') or '')[-_GATE_TAIL:]}")}
    work_store.record_push_gate(session_id, "pass", payload)
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
