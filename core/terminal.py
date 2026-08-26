import os
import re
import shlex
import shutil
import pty
import json
import struct
import asyncio
import fcntl
import termios
import signal
import subprocess
from starlette.websockets import WebSocket, WebSocketDisconnect

import core.state as state

MAX_SCROLLBACK = 1024 * 1024
TMUX_SOCKET = os.path.expanduser("~/.frshty-tmux")
LAUNCH_CONTEXT_DIR = os.path.expanduser("~/.frshty/launch")
def _tmux_bin():
    return shutil.which("tmux") or "tmux"

_terminals: dict[str, dict] = {}


AGENTS = ("claude", "codex")


def _env_prefix(env: dict) -> str:
    return "".join(
        f"{k}={shlex.quote(os.path.expanduser(v))} " for k, v in sorted(env.items())
    )


def claude_cmd(config: dict | None = None) -> str:
    """Interactive claude command line for one instance's tmux pane.

    Mirrors core.llm.ClaudeProvider: same bin, same env overrides, same
    CLAUDE_CONFIG_DIR. Without this a pane authenticates as the operator's
    default account instead of the account the instance is configured with,
    because _child_env() drops every variable it does not whitelist."""
    claude_cfg = ((config or {}).get("llm") or {}).get("claude") or {}
    env = {str(k): str(v) for k, v in (claude_cfg.get("env") or {}).items()}
    config_dir = claude_cfg.get("config_dir")
    if config_dir and "CLAUDE_CONFIG_DIR" not in env:
        env["CLAUDE_CONFIG_DIR"] = str(config_dir)
    bin_name = claude_cfg.get("bin", "claude")
    return f"{_env_prefix(env)}{bin_name} --dangerously-skip-permissions"


def codex_cmd(config: dict | None = None, subcommand: str = "") -> str:
    """Interactive codex command line for one instance's tmux pane.

    Same shape as claude_cmd: the instance's env overrides and CODEX_HOME are
    prepended because _child_env() drops every variable it does not
    whitelist, so without them the pane authenticates as the operator's
    default codex account. The subcommand goes before the flags because codex
    parses them per subcommand."""
    codex_cfg = ((config or {}).get("llm") or {}).get("codex") or {}
    env = {str(k): str(v) for k, v in (codex_cfg.get("env") or {}).items()}
    config_dir = codex_cfg.get("config_dir")
    if config_dir and "CODEX_HOME" not in env:
        env["CODEX_HOME"] = str(config_dir)
    bin_name = codex_cfg.get("bin", "codex")
    head = f"{_env_prefix(env)}{bin_name}"
    if subcommand:
        head = f"{head} {subcommand}"
    return f"{head} --dangerously-bypass-approvals-and-sandbox"


CODEX_NOTIFY_SCRIPT = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "scripts", "codex_notify.py")


def _codex_notify_flag(session_uuid: str) -> str:
    argv = json.dumps(["python3", CODEX_NOTIFY_SCRIPT, session_uuid])
    return f"-c {shlex.quote('notify=' + argv)}"


def _tmux_session_name(ticket_key: str) -> str:
    return f"term-{ticket_key}"


def _tmux_session_exists(session_name: str) -> bool:
    result = subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "has-session", "-t", session_name],
        capture_output=True,
    )
    return result.returncode == 0


def _process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def session_healthy(ticket_key: str, agent: str = "claude") -> dict:
    """Liveness of one pane: the tmux session exists, and the agent CLI
    (claude or codex) runs as a child of the pane."""
    session_name = _tmux_session_name(ticket_key)
    if not _tmux_session_exists(session_name):
        return {"alive": False, "agent_running": False}

    result = subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "list-panes", "-t", session_name, "-F", "#{pane_pid}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return {"alive": True, "agent_running": False}

    pane_pid = result.stdout.strip().splitlines()[0]
    agent_check = subprocess.run(
        ["pgrep", "-P", pane_pid, "-f", agent if agent in AGENTS else "claude"],
        capture_output=True, text=True,
    )
    return {"alive": True, "agent_running": bool(agent_check.stdout.strip())}


def list_sessions() -> list[dict]:
    """Every session on the frshty tmux socket with its last-activity epoch."""
    result = subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "list-sessions", "-F",
         "#{session_name} #{session_activity}"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        return []
    sessions = []
    for line in result.stdout.splitlines():
        name, _, activity = line.rpartition(" ")
        if not name or not activity.isdigit():
            continue
        sessions.append({"name": name, "activity": int(activity)})
    return sessions


def _resolve_cwd(config: dict, ticket_key: str) -> str | None:
    tickets = state.load("tickets")
    ts = tickets.get(ticket_key)
    if not ts:
        return None
    slug = ts.get("slug", "")
    if not slug:
        return None
    ticket_dir = config["workspace"]["root"] / config["workspace"]["tickets_dir"] / slug
    if ticket_dir.is_dir():
        return str(ticket_dir)
    return None


def _child_env():
    return {
        "HOME": os.path.expanduser("~"),
        "USER": os.environ.get("USER", "claude"),
        "TERM": "xterm-256color",
        "PATH": os.environ.get("PATH", "/usr/local/bin:/usr/bin:/bin"),
        "LANG": os.environ.get("LANG", "C.UTF-8"),
        "TMUX": "",
    }


def ensure_session(ticket_key: str, cwd: str):
    session_name = _tmux_session_name(ticket_key)
    if _tmux_session_exists(session_name):
        return session_name
    subprocess.run(
        [
            _tmux_bin(), "-S", TMUX_SOCKET, "new-session", "-d", "-s", session_name,
            "-c", cwd, "-x", "80", "-y", "24",
        ],
        env=_child_env(), capture_output=True,
    )
    return session_name


def send_keys(ticket_key: str, keys: str):
    session_name = _tmux_session_name(ticket_key)
    if not _tmux_session_exists(session_name):
        return
    subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "send-keys", "-t", session_name, keys, "Enter"],
        capture_output=True,
    )


def pane_text(ticket_key: str) -> str:
    """What the pane currently shows, or "" when the tmux session is gone."""
    session_name = _tmux_session_name(ticket_key)
    if not _tmux_session_exists(session_name):
        return ""
    out = subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "capture-pane", "-t", session_name, "-p"],
        capture_output=True, text=True,
    )
    return out.stdout if out.returncode == 0 else ""


CODEX_TRUST_PROMPT = "Do you trust the contents of this directory?"


def answer_codex_trust(ticket_key: str) -> bool:
    """Accept the codex directory-trust question when the pane shows it.

    Codex asks it the first time it opens a directory, before it reads the
    prompt it was given, so a work run in a directory codex has not seen sits
    on the question forever: the process is up, no turn ever starts, and no
    notification reaches the board. The answer is yes because the launcher
    already runs codex with approvals bypassed and the operator chose the
    directory. `Yes, continue` is preselected, so Enter takes it. Codex writes
    the answer to its own config, so the question comes once per directory.
    A `-c projects...trust_level` override does not work: codex reads trust
    only from the config file on disk."""
    if CODEX_TRUST_PROMPT not in pane_text(ticket_key):
        return False
    subprocess.run(
        [_tmux_bin(), "-S", TMUX_SOCKET, "send-keys", "-t",
         _tmux_session_name(ticket_key), "Enter"],
        capture_output=True,
    )
    return True


def launch_claude(key: str, cwd: str, session_uuid: str, context: str, first_run: bool,
                  config: dict | None = None):
    """Start (or resume) a Claude conversation in the `key` tmux session.

    First launch pins a deterministic --session-id and seeds context via
    --append-system-prompt; later launches resume the same id so closing the
    browser (or the process dying) returns to the same conversation. No-op if
    Claude is already running in the pane — the websocket just reattaches."""
    ensure_session(key, cwd)
    if session_healthy(key).get("agent_running"):
        return
    if first_run:
        os.makedirs(LAUNCH_CONTEXT_DIR, exist_ok=True)
        ctx_path = os.path.join(LAUNCH_CONTEXT_DIR, f"{session_uuid}.md")
        with open(ctx_path, "w") as f:
            f.write(context or "")
        cmd = (
            f"{claude_cmd(config)} --session-id {shlex.quote(session_uuid)} "
            f"--append-system-prompt \"$(cat {shlex.quote(ctx_path)})\""
        )
    else:
        cmd = f"{claude_cmd(config)} --resume {shlex.quote(session_uuid)}"
    send_keys(key, cmd)


def launch_codex(key: str, cwd: str, session_uuid: str, context: str, first_run: bool,
                 config: dict | None = None, agent_session_id: str = ""):
    """Start (or resume) a Codex conversation in the `key` tmux session.

    Codex has no system-prompt flag, so the seeded context is the first
    prompt. Codex also mints its own thread id, so the work session id
    travels as an argument to the notify program: every turn-complete
    notification carries it back and the board attributes the turn to this
    run. A resume needs the codex thread id recorded from the first
    notification; without one the pane continues the newest codex session in
    this directory."""
    ensure_session(key, cwd)
    if session_healthy(key, agent="codex").get("agent_running"):
        return
    notify = _codex_notify_flag(session_uuid)
    if first_run:
        os.makedirs(LAUNCH_CONTEXT_DIR, exist_ok=True)
        ctx_path = os.path.join(LAUNCH_CONTEXT_DIR, f"{session_uuid}.md")
        with open(ctx_path, "w") as f:
            f.write(context or "")
        cmd = f"{codex_cmd(config)} {notify} \"$(cat {shlex.quote(ctx_path)})\""
    else:
        target = shlex.quote(agent_session_id) if agent_session_id else "--last"
        cmd = f"{codex_cmd(config, 'resume')} {notify} {target}"
    send_keys(key, cmd)


def launch_agent(key: str, cwd: str, session_uuid: str, context: str, first_run: bool,
                 config: dict | None = None, agent: str = "claude",
                 agent_session_id: str = ""):
    """Start (or resume) the pane's agent CLI, claude or codex."""
    if agent == "codex":
        launch_codex(key, cwd, session_uuid, context, first_run, config=config,
                     agent_session_id=agent_session_id)
        return
    launch_claude(key, cwd, session_uuid, context, first_run, config=config)


def _get_or_spawn(ticket_key: str, cwd: str):
    entry = _terminals.get(ticket_key)
    if entry and _process_alive(entry["pid"]):
        return entry

    session_name = _tmux_session_name(ticket_key)
    env = _child_env()

    if not _tmux_session_exists(session_name):
        subprocess.run(
            [
                _tmux_bin(), "-S", TMUX_SOCKET, "new-session", "-d", "-s", session_name,
                "-c", cwd, "-x", "80", "-y", "24",
            ],
            env=env, capture_output=True,
        )

    pid, fd = pty.fork()
    if pid == 0:
        try:
            os.chdir(cwd)
            os.execve(
                _tmux_bin(),
                [_tmux_bin(), "-S", TMUX_SOCKET, "attach-session", "-t", session_name],
                env,
            )
        except Exception as e:
            import sys
            print(f"child exec failed: {e}", file=sys.stderr)
            os._exit(1)

    entry = {"pid": pid, "fd": fd, "scrollback": bytearray(), "readers": set(), "session": session_name}
    _terminals[ticket_key] = entry

    asyncio.get_event_loop().create_task(_background_reader(ticket_key))
    return entry


async def _background_reader(ticket_key: str):
    loop = asyncio.get_event_loop()
    entry = _terminals.get(ticket_key)
    if not entry:
        return
    fd = entry["fd"]

    while True:
        try:
            data = await loop.run_in_executor(None, os.read, fd, 4096)
        except OSError:
            break
        if not data:
            break

        buf = entry["scrollback"]
        buf.extend(data)
        if len(buf) > MAX_SCROLLBACK:
            del buf[: len(buf) - MAX_SCROLLBACK]

        dead = set()
        for ws in entry["readers"]:
            try:
                await ws.send_bytes(data)
            except Exception:
                dead.add(ws)
        entry["readers"] -= dead


def kill_terminal(ticket_key: str):
    entry = _terminals.pop(ticket_key, None)
    session_name = _tmux_session_name(ticket_key)

    if entry:
        pid = entry["pid"]
        fd = entry["fd"]
        try:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)
        except OSError:
            pass
        try:
            os.close(fd)
        except OSError:
            pass

    if _tmux_session_exists(session_name):
        subprocess.run([_tmux_bin(), "-S", TMUX_SOCKET, "kill-session", "-t", session_name], capture_output=True)


async def terminal_handler(websocket: WebSocket, ticket_key: str, config: dict):
    entry = _terminals.get(ticket_key)
    if not entry or not _process_alive(entry["pid"]):
        cwd = _resolve_cwd(config, ticket_key)
        if not cwd:
            session_name = _tmux_session_name(ticket_key)
            if _tmux_session_exists(session_name):
                result = subprocess.run(
                    [_tmux_bin(), "-S", TMUX_SOCKET, "display-message", "-t", session_name, "-p", "#{pane_current_path}"],
                    capture_output=True, text=True, timeout=5,
                )
                cwd = result.stdout.strip() if result.returncode == 0 and result.stdout.strip() else str(config["workspace"]["root"])
            else:
                await websocket.close(code=1008)
                return
        entry = _get_or_spawn(ticket_key, cwd)

    await websocket.accept()
    fd = entry["fd"]

    if entry["scrollback"]:
        clean = re.sub(rb'\x1b\[\?[0-9;]*c', b'', bytes(entry["scrollback"]))
        await websocket.send_bytes(clean)

    entry["readers"].add(websocket)

    try:
        while True:
            try:
                message = await websocket.receive()
            except WebSocketDisconnect:
                break

            if message.get("type") == "websocket.disconnect":
                break

            if "text" in message:
                text = message["text"]
                try:
                    msg = json.loads(text)
                    if isinstance(msg, dict) and msg.get("type") == "resize":
                        cols = msg.get("cols", 80)
                        rows = msg.get("rows", 24)
                        winsize = struct.pack("HHHH", rows, cols, 0, 0)
                        fcntl.ioctl(fd, termios.TIOCSWINSZ, winsize)
                        continue
                except (json.JSONDecodeError, ValueError):
                    pass
                try:
                    os.write(fd, text.encode())
                except OSError:
                    break
            elif "bytes" in message:
                try:
                    os.write(fd, message["bytes"])
                except OSError:
                    break
    finally:
        entry["readers"].discard(websocket)
