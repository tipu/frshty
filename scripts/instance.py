#!/usr/bin/env python3
"""Run one frshty instance in its own container.

    scripts/instance.py build
    scripts/instance.py check config/frshty.toml
    scripts/instance.py up    config/frshty.toml
    scripts/instance.py down  config/frshty.toml
    scripts/instance.py logs  config/frshty.toml
    scripts/instance.py gvoice-login config/personal.toml
    scripts/instance.py reload
    scripts/instance.py gateway-up --port 7130
    scripts/instance.py gateway-down

Every instance runs the same image. The container sees its own workspace at
the host path the config names, the model CLI logins, and one directory of its
own on the host, ~/.frshty-containers/<key>/. Its state/ is mounted at the
same path and named by FRSHTY_ROOT, so every worktree the container registers
in a shared repository names a path the host sees too. Its ssh/ is mounted at
~/.ssh. Its totp/, when present, is mounted read-only at ~/.totp for the
totp command; it holds only the TOTP secrets of that instance's accounts.
The container never sees the host's ~/.ssh, ~/.frshty, or another
instance's workspace or state.

Every instance container gets the host's Docker socket, because the proof
steps run `docker exec`, `docker logs` and `docker compose`. The socket gives
root on the host. ~/.frshty-containers/<key>/ssh/hosts.conf, when present,
adds ssh hosts such as a Mac or Windows box the proof reaches over ssh.

The container runs the code of the main checkout of this repository, mounted
read-only at /app, not the copy baked into the image. `reload` sends SIGHUP
to every instance container, and each restarts frshty.py on the new code while
the tmux sessions of its running agents live on. Rebuild the image only when
the Dockerfile changes.

The container reads its config from ~/.frshty-containers/<key>/config/<key>.toml
and the instance list from ~/.frshty-containers/peers.toml. Both paths outlive
every checkout and worktree. `up` and `check` copy the config they are given to
that path, and refuse when a different file is already there.

The gvoice CLI keeps its Google Voice session in a Chrome profile at
state/gvoice/, named by GVOICE_PROFILE_DIR, or at direct_inbox.gvoice_profile_dir
when the config sets it, which must lie inside state/. `gvoice-login` opens
Google Voice on that profile, in a throwaway container of the same image.
Google refuses a sign-in from a browser that reports automation, as the one
`gvoice login` opens does. Google also drops a session that a differently
launched Chrome made, as plain Chrome does. So `gvoice-login` launches Chrome
through gvoice's own Playwright, without the automation flag and marker. The
Chrome window opens on the host display named by DISPLAY and XAUTHORITY. Close
it when the sign-in is done.

On a macOS host the SQLite database lives on the Docker volume frshty-<key>-db,
not in state/. File locks do not hold across processes on a macOS bind mount,
so a hook process could truncate the WAL index under frshty.py and kill it with
SIGBUS. `up` copies an existing state/frshty.db into the volume once and
renames the original to frshty.db.pre-volume.

An optional [container] block in the instance config tunes the container:

    [container]
    port = 7131          # listen port on the host network; default job.port
    workers = 3          # FRSHTY_WORKER_COUNT
    llm = 9              # FRSHTY_MAX_CONCURRENT_LLM
    memory = "32g"       # RAM cap for the whole container, swap included
    mounts = ["~/Documents/dev/slack_int"]   # extra host paths, same path inside
    seed = ["~/.gitconfig-quill"]            # extra host files copied into ~
    devices = ["/dev/kvm"]                   # host devices passed through
    env_file = "~/.frshty-containers/aimyable.env"  # extra variables, e.g. BILLCOM_*
"""
import argparse
import json
import os
import platform
import subprocess
import sys
import tempfile
import time
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import core.git_util as git_util  # noqa: E402  (needs the path above)
IMAGE = "frshty-instance:latest"
GATEWAY = "frshty-gateway"
HOME = Path.home()
CONTAINERS_ROOT = Path(os.environ.get("FRSHTY_CONTAINERS") or HOME / ".frshty-containers")
SEED_DIR = "/run/frshty/seed"
MODEL_DIRS = [".claude", ".codex", ".gemini"]
SEED_FILES = [".claude.json", ".gitconfig"]
DOCKER_SOCKET = Path("/var/run/docker.sock")
PEERS = CONTAINERS_ROOT / "peers.toml"
GVOICE_PROFILE = "gvoice"
GVOICE_URL = "https://voice.google.com/u/0/messages"
GVOICE_DIR = "/usr/lib/node_modules/google-voice-cli"
GVOICE_LOGIN_JS = f"""
import {{ chromium }} from "playwright-core";
const context = await chromium.launchPersistentContext(process.env.GVOICE_PROFILE_DIR, {{
  channel: process.env.GVOICE_CHROME_CHANNEL || "chrome",
  headless: false,
  viewport: {{ width: 1280, height: 900 }},
  ignoreDefaultArgs: ["--enable-automation"],
  args: ["--disable-blink-features=AutomationControlled"],
}});
const page = context.pages()[0] || await context.newPage();
await page.goto("{GVOICE_URL}", {{ waitUntil: "domcontentloaded" }});
console.log("Sign in to Google Voice, wait for the inbox, then close the window.");
await context.waitForEvent("close", {{ timeout: 0 }});
"""
X11_SOCKETS = Path("/tmp/.X11-unix")
DB_DIR = "/var/lib/frshty"
MEMORY = "32g"
DB_COPY = """
import os, sqlite3, sys
src, dst = sys.argv[1], sys.argv[2]
if os.path.exists(dst):
    sys.exit(f"{dst} already exists in the volume")
part = dst + ".part"
for name in (part, part + "-journal", part + "-wal", part + "-shm"):
    if os.path.exists(name):
        os.remove(name)
s = sqlite3.connect(src)
d = sqlite3.connect(part)
s.backup(d)
check = d.execute("PRAGMA quick_check").fetchone()[0]
d.close()
s.close()
if check != "ok":
    sys.exit(f"quick_check failed: {check}")
os.replace(part, dst)
"""
TMP_MOUNT = "/tmp:rw,exec,nosuid,nodev,size=8g,mode=1777"


def _expand(path: str) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


def code_dir() -> Path:
    """The main checkout of this repository. A worktree is purged when its
    task ages out, so a container never runs code from one."""
    r = git_util.run_git_status(REPO, ["rev-parse", "--path-format=absolute", "--git-common-dir"])
    if r.returncode != 0:
        return REPO
    return Path(r.stdout.strip()).parent


def code_args() -> list[str]:
    """Mount the code read-only at /app. The checkout's own config/ and .env
    hold every instance's secrets, so a tmpfs hides the first and /dev/null
    the second."""
    code = code_dir()
    args = ["-v", f"{code}:/app:ro", "--mount", "type=tmpfs,destination=/app/config",
            "-e", f"FRSHTY_CODE_DIR={code}"]
    if (code / ".env").exists():
        args += ["-v", "/dev/null:/app/.env:ro"]
    return args


def hook_dir() -> str:
    """The checkout the operator's Claude hooks call work_hook.py from.
    ~/.claude is shared with every container, so that path has to exist in
    each of them, and the image links it to its own copy of frshty."""
    try:
        settings = json.loads((HOME / ".claude" / "settings.json").read_text())
    except (OSError, ValueError):
        return str(REPO)
    for entries in (settings.get("hooks") or {}).values():
        for entry in entries:
            for hook in entry.get("hooks") or []:
                for word in str(hook.get("command", "")).split():
                    if word.endswith("/scripts/work_hook.py"):
                        return str(Path(word).parent.parent)
    return str(REPO)


def load(config_path: str) -> dict:
    with open(config_path, "rb") as f:
        return tomllib.load(f)


def container_name(config: dict) -> str:
    return f"frshty-{config['job']['key']}"


def gh_token(config: dict) -> str:
    if config["job"].get("platform") != "github":
        return ""
    account = (config.get("github") or {}).get("account", "")
    cmd = ["gh", "auth", "token"] + (["--user", account] if account else [])
    r = subprocess.run(cmd, capture_output=True, text=True)
    if r.returncode != 0 or not r.stdout.strip():
        raise SystemExit(f"gh has no token for account {account!r}: {r.stderr.strip()}")
    return r.stdout.strip()


def write_env_file(root: Path, config: dict) -> Path:
    path = root / "env"
    token = gh_token(config)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        if token:
            f.write(f"GH_TOKEN={token}\n")
    os.chmod(path, 0o600)
    return path


def install_config(config_path: Path, root: Path, key: str) -> Path:
    """Copy the config to its stable path and return that path. The
    instance edits its own config from the web UI, so a copy that already
    differs from `config_path` is never overwritten."""
    target = root / "config" / f"{key}.toml"
    if config_path.resolve() == target.resolve():
        return target
    data = config_path.read_bytes()
    if target.exists():
        if target.read_bytes() != data:
            raise SystemExit(f"{key}: {target} differs from {config_path}; "
                             f"pass {target} or remove it first")
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(target, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    return target


def mounts(config: dict, config_path: Path, root: Path) -> list[tuple[str, str, bool]]:
    """(host path, container path, read only) for every bind mount."""
    key = config["job"]["key"]
    box = config.get("container") or {}
    out = [(str(root / "state"), str(root / "state"), False),
           (str(root / "ssh"), str(HOME / ".ssh"), False),
           (str(config_path), f"/app/config/{key}.toml", False)]
    if (root / "totp").is_dir():
        out.append((str(root / "totp"), str(HOME / ".totp"), True))
    workspace = _expand(config["workspace"]["root"])
    out.append((str(workspace), str(workspace), False))
    for name in MODEL_DIRS:
        if (HOME / name).is_dir():
            out.append((str(HOME / name), str(HOME / name), False))
    agy = HOME / ".local" / "bin" / "agy"
    if agy.is_file():
        out.append((str(agy), "/usr/local/bin/agy", True))
    if PEERS.is_file():
        out.append((str(PEERS), "/app/config/peers.toml", True))
    claude_dir = ((config.get("llm") or {}).get("claude") or {}).get("config_dir")
    if claude_dir:
        out.append((str(_expand(claude_dir)), str(_expand(claude_dir)), False))
    slack_dir = (config.get("slack") or {}).get("messages_dir")
    if slack_dir and (config.get("features") or {}).get("slack"):
        out.append((str(_expand(slack_dir)), str(_expand(slack_dir)), False))
    for extra in box.get("mounts") or []:
        out.append((str(_expand(extra)), str(_expand(extra)), False))
    for name in SEED_FILES + list(box.get("seed") or []):
        src = _expand(HOME / name if not str(name).startswith(("~", "/")) else name)
        if src.is_file():
            out.append((str(src), f"{SEED_DIR}/{src.name}", True))
    for host, _, _ in out:
        if not Path(host).exists():
            raise SystemExit(f"{key}: mount source {host} does not exist")
    return out


def db_volume(config: dict) -> str | None:
    """The Docker volume that holds the database, or None when the database
    stays in state/. Only a macOS host needs one."""
    if platform.system() != "Darwin":
        return None
    return f"{container_name(config)}-db"


def prepare_db_volume(config: dict, root: Path, move: bool) -> None:
    """Create the volume and give it to the container user. With `move`, also
    move an existing state/frshty.db into it. Move only while no container of
    this instance runs, because the copy needs the database at rest."""
    volume = db_volume(config)
    if not volume:
        return
    subprocess.run(["docker", "volume", "create", volume], check=True, capture_output=True)
    subprocess.run(["docker", "run", "--rm", "--user", "0", "--entrypoint", "chown",
                    "-v", f"{volume}:{DB_DIR}", IMAGE,
                    f"{os.getuid()}:{os.getgid()}", DB_DIR], check=True, capture_output=True)
    legacy = root / "state" / "frshty.db"
    if not move or not legacy.exists():
        return
    r = subprocess.run(["docker", "run", "--rm", "--entrypoint", "python",
                        "-v", f"{volume}:{DB_DIR}", "-v", f"{root / 'state'}:/src",
                        IMAGE, "-c", DB_COPY, "/src/frshty.db", f"{DB_DIR}/frshty.db"],
                       capture_output=True, text=True, check=False)
    if r.returncode != 0:
        raise SystemExit(f"{config['job']['key']}: copy of {legacy} into volume {volume} "
                         f"failed: {r.stderr.strip()}")
    for suffix in ("", "-wal", "-shm"):
        old = root / "state" / f"frshty.db{suffix}"
        if old.exists():
            old.rename(root / "state" / f"frshty.db.pre-volume{suffix}")


def docker_socket_gid() -> str:
    """The group of the Docker socket as a container sees it. On Linux it is
    the host group. Docker Desktop on macOS shows the host user's group on the
    host but root's group inside the container."""
    r = subprocess.run(["docker", "run", "--rm", "--entrypoint", "stat",
                        "-v", f"{DOCKER_SOCKET}:{DOCKER_SOCKET}", IMAGE,
                        "-c", "%g", str(DOCKER_SOCKET)],
                       capture_output=True, text=True, check=False)
    if r.returncode != 0 or not r.stdout.strip().isdigit():
        raise SystemExit(f"stat of {DOCKER_SOCKET} inside {IMAGE} failed: "
                         f"{(r.stderr or r.stdout).strip()}")
    return r.stdout.strip()


def run_args(config: dict, config_path: Path, check: bool) -> list[str]:
    key = config["job"]["key"]
    box = config.get("container") or {}
    root = CONTAINERS_ROOT / key
    for sub in ("state", "ssh"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (root / "ssh").chmod(0o700)
    config_path = install_config(config_path, root, key)
    env_file = write_env_file(root, config)
    port = int(box.get("port") or config["job"]["port"])
    memory = str(box.get("memory") or MEMORY)
    volume = db_volume(config)
    if check and (root / "state" / "frshty.db").exists():
        volume = None
    db_file = f"{DB_DIR}/frshty.db" if volume else str(root / "state" / "frshty.db")
    args = ["docker", "run", "--network", "host", "--init",
            "--label", f"frshty.instance={key}",
            "--env-file", str(env_file),
            "-e", f"HOME={HOME}",
            "-e", f"FRSHTY_ROOT={root / 'state'}",
            "-e", f"FRSHTY_DB={db_file}",
            "-e", f"FRSHTY_BOARD_FILE={root / 'state' / 'board.json'}",
            "-e", f"FRSHTY_BOARD_INSTANCE={key}",
            "-e", f"FRSHTY_PEER_SELF={key}",
            "-e", f"FRSHTY_TIMEZONE={os.environ.get('FRSHTY_TIMEZONE', 'America/Los_Angeles')}",
            "-e", f"FRSHTY_WORKER_COUNT={int(box.get('workers') or 3)}",
            "-e", f"FRSHTY_MAX_CONCURRENT_LLM={int(box.get('llm') or 9)}",
            "-e", f"GVOICE_PROFILE_DIR={root / 'state' / GVOICE_PROFILE}",
            "--memory", memory, "--memory-swap", memory,
            "--tmpfs", TMP_MOUNT]
    if box.get("env_file"):
        args += ["--env-file", str(_expand(box["env_file"]))]
    if DOCKER_SOCKET.exists():
        args += ["-v", f"{DOCKER_SOCKET}:{DOCKER_SOCKET}",
                 "--group-add", docker_socket_gid()]
    for device in box.get("devices") or []:
        if not Path(device).exists():
            raise SystemExit(f"{key}: device {device} does not exist")
        args += ["--device", device, "--group-add", str(Path(device).stat().st_gid)]
    if volume:
        args += ["-v", f"{volume}:{DB_DIR}"]
    args += code_args()
    for host, inside, ro in mounts(config, config_path, root):
        args += ["-v", f"{host}:{inside}" + (":ro" if ro else "")]
    if check:
        args += ["--rm", "--name", f"{container_name(config)}-check", IMAGE,
                 f"/app/config/{key}.toml", "--check"]
    else:
        args += ["-d", "--restart", "unless-stopped", "--name", container_name(config), IMAGE,
                 f"/app/config/{key}.toml", "--port", str(port)]
    return args


def gvoice_login_args(config: dict) -> list[str]:
    """A throwaway container that opens Google Voice on the instance's gvoice
    profile, launched the way gvoice launches it, on the host display."""
    state = CONTAINERS_ROOT / config["job"]["key"] / "state"
    settings = config.get("direct_inbox") or {}
    configured = settings.get("gvoice_profile_dir")
    profile = _expand(configured) if configured else state / GVOICE_PROFILE
    if not profile.is_relative_to(state):
        raise SystemExit(f"gvoice-login: {profile} is outside {state}, which the instance does not mount")
    display = os.environ.get("DISPLAY")
    if not display:
        raise SystemExit("gvoice-login: DISPLAY is not set; run it from the desktop session")
    xauth = Path(os.environ.get("XAUTHORITY") or HOME / ".Xauthority")
    for path in (state, xauth, X11_SOCKETS):
        if not path.exists():
            raise SystemExit(f"gvoice-login: {path} does not exist")
    profile.mkdir(parents=True, exist_ok=True)
    return ["docker", "run", "--rm", "-it", "--network", "host", "--init",
            "--shm-size", "1g",
            "-e", f"HOME={HOME}",
            "-e", f"DISPLAY={display}",
            "-e", "XAUTHORITY=/run/frshty/xauthority",
            "-v", f"{X11_SOCKETS}:{X11_SOCKETS}:ro",
            "-v", f"{xauth}:/run/frshty/xauthority:ro",
            "-e", f"GVOICE_PROFILE_DIR={profile}",
            "-e", f"GVOICE_CHROME_CHANNEL={settings.get('gvoice_chrome_channel') or 'chrome'}",
            "-v", f"{profile}:{profile}",
            "-w", GVOICE_DIR,
            "--entrypoint", "node", IMAGE, "--input-type=module", "-e", GVOICE_LOGIN_JS]


def build() -> int:
    """Build the image from the committed HEAD of this checkout. The shared
    checkout holds other agents' uncommitted edits, and none of them may
    reach the image."""
    cmd = ["docker", "build", "-t", IMAGE,
           "--build-arg", f"HOST_UID={os.getuid()}",
           "--build-arg", f"HOST_GID={os.getgid()}",
           "--build-arg", f"HOST_HOME={HOME}",
           "--build-arg", f"HOOK_DIR={hook_dir()}",
           "-"]
    with tempfile.TemporaryDirectory() as tmp:
        tar = Path(tmp) / "context.tar"
        git_util.run_git(REPO, ["archive", "--format=tar", "-o", str(tar), "HEAD"], timeout=600)
        with open(tar, "rb") as context:
            return subprocess.run(cmd, stdin=context).returncode


def gateway_args(port: int) -> list[str]:
    """The gateway container: no workspace, no credentials, no key. It sees
    only the peers file that names the instances it forwards to."""
    if not PEERS.is_file():
        raise SystemExit(f"{PEERS} does not exist; list the instance containers in it first")
    return ["docker", "run", "-d", "--restart", "unless-stopped", "--name", GATEWAY,
            "--network", "host", "--init", "--entrypoint", "python", *code_args(),
            "-v", f"{PEERS}:/app/config/peers.toml:ro",
            IMAGE, "/app/gateway.py", "--port", str(port)]


def instance_containers() -> list[str]:
    r = subprocess.run(["docker", "ps", "--filter", "label=frshty.instance",
                        "--format", "{{.Names}}"], capture_output=True, text=True, check=True)
    return sorted(r.stdout.split())


def app_pid(name: str) -> str:
    r = subprocess.run(["docker", "exec", name, "pgrep", "-o", "-f", "/app/frshty.py"],
                       capture_output=True, text=True)
    return r.stdout.strip()


def reload(timeout: float = 120) -> int:
    """SIGHUP every instance container and restart the gateway when the host
    runs one. Fails unless each instance runs a new frshty.py process
    afterwards, or a gateway that exists does not restart. The signal goes
    through `docker exec kill`, because `docker kill` marks the container as
    manually stopped and Docker then skips it at the next daemon start."""
    names = instance_containers()
    if not names:
        print("reload: no instance container is running", file=sys.stderr)
        return 1
    before = {name: app_pid(name) for name in names}
    for name in names:
        subprocess.run(["docker", "exec", name, "kill", "-HUP", "1"], check=True, capture_output=True)
    failed = []
    for name in names:
        deadline = time.monotonic() + timeout
        while True:
            pid = app_pid(name)
            if pid and pid != before[name]:
                print(f"reload: {name} frshty.py {before[name] or '-'} -> {pid}")
                break
            if time.monotonic() > deadline:
                failed.append(name)
                break
            time.sleep(1)
    gateway = subprocess.run(["docker", "ps", "-aq", "--filter", f"name=^{GATEWAY}$"],
                             capture_output=True, text=True, check=True).stdout.strip()
    if gateway and subprocess.run(["docker", "restart", GATEWAY], capture_output=True).returncode != 0:
        failed.append(GATEWAY)
    if failed:
        print(f"reload: {', '.join(failed)} did not restart", file=sys.stderr)
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="instance.py")
    parser.add_argument("action", choices=["build", "check", "up", "down", "logs", "reload",
                                           "gateway-up", "gateway-down", "gvoice-login"])
    parser.add_argument("config", nargs="?")
    parser.add_argument("--port", type=int, default=7130, help="gateway listen port")
    args = parser.parse_args(argv)
    if args.action == "build":
        return build()
    if args.action == "reload":
        return reload()
    if args.action == "gateway-up":
        subprocess.run(["docker", "rm", "-f", GATEWAY], capture_output=True)
        return subprocess.run(gateway_args(args.port)).returncode
    if args.action == "gateway-down":
        return subprocess.run(["docker", "rm", "-f", GATEWAY]).returncode
    if not args.config:
        parser.error(f"{args.action} needs a config path")
    config_path = Path(args.config).resolve()
    config = load(str(config_path))
    name = container_name(config)
    if args.action == "check":
        cmd = run_args(config, config_path, check=True)
        if f"{db_volume(config)}:{DB_DIR}" in cmd:
            prepare_db_volume(config, CONTAINERS_ROOT / config["job"]["key"], move=False)
        return subprocess.run(cmd).returncode
    if args.action == "up":
        cmd = run_args(config, config_path, check=False)
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        prepare_db_volume(config, CONTAINERS_ROOT / config["job"]["key"], move=True)
        return subprocess.run(cmd).returncode
    if args.action == "gvoice-login":
        return subprocess.run(gvoice_login_args(config)).returncode
    if args.action == "down":
        return subprocess.run(["docker", "rm", "-f", name]).returncode
    return subprocess.run(["docker", "logs", "--tail", "200", name]).returncode


if __name__ == "__main__":
    sys.exit(main())
