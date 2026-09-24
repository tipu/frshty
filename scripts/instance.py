#!/usr/bin/env python3
"""Run one frshty instance in its own container.

    scripts/instance.py build
    scripts/instance.py check config/frshty.toml
    scripts/instance.py up    config/frshty.toml
    scripts/instance.py down  config/frshty.toml
    scripts/instance.py logs  config/frshty.toml

Every instance runs the same image. The container sees its own workspace at
the host path the config names, the model CLI logins, and one directory of its
own on the host, ~/.frshty-containers/<key>/. Its state/ is mounted at the
same path and named by FRSHTY_ROOT, so every worktree the container registers
in a shared repository names a path the host sees too. Its ssh/ is mounted at
~/.ssh. The container never sees the host's ~/.ssh, ~/.frshty, or another
instance's workspace or state.

An optional [container] block in the instance config tunes the container:

    [container]
    port = 7131          # listen port on the host network; default job.port
    workers = 3          # FRSHTY_WORKER_COUNT
    llm = 9              # FRSHTY_MAX_CONCURRENT_LLM
    mounts = ["~/Documents/dev/slack_int"]   # extra host paths, same path inside
    seed = ["~/.gitconfig-quill"]            # extra host files copied into ~
    env_file = "~/.frshty-containers/aimyable.env"  # extra variables, e.g. BILLCOM_*
"""
import argparse
import json
import os
import subprocess
import sys
import tomllib
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
IMAGE = "frshty-instance:latest"
HOME = Path.home()
CONTAINERS_ROOT = Path(os.environ.get("FRSHTY_CONTAINERS") or HOME / ".frshty-containers")
SEED_DIR = "/run/frshty/seed"
MODEL_DIRS = [".claude", ".codex", ".gemini"]
SEED_FILES = [".claude.json", ".gitconfig"]


def _expand(path: str) -> Path:
    return Path(os.path.abspath(os.path.expanduser(str(path))))


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


def mounts(config: dict, config_path: Path, root: Path) -> list[tuple[str, str, bool]]:
    """(host path, container path, read only) for every bind mount."""
    key = config["job"]["key"]
    box = config.get("container") or {}
    out = [(str(root / "state"), str(root / "state"), False),
           (str(root / "ssh"), str(HOME / ".ssh"), False),
           (str(config_path), f"/app/config/{key}.toml", False)]
    workspace = _expand(config["workspace"]["root"])
    out.append((str(workspace), str(workspace), False))
    for name in MODEL_DIRS:
        if (HOME / name).is_dir():
            out.append((str(HOME / name), str(HOME / name), False))
    agy = HOME / ".local" / "bin" / "agy"
    if agy.is_file():
        out.append((str(agy), "/usr/local/bin/agy", True))
    peers = REPO / "config" / "peers.toml"
    if peers.is_file():
        out.append((str(peers), "/app/config/peers.toml", True))
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


def run_args(config: dict, config_path: Path, check: bool) -> list[str]:
    key = config["job"]["key"]
    box = config.get("container") or {}
    root = CONTAINERS_ROOT / key
    for sub in ("state", "ssh"):
        (root / sub).mkdir(parents=True, exist_ok=True)
    (root / "ssh").chmod(0o700)
    env_file = write_env_file(root, config)
    port = int(box.get("port") or config["job"]["port"])
    args = ["docker", "run", "--network", "host", "--init",
            "--env-file", str(env_file),
            "-e", f"HOME={HOME}",
            "-e", f"FRSHTY_ROOT={root / 'state'}",
            "-e", f"FRSHTY_DB={root / 'state' / 'frshty.db'}",
            "-e", f"FRSHTY_BOARD_FILE={root / 'state' / 'board.json'}",
            "-e", f"FRSHTY_BOARD_INSTANCE={key}",
            "-e", f"FRSHTY_PEER_SELF={key}",
            "-e", f"FRSHTY_TIMEZONE={os.environ.get('FRSHTY_TIMEZONE', 'America/Los_Angeles')}",
            "-e", f"FRSHTY_WORKER_COUNT={int(box.get('workers') or 3)}",
            "-e", f"FRSHTY_MAX_CONCURRENT_LLM={int(box.get('llm') or 9)}"]
    if box.get("env_file"):
        args += ["--env-file", str(_expand(box["env_file"]))]
    for host, inside, ro in mounts(config, config_path, root):
        args += ["-v", f"{host}:{inside}" + (":ro" if ro else "")]
    if check:
        args += ["--rm", "--name", f"{container_name(config)}-check", IMAGE,
                 f"/app/config/{key}.toml", "--check"]
    else:
        args += ["-d", "--restart", "unless-stopped", "--name", container_name(config), IMAGE,
                 f"/app/config/{key}.toml", "--port", str(port)]
    return args


def build() -> int:
    cmd = ["docker", "build", "-t", IMAGE,
           "--build-arg", f"HOST_UID={os.getuid()}",
           "--build-arg", f"HOST_GID={os.getgid()}",
           "--build-arg", f"HOST_HOME={HOME}",
           "--build-arg", f"HOOK_DIR={hook_dir()}",
           str(REPO)]
    return subprocess.run(cmd).returncode


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="instance.py")
    parser.add_argument("action", choices=["build", "check", "up", "down", "logs"])
    parser.add_argument("config", nargs="?")
    args = parser.parse_args(argv)
    if args.action == "build":
        return build()
    if not args.config:
        parser.error(f"{args.action} needs a config path")
    config_path = Path(args.config).resolve()
    config = load(str(config_path))
    name = container_name(config)
    if args.action == "check":
        return subprocess.run(run_args(config, config_path, check=True)).returncode
    if args.action == "up":
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        return subprocess.run(run_args(config, config_path, check=False)).returncode
    if args.action == "down":
        return subprocess.run(["docker", "rm", "-f", name]).returncode
    return subprocess.run(["docker", "logs", "--tail", "200", name]).returncode


if __name__ == "__main__":
    sys.exit(main())
