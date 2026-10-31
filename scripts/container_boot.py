"""Entrypoint of an instance container.

Logs gh in with the instance's GH_TOKEN, makes sure the instance's own SSH key
exists and is on its GitHub or Bitbucket account, proves git can reach every
repository, and then runs frshty.py as its child. SIGHUP restarts the child
and keeps the container, so a deploy loads new code while the tmux sessions
of running agents live on. The child first drains: it stops claiming jobs
and exits once its live jobs finish or core.drain.DRAIN_SECONDS pass. With
--check it stops after the proof and exits 0, so a container can be tested
next to a live instance without running a second copy of its pipeline.
"""
import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core.config as cfg  # noqa: E402  (needs the path above)
import core.drain as drain  # noqa: E402
import core.git_util as git_util  # noqa: E402
import core.ssh_keys as ssh_keys  # noqa: E402

APP = Path(__file__).resolve().parent.parent / "frshty.py"
SEED_DIR = Path("/run/frshty/seed")
DRAIN_GRACE = 60


def copy_seed_files(seed_dir: Path, home: Path) -> list[str]:
    """Copy the host files mounted read-only in `seed_dir` into the home
    directory. A single-file bind mount cannot be replaced by rename, and the
    CLIs rewrite files such as ~/.claude.json by writing a temporary file and
    renaming it over the old one, so each container works on its own copy.
    A seed can hold a secret such as ~/.netrc, so each copy is readable by
    the owner only, whatever mode the umask would give it."""
    copied = []
    for path in sorted(seed_dir.iterdir()) if seed_dir.is_dir() else []:
        if path.is_file():
            shutil.copyfile(path, home / path.name)
            os.chmod(home / path.name, 0o600)
            copied.append(path.name)
    return copied


def gh_login(token: str) -> None:
    """Store the token as a gh login, so `gh auth token --user <account>`
    answers inside the container exactly as it does on the host. gh refuses
    to store a login while GH_TOKEN is set, so the child runs without it."""
    env = {k: v for k, v in os.environ.items() if k != "GH_TOKEN"}
    r = subprocess.run(["gh", "auth", "login", "--hostname", "github.com",
                        "--git-protocol", "ssh", "--with-token"],
                       input=token, capture_output=True, text=True, env=env)
    if r.returncode != 0:
        raise ssh_keys.KeyBootstrapError(f"gh auth login failed: {r.stderr.strip()[:300]}")


def supervise(cmd: list[str], drain_timeout: float = drain.DRAIN_SECONDS + DRAIN_GRACE,
              kill_grace: float = drain.EXIT_GRACE) -> int:
    """Run `cmd` until it exits on its own. SIGHUP asks it to drain with
    DRAIN_SIGNAL and starts it again once it exits. A child still running
    drain_timeout seconds after the SIGHUP gets SIGTERM, and SIGKILL when it
    is still running kill_grace seconds after that. SIGTERM and SIGINT stop
    it and end the container."""
    restart = stopping = False
    hup_at: float | None = None
    child: subprocess.Popen | None = None

    def on_hup(signum, frame):
        nonlocal restart, hup_at
        if restart:
            return
        restart = True
        hup_at = time.monotonic()
        if child is not None:
            child.send_signal(drain.DRAIN_SIGNAL)

    def on_stop(signum, frame):
        nonlocal stopping
        stopping = True
        if child is not None:
            child.terminate()

    signal.signal(signal.SIGHUP, on_hup)
    signal.signal(signal.SIGTERM, on_stop)
    signal.signal(signal.SIGINT, on_stop)
    while True:
        restart = False
        hup_at = None
        child = subprocess.Popen(cmd)
        forced_at: float | None = None
        while True:
            try:
                code = child.wait(timeout=1)
                break
            except subprocess.TimeoutExpired:
                if hup_at is not None and forced_at is None and time.monotonic() - hup_at > drain_timeout:
                    child.terminate()
                    forced_at = time.monotonic()
                elif forced_at is not None and time.monotonic() - forced_at > kill_grace:
                    child.kill()
        if stopping or not restart:
            return code
        print(json.dumps({"restart": "SIGHUP", "exit": code}), flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="container_boot.py")
    parser.add_argument("config")
    parser.add_argument("--check", action="store_true",
                        help="prove the key and git access, then exit")
    args, app_args = parser.parse_known_args(argv)
    config = cfg.load_config(args.config)
    copy_seed_files(SEED_DIR, Path.home())
    git_util.run_git(Path.home(), ["config", "--global", "gc.worktreePruneExpire", "never"])
    git_util.run_git(Path.home(), ["config", "--global", "--replace-all", "safe.directory", "*"])
    token = os.environ.pop("GH_TOKEN", "")
    try:
        if token:
            gh_login(token)
        result = ssh_keys.bootstrap(config, Path.home() / ".ssh", token)
    except ssh_keys.KeyBootstrapError as e:
        print(json.dumps({"instance": config["job"]["key"], "error": str(e)}), flush=True)
        return 1
    print(json.dumps(result), flush=True)
    if args.check:
        return 0
    os.chdir(APP.parent)
    return supervise([sys.executable, str(APP), args.config, *app_args])


if __name__ == "__main__":
    sys.exit(main())
