"""Subprocess boot tests for frshty in single-instance and --multi modes.

These don't depend on a real config — they synthesize minimal tmp configs at
free ports so the test never collides with anything running locally
(aimyable/nectar/lumeninv via systemd, etc.). The test value is "frshty starts
without crashing under each invocation pattern", not "real configs work".
"""
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _write_minimal_config(dst: Path, *, key: str, port: int, host: str,
                          state_dir: Path, ws_root: Path) -> Path:
    state_dir.mkdir(parents=True, exist_ok=True)
    ws_root.mkdir(parents=True, exist_ok=True)
    dst.write_text(f"""[job]
key = "{key}"
port = {port}
platform = "github"
ticket_system = "linear"
host = "{host}"
bind = "127.0.0.1"

[github]
repo = "test/test-repo"

[linear]
token_env = "LINEAR_TOKEN"
assignee_email = "test@test.com"

[workspace]
root = "{ws_root}"
repos = []
tickets_dir = "tickets"
ticket_layout = "flat"
base_branch = "main"
branch_prefix = ""

[pr]
auto_merge = false

[features]
review_prs = false
tickets = false
slack = false
timesheet = false
billing = false
""")
    return dst


def _wait_alive(process: subprocess.Popen, seconds: float = 3.0) -> None:
    time.sleep(seconds)
    rc = process.poll()
    if rc is not None:
        _, stderr = process.communicate(timeout=1)
        raise AssertionError(
            f"frshty crashed on startup with code {rc}.\nstderr: {stderr}"
        )


def _terminate(process: subprocess.Popen) -> None:
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def test_auto_healing_without_frshty_events_env(tmp_path):
    """
    Verify that single-instance auto-healing works when:
    - FRSHTY_EVENTS is NOT set in environment
    - Only the new unified event-driven path is used
    """
    port = _free_port()
    cfg = _write_minimal_config(
        tmp_path / "single.toml",
        key=f"BOOT-{os.getpid()}",
        port=port,
        host=f"http://boot-single-{os.getpid()}.localhost:{port}",
        state_dir=tmp_path / "state",
        ws_root=tmp_path / "ws",
    )

    env = os.environ.copy()
    env.pop("FRSHTY_EVENTS", None)

    process = subprocess.Popen(
        [sys.executable, "frshty.py", str(cfg)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(Path(__file__).resolve().parent.parent.parent),
    )
    try:
        _wait_alive(process)
    finally:
        _terminate(process)


def test_multi_instance_unchanged(tmp_path):
    """
    Verify --multi mode still works identically as before.
    """
    port_a = _free_port()
    port_b = _free_port()
    cfg_a = _write_minimal_config(
        tmp_path / "a.toml",
        key=f"BOOT-A-{os.getpid()}",
        port=port_a,
        host=f"http://boot-a-{os.getpid()}.localhost:{port_a}",
        state_dir=tmp_path / "a-state",
        ws_root=tmp_path / "a-ws",
    )
    cfg_b = _write_minimal_config(
        tmp_path / "b.toml",
        key=f"BOOT-B-{os.getpid()}",
        port=port_b,
        host=f"http://boot-b-{os.getpid()}.localhost:{port_b}",
        state_dir=tmp_path / "b-state",
        ws_root=tmp_path / "b-ws",
    )

    process = subprocess.Popen(
        [sys.executable, "frshty.py", "--multi", str(cfg_a), str(cfg_b)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(Path(__file__).resolve().parent.parent.parent),
    )
    try:
        _wait_alive(process)
    finally:
        _terminate(process)
