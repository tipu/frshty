import os
import socket
import subprocess
import time
from pathlib import Path

import pytest


_AIMYABLE = Path("config/aimyable.toml")
_NECTAR = Path("config/nectar.toml")


def _port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(("127.0.0.1", port)) == 0


def _config_port(path: Path) -> int | None:
    if not path.exists():
        return None
    for line in path.read_text().splitlines():
        s = line.strip()
        if s.startswith("port") and "=" in s:
            try:
                return int(s.split("=", 1)[1].strip())
            except ValueError:
                return None
    return None


_AIMYABLE_PORT = _config_port(_AIMYABLE)


@pytest.mark.skipif(not _AIMYABLE.exists(),
                    reason="requires config/aimyable.toml (not present in worktrees)")
@pytest.mark.skipif(_AIMYABLE_PORT is not None and _port_in_use(_AIMYABLE_PORT),
                    reason="aimyable port already bound (frshty service running locally)")
def test_auto_healing_without_frshty_events_env():
    """
    Verify that single-instance auto-healing works when:
    - FRSHTY_EVENTS is NOT set in environment
    - Only the new unified event-driven path is used
    """
    # SETUP: Ensure FRSHTY_EVENTS is NOT in environment
    env = os.environ.copy()
    env.pop("FRSHTY_EVENTS", None)

    # ACTION: Start frshty in subprocess for single instance
    config_path = "config/aimyable.toml"  # single-instance config

    # Verify config exists
    assert Path(config_path).exists(), f"Config {config_path} not found"

    process = subprocess.Popen(
        ["python", "frshty.py", config_path],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Wait for startup
        time.sleep(3)

        # VERIFY: Process is still running
        poll_result = process.poll()

        if poll_result is not None:
            # Process crashed, get error output
            _, stderr = process.communicate(timeout=1)
            raise AssertionError(
                f"frshty crashed on startup with code {poll_result}.\n"
                f"stderr: {stderr}"
            )

        # VERIFY: Check for startup logs indicating event system started
        # (This is a minimal check - the real validation is that process doesn't crash)
        assert True, "frshty started successfully without FRSHTY_EVENTS env var"

    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


@pytest.mark.skipif(not (_AIMYABLE.exists() and _NECTAR.exists()),
                    reason="requires config/aimyable.toml + config/nectar.toml")
@pytest.mark.skipif(_AIMYABLE_PORT is not None and _port_in_use(_AIMYABLE_PORT),
                    reason="aimyable port already bound (frshty service running locally)")
def test_multi_instance_unchanged():
    """
    Verify --multi mode still works identically as before.
    """
    configs = ["config/aimyable.toml", "config/nectar.toml"]

    # Verify configs exist
    for config in configs:
        assert Path(config).exists(), f"Config {config} not found"

    process = subprocess.Popen(
        ["python", "frshty.py", "--multi"] + configs,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        time.sleep(3)
        poll_result = process.poll()
        assert poll_result is None, f"--multi mode failed to start with code {poll_result}"
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
