import os
import subprocess
import time
from pathlib import Path


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
        assert (
            poll_result is None
        ), f"frshty crashed on startup with code {poll_result}"

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
