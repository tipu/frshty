import importlib.util
import subprocess
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "instance_script", Path(__file__).resolve().parent.parent / "scripts" / "instance.py")
instance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(instance)


def fake_docker(gateway_exists: bool, gateway_restarts: bool):
    pids = iter(["100", "200"])
    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        out, code = "", 0
        if cmd[:2] == ["docker", "ps"] and "label=frshty.instance" in cmd:
            out = "frshty-frshty\n"
        elif cmd[:2] == ["docker", "ps"]:
            out = "abc123\n" if gateway_exists else ""
        elif cmd[:3] == ["docker", "exec", "frshty-frshty"] and "pgrep" in cmd:
            out = next(pids, "200")
        elif cmd[:2] == ["docker", "restart"]:
            code = 0 if gateway_restarts else 1
        return subprocess.CompletedProcess(cmd, code, stdout=out, stderr="")

    return run, calls


@pytest.mark.parametrize("exists,restarts,expected,restarted", [
    (False, False, 0, False),
    (True, True, 0, True),
    (True, False, 1, True),
])
def test_reload_restarts_the_gateway_only_when_it_exists(monkeypatch, exists, restarts, expected, restarted):
    run, calls = fake_docker(exists, restarts)
    monkeypatch.setattr(instance.subprocess, "run", run)
    monkeypatch.setattr(instance.time, "sleep", lambda _: None)
    assert instance.reload(timeout=5) == expected
    assert (["docker", "restart", instance.GATEWAY] in calls) == restarted
