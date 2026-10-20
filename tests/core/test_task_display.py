import os
import shutil
import signal
import subprocess
import sys
import time

import pytest

import core.terminal as terminal
from scripts import task_display

pytestmark = pytest.mark.skipif(not shutil.which("Xvfb"), reason="Xvfb is not installed")


def _wait_for(check, timeout=10.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return True
        time.sleep(0.05)
    return check()


def _xvfb_children(pid: int) -> list[int]:
    out = subprocess.run(["pgrep", "-P", str(pid), "-x", "Xvfb"], capture_output=True, text=True)
    return [int(p) for p in out.stdout.split()]


def _alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ", 1)[1].split()[0] != "Z"
    except FileNotFoundError:
        return False


def _start(tmp_path, name: str):
    report = tmp_path / f"{name}.out"
    release = tmp_path / f"{name}.release"
    command = (f'echo "$DISPLAY $$" > {report}.part && mv {report}.part {report}; '
               f'while [ ! -e {release} ]; do sleep 0.05; done')
    proc = subprocess.Popen([sys.executable, terminal.TASK_DISPLAY_SCRIPT, command],
                            env={"PATH": os.environ["PATH"]})
    assert _wait_for(report.exists), f"{name} never reported its display"
    display, pid = report.read_text().split()
    return proc, display, int(pid), release


class TestTaskDisplay:
    def test_concurrent_tasks_get_their_own_display_and_it_stops_with_them(self, tmp_path):
        first, first_display, first_pid, first_release = _start(tmp_path, "first")
        second, second_display, second_pid, second_release = _start(tmp_path, "second")
        try:
            assert first_display.startswith(":") and second_display.startswith(":")
            assert first_display != second_display
            first_xvfb = _xvfb_children(first_pid)
            second_xvfb = _xvfb_children(second_pid)
            assert len(first_xvfb) == 1 and len(second_xvfb) == 1

            first_release.touch()
            assert first.wait(timeout=10) == 0
            assert _wait_for(lambda: not _alive(first_xvfb[0]))
            assert _alive(second_xvfb[0])
        finally:
            second_release.touch()
            first_release.touch()
            second.wait(timeout=10)
        assert _wait_for(lambda: not _alive(second_xvfb[0]))

    def test_a_killed_task_takes_its_display_down(self, tmp_path):
        proc, display, pid, _release = _start(tmp_path, "killed")
        xvfb = _xvfb_children(pid)
        assert len(xvfb) == 1
        os.kill(pid, signal.SIGKILL)
        proc.wait(timeout=10)
        assert _wait_for(lambda: not _alive(xvfb[0]))
        assert _wait_for(lambda: not os.path.exists(f"/tmp/.X{display[1:]}-lock"))

    def test_the_command_runs_without_display_when_xvfb_is_missing(self, tmp_path):
        out = subprocess.run(
            [sys.executable, terminal.TASK_DISPLAY_SCRIPT, 'echo "display=[$DISPLAY]"'],
            env={"PATH": str(tmp_path), "DISPLAY": ":0"}, capture_output=True, text=True,
        )
        assert out.returncode == 0
        assert out.stdout == "display=[]\n"
        assert "Xvfb is not installed" in out.stderr

    def test_the_command_runs_without_display_when_xvfb_cannot_execute(self, tmp_path):
        fake = tmp_path / "Xvfb"
        fake.write_bytes(b"\x00not an executable format")
        fake.chmod(0o755)
        out = subprocess.run(
            [sys.executable, terminal.TASK_DISPLAY_SCRIPT, 'echo "display=[$DISPLAY]"'],
            env={"PATH": f"{tmp_path}:/usr/bin:/bin"}, capture_output=True, text=True,
        )
        assert out.returncode == 0
        assert out.stdout == "display=[]\n"
        assert "Xvfb did not start" in out.stderr

    def test_only_a_client_with_the_task_cookie_reaches_the_display(self, tmp_path):
        probe = tmp_path / "probe.py"
        probe.write_text(
            "import ctypes\n"
            "x = ctypes.CDLL('libX11.so.6')\n"
            "x.XOpenDisplay.restype = ctypes.c_void_p\n"
            "print('connected' if x.XOpenDisplay(None) else 'refused')\n")
        command = f"{sys.executable} {probe}; XAUTHORITY=/dev/null {sys.executable} {probe}"
        out = subprocess.run([sys.executable, terminal.TASK_DISPLAY_SCRIPT, command],
                             env={"PATH": os.environ["PATH"]}, capture_output=True, text=True)
        if "OSError" in out.stderr and "libX11" in out.stderr:
            pytest.skip("libX11 is not installed")
        assert out.stdout.split() == ["connected", "refused"]

    def test_a_start_removes_the_cookies_of_exited_tasks(self, tmp_path):
        os.makedirs(task_display.AUTH_DIR, mode=0o700, exist_ok=True)
        dead = subprocess.Popen(["true"])
        dead.wait()
        stale = os.path.join(task_display.AUTH_DIR, str(dead.pid))
        live = os.path.join(task_display.AUTH_DIR, str(os.getpid()))
        for path in (stale, live):
            with open(path, "wb") as f:
                f.write(b"x")
        try:
            task_display._prune_cookies()
            assert not os.path.exists(stale)
            assert os.path.exists(live)
        finally:
            os.unlink(live)
