import os
import shutil
import signal
import socket
import struct
import subprocess
import sys
import threading
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


def _children(pid: int, name: str = "") -> list[int]:
    args = ["pgrep", "-P", str(pid)] + (["-x", name] if name else [])
    out = subprocess.run(args, capture_output=True, text=True)
    return [int(p) for p in out.stdout.split()]


def _xvfb_of(pid: int) -> list[int]:
    found = _children(pid, "Xvfb")
    for child in _children(pid):
        found += _xvfb_of(child)
    return found


def _alive(pid: int) -> bool:
    try:
        with open(f"/proc/{pid}/stat") as f:
            return f.read().split(") ", 1)[1].split()[0] != "Z"
    except (FileNotFoundError, ProcessLookupError):
        return False


PROBE = ("import ctypes\n"
         "x = ctypes.CDLL('libX11.so.6')\n"
         "x.XOpenDisplay.restype = ctypes.c_void_p\n"
         "print('connected' if x.XOpenDisplay(None) else 'refused')\n")


def _probe(tmp_path, display: str, pid: int) -> str:
    probe = tmp_path / "probe.py"
    probe.write_text(PROBE)
    out = subprocess.run(
        [sys.executable, str(probe)], capture_output=True, text=True,
        env={"PATH": os.environ["PATH"], "DISPLAY": display,
             "XAUTHORITY": os.path.join(task_display.AUTH_DIR, str(pid))})
    if "OSError" in out.stderr and "libX11" in out.stderr:
        pytest.skip("libX11 is not installed")
    return out.stdout.strip()


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
    def test_a_task_that_opens_no_window_starts_no_xvfb(self, tmp_path):
        proc, display, pid, release = _start(tmp_path, "idle")
        try:
            assert display.startswith(":")
            time.sleep(0.5)
            assert _xvfb_of(pid) == []
        finally:
            release.touch()
            proc.wait(timeout=10)

    def test_concurrent_tasks_get_their_own_display_and_it_stops_with_them(self, tmp_path):
        first, first_display, first_pid, first_release = _start(tmp_path, "first")
        second, second_display, second_pid, second_release = _start(tmp_path, "second")
        try:
            assert first_display.startswith(":") and second_display.startswith(":")
            assert first_display != second_display
            assert _probe(tmp_path, first_display, first_pid) == "connected"
            assert _probe(tmp_path, second_display, second_pid) == "connected"
            first_xvfb = _xvfb_of(first_pid)
            second_xvfb = _xvfb_of(second_pid)
            assert len(first_xvfb) == 1 and len(second_xvfb) == 1
            assert first_xvfb != second_xvfb

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
        assert _probe(tmp_path, display, pid) == "connected"
        watcher = [c for c in _children(pid) if _children(c, "Xvfb")]
        xvfb = _xvfb_of(pid)
        assert len(watcher) == 1 and len(xvfb) == 1
        os.kill(pid, signal.SIGKILL)
        proc.wait(timeout=10)
        assert _wait_for(lambda: not _alive(watcher[0]))
        assert _wait_for(lambda: not _alive(xvfb[0]))

    def test_a_client_reaches_the_display_through_its_socket_file(self, tmp_path):
        proc, display, pid, release = _start(tmp_path, "path")
        path = f"{task_display.SOCKET_DIR}/X{display[1:]}"
        try:
            with open(os.path.join(task_display.AUTH_DIR, str(pid)), "rb") as f:
                cookie = f.read()[-16:]
            name = task_display.COOKIE_NAME
            setup = (b"l\0" + struct.pack("<HHHH", 11, 0, len(name), len(cookie)) + b"\0\0"
                     + name + b"\0" * (-len(name) % 4) + cookie)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(10)
                client.connect(path)
                client.sendall(setup)
                assert client.recv(1) == b"\x01"
        finally:
            release.touch()
            proc.wait(timeout=10)
        assert _wait_for(lambda: not os.path.exists(path))

    def test_the_command_runs_without_display_when_xvfb_is_missing(self, tmp_path):
        out = subprocess.run(
            [sys.executable, terminal.TASK_DISPLAY_SCRIPT, 'echo "display=[$DISPLAY]"'],
            env={"PATH": str(tmp_path), "DISPLAY": ":0"}, capture_output=True, text=True,
        )
        assert out.returncode == 0
        assert out.stdout == "display=[]\n"
        assert "Xvfb is not installed" in out.stderr

    def test_a_client_is_refused_when_xvfb_cannot_execute(self, tmp_path):
        fake = tmp_path / "Xvfb"
        fake.write_bytes(b"\x00not an executable format")
        fake.chmod(0o755)
        probe = tmp_path / "probe.py"
        probe.write_text(PROBE)
        out = subprocess.run(
            [sys.executable, terminal.TASK_DISPLAY_SCRIPT, f"{sys.executable} {probe}"],
            env={"PATH": f"{tmp_path}:/usr/bin:/bin"}, capture_output=True, text=True,
        )
        if "OSError" in out.stderr and "libX11" in out.stderr:
            pytest.skip("libX11 is not installed")
        assert out.returncode == 0
        assert out.stdout == "refused\n"
        assert "Xvfb did not start" in out.stderr

    def test_only_a_client_with_the_task_cookie_reaches_the_display(self, tmp_path):
        probe = tmp_path / "probe.py"
        probe.write_text(PROBE)
        command = f"{sys.executable} {probe}; XAUTHORITY=/dev/null {sys.executable} {probe}"
        out = subprocess.run([sys.executable, terminal.TASK_DISPLAY_SCRIPT, command],
                             env={"PATH": os.environ["PATH"]}, capture_output=True, text=True)
        if "OSError" in out.stderr and "libX11" in out.stderr:
            pytest.skip("libX11 is not installed")
        assert out.stdout.split() == ["connected", "refused"]

    def test_the_relay_passes_file_descriptors(self, tmp_path):
        client, relay_in = socket.socketpair()
        relay_out, server = socket.socketpair()
        threading.Thread(target=task_display._serve, args=(relay_in, relay_out), daemon=True).start()
        shared = tmp_path / "shared"
        shared.write_text("segment")
        with open(shared) as f:
            socket.send_fds(client, [b"attach"], [f.fileno()])
        data, fds, _flags, _addr = socket.recv_fds(server, 64, 4)
        assert data == b"attach" and len(fds) == 1
        with os.fdopen(fds[0]) as passed:
            assert passed.read() == "segment"
        server.sendall(b"reply")
        assert client.recv(64) == b"reply"
        client.close()
        assert server.recv(64) == b""
        server.close()

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

    def test_a_cookie_directory_others_can_open_is_refused(self, monkeypatch, tmp_path):
        shared = tmp_path / "xauth"
        shared.mkdir(mode=0o777)
        shared.chmod(0o777)
        monkeypatch.setattr(task_display, "AUTH_DIR", str(shared))
        with pytest.raises(OSError, match="not a private directory"):
            task_display.write_cookie()

    def test_a_cookie_replaces_a_planted_file_instead_of_writing_through_it(self, monkeypatch, tmp_path):
        private = tmp_path / "xauth"
        private.mkdir(mode=0o700)
        target = tmp_path / "target"
        target.write_text("keep")
        (private / str(os.getpid())).symlink_to(target)
        monkeypatch.setattr(task_display, "AUTH_DIR", str(private))
        path = task_display.write_cookie()
        assert target.read_text() == "keep"
        assert not os.path.islink(path)
        assert os.stat(path).st_mode & 0o777 == 0o600
