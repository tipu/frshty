import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

import core.runtime as runtime

REPO = Path(__file__).resolve().parents[2]


class FakePool:
    def __init__(self, left):
        self.left = left
        self.timeouts = []

    def drain(self, timeout):
        self.timeouts.append(timeout)
        return self.left


def test_drain_and_exit_drains_once_and_sends_sigterm(monkeypatch):
    pool = FakePool([41, 42])
    kills = []
    events = []
    monkeypatch.setattr(runtime, "_pool", pool)
    monkeypatch.setattr(runtime.os, "kill", lambda pid, sig: kills.append((pid, sig)))
    monkeypatch.setattr(runtime.time, "sleep", lambda s: kills.append(("sleep", s)))
    monkeypatch.setattr(runtime.os, "_exit", lambda code: kills.append(("_exit", code)))
    monkeypatch.setattr(runtime.log, "emit", lambda kind, msg, **kw: events.append((kind, msg)))
    try:
        runtime.drain_and_exit(7)
        runtime.drain_and_exit(7)
    finally:
        if runtime._drain_lock.locked():
            runtime._drain_lock.release()
    assert pool.timeouts == [7]
    assert kills == [(os.getpid(), signal.SIGTERM), ("sleep", runtime.drain.EXIT_GRACE), ("_exit", 1)]
    assert [k for k, _ in events] == ["drain_started", "drain_deadline_passed", "drain_exit_forced"]
    assert "job_ids=41,42" in events[1][1]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_the_server_answers_during_the_drain_and_exits_after_it(tmp_path):
    port = _free_port()
    marker = tmp_path / "drained"
    script = f"""
import sys, time
sys.path.insert(0, {str(REPO)!r})
import uvicorn
from fastapi import FastAPI
import core.runtime as runtime

class Pool:
    def drain(self, timeout):
        time.sleep(2)
        open({str(marker)!r}, "w").write("done")
        return []

runtime._pool = Pool()
runtime.log.emit = lambda *a, **k: None
runtime.install_drain_handler()
app = FastAPI()
app.get("/")(lambda: {{"ok": True}})
uvicorn.run(app, host="127.0.0.1", port={port}, log_level="warning")
"""
    proc = subprocess.Popen([sys.executable, "-c", script], cwd=tmp_path)
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
                break
            except httpx.HTTPError:
                assert time.monotonic() < deadline, "server did not start"
                time.sleep(0.1)
        proc.send_signal(signal.SIGUSR1)
        time.sleep(0.5)
        assert httpx.get(f"http://127.0.0.1:{port}/", timeout=1).json() == {"ok": True}
        assert not marker.exists()
        assert proc.wait(timeout=20) == -signal.SIGTERM
        assert marker.read_text() == "done"
    finally:
        proc.kill()


def test_the_process_exits_after_the_drain_when_a_request_never_ends(tmp_path):
    port = _free_port()
    script = f"""
import asyncio, sys
sys.path.insert(0, {str(REPO)!r})
import uvicorn
from fastapi import FastAPI
import core.runtime as runtime

class Pool:
    def drain(self, timeout):
        return []

runtime._pool = Pool()
runtime.log.emit = lambda *a, **k: None
runtime.drain.EXIT_GRACE = 1
runtime.install_drain_handler()
app = FastAPI()
app.get("/")(lambda: {{"ok": True}})

@app.get("/hang")
async def hang():
    await asyncio.sleep(3600)

uvicorn.run(app, host="127.0.0.1", port={port}, log_level="warning")
"""
    proc = subprocess.Popen([sys.executable, "-c", script], cwd=tmp_path)
    try:
        deadline = time.monotonic() + 30
        while True:
            try:
                httpx.get(f"http://127.0.0.1:{port}/", timeout=1)
                break
            except httpx.HTTPError:
                assert time.monotonic() < deadline, "server did not start"
                time.sleep(0.1)
        try:
            httpx.get(f"http://127.0.0.1:{port}/hang", timeout=0.5)
        except httpx.TimeoutException:
            pass
        proc.send_signal(signal.SIGUSR1)
        assert proc.wait(timeout=20) == 1
    finally:
        proc.kill()


def test_supervise_kills_a_child_that_ignores_sigterm_after_the_drain(tmp_path):
    runs = tmp_path / "runs"
    child = f"""
import pathlib, signal, time
runs = pathlib.Path({str(runs)!r})
first = not runs.exists()
runs.open("a").write("run ")
signal.signal(signal.SIGUSR1, signal.SIG_IGN)
if first:
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
time.sleep(3600)
"""
    script = f"""
import sys
sys.path.insert(0, {str(REPO)!r})
import scripts.container_boot as boot
sys.exit(boot.supervise([sys.executable, "-c", {child!r}], drain_timeout=0.5, kill_grace=0.5))
"""
    proc = subprocess.Popen([sys.executable, "-c", script], cwd=tmp_path)
    try:
        deadline = time.monotonic() + 30
        while not runs.exists():
            assert time.monotonic() < deadline, "child did not start"
            time.sleep(0.1)
        proc.send_signal(signal.SIGHUP)
        deadline = time.monotonic() + 20
        while runs.read_text().count("run") < 2:
            assert time.monotonic() < deadline, "child was not restarted"
            time.sleep(0.1)
        proc.send_signal(signal.SIGTERM)
        assert proc.wait(timeout=20) == -signal.SIGTERM % 256
    finally:
        proc.kill()
