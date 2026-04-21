import json
import os
import re
import subprocess
import threading
from pathlib import Path

from core.job_logs import active_live_log_path


def _env():
    return {**os.environ, "CLAUDE_CODE_ENTRYPOINT": "cli"}


def run_sonnet(prompt: str, worktree: Path | None = None, tools: list[str] | None = None, timeout: int = 600) -> str | None:
    cmd = ["claude", "-p", "-", "--model", "claude-sonnet-4-6"]
    if worktree and worktree.is_dir():
        cmd += ["--dangerously-skip-permissions", "--add-dir", str(worktree)]
        if tools:
            cmd += ["--allowedTools"] + tools
    result = subprocess.run(
        cmd, input=prompt.encode(), capture_output=True, env=_env(), timeout=timeout,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    return result.stdout.decode()


def run_haiku(prompt: str, timeout: int = 120) -> str | None:
    result = subprocess.run(
        ["claude", "-p", "-", "--model", "claude-haiku-4-5-20251001"],
        input=prompt.encode(), capture_output=True, env=_env(), timeout=timeout,
    )
    if result.returncode != 0 or not result.stdout:
        return None
    return result.stdout.decode().strip()


def run_claude_code(prompt: str, cwd: Path, timeout: int = 600) -> str | None:
    """Run `claude -p <prompt>` in cwd. Returns stdout on success, None on
    non-zero exit or timeout.

    When a live log path is set in the current contextvar (by the worker
    pool before running a task), stdout is tee'd to that file in real time
    so the web UI can tail it. Logging is best-effort; a file-write failure
    never stops the subprocess.
    """
    cmd = ["claude", "-p", prompt, "--dangerously-skip-permissions"]
    log_path = active_live_log_path()
    log_fh = None
    if log_path:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_fh = open(log_path, "ab", buffering=0)
        except OSError:
            log_fh = None

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        cwd=str(cwd), env=_env(), bufsize=0,
    )
    buf = bytearray()

    def _drain():
        assert proc.stdout is not None
        fd = proc.stdout.fileno()
        while True:
            try:
                chunk = os.read(fd, 4096)
            except OSError:
                return
            if not chunk:
                return
            buf.extend(chunk)
            if log_fh is not None:
                try:
                    log_fh.write(chunk)
                except OSError:
                    pass

    reader = threading.Thread(target=_drain, daemon=True)
    reader.start()
    timed_out = False
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        timed_out = True
        proc.kill()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pass
    reader.join(timeout=5)
    if log_fh is not None:
        try:
            if timed_out:
                log_fh.write(f"\n[TIMEOUT after {timeout}s]\n".encode())
            elif proc.returncode != 0:
                log_fh.write(f"\n[EXIT code={proc.returncode}]\n".encode())
        except OSError:
            pass
        try:
            log_fh.close()
        except OSError:
            pass

    if timed_out or proc.returncode != 0:
        return None
    return buf.decode("utf-8", errors="replace")


def extract_json(text: str) -> dict | None:
    m = re.search(r"```(?:json)?\s*\n(.*?)\n```", text, re.DOTALL)
    raw = m.group(1) if m else text
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            return obj
    except (json.JSONDecodeError, ValueError):
        pass
    for i in range(len(text) - 1, -1, -1):
        if text[i] == "{":
            try:
                obj = json.loads(text[i:])
                if isinstance(obj, dict):
                    return obj
            except (json.JSONDecodeError, ValueError):
                continue
    return None
