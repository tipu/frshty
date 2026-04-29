import json
import os
import re
import subprocess
import threading
from pathlib import Path

import core.log as log
from core.job_logs import active_live_log_path, active_live_pid_path


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


def _extract_text(evt: dict) -> str:
    if evt.get("type") != "stream_event":
        return ""
    inner = evt.get("event") or {}
    if inner.get("type") != "content_block_delta":
        return ""
    delta = inner.get("delta") or {}
    if delta.get("type") == "text_delta":
        return delta.get("text") or ""
    return ""


def run_claude_code(prompt: str, cwd: Path, timeout: int = 600) -> str | None:
    """Run `claude -p <prompt>` in cwd. Returns stdout on success, None on
    non-zero exit or timeout.

    When a live log path is set in the current contextvar (by the worker
    pool before running a task), stdout is tee'd to that file in real time
    so the web UI can tail it. Logging is best-effort; a file-write failure
    never stops the subprocess.
    """
    cmd = [
        "claude", "-p", prompt,
        "--dangerously-skip-permissions",
        "--output-format", "stream-json",
        "--include-partial-messages",
        "--verbose",
    ]
    log_path = active_live_log_path()
    log_fh = None
    if log_path:
        try:
            log_path.parent.mkdir(parents=True, exist_ok=True)
            if not log_path.exists():
                log_path.touch()
            log_fh = open(log_path, "ab", buffering=0)
        except OSError:
            log_fh = None

    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        cwd=str(cwd), env=_env(), text=True, bufsize=1, errors="replace",
        start_new_session=True,
    )
    pid_path = active_live_pid_path()
    if pid_path is not None:
        try:
            pid_path.parent.mkdir(parents=True, exist_ok=True)
            pid_path.write_text(str(proc.pid))
        except OSError as e:
            log.emit("job_pid_write_failed", f"Failed to write pid file: {e}")
    parts: list[str] = []

    def _drain():
        assert proc.stdout is not None
        for line in proc.stdout:
            try:
                evt = json.loads(line)
            except (json.JSONDecodeError, ValueError):
                continue
            text = _extract_text(evt)
            if not text:
                continue
            parts.append(text)
            if log_fh is not None:
                try:
                    log_fh.write(text.encode("utf-8"))
                    os.fsync(log_fh.fileno())
                except OSError as e:
                    log.emit("job_log_write_failed", f"Failed to write to job log: {e}")

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
        except OSError as e:
            log.emit("job_log_write_failed", f"Failed to write job status to log: {e}")
        try:
            log_fh.close()
        except OSError as e:
            log.emit("job_log_close_failed", f"Failed to close job log: {e}")

    if pid_path is not None:
        try:
            pid_path.unlink(missing_ok=True)
        except OSError as e:
            log.emit("job_pid_unlink_failed", f"Failed to remove pid file: {e}")

    if timed_out or proc.returncode != 0:
        return None
    return "".join(parts)


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
