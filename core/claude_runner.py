import json
import os
import re
import subprocess
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import core.db as _db
import core.log as log
import core.state as _state
from core.job_logs import active_live_log_path, active_live_pid_path
from core.state import _instance_key_cv


_CLAUDE_MAX_CONCURRENT = int(os.environ.get("FRSHTY_CLAUDE_MAX_CONCURRENT", "5"))
_claude_sem = threading.BoundedSemaphore(max(1, _CLAUDE_MAX_CONCURRENT))


def _env():
    return {**os.environ, "CLAUDE_CODE_ENTRYPOINT": "cli"}


def _mark_running(inv_id: str | None) -> None:
    if inv_id is None:
        return
    try:
        _db.execute("UPDATE claude_invocations SET status='running' WHERE id=?", (inv_id,))
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to mark running: {e}")


def _active_instance_key() -> str:
    k = _instance_key_cv.get()
    if k is not None:
        return k
    return _state._default_instance_key or ""


def _active_job_key() -> str:
    try:
        from core.log import _job_key_cv
        k = _job_key_cv.get()
        return k if k is not None else ""
    except (ImportError, LookupError):
        return ""


def _record_start(function_name: str, model: str, prompt: str,
                  cwd: Path | None = None, tools: list[str] | None = None,
                  timeout: int | None = None) -> str | None:
    inv_id = uuid4().hex[:16]
    started = datetime.now(timezone.utc).isoformat()
    try:
        _db.execute(
            "INSERT INTO claude_invocations(id, instance_key, job_key, function_name, model, "
            "prompt, prompt_length, cwd, tools, timeout_s, started_at, status) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                inv_id,
                _active_instance_key(),
                _active_job_key(),
                function_name,
                model,
                prompt,
                len(prompt),
                str(cwd) if cwd is not None else None,
                json.dumps(tools) if tools else None,
                timeout,
                started,
                "queued",
            ),
        )
        return inv_id
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to record claude start: {e}")
        return None


def _record_end(inv_id: str | None, started_ms: float, status: str,
                exit_code: int | None, output: str | None) -> None:
    if inv_id is None:
        return
    finished = datetime.now(timezone.utc).isoformat()
    duration_ms = int((time.monotonic() - started_ms) * 1000)
    out_text = output or ""
    try:
        _db.execute(
            "UPDATE claude_invocations SET finished_at=?, duration_ms=?, status=?, "
            "exit_code=?, output=?, output_length=? WHERE id=?",
            (finished, duration_ms, status, exit_code, out_text, len(out_text), inv_id),
        )
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to record claude end: {e}")


def run_sonnet(prompt: str, worktree: Path | None = None, tools: list[str] | None = None, timeout: int = 600) -> str | None:
    cmd = ["claude", "-p", "-", "--model", "claude-sonnet-4-6"]
    if worktree and worktree.is_dir():
        cmd += ["--dangerously-skip-permissions", "--add-dir", str(worktree)]
        if tools:
            cmd += ["--allowedTools"] + tools
    inv_id = _record_start("run_sonnet", "claude-sonnet-4-6", prompt, worktree, tools, timeout)
    t0 = time.monotonic()
    with _claude_sem:
        _mark_running(inv_id)
        try:
            result = subprocess.run(
                cmd, input=prompt.encode(), capture_output=True, env=_env(), timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            _record_end(inv_id, t0, "timeout", None, None)
            return None
        output = result.stdout.decode() if result.stdout else ""
        if result.returncode != 0 or not result.stdout:
            _record_end(inv_id, t0, "error", result.returncode, output)
            return None
        _record_end(inv_id, t0, "success", result.returncode, output)
        return output


def run_haiku(prompt: str, timeout: int = 120) -> str | None:
    inv_id = _record_start("run_haiku", "claude-haiku-4-5-20251001", prompt, None, None, timeout)
    t0 = time.monotonic()
    with _claude_sem:
        _mark_running(inv_id)
        try:
            result = subprocess.run(
                ["claude", "-p", "-", "--model", "claude-haiku-4-5-20251001"],
                input=prompt.encode(), capture_output=True, env=_env(), timeout=timeout,
            )
        except subprocess.TimeoutExpired:
            _record_end(inv_id, t0, "timeout", None, None)
            return None
        output = result.stdout.decode().strip() if result.stdout else ""
        if result.returncode != 0 or not result.stdout:
            _record_end(inv_id, t0, "error", result.returncode, output)
            return None
        _record_end(inv_id, t0, "success", result.returncode, output)
        return output


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
    inv_id = _record_start("run_claude_code", "claude-code", prompt, cwd, None, timeout)
    t0 = time.monotonic()
    _claude_sem.acquire()
    _mark_running(inv_id)
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

    try:
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
    finally:
        _claude_sem.release()

    output = "".join(parts)
    if timed_out:
        _record_end(inv_id, t0, "timeout", None, output)
        return None
    if proc.returncode != 0:
        _record_end(inv_id, t0, "error", proc.returncode, output)
        return None
    _record_end(inv_id, t0, "success", proc.returncode, output)
    return output


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
