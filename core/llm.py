import contextvars
import json
import os
import re
import subprocess
import threading
import time
from abc import ABC, abstractmethod
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import core.db as _db
import core.log as log
import core.state as _state
from core.job_logs import active_live_log_path, active_live_pid_path
from core.state import _instance_key_cv


_LLM_MAX_CONCURRENT = int(os.environ.get(
    "FRSHTY_MAX_CONCURRENT_LLM",
    os.environ.get("FRSHTY_CLAUDE_MAX_CONCURRENT", "15"),
))
_LLM_LIMIT_COOLDOWN_SECONDS = int(os.environ.get("FRSHTY_LLM_LIMIT_COOLDOWN_SECONDS", "1800"))
_THINKING_MODEL = os.environ.get("FRSHTY_THINKING_MODEL", "claude-sonnet-4-6")
_llm_sem = threading.BoundedSemaphore(max(1, _LLM_MAX_CONCURRENT))

_guard_blocked_cv: contextvars.ContextVar[bool] = contextvars.ContextVar(
    "frshty_llm_guard_blocked", default=False
)


def reset_guard_blocked() -> None:
    _guard_blocked_cv.set(False)


def consume_guard_blocked() -> bool:
    val = _guard_blocked_cv.get()
    if val:
        _guard_blocked_cv.set(False)
    return val


def _flag_guard_blocked() -> None:
    _guard_blocked_cv.set(True)


_providers: dict[str, "LLMProvider"] = {}
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_LLM_LIMIT_PATTERNS = (
    "out of extra usage",
    "hit your org's monthly usage limit",
    "hit your limit",
    "key limit exceeded",
    "requires more credits",
    "monthly usage limit",
)


def _env():
    return {**os.environ, "CLAUDE_CODE_ENTRYPOINT": "cli"}


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


def _guard_key() -> str:
    return _active_instance_key() or "__default__"


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _llm_limit_reason(output: str | None) -> str | None:
    if not output:
        return None
    cleaned = _strip_ansi(output).lower()
    for pattern in _LLM_LIMIT_PATTERNS:
        if pattern in cleaned:
            return pattern
    return None


def _guard_status() -> tuple[bool, str, int]:
    key = _guard_key()
    row = _db.query_one(
        "SELECT data FROM kv WHERE instance_key=? AND key='llm_guard'",
        (key,),
    )
    if not row or not row.get("data"):
        return False, "", 0
    try:
        payload = json.loads(row["data"])
        blocked_until = datetime.fromisoformat(payload["blocked_until"])
        reason = payload.get("reason", "recent usage limit response")
    except (json.JSONDecodeError, KeyError, ValueError, TypeError):
        _db.execute(
            "DELETE FROM kv WHERE instance_key=? AND key='llm_guard'", (key,)
        )
        return False, "", 0
    now = datetime.now(timezone.utc)
    if blocked_until <= now:
        _db.execute(
            "DELETE FROM kv WHERE instance_key=? AND key='llm_guard'", (key,)
        )
        return False, "", 0
    remaining_s = max(1, int((blocked_until - now).total_seconds()))
    return True, reason, remaining_s


def _trip_llm_guard(output: str | None) -> str | None:
    reason = _llm_limit_reason(output)
    if reason is None:
        return None
    key = _guard_key()
    now = datetime.now(timezone.utc)
    blocked_until = now + timedelta(seconds=max(1, _LLM_LIMIT_COOLDOWN_SECONDS))
    payload = json.dumps({
        "blocked_until": blocked_until.isoformat(),
        "reason": reason,
    })
    _db.execute(
        "INSERT INTO kv(instance_key, key, data, updated_at) "
        "VALUES (?, 'llm_guard', ?, ?) "
        "ON CONFLICT(instance_key, key) DO UPDATE SET "
        "data=excluded.data, updated_at=excluded.updated_at",
        (key, payload, now.isoformat()),
    )
    log.emit(
        "llm_guard_tripped",
        f"[{_active_instance_key()}] blocking new LLM invocations for "
        f"{_LLM_LIMIT_COOLDOWN_SECONDS}s after usage-limit response ({reason})",
        meta={"instance_key": _active_instance_key(), "reason": reason,
              "cooldown_s": _LLM_LIMIT_COOLDOWN_SECONDS},
    )
    _flag_guard_blocked()
    return reason


def _guard_block_output(reason: str, remaining_s: int) -> str:
    return (
        f"[guard] skipped LLM invocation: recent usage-limit response "
        f"({reason}); cooldown {remaining_s}s remaining"
    )


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


def _parse_claude_json_output(raw: str) -> tuple[str, dict | None]:
    """Parse `claude -p --output-format json` stdout. Returns (text, usage_dict).
    Falls back to (raw_stripped, None) if the payload isn't a result envelope."""
    text = raw.strip()
    try:
        parsed = json.loads(text)
    except (ValueError, TypeError):
        return text, None
    if not isinstance(parsed, dict) or parsed.get("type") != "result":
        return text, None
    return (parsed.get("result") or "").strip(), parsed


def _record_end(inv_id: str | None, started_ms: float, status: str,
                exit_code: int | None, output: str | None,
                usage: dict | None = None) -> None:
    if inv_id is None:
        return
    finished = datetime.now(timezone.utc).isoformat()
    duration_ms = int((time.monotonic() - started_ms) * 1000)
    out_text = output or ""
    cost_usd = None
    input_tokens = output_tokens = cc_tokens = cr_tokens = num_turns = None
    if usage:
        cost_usd = usage.get("total_cost_usd")
        u = usage.get("usage") or {}
        input_tokens = u.get("input_tokens")
        output_tokens = u.get("output_tokens")
        cc_tokens = u.get("cache_creation_input_tokens")
        cr_tokens = u.get("cache_read_input_tokens")
        num_turns = usage.get("num_turns")
    try:
        _db.execute(
            "UPDATE claude_invocations SET finished_at=?, duration_ms=?, status=?, "
            "exit_code=?, output=?, output_length=?, cost_usd=?, input_tokens=?, "
            "output_tokens=?, cache_creation_input_tokens=?, "
            "cache_read_input_tokens=?, num_turns=? WHERE id=?",
            (finished, duration_ms, status, exit_code, out_text, len(out_text),
             cost_usd, input_tokens, output_tokens, cc_tokens, cr_tokens, num_turns,
             inv_id),
        )
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to record claude end: {e}")


class LLMProvider(ABC):
    @abstractmethod
    def thinking(self, prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, **kwargs) -> str | None:
        ...

    @abstractmethod
    def balanced(self, prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 **kwargs) -> str | None:
        ...

    @abstractmethod
    def fast(self, prompt: str, *, timeout: int = 120, **kwargs) -> str | None:
        ...


class ClaudeProvider(LLMProvider):
    def __init__(self, config: dict | None = None):
        llm_cfg = (config or {}).get("llm", {})
        claude_cfg = llm_cfg.get("claude", {})
        self.bin = claude_cfg.get("bin", "claude")
        self.extra_args = list(claude_cfg.get("args", []))
        self.env_overrides = {
            str(k): str(v)
            for k, v in claude_cfg.get("env", {}).items()
        }
        config_dir = claude_cfg.get("config_dir")
        if config_dir and "CLAUDE_CONFIG_DIR" not in self.env_overrides:
            self.env_overrides["CLAUDE_CONFIG_DIR"] = str(config_dir)

    def _env(self) -> dict[str, str]:
        env = _env()
        for key, value in self.env_overrides.items():
            env[key] = os.path.expanduser(value)
        return env

    def _cmd(self, *args: str) -> list[str]:
        return [self.bin, *self.extra_args, *args]

    def thinking(self, prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, session_id: str | None = None,
                 resume: bool = False, model: str | None = None,
                 **kwargs) -> str | None:
        chosen_model = model or _THINKING_MODEL
        cmd = self._cmd(
            "-p", prompt,
            "--model", chosen_model,
            "--dangerously-skip-permissions",
            "--output-format", "stream-json",
            "--include-partial-messages",
            "--verbose",
        )
        if session_id and resume:
            cmd += ["--resume", session_id]
        elif session_id:
            cmd += ["--session-id", session_id]
        inv_id = _record_start("run_claude_code", chosen_model, prompt, cwd, None, timeout)
        t0 = time.monotonic()
        blocked, reason, remaining_s = _guard_status()
        if blocked:
            _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
            log.emit("llm_guard_blocked",
                     f"[{_active_instance_key()}] skipped Claude Code invocation "
                     f"while cooldown active ({remaining_s}s left)",
                     meta={"instance_key": _active_instance_key(),
                           "reason": reason, "remaining_s": remaining_s})
            _flag_guard_blocked()
            return None
        _llm_sem.acquire()
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
                cwd=str(cwd) if cwd else None, env=self._env(), text=True, bufsize=1, errors="replace",
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
            non_json: list[str] = []
            result_event: dict | None = None

            def _drain():
                nonlocal result_event
                assert proc.stdout is not None
                for line in proc.stdout:
                    try:
                        evt = json.loads(line)
                    except (json.JSONDecodeError, ValueError):
                        stripped = line.rstrip()
                        if stripped:
                            non_json.append(stripped)
                        continue
                    if evt.get("type") == "result":
                        result_event = evt
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
            _llm_sem.release()

        output = "".join(parts)
        if timed_out:
            if non_json:
                output = output + "\n[non-json output]\n" + "\n".join(non_json[-50:])
            _record_end(inv_id, t0, "timeout", None, output, usage=result_event)
            return None
        if proc.returncode != 0:
            if non_json:
                output = output + "\n[non-json output]\n" + "\n".join(non_json[-50:])
            _trip_llm_guard(output)
            _record_end(inv_id, t0, "error", proc.returncode, output, usage=result_event)
            return None
        _record_end(inv_id, t0, "success", proc.returncode, output, usage=result_event)
        return output

    def balanced(self, prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 **kwargs) -> str | None:
        cmd = self._cmd("-p", "-", "--output-format", "json", "--model", "claude-sonnet-4-6")
        if worktree and worktree.is_dir():
            cmd += ["--dangerously-skip-permissions", "--add-dir", str(worktree)]
            if tools:
                cmd += ["--allowedTools"] + tools
        inv_id = _record_start("run_sonnet", "claude-sonnet-4-6", prompt, worktree, tools, timeout)
        t0 = time.monotonic()
        blocked, reason, remaining_s = _guard_status()
        if blocked:
            _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
            log.emit("llm_guard_blocked",
                     f"[{_active_instance_key()}] skipped Claude Sonnet invocation "
                     f"while cooldown active ({remaining_s}s left)",
                     meta={"instance_key": _active_instance_key(),
                           "reason": reason, "remaining_s": remaining_s})
            _flag_guard_blocked()
            return None
        with _llm_sem:
            _mark_running(inv_id)
            try:
                result = subprocess.run(
                    cmd, input=prompt.encode(), capture_output=True, env=self._env(), timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                _record_end(inv_id, t0, "timeout", None, None)
                return None
            raw = result.stdout.decode() if result.stdout else ""
            if result.returncode != 0 or not result.stdout:
                err = result.stderr.decode() if result.stderr else ""
                if err:
                    raw = raw + "\n[stderr]\n" + err
                _trip_llm_guard(raw)
                _record_end(inv_id, t0, "error", result.returncode, raw)
                return None
            text, usage = _parse_claude_json_output(raw)
            _record_end(inv_id, t0, "success", result.returncode, text, usage=usage)
            return text

    def fast(self, prompt: str, *, timeout: int = 120, **kwargs) -> str | None:
        inv_id = _record_start("run_haiku", "claude-haiku-4-5-20251001", prompt, None, None, timeout)
        t0 = time.monotonic()
        blocked, reason, remaining_s = _guard_status()
        if blocked:
            _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
            log.emit("llm_guard_blocked",
                     f"[{_active_instance_key()}] skipped Claude Haiku invocation "
                     f"while cooldown active ({remaining_s}s left)",
                     meta={"instance_key": _active_instance_key(),
                           "reason": reason, "remaining_s": remaining_s})
            _flag_guard_blocked()
            return None
        with _llm_sem:
            _mark_running(inv_id)
            try:
                result = subprocess.run(
                    self._cmd("-p", "-", "--output-format", "json", "--model", "claude-haiku-4-5-20251001"),
                    input=prompt.encode(), capture_output=True, env=self._env(), timeout=timeout,
                )
            except subprocess.TimeoutExpired:
                _record_end(inv_id, t0, "timeout", None, None)
                return None
            raw = result.stdout.decode() if result.stdout else ""
            if result.returncode != 0 or not result.stdout:
                err = result.stderr.decode() if result.stderr else ""
                if err:
                    raw = raw + "\n[stderr]\n" + err
                _trip_llm_guard(raw)
                _record_end(inv_id, t0, "error", result.returncode, raw)
                return None
            text, usage = _parse_claude_json_output(raw)
            _record_end(inv_id, t0, "success", result.returncode, text, usage=usage)
            return text


class OpenCodeProvider(LLMProvider):
    def __init__(self, config: dict):
        oc = config.get("llm", {}).get("opencode", {})
        self.model_thinking = oc.get("model_thinking", "openrouter/deepseek/deepseek-v4-pro")
        self.model_balanced = oc.get("model_balanced", "openrouter/deepseek/deepseek-v4-flash")
        self.model_fast = oc.get("model_fast", "openrouter/deepseek/deepseek-v4-flash")

    def thinking(self, prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, **kwargs) -> str | None:
        cmd = ["opencode", "run", prompt, "--model", self.model_thinking,
               "--dangerously-skip-permissions"]
        return self._run(cmd, "run_thinking", self.model_thinking, prompt, cwd, timeout)

    def balanced(self, prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 **kwargs) -> str | None:
        cmd = ["opencode", "run", prompt, "--model", self.model_balanced,
               "--dangerously-skip-permissions"]
        cwd = worktree if worktree and worktree.is_dir() else None
        return self._run(cmd, "run_balanced", self.model_balanced, prompt, cwd, timeout)

    def fast(self, prompt: str, *, timeout: int = 120, **kwargs) -> str | None:
        cmd = ["opencode", "run", prompt, "--model", self.model_fast,
               "--dangerously-skip-permissions"]
        return self._run(cmd, "run_fast", self.model_fast, prompt, None, timeout)

    def _run(self, cmd: list[str], fn_name: str, model: str, prompt: str,
             cwd: Path | None, timeout: int) -> str | None:
        inv_id = _record_start(fn_name, model, prompt, cwd, None, timeout)
        t0 = time.monotonic()
        blocked, reason, remaining_s = _guard_status()
        if blocked:
            _record_end(inv_id, t0, "blocked", None, _guard_block_output(reason, remaining_s))
            log.emit("llm_guard_blocked",
                     f"[{_active_instance_key()}] skipped {model} invocation "
                     f"while cooldown active ({remaining_s}s left)",
                     meta={"instance_key": _active_instance_key(),
                           "reason": reason, "remaining_s": remaining_s})
            _flag_guard_blocked()
            return None
        with _llm_sem:
            _mark_running(inv_id)
            try:
                result = subprocess.run(
                    cmd, capture_output=True, cwd=str(cwd) if cwd else None,
                    timeout=timeout, env=os.environ,
                )
            except subprocess.TimeoutExpired:
                _record_end(inv_id, t0, "timeout", None, None)
                return None
            output = result.stdout.decode().strip() if result.stdout else ""
            if result.returncode != 0 or not result.stdout:
                err = result.stderr.decode() if result.stderr else ""
                if err:
                    output = output + "\n[stderr]\n" + err
                _trip_llm_guard(output)
                _record_end(inv_id, t0, "error", result.returncode, output)
                return None
            _record_end(inv_id, t0, "success", result.returncode, output)
            return output


def _mark_running(inv_id: str | None) -> None:
    if inv_id is None:
        return
    try:
        _db.execute("UPDATE claude_invocations SET status='running' WHERE id=?", (inv_id,))
    except Exception as e:
        log.emit("claude_invocation_log_failed", f"failed to mark running: {e}")


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


def _get_provider() -> LLMProvider:
    key = _active_instance_key()
    if key not in _providers:
        _providers[key] = ClaudeProvider()
    return _providers[key]


def configure(config: dict) -> None:
    key = config["job"]["key"]
    llm_cfg = config.get("llm", {})
    provider_name = llm_cfg.get("provider", "claude")
    if provider_name == "opencode":
        _providers[key] = OpenCodeProvider(config)
    else:
        _providers[key] = ClaudeProvider(config)


def run_thinking(prompt: str, *, cwd: Path | None = None,
                 timeout: int = 600, **kwargs) -> str | None:
    return _get_provider().thinking(prompt, cwd=cwd, timeout=timeout, **kwargs)


def run_balanced(prompt: str, *, worktree: Path | None = None,
                 tools: list[str] | None = None, timeout: int = 600,
                 **kwargs) -> str | None:
    return _get_provider().balanced(prompt, worktree=worktree, tools=tools,
                                    timeout=timeout, **kwargs)


def run_fast(prompt: str, *, timeout: int = 120, **kwargs) -> str | None:
    return _get_provider().fast(prompt, timeout=timeout, **kwargs)


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
