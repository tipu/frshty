"""Per-job live log file path, contextvar-routed.

The worker sets the active job's log path before calling run_task, and
run_claude_code reads it when deciding whether to stream stdout to disk.
No call-site changes in tasks; streaming is automatic within a worker
context.
"""
from contextvars import ContextVar, Token
from pathlib import Path

_active_live_log: ContextVar[Path | None] = ContextVar("active_live_log", default=None)


def job_log_path(instance_key: str, job_id: int) -> Path:
    return Path.home() / ".frshty" / instance_key / "jobs" / f"{job_id}.log"


def use_live_job(instance_key: str, job_id: int) -> Token:
    return _active_live_log.set(job_log_path(instance_key, job_id))


def reset_live_job(token: Token) -> None:
    _active_live_log.reset(token)


def active_live_log_path() -> Path | None:
    return _active_live_log.get()


def trim_to_utf8_boundary(data: bytes) -> bytes:
    """Return a prefix of `data` ending at a valid UTF-8 codepoint boundary.

    If the tail of `data` is a partial multi-byte sequence that has not yet
    been fully received (common when reading mid-flush from the log file),
    trim those bytes so UTF-8 decode succeeds cleanly and the next poll
    picks the sequence up from the start. The caller advances the read
    offset only to the returned length, not to the full raw size.
    """
    n = len(data)
    if n == 0:
        return data
    i = n - 1
    while i >= 0 and (data[i] & 0xC0) == 0x80:
        i -= 1
        if n - i > 3:
            return data
    if i < 0:
        return data
    b = data[i]
    if b < 0x80:
        return data
    if (b & 0xE0) == 0xC0:
        return data if n - i == 2 else data[:i]
    if (b & 0xF0) == 0xE0:
        return data if n - i == 3 else data[:i]
    if (b & 0xF8) == 0xF0:
        return data if n - i == 4 else data[:i]
    return data
