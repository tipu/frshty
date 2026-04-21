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
