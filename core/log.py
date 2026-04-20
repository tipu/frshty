import fcntl
import json
from contextvars import ContextVar
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

_default_job_key: str = ""
_default_log_path: Path | None = None
_default_read_state_path: Path | None = None

_job_key_cv: ContextVar[str | None] = ContextVar("frshty_log_job_key", default=None)
_log_path_cv: ContextVar[Path | None] = ContextVar("frshty_log_path", default=None)
_read_state_path_cv: ContextVar[Path | None] = ContextVar("frshty_log_read_state", default=None)


def init(state_dir: Path, job_key: str):
    global _default_job_key, _default_log_path, _default_read_state_path
    _default_job_key = job_key
    logs_dir = state_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    _default_log_path = logs_dir / f"{job_key}.jsonl"
    _default_read_state_path = logs_dir / "read_state.json"


def use(state_dir: Path, job_key: str):
    logs_dir = state_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    k_token = _job_key_cv.set(job_key)
    l_token = _log_path_cv.set(logs_dir / f"{job_key}.jsonl")
    r_token = _read_state_path_cv.set(logs_dir / "read_state.json")
    return (k_token, l_token, r_token)


def reset(tokens):
    k, l, r = tokens
    _job_key_cv.reset(k)
    _log_path_cv.reset(l)
    _read_state_path_cv.reset(r)


def _active_job_key() -> str:
    k = _job_key_cv.get()
    return k if k is not None else _default_job_key


def _active_log_path() -> Path | None:
    p = _log_path_cv.get()
    return p if p is not None else _default_log_path


def _active_read_state_path() -> Path | None:
    p = _read_state_path_cv.get()
    return p if p is not None else _default_read_state_path


def emit(event: str, summary: str, links: dict | None = None, meta: dict | None = None):
    clean_links = {k: v for k, v in (links or {}).items() if v}
    job = _active_job_key()
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "job": job,
        "event": event,
        "id": uuid4().hex[:12],
        "summary": summary,
        "links": clean_links,
        "meta": meta or {},
    }
    log_path = _active_log_path()
    if log_path is not None:
        with open(log_path, "a") as f:
            f.write(json.dumps(record) + "\n")
    print(f"[{job}] {event}: {summary}", flush=True)
    return record


MAX_LOG_LINES = 2000


def get_events(limit: int = 100, after: str | None = None, unread_only: bool = False) -> list[dict]:
    log_path = _active_log_path()
    if not log_path or not log_path.exists():
        return []
    dismissed = _load_read_state()
    lines = log_path.read_text().splitlines()
    lines = lines[-MAX_LOG_LINES:] if len(lines) > MAX_LOG_LINES else lines
    events = []
    for line in lines:
        if not line.strip():
            continue
        try:
            ev = json.loads(line)
        except json.JSONDecodeError:
            continue
        if after and ev["ts"] <= after:
            continue
        ev["read"] = ev["id"] in dismissed
        if unread_only and ev["read"]:
            continue
        events.append(ev)
    events.reverse()
    return events[:limit]


def dismiss(event_id: str):
    dismissed = _load_read_state()
    dismissed.add(event_id)
    _save_read_state(dismissed)


def dismiss_all():
    log_path = _active_log_path()
    if not log_path or not log_path.exists():
        return
    lock_path = log_path.parent / "log.lock"
    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        lines = log_path.read_text().splitlines()
        ids = set()
        for line in lines:
            try:
                ev = json.loads(line)
                ids.add(ev["id"])
            except (json.JSONDecodeError, KeyError):
                continue
        if len(lines) > MAX_LOG_LINES:
            log_path.write_text("\n".join(lines[-MAX_LOG_LINES:]) + "\n")
            retained = set(lines[-MAX_LOG_LINES:])
            ids = {eid for eid in ids if any(eid in l for l in retained)}
    _save_read_state(ids)


def _read_state_lock_path():
    p = _active_read_state_path()
    return p.parent / "read_state.lock" if p else None


def _load_read_state() -> set:
    p = _active_read_state_path()
    if not p or not p.exists():
        return set()
    lock = _read_state_lock_path()
    if lock is None:
        return set()
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_SH)
        return set(json.loads(p.read_text()))


def _save_read_state(ids: set):
    import os, tempfile
    p = _active_read_state_path()
    if p is None:
        return
    lock = _read_state_lock_path()
    if lock is None:
        return
    with open(lock, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=str(p.parent), suffix=".tmp", delete=False) as f:
                tmp = f.name
                json.dump(list(ids), f)
            os.replace(tmp, str(p))
        except Exception:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
            raise
