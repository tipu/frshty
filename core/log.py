import fcntl
import json
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

_job_key: str = ""
_log_path: Path | None = None
_read_state_path: Path | None = None


def init(state_dir: Path, job_key: str):
    global _job_key, _log_path, _read_state_path
    _job_key = job_key
    logs_dir = state_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    _log_path = logs_dir / f"{job_key}.jsonl"
    _read_state_path = logs_dir / "read_state.json"


def emit(event: str, summary: str, links: dict | None = None, meta: dict | None = None):
    clean_links = {k: v for k, v in (links or {}).items() if v}
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "job": _job_key,
        "event": event,
        "id": uuid4().hex[:12],
        "summary": summary,
        "links": clean_links,
        "meta": meta or {},
    }
    with open(_log_path, "a") as f:
        f.write(json.dumps(record) + "\n")
    print(f"[{_job_key}] {event}: {summary}", flush=True)
    return record


MAX_LOG_LINES = 2000


def get_events(limit: int = 100, after: str | None = None, unread_only: bool = False) -> list[dict]:
    if not _log_path or not _log_path.exists():
        return []
    dismissed = _load_read_state()
    lines = _log_path.read_text().splitlines()
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
    if not _log_path or not _log_path.exists():
        return
    lock_path = _log_path.parent / "log.lock"
    with open(lock_path, "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        lines = _log_path.read_text().splitlines()
        ids = set()
        for line in lines:
            try:
                ev = json.loads(line)
                ids.add(ev["id"])
            except (json.JSONDecodeError, KeyError):
                continue
        if len(lines) > MAX_LOG_LINES:
            _log_path.write_text("\n".join(lines[-MAX_LOG_LINES:]) + "\n")
            retained = set(lines[-MAX_LOG_LINES:])
            ids = {eid for eid in ids if any(eid in l for l in retained)}
    _save_read_state(ids)


def _read_state_lock_path():
    return _read_state_path.parent / "read_state.lock"


def _load_read_state() -> set:
    if not _read_state_path or not _read_state_path.exists():
        return set()
    with open(_read_state_lock_path(), "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_SH)
        return set(json.loads(_read_state_path.read_text()))


def _save_read_state(ids: set):
    import os, tempfile
    with open(_read_state_lock_path(), "w") as lf:
        fcntl.flock(lf, fcntl.LOCK_EX)
        tmp = None
        try:
            with tempfile.NamedTemporaryFile(mode="w", dir=str(_read_state_path.parent), suffix=".tmp", delete=False) as f:
                tmp = f.name
                json.dump(list(ids), f)
            os.replace(tmp, str(_read_state_path))
        except Exception:
            if tmp and os.path.exists(tmp):
                os.unlink(tmp)
            raise
