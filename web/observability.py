import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.log as log
from web.state import _config, _configs_by_host, primary_config, events_enabled


router = APIRouter()


@router.get("/api/claude/invocations")
def api_claude_invocations(
    limit: int = 200,
    status: str = "",
    function: str = "",
    model: str = "",
    q: str = "",
    since_hours: int = 24,
    all_instances: bool = False,
):
    import core.db as _db
    where = ["1=1"]
    params: list = []
    if not all_instances:
        instance_key = _config.get("job", {}).get("key", "")
        where.append("instance_key = ?")
        params.append(instance_key)
    if since_hours and since_hours > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
        where.append("started_at >= ?")
        params.append(cutoff)
    if status:
        where.append("status = ?")
        params.append(status)
    if function:
        where.append("function_name = ?")
        params.append(function)
    if model:
        where.append("model = ?")
        params.append(model)
    if q:
        where.append("(prompt LIKE ? OR output LIKE ? OR job_key LIKE ?)")
        like = f"%{q}%"
        params += [like, like, like]
    sql = (
        "SELECT id, instance_key, job_key, function_name, model, prompt_length, "
        "cwd, tools, timeout_s, started_at, finished_at, duration_ms, status, "
        "exit_code, output_length, substr(prompt, 1, 240) AS prompt_preview "
        f"FROM claude_invocations WHERE {' AND '.join(where)} "
        "ORDER BY started_at DESC LIMIT ?"
    )
    params.append(max(1, min(limit, 1000)))
    rows = _db.query_all(sql, tuple(params))
    return {"invocations": rows}


@router.get("/api/claude/invocations/{inv_id}")
def api_claude_invocation_detail(inv_id: str):
    import core.db as _db
    row = _db.query_one("SELECT * FROM claude_invocations WHERE id = ?", (inv_id,))
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    return row


@router.get("/api/events")
def api_events(limit: int = 100, after: str = "", unread: bool = False, since_hours: int = 0):
    if since_hours > 0 and not after:
        after = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
    return log.get_events(limit=limit, after=after or None, unread_only=unread)


@router.post("/api/events/{event_id}/dismiss")
def api_dismiss_event(event_id: str):
    log.dismiss(event_id)
    return {"status": "ok"}


@router.post("/api/events/dismiss-batch")
def api_dismiss_batch(body: dict):
    ids = body.get("ids") or []
    if not isinstance(ids, list):
        return JSONResponse({"error": "ids must be a list"}, status_code=400)
    added = log.dismiss_ids([str(i) for i in ids])
    return {"status": "ok", "added": added, "total": len(ids)}


@router.post("/api/events/dismiss-all")
def api_dismiss_all():
    log.dismiss_all()
    return {"status": "ok"}


_global_remote_cache: dict[str, tuple[float, dict]] = {}
_GLOBAL_REMOTE_TTL = 3.0


def _fetch_local_global_events(limit: int, unread_only: bool, after: str) -> list[dict]:
    out = []
    configs = list(_configs_by_host.values())
    primary = primary_config()
    if not configs and primary:
        configs = [primary]
    for config in configs:
        state_dir = config["_state_dir"]
        key = config["job"]["key"]
        log_tokens = log.use(state_dir, key)
        try:
            for ev in log.get_events(limit=limit, unread_only=unread_only, after=after or None):
                ev["instance_key"] = key
                ev["global_id"] = f"{key}:{ev['id']}"
                ev["base_url"] = config.get("_base_url") or config["job"].get("host", "")
                out.append(ev)
        finally:
            log.reset(log_tokens)
    return out


async def _fetch_remote_global_events(limit: int, unread_only: bool, since_hours: int) -> tuple[list[dict], dict[str, str]]:
    from core.discovery import discover_instances, call_instance

    local_key = _config.get("job", {}).get("key", "")
    remote = [i for i in discover_instances() if i["key"] != local_key]
    if not remote:
        return [], {}

    cache_key = f"{local_key}:{limit}:{unread_only}:{since_hours}"
    cached = _global_remote_cache.get(cache_key)
    if cached and (time.time() - cached[0]) < _GLOBAL_REMOTE_TTL:
        return cached[1]["events"], cached[1]["errors"]

    since_param = f"&since_hours={since_hours}" if since_hours > 0 else ""
    path = f"/api/events?limit={limit}&unread={'true' if unread_only else 'false'}{since_param}"
    errors: dict[str, str] = {}
    events: list[dict] = []

    async def _one(inst):
        result = await call_instance(inst["base_url"], "GET", path, timeout=3.0)
        return inst, result

    results = await asyncio.gather(*[_one(i) for i in remote], return_exceptions=True)
    for item in results:
        if isinstance(item, Exception):
            continue
        inst, payload = item
        if isinstance(payload, dict) and payload.get("error"):
            errors[inst["key"]] = str(payload["error"])[:200]
            continue
        rows = payload if isinstance(payload, list) else (payload.get("events") if isinstance(payload, dict) else None)
        if not isinstance(rows, list):
            errors[inst["key"]] = "unexpected response shape"
            continue
        for ev in rows:
            if not isinstance(ev, dict):
                continue
            ev["instance_key"] = inst["key"]
            ev["global_id"] = f"{inst['key']}:{ev.get('id', '')}"
            ev["base_url"] = inst["base_url"]
            events.append(ev)

    _global_remote_cache[cache_key] = (time.time(), {"events": events, "errors": errors})
    return events, errors


@router.get("/api/global/events")
async def api_global_events(limit: int = 5000, unread: bool = False, since_hours: int = 8):
    after = ""
    if since_hours > 0:
        after = (datetime.now(timezone.utc) - timedelta(hours=since_hours)).isoformat()
    local_events = _fetch_local_global_events(limit=limit, unread_only=unread, after=after)
    remote_events, errors = await _fetch_remote_global_events(limit=limit, unread_only=unread, since_hours=since_hours)
    merged = local_events + remote_events
    merged.sort(key=lambda e: e.get("ts") or "", reverse=True)
    return {"events": merged[:limit], "errors": errors}


SYSTEM_EVENTS = {"cycle_start", "cycle_end", "cycle_sleep"}


@router.get("/api/status")
def api_status():
    events = log.get_events(limit=500, unread_only=True)
    filtered = [ev for ev in events if ev["event"] not in SYSTEM_EVENTS]
    counts: dict[str, int] = {}
    for ev in filtered:
        t = ev["event"].split("_")[0]
        counts[t] = counts.get(t, 0) + 1

    slack_alive = False
    raw_path = _config.get("slack", {}).get("raw_path", "")
    if raw_path:
        try:
            mtime = os.path.getmtime(raw_path)
            slack_alive = (time.time() - mtime) < 120
        except OSError:
            pass

    return {
        "job": _config.get("job", {}),
        "features": _config.get("features", {}),
        "unread_total": len(filtered),
        "counts": counts,
        "slack_alive": slack_alive,
    }


@router.get("/api/config")
def api_config():
    return {
        "job": _config.get("job", {}),
        "features": _config.get("features", {}),
        "workspace": {
            "root": str(_config.get("workspace", {}).get("root", "")),
        },
        "run_commands": _config.get("workspace", {}).get("run_commands", []),
    }


@router.get("/api/config/raw")
def api_config_raw():
    config_path = _config.get("_config_path")
    if not config_path or not config_path.exists():
        return JSONResponse({"error": "config not found"}, status_code=404)
    return {"content": config_path.read_text(), "path": str(config_path)}


@router.post("/api/config/raw")
def api_config_raw_save(body: dict):
    config_path = _config.get("_config_path")
    if not config_path:
        return JSONResponse({"error": "config not found"}, status_code=404)
    config_path.write_text(body.get("content", ""))
    return {"ok": True}


@router.post("/api/poll")
def api_poll():
    try:
        import core.queue as _q
        _q.emit_event(source="manual", kind="cron_tick", payload={},
                      instance_key=_config.get("job", {}).get("key", ""))
        return {"status": "triggered"}
    except Exception as e:
        return {"status": "error", "error": str(e)}


_TERMINAL_JOB_STATUSES = {"ok", "failed", "skipped"}


def _trim_to_utf8_boundary(data: bytes) -> bytes:
    """Return a prefix of data that ends at a valid UTF-8 codepoint boundary.

    Walks up to 3 continuation bytes back from the end. If the trailing byte
    is a start byte of a multi-byte sequence we haven't fully received yet,
    trim those bytes so decode() with strict=False works cleanly and the
    next poll picks them up."""
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
    if (b & 0xE0) == 0xC0 and n - i == 2:
        return data
    if (b & 0xE0) == 0xC0:
        return data[:i]
    if (b & 0xF0) == 0xE0 and n - i == 3:
        return data
    if (b & 0xF0) == 0xE0:
        return data[:i]
    if (b & 0xF8) == 0xF0 and n - i == 4:
        return data
    if (b & 0xF8) == 0xF0:
        return data[:i]
    return data


def _pid_alive_for_job(instance_key: str, job_id: int) -> bool:
    from core.job_logs import job_pid_path
    pid_path = job_pid_path(instance_key, job_id)
    try:
        pid = int(pid_path.read_text().strip())
    except (OSError, ValueError):
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


@router.get("/api/tickets/{key}/jobs")
def api_ticket_jobs(key: str, limit: int = 100):
    if not events_enabled():
        return []
    import core.queue as q
    instance_key = _config.get("job", {}).get("key", "")
    rows = q.jobs_for_ticket(instance_key, key, limit)
    for row in rows:
        if row.get("status") == "running":
            row["pid_alive"] = _pid_alive_for_job(instance_key, row["id"])
    return rows


@router.get("/api/jobs/{job_id}/live")
def api_job_live(job_id: int, offset: int = 0):
    from core.job_logs import job_log_path
    import core.db as _db
    instance_key = _config.get("job", {}).get("key", "")
    row = _db.query_one(
        "SELECT status FROM jobs WHERE id=? AND instance_key=?",
        (job_id, instance_key),
    )
    if not row:
        return JSONResponse({"error": "not found"}, status_code=404)
    done = row["status"] in _TERMINAL_JOB_STATUSES
    pid_alive = _pid_alive_for_job(instance_key, job_id) if not done else None
    log_path = job_log_path(instance_key, job_id)
    if not log_path.exists():
        return {"content": "", "offset": 0, "done": done, "pid_alive": pid_alive}
    if offset < 0:
        return JSONResponse({"error": "negative offset"}, status_code=400)
    try:
        with open(log_path, "rb") as f:
            f.seek(offset)
            raw = f.read()
    except OSError as e:
        return JSONResponse({"error": str(e)}, status_code=500)
    trimmed = _trim_to_utf8_boundary(raw)
    return {
        "content": trimmed.decode("utf-8", errors="replace"),
        "offset": offset + len(trimmed),
        "done": done,
        "pid_alive": pid_alive,
    }
