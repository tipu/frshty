import atexit
import sqlite3
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.log as log
from core.paths import db_path

SKIP_EXACT = frozenset({
    "/api/tickets/list",
    "/api/events",
    "/api/config",
    "/api/status",
    "/api/scheduled",
    "/api/usage/ui",
    "/api/usage/report",
})
SKIP_PREFIXES = ("/static/", "/favicon")

_MAX_NAME_LEN = 200
_MAX_COUNT = 1_000_000
_FLUSH_INTERVAL_S = 5.0

_pending: dict[tuple[str, str, str, str], int] = {}
_pending_lock = threading.Lock()
_flush_lock = threading.Lock()
_flusher: threading.Thread | None = None


def is_tracked(path: str) -> bool:
    if path in SKIP_EXACT:
        return False
    return not path.startswith(SKIP_PREFIXES)


def ensure_db() -> None:
    if getattr(db, "_DB_PATH", None) is None:
        migrations = Path(__file__).resolve().parent.parent / "migrations"
        db.init(db_path(), migrations)


def record(kind: str, name: str, instance: str = "", n: int = 1) -> None:
    """Count one use in memory. A background thread writes the counts.

    No caller waits on the database. A response that awaited this write
    stalled for seconds whenever the host disk was slow.
    """
    name = " ".join(name.split())[:_MAX_NAME_LEN]
    if not name or n < 1 or n > _MAX_COUNT:
        return
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    key = (instance, kind, name, day)
    with _pending_lock:
        _pending[key] = _pending.get(key, 0) + n
    _ensure_flusher()


def _ensure_flusher() -> None:
    global _flusher
    if _flusher is not None and _flusher.is_alive():
        return
    with _pending_lock:
        if _flusher is not None and _flusher.is_alive():
            return
        _flusher = threading.Thread(target=_flush_loop, name="usage-flush", daemon=True)
        _flusher.start()


def _flush_loop() -> None:
    while True:
        time.sleep(_FLUSH_INTERVAL_S)
        flush()


def flush() -> None:
    with _flush_lock:
        with _pending_lock:
            batch = dict(_pending)
            _pending.clear()
        if not batch:
            return
        try:
            with db.tx() as c:
                for (instance, kind, name, day), n in batch.items():
                    c.execute(
                        "INSERT INTO usage_counters(instance, kind, name, day, count) "
                        "VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT(instance, kind, name, day) "
                        "DO UPDATE SET count = count + excluded.count",
                        (instance, kind, name, day, n),
                    )
        except Exception as e:
            if isinstance(e, sqlite3.OperationalError):
                with _pending_lock:
                    for key, n in batch.items():
                        _pending[key] = _pending.get(key, 0) + n
            log.emit("usage_record_failed",
                     f"could not record {len(batch)} usage counters: {type(e).__name__}: {e}",
                     meta={"counters": len(batch)})


atexit.register(flush)


def aggregates(kind: str) -> list[dict]:
    flush()
    rows = db.query_all(
        "SELECT name, SUM(count) AS count, MIN(day) AS first_day, "
        "MAX(day) AS last_day, COUNT(DISTINCT day) AS days_used, "
        "GROUP_CONCAT(DISTINCT instance) AS instances "
        "FROM usage_counters WHERE kind = ? "
        "GROUP BY name ORDER BY count DESC",
        (kind,),
    )
    for r in rows:
        r["instances"] = sorted(x for x in (r["instances"] or "").split(",") if x)
    return rows


def tracking_since() -> str | None:
    flush()
    row = db.query_one("SELECT MIN(day) AS d FROM usage_counters")
    return row["d"] if row else None
