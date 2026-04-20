"""Boot helpers for the event-driven worker system.

Call start_events(instance_configs) to initialize the shared SQLite DB, build
the Instances registry, start the dispatcher, and start the worker pool.

Safe to call once per process. Re-entrancy is guarded.
"""
from __future__ import annotations
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.log as log
import core.queue as q
import core.tasks  # noqa: F401  (registers tasks + routes)
from core.event_bus import Dispatcher
from core.registry import Instances
from core.worker import WorkerPool

DEFAULT_DB_PATH = Path.home() / ".frshty" / "frshty.db"
DEFAULT_MIGRATIONS = Path(__file__).resolve().parent.parent / "migrations"

_started_lock = threading.Lock()
_started = False
_instances: Instances | None = None
_pool: WorkerPool | None = None
_dispatcher: Dispatcher | None = None
_cron_stop = threading.Event()


def instances() -> Instances | None:
    return _instances


def pool() -> WorkerPool | None:
    return _pool


def _cron_ticker(interval: int = 240) -> None:
    while not _cron_stop.is_set():
        if _instances is not None:
            for instance_key in _instances.keys():
                try:
                    q.emit_event(source="cron", kind="cron_tick",
                                  payload={"at": datetime.now(timezone.utc).isoformat()},
                                  instance_key=instance_key)
                except Exception as e:
                    log.emit("cron_emit_error", f"{type(e).__name__}: {e}")
        if _cron_stop.wait(interval):
            return


def start_events(
    instance_configs: list[dict],
    db_path: Path = DEFAULT_DB_PATH,
    migrations_dir: Path = DEFAULT_MIGRATIONS,
    worker_count: int = 4,
    cron_interval: int = 240,
) -> Instances:
    global _started, _instances, _pool, _dispatcher
    with _started_lock:
        if _started:
            return _instances  # type: ignore[return-value]
        db.init(db_path, migrations_dir)
        _instances = Instances()
        for c in instance_configs:
            _instances.add(c)

        registries_by_key = {k: _instances.get(k) for k in _instances.keys()}
        _pool = WorkerPool(registries_by_key, size=worker_count)
        _dispatcher = Dispatcher(registries_by_key)
        _pool.start()
        _dispatcher.start()

        cron = threading.Thread(target=_cron_ticker, args=(cron_interval,),
                                 daemon=True, name="cron-ticker")
        cron.start()

        log.emit("events_started",
                 f"event system up: {len(instance_configs)} instance(s), {worker_count} workers, cron={cron_interval}s")
        _started = True
        return _instances


def stop_events() -> None:
    global _started
    _cron_stop.set()
    if _pool:
        _pool.stop()
    if _dispatcher:
        _dispatcher.stop()
    _started = False
