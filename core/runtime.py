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
import core.scheduler as scheduler
import core.tasks  # noqa: F401  (registers tasks + routes)
from core.beat import BeatThread
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


_beat: BeatThread | None = None


def start_events(
    instance_configs: list[dict],
    db_path: Path = DEFAULT_DB_PATH,
    migrations_dir: Path = DEFAULT_MIGRATIONS,
    worker_count: int = 4,
    cron_interval: int = 240,
    beat_interval: int = 60,
) -> Instances:
    global _started, _instances, _pool, _dispatcher, _beat
    with _started_lock:
        if _started:
            return _instances  # type: ignore[return-value]
        db.init(db_path, migrations_dir)
        _instances = Instances()
        for c in instance_configs:
            _instances.add(c)

        _seed_recurring_schedules(instance_configs)

        registries_by_key = {k: _instances.get(k) for k in _instances.keys()}
        _pool = WorkerPool(registries_by_key, size=worker_count)
        _dispatcher = Dispatcher(registries_by_key)
        _pool.start()
        _dispatcher.start()

        if beat_interval > 0:
            _beat = BeatThread(interval=beat_interval)
            _beat.start()

        if cron_interval > 0:
            cron = threading.Thread(target=_cron_ticker, args=(cron_interval,),
                                     daemon=True, name="cron-ticker")
            cron.start()

        log.emit("events_started",
                 f"event system up: {len(instance_configs)} instance(s), {worker_count} workers, "
                 f"cron={'off' if cron_interval <= 0 else f'{cron_interval}s'}, "
                 f"beat={'off' if beat_interval <= 0 else f'{beat_interval}s'}")
        _started = True
        return _instances  # type: ignore[return-value]


def _seed_recurring_schedules(instance_configs: list[dict]) -> None:
    """For each instance with features.billing or features.timesheet enabled,
    upsert a recurring scheduler row so the beat thread owns its firing."""
    from datetime import datetime as _dt
    from features.billing import _next_fire as billing_next_fire, FIRE_TZ as BILLING_TZ
    from zoneinfo import ZoneInfo as _ZI

    pst = _ZI("America/Los_Angeles")
    now_billing = _dt.now(BILLING_TZ)
    now_pst = _dt.now(pst)

    for c in instance_configs:
        key = c["job"]["key"]
        feats = c.get("features", {})
        if feats.get("billing"):
            freq = c.get("billing", {}).get("billing_freq", "weekly")
            next_fire = billing_next_fire(now_billing, freq)
            if next_fire is not None:
                scheduler.upsert_recurring(key, "billing_check", "billing_check",
                                            cadence=freq, next_run_at=next_fire)
        if feats.get("timesheet"):
            candidate = now_pst.replace(hour=19, minute=0, second=0, microsecond=0)
            if candidate <= now_pst:
                from datetime import timedelta as _td
                candidate = candidate + _td(days=1)
            scheduler.upsert_recurring(key, "timesheet_check", "timesheet_check",
                                        cadence="daily_19pst", next_run_at=candidate)


def stop_events() -> None:
    global _started
    _cron_stop.set()
    if _beat:
        _beat.stop()
    if _pool:
        _pool.stop()
    if _dispatcher:
        _dispatcher.stop()
    _started = False
