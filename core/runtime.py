"""Boot helpers for the event-driven worker system.

Call start_events(instance_configs) to initialize the shared SQLite DB, build
the Instances registry, start the dispatcher, and start the worker pool.

Safe to call once per process. Re-entrancy is guarded.
"""
from __future__ import annotations
import os
import random
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.log as log
import core.queue as q
import core.scheduler as scheduler
import core.tasks  # noqa: F401  (registers tasks + routes)
import core.tz as _ctz
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


_DEFAULT_QUIET_HOURS = (20, 7)
_DEFAULT_QUIET_CADENCE = 1800
# Active-hours base cadence. Historically the ticker emitted on a fixed
# random 300-420s wake; _DEFAULT_TICK_INTERVAL preserves that ~6 min default.
# A workspace can opt into a faster pulse with [job].tick_interval (seconds).
_DEFAULT_TICK_INTERVAL = 360
# Floor on how often the ticker thread itself wakes. The base sleep is the
# smallest per-instance interval, clamped here so a misconfigured 1s value
# can't busy-spin the thread.
_MIN_TICK_FLOOR = 15


def _quiet_hours_for(config: dict) -> tuple[int, int] | None:
    raw = (config.get("job") or {}).get("quiet_hours", list(_DEFAULT_QUIET_HOURS))
    if not raw:
        return None
    return (int(raw[0]), int(raw[1]))


def _quiet_cadence_for(config: dict) -> int:
    return int((config.get("job") or {}).get("quiet_cadence", _DEFAULT_QUIET_CADENCE))


def _tick_interval_for(config: dict) -> int:
    return int((config.get("job") or {}).get("tick_interval", _DEFAULT_TICK_INTERVAL))


def _in_quiet_hours(now_local: datetime, quiet: tuple[int, int]) -> bool:
    start, end = quiet
    if start == end:
        return False
    h = now_local.hour
    if start < end:
        return start <= h < end
    return h >= start or h < end


def _should_emit_cron(config: dict, last_emit_ts: float, now_ts: float, now_local: datetime) -> bool:
    quiet = _quiet_hours_for(config)
    elapsed = now_ts - last_emit_ts
    if quiet is None or not _in_quiet_hours(now_local, quiet):
        # Active hours: honour the per-workspace tick interval so a fast
        # workspace (e.g. tick_interval=30) emits every ~30s while a default
        # one stays at ~6 min, even though the ticker thread wakes more often.
        return elapsed >= _tick_interval_for(config)
    return elapsed >= _quiet_cadence_for(config)


def _ticker_sleep_seconds() -> int:
    """Base sleep for the ticker thread: the smallest per-instance tick
    interval (so the fastest workspace can actually fire at its cadence),
    floored to avoid busy-spinning. Each instance is still gated by
    _should_emit_cron, so waking often is cheap when nothing is due."""
    intervals = [_DEFAULT_TICK_INTERVAL]
    if _instances is not None:
        for k in _instances.keys():
            try:
                intervals.append(_tick_interval_for(_instances.get(k).config))
            except Exception:
                pass
    return max(_MIN_TICK_FLOOR, min(intervals))


def _cron_ticker(interval: int = 240) -> None:
    last_emit: dict[str, float] = {}
    while not _cron_stop.is_set():
        if _instances is not None:
            now_ts = time.time()
            now_local = datetime.now(_ctz.local_tz())
            for instance_key in _instances.keys():
                try:
                    config = _instances.get(instance_key).config
                    if not _should_emit_cron(config, last_emit.get(instance_key, 0.0), now_ts, now_local):
                        continue
                    q.emit_event(source="cron", kind="cron_tick",
                                  payload={"at": datetime.now(timezone.utc).isoformat()},
                                  instance_key=instance_key)
                    last_emit[instance_key] = now_ts
                except Exception as e:
                    log.emit("cron_emit_error", f"{type(e).__name__}: {e}")
        wait_time = _ticker_sleep_seconds() if interval > 0 else interval
        if _cron_stop.wait(wait_time):
            return


_beat: BeatThread | None = None


def _enforce_git_identity(instance_configs: list[dict]) -> set[str]:
    """Write and verify each instance's commit identity, and report the
    instances that could not prove theirs.

    frshty is not the only committer in its worktrees — the headless agent and
    the tmux panes run their own `git commit` — so the identity is written to
    each repo's local config, which all of them read, rather than passed around
    our own subprocesses. An instance that cannot prove the identity is not
    loaded at all: a commit authored by the wrong person is not recoverable
    once the branch is pushed. Instances without a [git] block keep whatever
    the machine already gives them and are never gated.

    Two passes on purpose. Everything is resolved and every collision is known
    before the first repo is written, so a config that will be rejected never
    leaves a half-applied identity behind.

    Two gaps remain by choice. A repo that appears under projects_dir after
    startup is not configured until the next restart, and a raw `git push` run
    by a person or the agent skips the pre-push guard in features.platforms.
    Neither is worth chasing: the identity lives in repo-local config, so every
    later `git commit` in a configured repo reads it whoever runs it."""
    from core.config import get_repos
    from core.git_util import configure_repo_identity, git_common_dir

    failed: set[str] = set()
    planned: list[tuple[str, dict, str, str, str]] = []
    claimed: dict[str, list[tuple[str, str, str]]] = {}

    for config in instance_configs:
        key = (config.get("job") or {}).get("key") or "<unknown>"
        if "git" not in config:
            continue
        git_cfg = config.get("git") or {}
        name, email = git_cfg.get("name", ""), git_cfg.get("email", "")
        if not (name and email):
            # A typo in either field must not read as "no identity wanted".
            log.emit("git_identity_incomplete",
                     f"[{key}] [git] needs both name and email; got "
                     f"name={name!r} email={email!r}",
                     meta={"instance_key": key})
            failed.add(key)
            continue
        want = f"{name} <{email}>"
        injected = sorted(
            k for k in ((config.get("llm") or {}).get("claude") or {}).get("env", {})
            if str(k).startswith(("GIT_AUTHOR_", "GIT_COMMITTER_"))
        )
        if injected:
            # The agent commits inside the worktree, so anything we hand its
            # process outranks the repo config we just wrote.
            log.emit("git_identity_env_conflict",
                     f"[{key}] [llm.claude.env] sets {', '.join(injected)}, which "
                     f"overrides the identity this instance declares",
                     meta={"instance_key": key, "keys": injected})
            failed.add(key)
            continue
        try:
            repos = get_repos(config)
        except Exception as e:
            log.emit("git_identity_error",
                     f"[{key}] could not list repos: {type(e).__name__}: {e}",
                     meta={"instance_key": key})
            failed.add(key)
            continue
        for repo in repos:
            try:
                common = git_common_dir(repo["path"])
            except Exception as e:
                log.emit("git_identity_error",
                         f"[{key}] {repo['name']}: {type(e).__name__}: {e}",
                         meta={"instance_key": key, "repo": repo["name"]})
                failed.add(key)
                continue
            if common:
                claimed.setdefault(common, []).append((key, want, repo["name"]))
            planned.append((key, repo, name, email, want))

    for common, claimants in claimed.items():
        wants = {want for _, want, _ in claimants}
        if len(wants) < 2:
            continue
        # Every claimant fails, not just the newest: one config file cannot hold
        # two identities, and there is no way to tell which claimant is right.
        for key, want, repo_name in claimants:
            log.emit("git_identity_collision",
                     f"[{key}] {repo_name} shares the git directory {common} with "
                     f"other instances claiming {sorted(wants - {want})}; this "
                     f"instance claims {want!r}. One clone cannot hold two "
                     f"identities.",
                     meta={"instance_key": key, "repo": repo_name,
                           "common_dir": common, "claims": sorted(wants)})
            failed.add(key)

    for key, repo, name, email, want in planned:
        if key in failed:
            continue
        try:
            ok, detail = configure_repo_identity(repo["path"], name, email)
        except Exception as e:
            log.emit("git_identity_error",
                     f"[{key}] {repo['name']}: {type(e).__name__}: {e}",
                     meta={"instance_key": key, "repo": repo["name"]})
            failed.add(key)
            continue
        if ok:
            log.emit("git_identity_set",
                     f"[{key}] {repo['name']} commits as {detail}",
                     meta={"instance_key": key, "repo": repo["name"],
                           "category": "noise"})
        else:
            log.emit("git_identity_unverified",
                     f"[{key}] {repo['name']}: {detail}",
                     meta={"instance_key": key, "repo": repo["name"]})
            failed.add(key)
    return failed


def start_events(
    instance_configs: list[dict],
    db_path: Path = DEFAULT_DB_PATH,
    migrations_dir: Path = DEFAULT_MIGRATIONS,
    worker_count: int = int(os.environ.get("FRSHTY_WORKER_COUNT", "4")),
    cron_interval: int = 240,
    beat_interval: int = 60,
) -> Instances:
    global _started, _instances, _pool, _dispatcher, _beat
    with _started_lock:
        if _started:
            return _instances  # type: ignore[return-value]
        db.init(db_path, migrations_dir)
        try:
            db.execute(
                "UPDATE claude_invocations SET status='error', finished_at=?, "
                "output=COALESCE(output,'') || '[killed by restart]' "
                "WHERE status IN ('running','queued')",
                (datetime.now(timezone.utc).isoformat(),),
            )
        except Exception as e:
            log.emit("claude_invocation_recovery_failed",
                     f"could not mark stuck claude invocations: {e}")
        unverified = _enforce_git_identity(instance_configs)
        for key in sorted(unverified):
            try:
                dropped = scheduler.delete_instance(key)
                if dropped:
                    log.emit("scheduler_rows_dropped",
                             f"[{key}] removed {dropped} scheduled row(s); the "
                             f"instance is not loaded, so beat would fire them "
                             f"into nothing",
                             meta={"instance_key": key, "rows": dropped})
            except Exception as e:
                log.emit("scheduler_cleanup_failed",
                         f"[{key}] could not drop scheduled rows: "
                         f"{type(e).__name__}: {e}",
                         meta={"instance_key": key})
            log.emit("instance_not_loaded",
                     f"[{key}] not loaded: its commit identity could not be "
                     f"verified. No job runs and no host routes to it until "
                     f"the identity is fixed and frshty restarts.",
                     meta={"instance_key": key})
        instance_configs = [
            c for c in instance_configs
            if ((c.get("job") or {}).get("key") or "<unknown>") not in unverified
        ]
        _instances = Instances()
        for c in instance_configs:
            _instances.add(c)

        try:
            from core.preflight import run_preflight
            run_preflight(instance_configs)
        except Exception as e:
            log.emit("preflight_error",
                     f"preflight stage crashed: {type(e).__name__}: {e}")

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
    import core.tz as _ctz

    local_tz = _ctz.local_tz()
    now_billing = _dt.now(BILLING_TZ)
    now_pst = _dt.now(local_tz)

    for c in instance_configs:
        key = c["job"]["key"]
        feats = c.get("features", {})
        if feats.get("billing"):
            freq = c.get("billing", {}).get("billing_freq", "weekly")
            next_fire = billing_next_fire(now_billing, freq)
            if next_fire is not None:
                scheduler.upsert_recurring(key, "billing_check", "billing_check",
                                            cadence=freq, next_run_at=next_fire)
        else:
            scheduler.delete(key, "billing_check")
        if feats.get("timesheet"):
            candidate = now_pst.replace(hour=19, minute=0, second=0, microsecond=0)
            if candidate <= now_pst:
                from datetime import timedelta as _td
                candidate = candidate + _td(days=1)
            scheduler.upsert_recurring(key, "timesheet_check", "timesheet_check",
                                        cadence="daily_19pst", next_run_at=candidate)
        else:
            scheduler.delete(key, "timesheet_check")
        pm_cfg = c.get("pm_agent") or {}
        prd_enabled = (c.get("prd") or {}).get("enabled")
        if pm_cfg.get("enabled", True) and prd_enabled:
            cadence = pm_cfg.get("post_shipping_cadence", "weekly")
            from datetime import timedelta as _td
            next_fire = now_billing + (_td(weeks=1) if cadence == "weekly" else _td(days=1))
            scheduler.upsert_recurring(key, "pm_post_shipping", "pm_post_shipping",
                                        cadence=cadence, next_run_at=next_fire)
        else:
            scheduler.delete(key, "pm_post_shipping")
        mgr_cfg = c.get("manager") or {}
        if mgr_cfg.get("enabled"):
            cadence = mgr_cfg.get("cadence", "daily_9_local")
            hour = 9
            if cadence.startswith("daily_") and cadence.endswith("_local"):
                try:
                    hour = int(cadence[len("daily_"):-len("_local")])
                except ValueError:
                    hour = 9
            candidate = now_pst.replace(hour=hour, minute=0, second=0, microsecond=0)
            if candidate <= now_pst:
                from datetime import timedelta as _td
                candidate = candidate + _td(days=1)
            scheduler.upsert_recurring(key, "manager_daily_digest", "manager_daily_digest",
                                        cadence=cadence, next_run_at=candidate)
        else:
            scheduler.delete(key, "manager_daily_digest")
        today_cfg = c.get("today_agent") or {}
        if today_cfg.get("enabled"):
            cadence = today_cfg.get("cadence", "daily_8_local")
            hour = 8
            if cadence.startswith("daily_") and cadence.endswith("_local"):
                try:
                    hour = int(cadence[len("daily_"):-len("_local")])
                except ValueError:
                    hour = 8
            candidate = now_pst.replace(hour=hour, minute=0, second=0, microsecond=0)
            if candidate <= now_pst:
                from datetime import timedelta as _td
                candidate = candidate + _td(days=1)
            scheduler.upsert_recurring(key, "today_agent_tick", "today_agent_tick",
                                        cadence=cadence, next_run_at=candidate)
        else:
            scheduler.delete(key, "today_agent_tick")


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
