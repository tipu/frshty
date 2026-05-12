#!/usr/bin/env python3
import contextlib
import sys
from pathlib import Path

import uvicorn
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

import core.config as cfg
import core.events as events
import core.log as log
import core.state as state
from actions.record_demo import handle as _record_demo_action
from actions.schedule_pr import handle as _schedule_pr_action

events.register_action("record_demo", _record_demo_action)
events.register_action("schedule_pr", _schedule_pr_action)

STATIC_DIR = Path(__file__).parent / "static"

from web.state import _config, _configs_by_host, set_primary_config as _set_primary_config, ensure_path as _ensure_path
import web.middleware as _middleware
from web.pages import router as _pages_router
from web.slack import router as _slack_router
from web.timesheet import router as _timesheet_router
from web.billing import router as _billing_router
from web.scheduling import router as _scheduling_router
from web.prd import router as _prd_router
from web.manager import router as _manager_router
from web.today import router as _today_router
from web.observability import router as _observability_router
from web.reviews import router as _reviews_router
from web.tickets import router as _tickets_router


if len(sys.argv) >= 2 and Path(sys.argv[1]).is_file():
    _primary = cfg.load_config(sys.argv[1])
    _set_primary_config(_primary)
    import core.llm as _llm
    _llm.configure(_primary)
    state.init(_primary["_state_dir"])
    log.init(_primary["_state_dir"], _primary["job"]["key"])
    # db.init will be called by start_events() in main()


@contextlib.asynccontextmanager
async def _lifespan(a):
    # Event system started in main() before uvicorn
    yield


app = FastAPI(lifespan=_lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
_middleware.install(app)

app.include_router(_pages_router)
app.include_router(_slack_router)
app.include_router(_timesheet_router)
app.include_router(_billing_router)
app.include_router(_scheduling_router)
app.include_router(_prd_router)
app.include_router(_manager_router)
app.include_router(_today_router)
app.include_router(_observability_router)
app.include_router(_reviews_router)
app.include_router(_tickets_router)


def main():
    import argparse
    parser = argparse.ArgumentParser(prog="frshty.py")
    parser.add_argument("config", nargs="?", help="single-instance config.toml path")
    parser.add_argument("--multi", nargs="+", metavar="CONFIG",
                        help="boot all listed configs in one process with shared event system")
    parser.add_argument("--port", type=int, default=None,
                        help="override listen port (multi mode default: first config's port)")
    parser.add_argument("--host", default=None, help="override bind host")
    args = parser.parse_args()

    if not args.config and not args.multi:
        parser.error("pass a config path or --multi <config1> <config2> ...")

    _ensure_path()

    configs = [cfg.load_config(p) for p in (args.multi or [args.config])]
    import core.llm as _llm
    for c in configs:
        _llm.configure(c)
    primary = configs[0]
    _set_primary_config(primary)
    # Fully event-driven: all periodic work via cron_tick -> Dispatcher -> WorkerPool
    try:
        import core.runtime as _rt
        _rt.start_events(configs, cron_interval=240)
    except Exception as e:
        log.emit("events_boot_failed", f"{type(e).__name__}: {e}")
        raise


    if args.multi:
        for c in configs:
            host = c.get("job", {}).get("host", "")
            if host.startswith("http://"):
                host = host[len("http://"):]
            elif host.startswith("https://"):
                host = host[len("https://"):]
            host = host.split(":")[0].split("/")[0].lower()
            if host:
                if host in _configs_by_host:
                    raise ValueError(f"hostname {host} claimed by two configs")
                _configs_by_host[host] = c

    state.init(primary["_state_dir"])
    log.init(primary["_state_dir"], primary["job"]["key"])

    port = args.port or _config["job"]["port"]

    host = args.host or _config["job"].get("bind", "127.0.0.1")
    # Always disable reload: event system (started before uvicorn) handles cron internally
    reload = False
    src = Path(__file__).parent
    reload_dirs = [str(src / d) for d in ("core", "features", "templates") if (src / d).exists()] if reload else None
    log_level = _config["job"].get("log_level", "info")
    if args.multi:
        # Pass app object directly so _configs_by_host populated in __main__ is
        # visible to middleware (vs. uvicorn re-importing frshty as a fresh module).
        uvicorn.run(app, host=host, port=port, log_level=log_level,
                    ws_ping_interval=60, ws_ping_timeout=60)
    else:
        uvicorn.run("frshty:app", host=host, port=port, log_level=log_level,
                    reload=reload, reload_dirs=reload_dirs,
                    ws_ping_interval=60, ws_ping_timeout=60)


if __name__ == "__main__":
    main()
