import asyncio
import json
import logging
import signal
import subprocess
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

from core.discovery import discover_instances, fan_out, call_instance
from core.claude_runner import run_claude_code
import core.db as _db

STATE_DIR = Path.home() / ".frshty"
STATE_FILE = STATE_DIR / "supervisor.json"
LOG_FILE = STATE_DIR / "supervisor.log"
PROJECT_DIR = Path(__file__).parent

POLL_INTERVAL = 120
AUTOFIX_COOLDOWN = 300
ESCALATION_COOLDOWN = 1800
STUCK_THRESHOLD = 1800
STALE_CYCLE_THRESHOLD = 900
MAX_AUTOFIX_ATTEMPTS = 3

log = logging.getLogger("supervisor")


def _setup_logging():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    log.addHandler(fh)
    log.addHandler(sh)
    log.setLevel(logging.INFO)


def _load_state() -> dict:
    try:
        actions = {r["key"]: {"count": r["count"], "ts": r["ts"]}
                   for r in _db.query_all("SELECT key, count, ts FROM supervisor_actions")}
        escalations = {r["key"]: r["ts"]
                       for r in _db.query_all("SELECT key, ts FROM supervisor_escalations")}
        return {"actions": actions, "escalations": escalations}
    except Exception:
        pass
    return {"actions": {}, "escalations": {}}


def _save_state(state: dict):
    with _db.tx() as conn:
        conn.execute("DELETE FROM supervisor_actions")
        conn.execute("DELETE FROM supervisor_escalations")
        for k, v in state.get("actions", {}).items():
            conn.execute("INSERT INTO supervisor_actions(key, count, ts) VALUES(?,?,?)", (k, v["count"], v["ts"]))
        for k, ts in state.get("escalations", {}).items():
            conn.execute("INSERT INTO supervisor_escalations(key, ts) VALUES(?,?)", (k, ts))


def _now() -> float:
    return time.time()


def _action_key(instance: str, ticket: str = "", problem: str = "") -> str:
    return f"{instance}:{ticket}:{problem}"


def _can_autofix(state: dict, key: str) -> bool:
    last = state["actions"].get(key, {})
    if last.get("count", 0) >= MAX_AUTOFIX_ATTEMPTS:
        return False
    if _now() - last.get("ts", 0) < AUTOFIX_COOLDOWN:
        return False
    return True


def _record_autofix(state: dict, key: str):
    entry = state["actions"].setdefault(key, {"count": 0, "ts": 0})
    entry["count"] = entry.get("count", 0) + 1
    entry["ts"] = _now()


def _can_escalate(state: dict, key: str) -> bool:
    last_ts = state["escalations"].get(key, 0)
    return _now() - last_ts > ESCALATION_COOLDOWN


def _record_escalation(state: dict, key: str):
    state["escalations"][key] = _now()


async def _detect_problems(instances: list[dict]) -> list[dict]:
    problems = []

    statuses = await fan_out(instances, "GET", "/api/status")
    events_all = await fan_out(instances, "GET", "/api/events?unread=true&limit=50")

    for inst in instances:
        key = inst["key"]
        status = statuses.get(key, {})
        events = events_all.get(key, [])

        if "error" in status:
            problems.append({"type": "unresponsive", "instance": key, "error": status["error"]})
            continue

        if isinstance(events, list):
            for ev in events:
                if "error" in ev.get("event", ""):
                    problems.append({"type": "error_event", "instance": key, "event": ev})

    return problems


async def _autofix(problem: dict, instances: list[dict], state: dict) -> bool:
    ptype = problem["type"]
    inst_key = problem["instance"]
    inst = next((i for i in instances if i["key"] == inst_key), None)
    if not inst:
        return False

    ak = _action_key(inst_key, problem.get("ticket", ""), ptype)
    if not _can_autofix(state, ak):
        return False

    if ptype == "unresponsive":
        log.info(f"[{inst_key}] instance unresponsive, attempting restart")
        config_path = inst["config_path"]
        try:
            subprocess.run(
                ["lsof", "-ti", f":{inst['port']}"],
                capture_output=True, text=True, timeout=5,
            )
            subprocess.run(
                ["fuser", "-k", f"{inst['port']}/tcp"],
                capture_output=True, timeout=10,
            )
        except Exception:
            pass
        try:
            subprocess.Popen(
                ["uv", "run", "python", "frshty.py", config_path],
                cwd=str(PROJECT_DIR),
                stdout=open(STATE_DIR / f"{inst_key}_stdout.log", "a"),
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            _record_autofix(state, ak)
            log.info(f"[{inst_key}] restarted frshty process")
            return True
        except Exception as e:
            log.error(f"[{inst_key}] failed to restart: {e}")
            return False

    elif ptype == "error_event":
        log.info(f"[{inst_key}] error event detected, triggering cycle")
        await call_instance(inst["base_url"], "POST", "/api/poll")
        _record_autofix(state, ak)
        return True

    return False


async def _escalate(problem: dict, state: dict):
    inst_key = problem["instance"]
    ek = _action_key(inst_key, problem.get("ticket", ""), problem["type"])
    if not _can_escalate(state, ek):
        log.info(f"[{inst_key}] escalation on cooldown for {problem['type']}")
        return

    prompt = f"""A frshty instance is experiencing a problem that auto-fix couldn't resolve.

Instance: {inst_key}
Problem: {json.dumps(problem, indent=2)}

Investigate the frshty codebase, identify the root cause, and fix it. The relevant code is in this directory.
After fixing, the supervisor will restart affected instances automatically."""

    log.info(f"[{inst_key}] escalating to Claude Code: {problem['type']}")
    _record_escalation(state, ek)

    try:
        result = run_claude_code(prompt, PROJECT_DIR, timeout=900)
        if result:
            log.info(f"[{inst_key}] Claude Code fix applied, output: {result[:200]}")
        else:
            log.warning(f"[{inst_key}] Claude Code returned no output")
    except Exception as e:
        log.error(f"[{inst_key}] Claude Code escalation failed: {e}")


async def run_once(state: dict):
    instances = discover_instances()
    if not instances:
        log.warning("No instances discovered")
        return

    log.info(f"Polling {len(instances)} instances: {[i['key'] for i in instances]}")
    problems = await _detect_problems(instances)

    if not problems:
        log.info("All instances healthy")
        return

    log.info(f"Detected {len(problems)} problems")
    for problem in problems:
        fixed = await _autofix(problem, instances, state)
        if not fixed:
            await _escalate(problem, state)

    _save_state(state)


async def main():
    _setup_logging()
    log.info("Supervisor starting")
    _db.init(STATE_DIR / "frshty.db", PROJECT_DIR / "migrations")
    state = _load_state()

    loop = asyncio.get_event_loop()
    stop = asyncio.Event()

    def _handle_signal():
        log.info("Shutting down")
        stop.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    while not stop.is_set():
        try:
            await run_once(state)
        except Exception as e:
            log.error(f"Poll cycle error: {e}", exc_info=True)

        try:
            await asyncio.wait_for(stop.wait(), timeout=POLL_INTERVAL)
        except asyncio.TimeoutError:
            pass

    log.info("Supervisor stopped")


if __name__ == "__main__":
    asyncio.run(main())
