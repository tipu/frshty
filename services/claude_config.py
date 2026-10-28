"""The Claude CLI plugin files that must parse for any marketplace plugin to load.

The CLI reads plugins/known_marketplaces.json and plugins/installed_plugins.json
under its config dir. When either file is empty or holds invalid JSON, the CLI
starts every session without its marketplace plugins and reports nothing to the
session. `check` runs from the work board scan loop. A claude_config_invalid
event goes to the feed when a file becomes unreadable and names it, and a
claude_config_ok event when every file reads again. A missing file is not an
error, because a fresh config dir has none."""
import json
import os
from pathlib import Path

import core.log as log
import core.state as state

PLUGIN_FILES = ("known_marketplaces.json", "installed_plugins.json")
_STATE_MODULE = "claude_config"


def config_dir() -> Path:
    return Path(os.environ.get("CLAUDE_CONFIG_DIR") or Path.home() / ".claude")


def problems() -> list[str]:
    """One line per plugin file that exists and does not hold a JSON object."""
    found = []
    for name in PLUGIN_FILES:
        path = config_dir() / "plugins" / name
        try:
            text = path.read_text()
        except FileNotFoundError:
            continue
        except OSError as e:
            found.append(f"{path}: {type(e).__name__}: {e}")
            continue
        if not text.strip():
            found.append(f"{path}: empty ({len(text)} bytes)")
            continue
        try:
            data = json.loads(text)
        except ValueError as e:
            found.append(f"{path}: invalid JSON: {e}")
            continue
        if not isinstance(data, dict):
            found.append(f"{path}: holds a JSON {type(data).__name__}, not an object")
    return found


def check() -> list[str]:
    found = problems()
    prior = state.load(_STATE_MODULE) or {}
    if found and found != prior.get("problems"):
        log.emit("claude_config_invalid",
                 "Claude CLI plugin config is unreadable, so every marketplace plugin "
                 f"is off in new sessions: {'; '.join(found)}",
                 meta={"problems": found})
    elif not found and prior.get("problems"):
        log.emit("claude_config_ok", "Claude CLI plugin config reads again")
    if found != (prior.get("problems") or []):
        state.save(_STATE_MODULE, {"problems": found})
    return found
