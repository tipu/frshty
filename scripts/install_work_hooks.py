import json
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import core.config as cfg
import core.terminal as terminal

EVENTS = ("SessionStart", "UserPromptSubmit", "Stop", "SessionEnd", "Notification", "PreToolUse")
DEFAULT_DIRS = ("~/.claude", "~/.quill-claude", "~/.aimyable-claude")
PUSH_GATE_TIMEOUT = 2700
WRITE_GATE_TIMEOUT = 300
WRITE_GATE_MATCHER = "Edit|Write|NotebookEdit|MultiEdit"
CONFIG_DIR = Path(__file__).resolve().parent.parent / "config"
SKIP_CONFIGS = ("example.toml", "discovery.toml", "peers.toml", "peers.example.toml")


def claude_config_dirs() -> list[str]:
    """Every Claude configuration directory a work session can run under.

    An instance names its own directory, or an env override of
    CLAUDE_CONFIG_DIR, and a session launched under a directory this
    installer never visits has no hooks and therefore no gates.
    `agent_config_dir` returns "" for an instance that runs the operator's
    default account, and that means ~/.claude, so the empty string is mapped
    rather than dropped."""
    dirs = [os.path.expanduser(d) for d in DEFAULT_DIRS]
    for path in sorted(CONFIG_DIR.glob("*.toml")):
        if path.name in SKIP_CONFIGS:
            continue
        try:
            config = cfg.load_config(str(path))
        except Exception as e:
            print(f"skip {path.name}: {type(e).__name__}: {e}")
            continue
        raw = terminal.agent_config_dir(config, "claude")
        resolved = os.path.expanduser(raw) if raw else os.path.expanduser("~/.claude")
        if resolved not in dirs:
            dirs.append(resolved)
    return dirs


def hook_command() -> str:
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "work_hook.py")
    return f"python3 {script}"


def _wanted_entries(event: str, command: str) -> list[dict]:
    if event == "PreToolUse":
        return [
            {"matcher": "AskUserQuestion",
             "hooks": [{"type": "command", "command": command, "timeout": 10}]},
            {"matcher": "Bash",
             "hooks": [{"type": "command", "command": command,
                        "timeout": PUSH_GATE_TIMEOUT}]},
            {"matcher": WRITE_GATE_MATCHER,
             "hooks": [{"type": "command", "command": command,
                        "timeout": WRITE_GATE_TIMEOUT}]},
        ]
    return [{"matcher": "",
             "hooks": [{"type": "command", "command": command, "timeout": 5, "async": True}]}]


def install_into(settings_path: str, events=EVENTS) -> list[str]:
    if os.path.isfile(settings_path):
        with open(settings_path) as f:
            settings = json.load(f)
    else:
        settings = {}
    hooks = settings.setdefault("hooks", {})
    command = hook_command()
    added = []
    for event in events:
        entries = hooks.setdefault(event, [])
        for wanted in _wanted_entries(event, command):
            present = any(
                (e.get("matcher") or "") == wanted["matcher"]
                and command in [h.get("command") for h in (e.get("hooks") or [])
                                if isinstance(h, dict)]
                for e in entries
            )
            if present:
                continue
            entries.append(wanted)
            label = f"{event}[{wanted['matcher']}]" if wanted["matcher"] else event
            added.append(label)
    if added:
        os.makedirs(os.path.dirname(settings_path), exist_ok=True)
        with open(settings_path, "w") as f:
            json.dump(settings, f, indent=2)
            f.write("\n")
    return added


def main(argv: list[str]) -> int:
    dirs = argv[1:] or claude_config_dirs()
    events = EVENTS
    only = os.environ.get("WORK_HOOK_EVENTS", "")
    if only:
        events = tuple(e for e in EVENTS if e in only.split(","))
    for d in dirs:
        if not os.path.isdir(d):
            print(f"skip {d}: not a directory")
            continue
        path = os.path.join(d, "settings.json")
        added = install_into(path, events)
        print(f"{path}: added {added or 'nothing (already installed)'}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
