import json
import os
import sys

EVENTS = ("SessionStart", "UserPromptSubmit", "Stop", "SessionEnd", "Notification")
DEFAULT_DIRS = ("~/.claude", "~/.quill-claude", "~/.aimyable-claude")


def hook_command() -> str:
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "work_hook.py")
    return f"python3 {script}"


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
        commands = [
            h.get("command")
            for e in entries
            for h in (e.get("hooks") or [])
            if isinstance(h, dict)
        ]
        if command in commands:
            continue
        entries.append({
            "matcher": "",
            "hooks": [{"type": "command", "command": command, "timeout": 5, "async": True}],
        })
        added.append(event)
    if added:
        os.makedirs(os.path.dirname(settings_path), exist_ok=True)
        with open(settings_path, "w") as f:
            json.dump(settings, f, indent=2)
            f.write("\n")
    return added


def main(argv: list[str]) -> int:
    dirs = argv[1:] or [os.path.expanduser(d) for d in DEFAULT_DIRS]
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
