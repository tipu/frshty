import os
import tomllib
from pathlib import Path


def load_config(path: str) -> dict:
    with open(path, "rb") as f:
        raw = tomllib.load(f)

    raw.setdefault("features", {})
    raw.setdefault("pr", {})
    raw.setdefault("workspace", {})
    raw.setdefault("slack", {})
    raw.setdefault("timesheet", {})

    ws = raw["workspace"]
    ws["root"] = Path(ws["root"])
    ws.setdefault("tickets_dir", "tickets")
    ws.setdefault("ticket_layout", "flat")
    ws.setdefault("base_branch", "main")
    ws.setdefault("branch_prefix", "")
    ws.setdefault("exclude", [])
    ws.setdefault("dep_commands", [])

    raw["pr"].setdefault("auto_merge", False)
    raw["pr"].setdefault("merge_strategy", "squash")
    raw["pr"].setdefault("merge_flags", [])

    raw["_config_path"] = Path(path)
    raw["_state_dir"] = Path.home() / ".frshty" / raw["job"]["key"]
    raw["_base_url"] = raw["job"].get("host") or f"http://localhost:{raw['job']['port']}"

    return raw


def resolve_env(config: dict, section: str, key: str) -> str:
    section_data = config.get(section, {})
    direct_key = key.replace("_env", "")
    if direct_key in section_data:
        return section_data[direct_key]
    env_var = section_data.get(key, "")
    return os.environ.get(env_var, "") if env_var else ""


def get_repos(config: dict) -> list[dict]:
    ws = config["workspace"]
    root = ws["root"]

    if "repos" in ws:
        return [
            {"name": name, "path": root / name}
            for name in ws["repos"]
        ]

    if "projects_dir" in ws:
        projects_dir = root / ws["projects_dir"]
        exclude = set(ws.get("exclude", []))
        repos = []
        for d in sorted(projects_dir.iterdir()):
            if d.is_dir() and (d / ".git").exists() and d.name not in exclude:
                repos.append({"name": d.name, "path": d})
        return repos

    return []


def ticket_worktree_path(config: dict, ticket_slug: str, repo_name: str) -> Path:
    ws = config["workspace"]
    root = ws["root"]
    tickets_dir = root / ws["tickets_dir"]
    if ws["ticket_layout"] == "workspace":
        return tickets_dir / ticket_slug / "workspace" / repo_name
    return tickets_dir / ticket_slug / repo_name


def save_feature_toggle(config: dict, feature: str, enabled: bool):
    import re
    config_path = config["_config_path"]
    text = config_path.read_text()
    pattern = rf"^(\s*{re.escape(feature)}\s*=\s*)(?:true|false)"
    replacement = rf"\g<1>{'true' if enabled else 'false'}"
    new_text = re.sub(pattern, replacement, text, flags=re.MULTILINE)
    config_path.write_text(new_text)
    config["features"][feature] = enabled
