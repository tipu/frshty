"""Per-instance preflight checks. Run at startup to surface config drift loudly
instead of letting it manifest silently mid-pipeline.

Also provides gh_active_account() and gh_switch_to() used by the per-call
diagnostic + auto-switch in features/platforms.py.
"""
import json
import subprocess
from pathlib import Path

import core.log as log


def _run(cmd: list[str], timeout: int = 15) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def gh_active_account() -> str | None:
    """Return the currently active gh account login, or None if gh isn't
    authenticated."""
    r = _run(["gh", "api", "user", "--jq", ".login"])
    if r.returncode != 0:
        return None
    return r.stdout.strip() or None


def gh_logged_in_accounts() -> list[str]:
    """All accounts currently logged in via gh, parsed from `gh auth status`.
    Returns logins in order of appearance."""
    r = _run(["gh", "auth", "status"])
    if r.returncode != 0:
        return []
    accounts: list[str] = []
    for line in r.stdout.splitlines():
        s = line.strip()
        if s.startswith("- Logged in to") or "Logged in to github.com" in s:
            parts = s.split("account")
            if len(parts) >= 2:
                login = parts[1].strip().split()[0]
                if login and login not in accounts:
                    accounts.append(login)
    return accounts


def gh_switch_to(login: str) -> tuple[bool, str]:
    """Attempt to switch active gh account. Returns (ok, message)."""
    r = _run(["gh", "auth", "switch", "--user", login])
    if r.returncode == 0:
        return True, r.stdout.strip() or f"switched to {login}"
    return False, (r.stderr.strip() or r.stdout.strip())[:300]


def gh_repo_push_ok(repo: str) -> tuple[bool, str]:
    """Non-destructive permission check via GitHub API. Returns (ok, reason).
    'ok' means the active gh account has push permission on the repo."""
    r = _run(["gh", "api", f"repos/{repo}", "--jq", ".permissions.push"])
    if r.returncode != 0:
        msg = (r.stderr.strip() or r.stdout.strip())[:300]
        if "Not Found" in msg or "404" in msg:
            return False, "repo not visible to active gh account (404)"
        return False, msg or "gh api failed"
    out = r.stdout.strip()
    if out == "true":
        return True, "ok"
    return False, f"no push permission (.permissions.push={out!r})"


def _check_workspace(config: dict) -> list[tuple[str, bool, str]]:
    results: list[tuple[str, bool, str]] = []
    ws = config.get("workspace") or {}
    root = ws.get("root")
    if root is None:
        results.append(("workspace.root", False, "missing"))
        return results
    root_p = Path(root) if isinstance(root, str) else root
    if not root_p.is_dir():
        results.append(("workspace.root", False, f"not a directory: {root_p}"))
        return results
    results.append(("workspace.root", True, str(root_p)))

    repos = ws.get("repos") or []
    for repo_name in repos:
        repo_dir = root_p / repo_name
        if not (repo_dir / ".git").exists():
            results.append((f"workspace.repos[{repo_name}]", False,
                            f"not a git repo at {repo_dir}"))
        else:
            results.append((f"workspace.repos[{repo_name}]", True, str(repo_dir)))
    return results


def preflight_instance(config: dict) -> tuple[bool, list[dict]]:
    """Run all preflight checks for one instance. Returns (all_ok, checks)
    where checks is a list of {name, ok, detail} dicts. Caller is expected to
    emit log events; this function does not emit, so it can be unit-tested."""
    checks: list[dict] = []

    for name, ok, detail in _check_workspace(config):
        checks.append({"name": name, "ok": ok, "detail": detail})

    platform = (config.get("job") or {}).get("platform") or ""
    if platform == "github":
        repo = (config.get("github") or {}).get("repo") or ""
        if not repo:
            checks.append({"name": "github.repo", "ok": False, "detail": "not configured"})
        else:
            ok, reason = gh_repo_push_ok(repo)
            checks.append({"name": f"github.repo[{repo}]", "ok": ok, "detail": reason})
            if not ok:
                active = gh_active_account() or "<none>"
                checks.append({"name": "gh active account", "ok": False,
                               "detail": f"{active} (cannot access {repo})"})

    all_ok = all(c["ok"] for c in checks)
    return all_ok, checks


def run_preflight(instance_configs: list[dict]) -> None:
    """Run preflight for every instance and emit events. Doesn't refuse to
    start the pool — frshty always starts; we just want loud signal."""
    for cfg in instance_configs:
        instance_key = (cfg.get("job") or {}).get("key") or "<unknown>"
        try:
            all_ok, checks = preflight_instance(cfg)
        except Exception as e:
            log.emit("preflight_error",
                     f"[{instance_key}] preflight crashed: {type(e).__name__}: {e}",
                     meta={"instance_key": instance_key})
            continue
        failures = [c for c in checks if not c["ok"]]
        if all_ok:
            log.emit("preflight_pass",
                     f"[{instance_key}] all checks pass ({len(checks)})",
                     meta={"instance_key": instance_key, "checks": checks,
                           "category": "noise"})
        else:
            details = "; ".join(f"{c['name']}: {c['detail']}" for c in failures)
            log.emit("preflight_fail",
                     f"[{instance_key}] {len(failures)} check(s) failed: {details[:300]}",
                     meta={"instance_key": instance_key, "checks": checks})
