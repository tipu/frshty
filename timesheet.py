import re
import subprocess
from datetime import datetime, timezone, date, timedelta

import httpx

import log
from config import resolve_env, get_repos


def check(config: dict):
    pass


def build_timesheet(config: dict, start: str = "", end: str = "") -> dict:
    if not start:
        start = (date.today() - timedelta(days=30)).isoformat()
    if not end:
        end = date.today().isoformat()

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)

    worklogs = _fetch_worklogs(config, start_date, end_date)
    commits = _fetch_git_commits(config, start_date, end_date)
    recurring = _get_recurring(config, start_date, end_date)

    days = {}
    current = start_date
    while current <= end_date:
        ds = current.isoformat()
        days[ds] = {
            "date": ds,
            "day": current.strftime("%A"),
            "worklogs": worklogs.get(ds, []),
            "commits": commits.get(ds, []),
            "recurring": recurring.get(ds, []),
        }
        current += timedelta(days=1)

    return {"start": start, "end": end, "days": days}


def log_work(config: dict, ticket: str, date_str: str, time_str: str) -> dict:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return {"error": "jira not configured"}

    seconds = _parse_time(time_str)
    if not seconds:
        return {"error": f"invalid time format: {time_str}"}

    url = f"{base_url}/rest/api/3/issue/{ticket}/worklog"
    payload = {
        "timeSpentSeconds": seconds,
        "started": f"{date_str}T09:00:00.000+0000",
    }
    with httpx.Client(auth=(user, token), timeout=30) as client:
        resp = client.post(url, json=payload)
        if resp.status_code in (200, 201):
            log.emit("timesheet_generated", f"Logged {time_str} on {ticket} for {date_str}",
                meta={"ticket": ticket, "date": date_str, "time": time_str})
            return {"status": "logged"}
        return {"error": resp.text}


def update_worklog(config: dict, ticket: str, worklog_id: str, time_str: str) -> dict:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return {"error": "jira not configured"}

    seconds = _parse_time(time_str)
    if not seconds:
        return {"error": f"invalid time format: {time_str}"}

    url = f"{base_url}/rest/api/3/issue/{ticket}/worklog/{worklog_id}"
    with httpx.Client(auth=(user, token), timeout=30) as client:
        resp = client.put(url, json={"timeSpentSeconds": seconds})
        if resp.status_code in (200, 201):
            return {"status": "updated"}
        return {"error": resp.text}


def _fetch_worklogs(config: dict, start_date: date, end_date: date) -> dict:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return {}

    jql = f"worklogAuthor = currentUser() AND worklogDate >= '{start_date}' AND worklogDate <= '{end_date}'"
    url = f"{base_url}/rest/api/3/search/jql?jql={jql}&maxResults=100&fields=key,summary,worklog"
    result = {}

    with httpx.Client(auth=(user, token), timeout=30) as client:
        resp = client.get(url)
        if resp.status_code != 200:
            return {}
        for issue in resp.json().get("issues", []):
            key = issue["key"]
            summary = issue["fields"]["summary"]
            for wl in issue["fields"].get("worklog", {}).get("worklogs", []):
                started = wl.get("started", "")[:10]
                if started < str(start_date) or started > str(end_date):
                    continue
                seconds = wl.get("timeSpentSeconds", 0)
                hours = round(seconds / 3600, 1)
                result.setdefault(started, []).append({
                    "ticket": key,
                    "summary": summary,
                    "hours": hours,
                    "worklog_id": wl.get("id"),
                })
    return result


def _fetch_git_commits(config: dict, start_date: date, end_date: date) -> dict:
    repos = get_repos(config)
    result = {}
    for repo in repos:
        git_log = subprocess.run(
            ["git", "log", "--all", f"--since={start_date}", f"--until={end_date}",
             "--pretty=format:%H|%ai|%s", "--author=danial"],
            cwd=str(repo["path"]), capture_output=True, text=True, timeout=60,
        )
        if git_log.returncode != 0:
            continue
        for line in git_log.stdout.strip().splitlines():
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            sha, ts, msg = parts
            ds = ts[:10]
            result.setdefault(ds, []).append({
                "repo": repo["name"],
                "sha": sha[:8],
                "message": msg,
            })
    return result


def _get_recurring(config: dict, start_date: date, end_date: date) -> dict:
    recurring = config.get("timesheet", {}).get("recurring", [])
    day_map = {"mon": 0, "tue": 1, "wed": 2, "thu": 3, "fri": 4, "sat": 5, "sun": 6}
    result = {}
    current = start_date
    while current <= end_date:
        weekday = current.weekday()
        for entry in recurring:
            days = [day_map.get(d.lower(), -1) for d in entry.get("days", [])]
            if weekday in days:
                result.setdefault(current.isoformat(), []).append({
                    "ticket": entry["ticket"],
                    "time": entry["time"],
                    "label": entry.get("label", ""),
                })
        current += timedelta(days=1)
    return result


def _parse_time(time_str: str) -> int | None:
    m = re.match(r"^(?:(\d+(?:\.\d+)?)\s*h)?\s*(?:(\d+(?:\.\d+)?)\s*m)?$", time_str.strip())
    if not m or (not m.group(1) and not m.group(2)):
        return None
    hours = float(m.group(1) or 0)
    minutes = float(m.group(2) or 0)
    seconds = int((hours * 3600) + (minutes * 60))
    return seconds if seconds >= 60 else None
