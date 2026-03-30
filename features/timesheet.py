import json
import re
import subprocess
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

import httpx

import core.log as log
from core.config import resolve_env, get_repos
from core.claude_runner import run_haiku, extract_json
from features.platforms import make_platform

CACHE_FILE = None
_day_cache = {}
_ticket_cache = {}
_analysis_cache = {}


def _init_cache(config: dict):
    global CACHE_FILE, _day_cache, _ticket_cache, _analysis_cache
    CACHE_FILE = config["_state_dir"] / "timesheet_cache.json"
    if CACHE_FILE.exists():
        try:
            data = json.loads(CACHE_FILE.read_text())
            _day_cache = data.get("days", {})
            _ticket_cache = data.get("tickets", {})
            _analysis_cache = data.get("analysis", {})
        except (json.JSONDecodeError, KeyError):
            pass


def _save_cache():
    if not CACHE_FILE:
        return
    try:
        CACHE_FILE.write_text(json.dumps({"days": _day_cache, "tickets": _ticket_cache, "analysis": _analysis_cache}))
    except OSError:
        pass


def check(config: dict):
    pass


def build_timesheet(config: dict, start: str = "", end: str = "", force: bool = False) -> dict:
    if CACHE_FILE is None:
        _init_cache(config)

    today = date.today()
    if not start:
        start = (today - timedelta(days=30)).isoformat()
    if not end:
        end = today.isoformat()

    if force:
        _day_cache.clear()
        _ticket_cache.clear()
        _analysis_cache.clear()

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    all_days = _date_range(start, end)

    cached_days = {d: _day_cache[d] for d in all_days if d in _day_cache}
    need_fetch = len(cached_days) < len(all_days)

    claude_sessions = _fetch_claude_sessions(config, start, end)

    if need_fetch:
        worklogs = _fetch_worklogs(config, start_date, end_date)
        commits = _fetch_git_commits(config, start_date, end_date)
        pr_reviews = _fetch_pr_reviews(config, start, end)
    else:
        worklogs, commits, pr_reviews = {}, {}, {}

    final_wl, final_commits, final_reviews, final_claude = {}, {}, {}, {}
    for day in all_days:
        if day in cached_days:
            c = cached_days[day]
            if c.get("worklogs"): final_wl[day] = c["worklogs"]
            if c.get("commits"): final_commits[day] = c["commits"]
            if c.get("reviews"): final_reviews[day] = c["reviews"]
            if day in claude_sessions: final_claude[day] = claude_sessions[day]
            elif c.get("claude"): final_claude[day] = c["claude"]
        else:
            if day in worklogs: final_wl[day] = worklogs[day]
            if day in commits: final_commits[day] = commits[day]
            if day in pr_reviews: final_reviews[day] = pr_reviews[day]
            if day in claude_sessions: final_claude[day] = claude_sessions[day]

            day_hours = sum(w.get("hours", 0) for w in worklogs.get(day, []))
            if day_hours >= 8:
                _day_cache[day] = {
                    "worklogs": worklogs.get(day, []),
                    "commits": commits.get(day, []),
                    "reviews": pr_reviews.get(day, []),
                    "claude": claude_sessions.get(day, []),
                }

    ticket_ids = set()
    for entries in final_wl.values():
        for e in entries:
            ticket_ids.add(e["ticket"])
    for entries in final_commits.values():
        for e in entries:
            tid = _extract_ticket(e.get("branch", "") or e.get("message", ""))
            if tid: ticket_ids.add(tid)
    for entries in final_reviews.values():
        for e in entries:
            tid = _extract_ticket(e.get("branch", ""))
            if tid: ticket_ids.add(tid)

    uncached_tids = [t for t in sorted(ticket_ids) if t not in _ticket_cache]
    if uncached_tids:
        fresh = _fetch_ticket_info(config, uncached_tids, final_wl)
        for t in fresh:
            _ticket_cache[t["key"]] = t

    analysis_key = f"{start}|{end}"
    cached_analysis = _analysis_cache.get(analysis_key)
    if isinstance(cached_analysis, dict) and not force:
        daily_summaries = cached_analysis
    else:
        grouped = _group_daily_activity(final_commits, final_reviews, final_claude)
        daily_summaries = _summarize_daily_activity(grouped)
        if daily_summaries:
            _analysis_cache[analysis_key] = daily_summaries

    recurring = _get_recurring(config, start_date, end_date)
    for day_str in all_days:
        for r in recurring.get(day_str, []):
            daily_summaries.setdefault(day_str, {})[r["ticket"]] = r.get("label", "recurring")
            ticket_ids.add(r["ticket"])

    uncached_recurring = [t for t in sorted(ticket_ids) if t not in _ticket_cache]
    if uncached_recurring:
        fresh = _fetch_ticket_info(config, uncached_recurring, final_wl)
        for t in fresh:
            _ticket_cache[t["key"]] = t

    hours_by_ticket = {}
    for entries in final_wl.values():
        for e in entries:
            hours_by_ticket[e["ticket"]] = hours_by_ticket.get(e["ticket"], 0) + e.get("hours", 0)

    tickets = []
    for tid in sorted(ticket_ids):
        t = _ticket_cache.get(tid)
        if t:
            tickets.append({**t, "hoursLogged": round(hours_by_ticket.get(tid, 0), 1)})

    _save_cache()

    return {
        "worklogs": final_wl,
        "gitCommits": final_commits,
        "prReviews": final_reviews,
        "claudeSessions": final_claude,
        "tickets": tickets,
        "startDate": start,
        "endDate": end,
        "dailySummaries": daily_summaries,
        "recurring": {d: entries for d, entries in recurring.items()},
    }


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
            _day_cache.pop(date_str, None)
            for k in list(_analysis_cache):
                if date_str >= k.split("|")[0] and date_str <= k.split("|")[1]:
                    _analysis_cache.pop(k, None)
            _save_cache()
            log.emit("timesheet_logged", f"Logged {time_str} on {ticket} for {date_str}",
                meta={"ticket": ticket, "date": date_str, "time": time_str})
            return {"ok": True}
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
            _day_cache.clear()
            _save_cache()
            return {"ok": True}
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
                    "worklog_id": str(wl.get("id", "")),
                })
    return result


def _fetch_git_commits(config: dict, start_date: date, end_date: date) -> dict:
    repos = get_repos(config)
    author = resolve_env(config, "jira", "user_env")
    result = {}
    for repo in repos:
        cmd = ["git", "log", "--all", f"--since={start_date}", f"--until={end_date}",
               "--pretty=format:%H|%ai|%s|%D"]
        if author:
            cmd.append(f"--author={author}")
        git_log = subprocess.run(
            cmd, cwd=str(repo["path"]), capture_output=True, text=True, timeout=60,
        )
        if git_log.returncode != 0:
            continue
        seen = set()
        for line in git_log.stdout.strip().splitlines():
            parts = line.split("|", 3)
            if len(parts) < 3:
                continue
            sha = parts[0]
            if sha in seen:
                continue
            seen.add(sha)
            ts = parts[1]
            msg = parts[2]
            refs = parts[3] if len(parts) > 3 else ""
            branch = ""
            for ref in refs.split(","):
                ref = ref.strip()
                if ref and "HEAD" not in ref and "tag:" not in ref:
                    branch = ref.split("/")[-1] if "/" in ref else ref
                    break
            ds = ts[:10]
            result.setdefault(ds, []).append({
                "repo": repo["name"],
                "sha": sha[:8],
                "message": msg,
                "branch": _truncate_branch(branch),
            })
    return result


def _fetch_pr_reviews(config: dict, start: str, end: str) -> dict:
    try:
        platform = make_platform(config)
    except (ValueError, KeyError):
        return {}

    user_identifier = ""
    if config["job"]["platform"] == "bitbucket":
        user_identifier = config.get("bitbucket", {}).get("user_account_id", "")
    elif config["job"]["platform"] == "github":
        user_identifier = "@me"

    result = {}
    try:
        all_prs = platform.list_my_open_prs() + platform.list_review_prs()
    except Exception:
        return {}

    for pr in all_prs:
        if (pr.get("updated_on", "")[:10] or "") < start:
            continue
        try:
            comments = platform.get_pr_comments(pr["repo"], pr["id"])
        except Exception:
            continue
        for c in comments:
            c_date = (c.get("created_on", "") or "")[:10]
            if not (start <= c_date <= end):
                continue
            is_mine = False
            if config["job"]["platform"] == "bitbucket":
                is_mine = c.get("author_id", "") == user_identifier
            elif config["job"]["platform"] == "github":
                is_mine = True
            if not is_mine:
                continue
            body = (c.get("body", "") or "")[:200]
            if not body:
                continue
            result.setdefault(c_date, []).append({
                "repo": pr["repo"],
                "pr": pr["id"],
                "branch": _truncate_branch(pr.get("branch", "")),
                "summary": body[:80],
            })
    return result


def _fetch_claude_sessions(config: dict, start: str, end: str) -> dict:
    history_path = Path.home() / ".claude" / "history.jsonl"
    if not history_path.exists():
        history_path = Path("/root/.claude/history.jsonl")
    if not history_path.exists():
        return {}

    ws_root = str(config.get("workspace", {}).get("root", ""))
    if not ws_root:
        return {}

    start_ts = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int((datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000)

    sessions = {}
    try:
        with open(history_path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = entry.get("timestamp", 0)
                if not (start_ts <= ts < end_ts):
                    continue
                project = entry.get("project", "")
                if not project.startswith(ws_root):
                    continue
                prompt = (entry.get("display") or "")[:80]
                if not prompt:
                    continue
                dt = datetime.fromtimestamp(ts / 1000)
                day_str = dt.strftime("%Y-%m-%d")
                time_str = dt.strftime("%H:%M")
                relative = project[len(ws_root):].strip("/")
                short = relative.split("/")[0] if relative else config["job"]["key"]
                sessions.setdefault(day_str, []).append({
                    "project": short,
                    "prompt": prompt,
                    "time": time_str,
                })
    except OSError:
        pass
    return sessions


def _extract_ticket(text: str) -> str:
    m = re.search(r"[A-Z]+-\d+", text)
    return m.group().upper() if m else ""


def _truncate_branch(name: str, max_words: int = 6) -> str:
    parts = re.split(r"[-_/]", name)
    if len(parts) <= max_words:
        return name
    return "-".join(parts[:max_words])


def _group_daily_activity(commits: dict, reviews: dict, claude: dict) -> dict:
    grouped = {}
    all_days = set(list(commits) + list(reviews) + list(claude))
    for day in all_days:
        by_ticket = {}
        for c in commits.get(day, []):
            tid = _extract_ticket(c.get("branch", "") or c.get("message", "")) or "general"
            by_ticket.setdefault(tid, []).append(f"commit {c['repo']}: {c['message'][:500]}")
        for r in reviews.get(day, []):
            tid = _extract_ticket(r.get("branch", "")) or "general"
            by_ticket.setdefault(tid, []).append(f"review {r['repo']} PR#{r['pr']}: {r['summary'][:60]}")
        for s in claude.get(day, []):
            tid = _extract_ticket(s.get("project", "")) or "general"
            by_ticket.setdefault(tid, []).append(f"claude ({s['project']}): {s['prompt']}")
        if by_ticket:
            grouped[day] = by_ticket
    return grouped


def _summarize_daily_activity(grouped: dict) -> dict:
    if not grouped:
        return {}

    prompt = (
        "Summarize daily developer activity grouped by ticket. "
        "For each day+ticket, write ONE concise line (max 20 words) describing what was done. "
        "Combine commits, reviews, and claude sessions into a single summary per ticket per day. "
        'Return ONLY valid JSON: {"YYYY-MM-DD": {"TICKET": "summary", ...}, ...}\n\n'
        + json.dumps(grouped)[:8000]
    )

    try:
        raw = run_haiku(prompt, timeout=180)
        if raw:
            result = extract_json(raw)
            if result:
                return result
    except Exception:
        pass

    fallback = {}
    for day, tickets in grouped.items():
        fallback[day] = {}
        for tid, items in tickets.items():
            fallback[day][tid] = items[0][:80]
    return fallback


def _fetch_ticket_info(config: dict, ticket_ids: list, worklogs: dict) -> list:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return [{"key": t, "summary": "?", "status": "?"} for t in ticket_ids]

    hours_by_ticket = {}
    for entries in worklogs.values():
        for e in entries:
            hours_by_ticket[e["ticket"]] = hours_by_ticket.get(e["ticket"], 0) + e.get("hours", 0)

    tickets = []
    with httpx.Client(auth=(user, token), timeout=30) as client:
        for tid in ticket_ids:
            try:
                resp = client.get(f"{base_url}/rest/api/3/issue/{tid}",
                                  params={"fields": "summary,status,timeoriginalestimate"})
                if resp.status_code == 200:
                    data = resp.json()
                    estimate_secs = data["fields"].get("timeoriginalestimate")
                    tickets.append({
                        "key": tid,
                        "summary": data["fields"]["summary"],
                        "status": data["fields"]["status"]["name"],
                        "hoursLogged": round(hours_by_ticket.get(tid, 0), 1),
                        "hoursEstimated": round(estimate_secs / 3600, 1) if estimate_secs else None,
                    })
                else:
                    tickets.append({"key": tid, "summary": "?", "status": "?"})
            except Exception:
                tickets.append({"key": tid, "summary": "?", "status": "?"})
    return tickets


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


def _date_range(start: str, end: str) -> list[str]:
    d = date.fromisoformat(start)
    end_d = date.fromisoformat(end)
    days = []
    while d <= end_d:
        days.append(d.isoformat())
        d += timedelta(days=1)
    return days


def _parse_time(time_str: str) -> int | None:
    m = re.match(r"^(?:(\d+(?:\.\d+)?)\s*h)?\s*(?:(\d+(?:\.\d+)?)\s*m)?$", time_str.strip())
    if not m or (not m.group(1) and not m.group(2)):
        return None
    hours = float(m.group(1) or 0)
    minutes = float(m.group(2) or 0)
    seconds = int((hours * 3600) + (minutes * 60))
    return seconds if seconds >= 60 else None
