import json
import os
import re
import subprocess
import hashlib
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
from threading import Lock, Event

import httpx

from core import external_log

import core.log as log
import core.state as state
import core.tz as tz
from core.config import resolve_env, get_repos
from core.claude_runner import run_haiku, extract_json
from features.platforms import make_platform
from features import timesheet_select as tsel

CACHE_FILE = None
_day_cache = {}
_ticket_cache = {}
_analysis_cache = {}
_summarization_lock = Lock()
_in_flight_summaries: dict[str, Event] = {}
_summarization_results: dict[str, dict] = {}
_review_minutes_cache: dict[tuple[str, int, str], int] = {}


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


def _hash_activity(activity: dict) -> str:
    s = json.dumps(activity, sort_keys=True)
    return hashlib.sha256(s.encode()).hexdigest()[:16]


def check(config: dict):
    if CACHE_FILE is None:
        _init_cache(config)

    today = tz.today_local()
    start_date = today
    end_date = today
    recurring = _get_recurring(config, start_date, end_date)

    today_str = today.isoformat()
    if today_str not in recurring:
        return

    existing_entries = set()
    for entry in recurring[today_str]:
        for hours in _fetch_ticket_worklogs_today(config, entry["ticket"], today_str):
            existing_entries.add(f"{entry['ticket']}|{hours}")

    for entry in recurring[today_str]:
        ticket = entry["ticket"]
        time_str = entry["time"]
        hours = round((_parse_time(time_str) or 0) / 3600, 1)
        if f"{ticket}|{hours}" in existing_entries:
            continue
        result = log_work(config, ticket, today_str, time_str)
        if result.get("ok"):
            existing_entries.add(f"{ticket}|{hours}")
            log.emit("scheduled_worklog_queued", f"Queued {time_str} on {ticket}",
                meta={"ticket": ticket, "time": time_str, "date": today_str})
        else:
            log.emit("scheduled_worklog_failed", f"Failed to queue {time_str} on {ticket}: {result.get('error', 'unknown')}",
                meta={"ticket": ticket, "time": time_str, "date": today_str})

    _auto_fill(config)


def _build_candidates(data: dict, user_account_id: str) -> list:
    tickets = {t["key"]: t for t in data.get("tickets", [])}
    activity: dict[str, dict[str, dict]] = {}
    commit_dates: dict[str, list] = {}

    def _act(tid: str, day: str) -> dict:
        return activity.setdefault(tid, {}).setdefault(
            day, {"commit": 0, "review": 0, "session": 0, "summary": False})

    for day, commits in data.get("gitCommits", {}).items():
        for c in commits:
            tid = _extract_ticket(c.get("branch", "") or c.get("message", ""))
            if not tid:
                continue
            _act(tid, day)["commit"] += 1
            try:
                commit_dates.setdefault(tid, []).append(date.fromisoformat(day))
            except ValueError:
                pass
    for day, reviews in data.get("prReviews", {}).items():
        for r in reviews:
            tid = _extract_ticket(r.get("branch", ""))
            if tid:
                _act(tid, day)["review"] += r.get("review_minutes", 30)
    for day, sessions in data.get("claudeSessions", {}).items():
        for s in sessions:
            src = (s.get("cwd", "") or "") + " " + (s.get("prompt", "") or "")
            m = re.search(r"\b([A-Z]+-\d{2,})\b", src, re.IGNORECASE)
            if m:
                _act(m.group(1).upper(), day)["session"] += 1
    for day, summaries in data.get("dailySummaries", {}).items():
        for tid in summaries.keys():
            if tid != "general" and re.match(r"[A-Z]+-\d{2,}", tid):
                _act(tid, day)["summary"] = True

    candidates = []
    for tid, days_act in activity.items():
        t = tickets.get(tid)
        if not t or t.get("assignee_id") != user_account_id:
            continue
        cds = sorted(commit_dates.get(tid, []))
        first_commit = cds[0] if cds else None
        last_commit = cds[-1] if cds else None
        ip_raw = t.get("in_progress_at") or ""
        try:
            in_progress = date.fromisoformat(ip_raw) if ip_raw else first_commit
        except ValueError:
            in_progress = first_commit
        spent = t.get("hoursSpentTotal")
        logged = spent if spent is not None else t.get("hoursLogged", 0.0)
        abd = {day: tsel.TicketDayActivity(
                    commit_count=v["commit"], review_minutes=v["review"],
                    session_count=v["session"], summary_present=v["summary"])
               for day, v in days_act.items()}
        candidates.append(tsel.Candidate(
            ticket=tid, summary=t.get("summary", ""), status=t.get("status", ""),
            assignee_id=t.get("assignee_id", ""), estimate_hours=t.get("hoursEstimated"),
            logged_hours=logged or 0.0, in_progress_at=in_progress,
            last_commit_date=last_commit, activity_by_day=abd))
    return candidates


def _build_demands(data: dict, fill_target: float, days: list) -> list:
    worklogs = data.get("worklogs", {})
    recurring = data.get("recurring", {})
    demands = []
    for day in days:
        logged = sum(w.get("hours", 0) for w in worklogs.get(day, []))
        pending = 0.0
        for r in recurring.get(day, []):
            if not r.get("logged"):
                pending += (_parse_time(r["time"]) or 0) / 3600
        demands.append(tsel.Demand(day=day, target_hours=fill_target,
                                   already_logged_hours=logged, recurring_pending_hours=pending))
    return demands


def _auto_fill(config: dict):
    ts_config = config.get("timesheet", {})
    if not ts_config.get("auto_fill"):
        return

    today = tz.today_local()
    if today.weekday() >= 5:
        return

    now_local = tz.now_local()
    fill_window = ts_config.get("fill_window", [18, 20])
    if not (fill_window[0] <= now_local.hour < fill_window[1]):
        return

    today_str = today.isoformat()
    fill_state = state.load("timesheet_fill")
    if fill_state.get(today_str):
        return

    cfg = _selection_config(config)
    fill_target = ts_config.get("fill_target", 8)
    window_start = (today - timedelta(days=cfg.recency_days)).isoformat()
    data = build_timesheet(config, window_start, today_str)
    user_id = data.get("userAccountId", "")
    ticket_map = {t["key"]: t for t in data.get("tickets", [])}

    demands = _build_demands(data, fill_target, [today_str])
    today_demand = demands[0]
    remaining = round(today_demand.target_hours - today_demand.already_logged_hours
                      - today_demand.recurring_pending_hours, 1)
    if remaining <= 0:
        fill_state[today_str] = {"filled": True, "entries": []}
        state.save("timesheet_fill", fill_state)
        return

    candidates = _build_candidates(data, user_id)
    allocations = tsel.select_allocations([today_str], candidates, demands, cfg)
    entries = [{"ticket": a.ticket, "hours": a.hours, "source": a.tier} for a in allocations]

    fill_state[today_str] = {"filled": True, "entries": [{"ticket": e["ticket"], "hours": e["hours"]} for e in entries]}
    state.save("timesheet_fill", fill_state)

    for entry in entries:
        time_str = f"{entry['hours']}h"
        desc = " ".join((ticket_map.get(entry["ticket"], {}).get("summary", "") or "").split()[:7])
        result = log_work(config, entry["ticket"], today_str, time_str)
        if result.get("ok"):
            log.emit("auto_fill_logged", f"Auto-filled {time_str} on {entry['ticket']} — {desc} ({entry['source']})",
                meta={"ticket": entry["ticket"], "hours": entry["hours"], "source": entry["source"], "date": today_str})
        else:
            log.emit("auto_fill_failed", f"Failed to auto-fill {time_str} on {entry['ticket']} — {desc}: {result.get('error', 'unknown')}",
                meta={"ticket": entry["ticket"], "hours": entry["hours"], "date": today_str})

    if not entries:
        log.emit("auto_fill_skipped", "No eligible tickets found for auto-fill (accepted the gap)",
            meta={"date": today_str})


def backtest_timesheet_selection(config: dict, start: str, end: str, force: bool = False) -> dict:
    try:
        start_d = date.fromisoformat(start)
        end_d = date.fromisoformat(end)
    except ValueError:
        return {"error": "invalid date range"}
    if end_d < start_d:
        return {"error": "invalid date range"}

    data = build_timesheet(config, start, end, force=force)
    user_id = data.get("userAccountId", "")
    cfg = _selection_config(config)
    fill_target = config.get("timesheet", {}).get("fill_target", 8)
    weekdays = [d for d in _date_range(start, end) if date.fromisoformat(d).weekday() < 5]

    candidates = _build_candidates(data, user_id)
    demands = _build_demands(data, fill_target, weekdays)
    allocations = tsel.select_allocations(weekdays, candidates, demands, cfg)

    predicted: dict[str, set] = {}
    for a in allocations:
        predicted.setdefault(a.day, set()).add(a.ticket)

    compared = 0
    matched = 0
    per_day = []
    for day in weekdays:
        actual = {w["ticket"] for w in data.get("worklogs", {}).get(day, [])}
        pred = predicted.get(day, set())
        if not actual and not pred:
            continue
        compared += 1
        overlap = bool(actual & pred)
        if overlap:
            matched += 1
        per_day.append({"day": day, "actual": sorted(actual),
                        "predicted": sorted(pred), "match": overlap})

    return {
        "start": start,
        "end": end,
        "weekdays": len(weekdays),
        "compared_days": compared,
        "matched_days": matched,
        "agreement": round(matched / compared, 3) if compared else 0.0,
        "per_day": per_day,
        "allocations": [{"day": a.day, "ticket": a.ticket, "hours": a.hours, "tier": a.tier}
                        for a in allocations],
    }


def build_timesheet(config: dict, start: str = "", end: str = "", force: bool = False) -> dict:
    if CACHE_FILE is None:
        _init_cache(config)

    today = tz.today_local()
    if not start:
        start = (today - timedelta(days=30)).isoformat()
    if not end:
        end = today.isoformat()

    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    all_days = _date_range(start, end)

    if force:
        # Clear analysis for this specific range and days in range to force re-fetch
        _analysis_cache.pop(f"{start}|{end}", None)
        for d in all_days:
            _day_cache.pop(d, None)

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

    uncached_tids = [t for t in sorted(ticket_ids) if t not in _ticket_cache or "assignee_id" not in _ticket_cache[t]]
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
        daily_summaries = {}
        to_summarize = {}

        for day, activity in grouped.items():
            h = _hash_activity(activity)
            day_key = f"day|{day}|{h}"
            if day_key in _analysis_cache and not force:
                daily_summaries[day] = _analysis_cache[day_key]
            else:
                to_summarize[day] = activity

        if to_summarize:
            new_summaries = _summarize_daily_activity(to_summarize)
            for day, summary in new_summaries.items():
                activity = to_summarize[day]
                h = _hash_activity(activity)
                _analysis_cache[f"day|{day}|{h}"] = summary
                daily_summaries[day] = summary

        if daily_summaries:
            _analysis_cache[analysis_key] = daily_summaries

    recurring = _get_recurring(config, start_date, end_date)
    for day_str in all_days:
        for r in recurring.get(day_str, []):
            daily_summaries.setdefault(day_str, {})[r["ticket"]] = r.get("label", "recurring")
            ticket_ids.add(r["ticket"])
            expected_hours = round((_parse_time(r["time"]) or 0) / 3600, 1)
            day_wl = final_wl.get(day_str, [])
            r["logged"] = any(w["ticket"] == r["ticket"] and w.get("hours") == expected_hours for w in day_wl)

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

    user_account_id = config.get("bitbucket", {}).get("user_account_id", "") or config.get("jira", {}).get("user_account_id", "")

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
        "userAccountId": user_account_id,
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
    with external_log.client("jira", auth=(user, token), timeout=30) as client:
        resp = client.post(url, json=payload)
        if resp.status_code in (200, 201):
            _day_cache.pop(date_str, None)
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
    with external_log.client("jira", auth=(user, token), timeout=30) as client:
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

    user_account_id = jira.get("user_account_id")
    result = {}
    jql = f"worklogAuthor = currentUser() AND worklogDate >= '{start_date}' AND worklogDate <= '{end_date}'"

    with external_log.client("jira", auth=(user, token), timeout=30) as client:
        next_page_token = None
        while True:
            params = {
                "jql": jql,
                "maxResults": 50,
                "fields": "summary,worklog",
            }
            if next_page_token:
                params["nextPageToken"] = next_page_token
            resp = client.get(f"{base_url}/rest/api/3/search/jql", params=params)
            if resp.status_code != 200:
                log.emit("fetch_worklogs_failed", f"Jira search failed: {resp.status_code} {resp.text}")
                break

            data = resp.json()
            issues = data.get("issues", [])
            if not issues:
                break

            for issue in issues:
                key = issue["key"]
                summary = issue["fields"]["summary"]
                worklog_data = issue["fields"].get("worklog", {})
                worklogs = worklog_data.get("worklogs", [])
                total_wl = worklog_data.get("total", 0)
                
                if total_wl > len(worklogs):
                    wl_resp = client.get(f"{base_url}/rest/api/3/issue/{key}/worklog")
                    if wl_resp.status_code == 200:
                        worklogs = wl_resp.json().get("worklogs", [])
                
                for wl in worklogs:
                    author = wl.get("author", {})
                    author_email = author.get("emailAddress")
                    author_id = author.get("accountId")
                    
                    matches = False
                    if user_account_id and author_id == user_account_id:
                        matches = True
                    elif author_email and user and author_email.lower() == user.lower():
                        matches = True
                    elif not author_email and not author_id:
                        matches = True
                        
                    if not matches:
                        continue
                        
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
            
            next_page_token = data.get("nextPageToken")
            if data.get("isLast", True) or not next_page_token:
                break
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
        if config["job"]["platform"] == "bitbucket":
            all_prs = platform.list_review_prs()
        else:
            all_prs = platform.list_my_open_prs() + platform.list_review_prs()
    except Exception as e:
        log.emit("list_prs_failed", f"Failed to list PRs for timesheet: {e}")
        return {}

    from concurrent.futures import ThreadPoolExecutor

    eligible_prs = [pr for pr in all_prs if (pr.get("updated_on", "")[:10] or "") >= start]

    def fetch_comments(pr):
        try:
            return pr, platform.get_pr_comments(pr["repo"], pr["id"])
        except Exception:
            return pr, []

    with ThreadPoolExecutor(max_workers=4) as pool:
        comment_results = list(pool.map(fetch_comments, eligible_prs))

    prs_needing_diff: set[tuple[str, int, str]] = set()
    for pr, comments in comment_results:
        has_my_comments = False
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
            has_my_comments = True
            result.setdefault(c_date, []).append({
                "repo": pr["repo"],
                "pr": pr["id"],
                "branch": _truncate_branch(pr.get("branch", "")),
                "summary": body[:80],
                "url": pr.get("url", ""),
            })
        if has_my_comments:
            prs_needing_diff.add((pr["repo"], pr["id"], pr.get("updated_on", "") or ""))

    uncached = [k for k in prs_needing_diff if k not in _review_minutes_cache]
    with ThreadPoolExecutor(max_workers=4) as pool:
        diff_results = list(pool.map(lambda k: (k, _estimate_review_minutes(platform, k[0], k[1])), uncached))
    for k, mins in diff_results:
        _review_minutes_cache[k] = mins

    pr_review_minutes = {f"{r}/{pid}": _review_minutes_cache.get((r, pid, upd), 30)
                        for (r, pid, upd) in prs_needing_diff}

    for entries in result.values():
        for entry in entries:
            pr_key = f"{entry['repo']}/{entry['pr']}"
            entry["review_minutes"] = pr_review_minutes.get(pr_key, 30)
    return result


def _estimate_review_minutes(platform, repo: str, pr_id: int) -> int:
    try:
        diff = platform.get_pr_diff(repo, pr_id)
    except Exception:
        return 30
    if not diff:
        return 15
    truncated = diff[:6000]
    prompt = (
        "You are estimating how long a code review takes. "
        "Given this PR diff, respond with ONLY a number between 15 and 45 representing minutes. "
        "Small trivial changes = 15. Medium complexity = 30. Large or complex changes = 45.\n\n"
        f"{truncated}"
    )
    try:
        resp = run_haiku(prompt, timeout=30)
        if resp:
            import re
            m = re.search(r'\d+', resp)
            if m:
                mins = int(m.group())
                return max(15, min(45, mins))
    except Exception:
        pass
    return 30


def _claude_history_paths(config: dict) -> list[Path]:
    paths = [Path.home() / ".claude" / "history.jsonl"]
    claude_cfg = (config.get("llm") or {}).get("claude") or {}
    env_cfg = claude_cfg.get("env") or {}
    config_dir = env_cfg.get("CLAUDE_CONFIG_DIR") or claude_cfg.get("config_dir")
    if config_dir:
        extra = Path(os.path.expanduser(str(config_dir))) / "history.jsonl"
        if extra not in paths:
            paths.append(extra)
    return paths


def _fetch_claude_sessions(config: dict, start: str, end: str) -> dict:
    ws_root = str(config.get("workspace", {}).get("root", ""))
    if not ws_root:
        return {}

    start_ts = int(datetime.strptime(start, "%Y-%m-%d").timestamp() * 1000)
    end_ts = int((datetime.strptime(end, "%Y-%m-%d") + timedelta(days=1)).timestamp() * 1000)

    sessions = {}
    for history_path in _claude_history_paths(config):
        if not history_path.exists():
            continue
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
                        "cwd": relative,
                    })
        except OSError:
            pass
    return sessions


def _extract_ticket(text: str) -> str:
    m = re.search(r"[A-Za-z]+-\d+", text)
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

    prompt_data = json.dumps(grouped, sort_keys=True)
    prompt_hash = hashlib.sha256(prompt_data.encode()).hexdigest()

    event = None
    with _summarization_lock:
        if prompt_hash in _summarization_results:
            return _summarization_results[prompt_hash]
        if prompt_hash in _in_flight_summaries:
            event = _in_flight_summaries[prompt_hash]
        else:
            event = Event()
            _in_flight_summaries[prompt_hash] = event

    if event.is_set():
        return _summarization_results.get(prompt_hash, {})

    # If we are the ones who should run it (or wait for it)
    # Actually, we need to know if WE created the event.
    # Simple trick: the thread that sets the event is the one that ran it.
    # But only one thread should run it.
    
    # Let's use a simpler "try to claim" pattern
    mine = False
    with _summarization_lock:
        if not hasattr(event, "_claimed"):
            event._claimed = True # type: ignore
            mine = True
    
    if not mine:
        event.wait(timeout=180)
        return _summarization_results.get(prompt_hash, {})

    # We are the ones running it
    try:
        prompt = (
            "Summarize daily developer activity grouped by ticket. "
            "For each day+ticket, write ONE concise line (max 20 words) describing what was done. "
            "Combine commits, reviews, and claude sessions into a single summary per ticket per day. "
            'Return ONLY valid JSON: {"YYYY-MM-DD": {"TICKET": "summary", ...}, ...}\n\n'
            + prompt_data[:8000]
        )

        result = {}
        try:
            raw = run_haiku(prompt, timeout=180)
            if raw:
                parsed = extract_json(raw)
                if parsed:
                    result = parsed
        except Exception:
            pass

        if not result:
            # Fallback
            for day, tickets in grouped.items():
                result[day] = {}
                for tid, items in tickets.items():
                    result[day][tid] = items[0][:80]
        
        _summarization_results[prompt_hash] = result
        return result
    finally:
        event.set()
        with _summarization_lock:
            _in_flight_summaries.pop(prompt_hash, None)


def _fetch_ticket_info(config: dict, ticket_ids: list, worklogs: dict) -> list:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return [{"key": t, "summary": "?", "status": "?", "in_progress_at": ""} for t in ticket_ids]

    in_progress_statuses = _selection_config(config).in_progress_statuses

    hours_by_ticket = {}
    for entries in worklogs.values():
        for e in entries:
            hours_by_ticket[e["ticket"]] = hours_by_ticket.get(e["ticket"], 0) + e.get("hours", 0)

    tickets = []
    fetched = set()
    with external_log.client("jira", auth=(user, token), timeout=30) as client:
        for i in range(0, len(ticket_ids), 50):
            batch = ticket_ids[i:i + 50]
            jql = f"issueKey in ({','.join(batch)})"
            try:
                resp = client.get(f"{base_url}/rest/api/3/search/jql",
                                  params={"jql": jql, "maxResults": 50, "expand": "changelog",
                                          "fields": "summary,status,timeoriginalestimate,timespent,assignee"})
                if resp.status_code == 200:
                    for issue in resp.json().get("issues", []):
                        tid = issue["key"]
                        fetched.add(tid)
                        estimate_secs = issue["fields"].get("timeoriginalestimate")
                        spent_secs = issue["fields"].get("timespent")
                        assignee = issue["fields"].get("assignee")
                        assignee_id = assignee.get("accountId", "") if assignee else ""
                        in_progress_at = _extract_in_progress_at(
                            issue.get("changelog", {}), in_progress_statuses)
                        tickets.append({
                            "key": tid,
                            "summary": issue["fields"]["summary"],
                            "status": issue["fields"]["status"]["name"],
                            "url": f"{base_url}/browse/{tid}",
                            "hoursLogged": round(hours_by_ticket.get(tid, 0), 1),
                            "hoursSpentTotal": round(spent_secs / 3600, 1) if spent_secs else None,
                            "hoursEstimated": round(estimate_secs / 3600, 1) if estimate_secs else None,
                            "assignee_id": assignee_id,
                            "in_progress_at": in_progress_at.isoformat() if in_progress_at else "",
                        })
            except Exception:
                pass
        for tid in ticket_ids:
            if tid not in fetched:
                tickets.append({"key": tid, "summary": "?", "status": "?", "in_progress_at": ""})
    return tickets


def _extract_in_progress_at(changelog: dict, in_progress_statuses) -> date | None:
    earliest = None
    for history in changelog.get("histories", []):
        created = history.get("created", "")
        for item in history.get("items", []):
            if item.get("field") != "status":
                continue
            if (item.get("toString", "") or "").strip().lower() not in in_progress_statuses:
                continue
            try:
                d = date.fromisoformat(created[:10])
            except ValueError:
                continue
            if earliest is None or d < earliest:
                earliest = d
    return earliest


def _selection_config(config: dict) -> "tsel.SelectionConfig":
    ts = config.get("timesheet", {})
    sel = ts.get("selection", {}) if isinstance(ts.get("selection"), dict) else {}
    defaults = tsel.SelectionConfig()

    def _statuses(key, fallback):
        vals = sel.get(key)
        if isinstance(vals, list) and vals:
            return frozenset(s.strip().lower() for s in vals if isinstance(s, str))
        return fallback

    def _num(key, fallback):
        v = sel.get(key)
        return v if isinstance(v, (int, float)) and not isinstance(v, bool) else fallback

    return tsel.SelectionConfig(
        terminal_statuses=_statuses("terminal_statuses", defaults.terminal_statuses),
        in_progress_statuses=_statuses("in_progress_statuses", defaults.in_progress_statuses),
        recency_days=int(_num("recency_days", defaults.recency_days)),
        max_chunk_hours=float(_num("max_chunk_hours", defaults.max_chunk_hours)),
        w_review=float(_num("w_review", defaults.w_review)),
        w_commit=float(_num("w_commit", defaults.w_commit)),
        w_session=float(_num("w_session", defaults.w_session)),
        w_headroom=float(_num("w_headroom", defaults.w_headroom)),
        w_recency=float(_num("w_recency", defaults.w_recency)),
    )


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


def _fetch_ticket_worklogs_today(config: dict, ticket: str, today_str: str) -> list[float]:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return []
    started_after = int(datetime.strptime(today_str, "%Y-%m-%d").timestamp() * 1000)
    url = f"{base_url}/rest/api/3/issue/{ticket}/worklog?startedAfter={started_after}"
    with external_log.client("jira", auth=(user, token), timeout=30) as client:
        resp = client.get(url)
        if resp.status_code != 200:
            return []
        hours = []
        for wl in resp.json().get("worklogs", []):
            if wl.get("started", "")[:10] == today_str:
                hours.append(round(wl.get("timeSpentSeconds", 0) / 3600, 1))
        return hours


def _parse_time(time_str: str) -> int | None:
    m = re.match(r"^(?:(\d+(?:\.\d+)?)\s*h)?\s*(?:(\d+(?:\.\d+)?)\s*m)?$", time_str.strip())
    if not m or (not m.group(1) and not m.group(2)):
        return None
    hours = float(m.group(1) or 0)
    minutes = float(m.group(2) or 0)
    seconds = int((hours * 3600) + (minutes * 60))
    return seconds if seconds >= 60 else None
