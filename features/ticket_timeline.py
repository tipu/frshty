"""The ticket detail page as a pipeline timeline.

Merges jobs, ticket_transitions, log_events and the ticket's docs/ listing
into one ordered array of nodes. Each node is one circle on the spine. The
gap to the previous node, the reason for that gap, the artifacts a phase
produced and the commits a PR comment caused are all resolved here, so the
page renders what it is handed and computes nothing.

Sources, one per element:

    circle, label, glyph      jobs.task and log_events.event
    phase boundary and actor  ticket_transitions
    the gap pill              subtraction of two adjacent node timestamps
    the reason for the gap    the ticket status in effect during the gap
    commits under a comment   log_events.meta.commit and meta.files
    threads a commit closed   log_events.meta.comment_ids
    artifacts of a phase      docs/ file mtime falling inside the phase window
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import core.db as db
import core.log as log

_GROUP_GAP = timedelta(minutes=30)
_TRANSITION_SLACK = timedelta(seconds=120)
_COLLAPSE_WINDOW = timedelta(seconds=120)
_RIBBON_BRIDGE = timedelta(minutes=5)

_NOISE_TASKS = frozenset({
    "advance_ticket", "scan_tickets", "scheduler_check", "set_state",
    "validate_merged_ticket", "poll_own_prs", "poll_reviewer",
    "poll_peer_reviews", "poll_pr_autofix", "apply_note_reset",
})

_JOB_PHASES = {
    "start_planning": ("plan", "Planning", "Planned", "◈"),
    "start_reviewing": ("review", "Tri-review", "Reviewed", "◐"),
    "fix_review_findings": ("fix_review", "Fix review findings", None, "✎"),
    "enter_testing": ("test", "Testing", "Tested", "▦"),
    "plan_tests": ("test", "Testing", "Tested", "▦"),
    "write_tests": ("test", "Testing", "Tested", "▦"),
    "run_tests_and_fix": ("test", "Testing", "Tested", "▦"),
    "enter_proving": ("prove", "Proving", "Proved", "▶"),
    "prove": ("prove", "Proving", "Proved", "▶"),
    "mark_ready": ("ready", "PR ready", "PR ready", "●"),
    "generate_pr_descriptions": ("ready", "PR ready", "PR ready", "●"),
    "create_pr": ("pr", "PRs opened", "In review", "⎇"),
    "sync_pr_base": ("sync", "Base sync", None, "↺"),
    "scope_review": ("scope", "Scope review", None, "⚖"),
    "fix_ci_failures": ("ci_fix", "CI fix", None, "⚙"),
    "fix_reported_bug": ("bug", "Reported bug fix", None, "✱"),
    "resolve_conflicts": ("conflicts", "Resolve conflicts", None, "⑂"),
    "do_research": ("research", "Research", "Researched", "◇"),
    "setup_prd_ticket": ("prd", "PRD setup", None, "▤"),
    "pm_pre_approval": ("pm", "PM checkpoint", None, "⚑"),
    "address_pm_findings": ("pm", "PM checkpoint", None, "⚑"),
    "pm_post_shipping": ("pm", "PM checkpoint", None, "⚑"),
    "substantiate_reply": ("defence", "Defend a review comment", None, "⚖"),
}

_PHASE_DOCS = {
    "plan": ["ticket.md", "comments.md", "technical-plan.md", "change-manifest.md",
             "change-explainer.html"],
    "review": ["tri-review.md"],
    "fix_review": ["tri-review.md"],
    "test": ["test-plan.md", "test-files-written.txt", "test-runs.md", "testing.md"],
    "prove": ["proof.md", "db-proof.txt", "proof.js", "screenshot_check.js"],
    "ready": ["pr-descriptions.json"],
    "scope": ["scope-review.md"],
    "research": ["research.md"],
    "prd": ["prd.md"],
}

_DOC_ICONS = {
    ".md": "▤", ".txt": "▤", ".json": "⚙", ".html": "◉",
    ".js": "⌨", ".py": "⌨", ".png": "◉", ".jpg": "◉",
    ".webm": "▶", ".mp4": "▶", ".mov": "▶", ".mkv": "▶",
}

_MEDIA_EXTS = frozenset({".webm", ".mp4", ".mov", ".mkv"})
_IMAGE_EXTS = frozenset({".png", ".jpg", ".jpeg", ".gif", ".svg"})
_TEXT_EXTS = frozenset({".md", ".txt", ".json"})

_WHY = {
    "new": "queued — no free worker slot",
    "blocked": "blocked, nobody was watching",
    "pr_ready": "waiting for a human to press Submit PR",
    "in_review": "waiting for reviewers",
    "pr_failed": "PR failed, waiting for a human",
    "done": "marked done, the PRs were still open",
    "merged": "merged, waiting on the next pass",
    "planning": "the planning agent was working",
    "reviewing": "the review agent was working",
    "testing": "the test agent was working",
    "proving": "the proving agent was working",
}

_STOP_LABELS = {
    "new": "Created", "planning": "Planning", "reviewing": "Reviewed",
    "testing": "Tested", "proving": "Proved", "pr_ready": "PR ready",
    "in_review": "In review", "merged": "Merged", "done": "Done",
    "blocked": "Blocked", "pr_failed": "PR failed",
}

_SCAN_EVENTS = frozenset({
    "ticket_pr_comments_detected", "ticket_pr_comment_registered",
    "ticket_pr_comment_needs_reply", "ticket_pr_comment_classify_failed",
    "ticket_pr_comment_already_addressed", "ticket_pr_comment_fix_failed",
})

_CHECK_EVENTS = frozenset({"ticket_check_error", "ticket_checks_passed"})

_MAINTENANCE_PHASES = frozenset({"sync", "scope", "defence", "ci_fix", "conflicts"})

_PHASE_OWNED_EVENTS = frozenset({
    "ticket_scope_review_started", "ticket_scope_review_failed",
    "scope_review_fanout_complete", "ticket_base_synced",
    "ticket_base_sync_blocked", "sync_pr_base_error",
    "ticket_worktree_rebased", "ticket_worktree_preserved",
    "ticket_planning_started", "ticket_review_started",
    "ticket_review_fixing", "ticket_review_verifying",
    "ticket_test_planning_started", "ticket_test_writing_started",
    "ticket_prove_started", "ticket_pr_descriptions_generated",
    "ticket_acceptance_enriched", "ctp_fanout_complete", "ctp_complete",
    "schedule_pr_skipped", "ticket_bug_fix_started",
})

_COMMENT_EVENTS = frozenset({
    "ticket_pr_comment_fixed", "ticket_pr_comment_fix_capped",
    "ticket_pr_comment_reclassified", "ticket_pr_comment_resolution_reconciled",
})

_RIBBON_KINDS = {
    "blocked": "block", "in_review": "review", "done": "review",
    "pr_failed": "block",
}


def _dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.isoformat()


def fmt_span(seconds: float) -> str:
    """Human span used on the gap pill: 42s, 6m 12s, 3h 04m, 1d 19h."""
    total = int(round(seconds))
    if total < 60:
        return f"{total}s"
    minutes, sec = divmod(total, 60)
    if minutes < 60:
        return f"{minutes}m {sec}s" if sec else f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes:02d}m" if minutes else f"{hours}h"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h" if hours else f"{days}d"


def _union_seconds(spans: list[tuple[datetime, datetime]]) -> float:
    if not spans:
        return 0.0
    ordered = sorted(spans)
    total = 0.0
    start, end = ordered[0]
    for lo, hi in ordered[1:]:
        if lo > end:
            total += (end - start).total_seconds()
            start, end = lo, hi
        elif hi > end:
            end = hi
    return total + (end - start).total_seconds()


def _merge_spans(spans: list[tuple[datetime, datetime]],
                 bridge: timedelta) -> list[tuple[datetime, datetime]]:
    if not spans:
        return []
    ordered = sorted(spans)
    merged = [list(ordered[0])]
    for lo, hi in ordered[1:]:
        if lo - merged[-1][1] <= bridge:
            merged[-1][1] = max(merged[-1][1], hi)
        else:
            merged.append([lo, hi])
    return [(a, b) for a, b in merged]


def _load_jobs(instance_key: str, ticket_key: str) -> list[dict]:
    rows = db.query_all(
        "SELECT id, task, status, enqueued_at, started_at, finished_at, response"
        " FROM jobs WHERE instance_key=? AND ticket_key=? ORDER BY id ASC",
        (instance_key, ticket_key),
    )
    out = []
    for row in rows:
        started = _dt(row["started_at"]) or _dt(row["enqueued_at"])
        if not started:
            continue
        out.append({
            "id": row["id"],
            "task": row["task"],
            "status": row["status"],
            "response": row["response"] or "",
            "started": started,
            "finished": _dt(row["finished_at"]),
        })
    return out


def _load_transitions(instance_key: str, ticket_key: str) -> list[dict]:
    rows = db.query_all(
        "SELECT prior_status, new_status, rejected, rejection_reason, actor, reason, ts"
        " FROM ticket_transitions WHERE instance_key=? AND ticket_key=?"
        " ORDER BY ts ASC, id ASC",
        (instance_key, ticket_key),
    )
    out = []
    for row in rows:
        at = _dt(row["ts"])
        if not at:
            continue
        item = dict(row)
        item["at"] = at
        item["rejected"] = bool(item["rejected"])
        out.append(item)
    return out


def _status_at(transitions: list[dict], moment: datetime) -> str:
    status = ""
    for tr in transitions:
        if tr["at"] > moment:
            break
        status = tr["new_status"]
    return status


def _load_docs(docs_dir: Path) -> list[dict]:
    if not docs_dir.is_dir():
        return []
    out = []
    for path in sorted(docs_dir.iterdir()):
        if not path.is_file() or path.name.startswith("."):
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        out.append({
            "name": path.name,
            "suffix": path.suffix.lower(),
            "size": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc),
        })
    return out


def _size_label(size: int) -> str:
    if size < 1024:
        return f"{size} B"
    if size < 1024 * 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size / (1024 * 1024):.1f} MB"


def _art(doc: dict, sub: str) -> dict:
    kind = "text"
    if doc["suffix"] in _MEDIA_EXTS:
        kind = "video"
    elif doc["suffix"] in _IMAGE_EXTS:
        kind = "image"
    elif doc["suffix"] == ".html":
        kind = "page"
    elif doc["suffix"] not in _TEXT_EXTS:
        kind = "file"
    return {
        "ic": _DOC_ICONS.get(doc["suffix"], "▤"),
        "name": doc["name"],
        "sub": sub,
        "size": _size_label(doc["size"]),
        "kind": kind,
    }


def _block(kind: str, title: str | None = None, **rest) -> dict:
    out = {"k": kind}
    if title:
        out["title"] = title
    out.update(rest)
    return out


def _group_jobs(jobs: list[dict]) -> list[dict]:
    """Fold consecutive runs of the same pipeline phase into one group.

    A phase breaks when the phase key changes or when more than 30 minutes
    of quiet separates one job from the next, so a second pass over the same
    phase is its own circle rather than an extension of the first."""
    groups: list[dict] = []
    for job in jobs:
        phase = _JOB_PHASES.get(job["task"])
        if not phase or job["task"] in _NOISE_TASKS:
            continue
        key, name, stop, glyph = phase
        current = groups[-1] if groups else None
        if (current and current["key"] == key
                and job["started"] - current["end"] <= _GROUP_GAP):
            current["jobs"].append(job)
            current["end"] = max(current["end"], job["finished"] or job["started"])
            continue
        groups.append({
            "key": key, "name": name, "stop": stop, "glyph": glyph,
            "start": job["started"],
            "end": job["finished"] or job["started"],
            "jobs": [job],
        })
    return groups


def _job_nodes(groups: list[dict]) -> list[dict]:
    nodes = []
    for index, group in enumerate(groups):
        jobs = group["jobs"]
        failed = [j for j in jobs if j["status"] == "failed"]
        running = [j for j in jobs if j["status"] == "running"]
        worked = _union_seconds([
            (j["started"], j["finished"]) for j in jobs if j["finished"]])
        tone = "fail" if failed else ("run" if running else "ok")
        steps = []
        for job in jobs:
            span = ""
            if job["finished"]:
                span = " — " + fmt_span((job["finished"] - job["started"]).total_seconds())
            steps.append([
                job["started"].strftime("%H:%M:%S"),
                f"{job['task']}{span} → {job['status']}",
            ])
        chips = [["", f"{len(jobs)} job" + ("" if len(jobs) == 1 else "s")]]
        if worked:
            chips.append(["", fmt_span(worked)])
        if failed:
            chips.append(["warn", f"{len(failed)} failed"])
        sub = " · ".join(sorted({j["task"] for j in jobs}))
        blocks = [_block("steps", "Inside the phase", rows=steps)]
        for job in failed:
            if job["response"]:
                blocks.append(_block("pre", f"Failure — job {job['id']}",
                                     text=job["response"][:2000]))
        nodes.append({
            "id": f"job-{jobs[0]['id']}",
            "kind": "phase",
            "phase": group["key"],
            "ts": _iso(group["start"]),
            "at": group["start"],
            "end": group["end"],
            "lane": 0,
            "tone": tone,
            "glyph": group["glyph"],
            "name": group["name"] + (" — failed" if failed else ""),
            "sub": sub,
            "stop": group["stop"] if not failed else None,
            "live": bool(running),
            "chips": chips,
            "job_ids": [j["id"] for j in jobs],
            "detail": {
                "meta": f"{jobs[0]['task']} · job {jobs[0]['id']} · "
                        + (fmt_span(worked) if worked else "running"),
                "blocks": blocks,
            },
            "_index": index,
        })
    return nodes


def _window_index(transitions: list[dict], moment: datetime) -> int:
    index = -1
    for position, tr in enumerate(transitions):
        if tr["at"] <= moment:
            index = position
        else:
            break
    return index


def _maintenance_nodes(groups: list[dict], transitions: list[dict]) -> list[dict]:
    """One circle per maintenance loop, not one per run.

    While a ticket sits in review the pipeline re-syncs the base branch,
    re-runs the scope review and defends comments every few minutes. Drawn
    one circle per run that is seventy circles of churn, so every run of a
    maintenance phase inside the same status window folds into one circle
    carrying the count, the total agent time and the failures."""
    buckets: dict[tuple, list[dict]] = {}
    order: list[tuple] = []
    for group in groups:
        key = (group["key"], _window_index(transitions, group["start"]))
        if key not in buckets:
            buckets[key] = []
            order.append(key)
        buckets[key].append(group)

    nodes = []
    for key in order:
        runs = buckets[key]
        jobs = [j for run in runs for j in run["jobs"]]
        failed = [j for j in jobs if j["status"] == "failed"]
        running = [j for j in jobs if j["status"] == "running"]
        worked = _union_seconds([(j["started"], j["finished"])
                                 for j in jobs if j["finished"]])
        first, last = runs[0], runs[-1]
        count = len(jobs)
        label = first["name"] if count == 1 else "%s — %d runs" % (first["name"], count)
        chips = [["", "%d run%s" % (count, "" if count == 1 else "s")]]
        if worked:
            chips.append(["", fmt_span(worked)])
        if failed:
            chips.append(["warn", "%d failed" % len(failed)])
        steps = [[j["started"].strftime("%m/%d %H:%M:%S"),
                  "%s → %s" % (j["task"], j["status"])] for j in jobs[-40:]]
        blocks = [_block("steps", "Runs (most recent 40)", rows=steps)]
        blocks.append(_block("kv", "Totals", rows=[
            ["runs", str(count)],
            ["agent time", fmt_span(worked)],
            ["failed", str(len(failed))],
            ["first", first["start"].isoformat(timespec="seconds")],
            ["last", last["end"].isoformat(timespec="seconds")],
        ]))
        nodes.append({
            "id": "mt-%s-%s" % key,
            "kind": "phase",
            "phase": first["key"],
            "ts": _iso(first["start"]),
            "at": first["start"],
            "end": last["end"],
            "lane": 0,
            "tone": "run" if running else ("fail" if failed and not
                                           [j for j in jobs if j["status"] == "ok"]
                                           else "ok"),
            "glyph": first["glyph"],
            "name": label,
            "sub": "repeated while the ticket sat in %s" % (
                transitions[key[1]]["new_status"] if key[1] >= 0 else "the pipeline"),
            "stop": None,
            "live": bool(running),
            "chips": chips,
            "job_ids": [j["id"] for j in jobs],
            "detail": {
                "meta": "%s × %d · %s of agent time" % (
                    jobs[0]["task"], count, fmt_span(worked)),
                "blocks": blocks,
            },
        })
    return nodes


def _first_line(text: str) -> str:
    """First line of an event summary, with the markdown escapes upstream
    trackers add stripped so a reviewer's sentence reads as they wrote it."""
    line = (text or "").strip().split("\n")[0]
    for token in ("\\+", "\\(", "\\)", "\\[", "\\]", "\\*", "\\_", "\\`", "\\#", "\\-"):
        line = line.replace(token, token[1])
    return line.strip().lstrip("#").replace("`", "").strip()


def _event_nodes(events: list[dict]) -> list[dict]:
    nodes: list[dict] = []
    by_event: dict[str, list[dict]] = {}
    for ev in events:
        by_event.setdefault(ev["event"], []).append(ev)

    def one(name):
        rows = by_event.get(name) or []
        return rows[0] if rows else None

    found = one("ticket_found")
    if found:
        worktree = one("ticket_worktree_created")
        classified = one("ticket_classified")
        chips = []
        if worktree:
            chips.append(["", (worktree["meta"].get("branch") or "").split("/")[-1]])
        if classified:
            chips.append(["", "work_type=" + str(classified["meta"].get("work_type", ""))])
        rows = [["source", found["links"].get("ticket", "")]]
        if worktree:
            rows.append(["branch", worktree["meta"].get("branch", "")])
        if classified:
            rows.append(["work type", str(classified["meta"].get("work_type", ""))])
        nodes.append(_node(found, "ok", "✦", "Ticket discovered",
                           _first_line(found["summary"]), stop="Created", chips=chips,
                           blocks=[_block("kv", "What happened", rows=rows)]))

    for ev in by_event.get("ticket_pr_created", []):
        repos = ev["meta"].get("repos") or []
        nodes.append(_node(
            ev, "ok", "⎇", "PRs opened — %d repo%s" % (
                len(repos), "" if len(repos) == 1 else "s"),
            _first_line(ev["summary"]), stop="In review",
            chips=[["commit", r] for r in repos],
            blocks=[_block("files", "Repos", items=[[r, ""] for r in repos])]))

    for ev in by_event.get("ticket_merged", []):
        nodes.append(_node(ev, "ok", "✔", "All PRs merged",
                           _first_line(ev["summary"]), stop="Merged"))

    for ev in by_event.get("ticket_requeued", []):
        deleted = ev["meta"].get("deleted_docs") or []
        nodes.append(_node(
            ev, "human", "⟲",
            "Re-queued — reopened #%s" % ev["meta"].get("reopened_count", "?"),
            _first_line(ev["summary"]),
            chips=[["warn", f"{len(deleted)} docs deleted"]] if deleted else [],
            blocks=[
                _block("kv", "Re-queue", rows=[
                    ["reopened count", str(ev["meta"].get("reopened_count", ""))],
                    ["comment check", str(ev["meta"].get("comment_check", ""))],
                    ["merged comments", str(ev["meta"].get("merged_comment_count", ""))],
                    ["current comments", str(ev["meta"].get("current_comment_count", ""))],
                ]),
                _block("files", "Artifacts deleted before the next pass",
                       items=[[d, ""] for d in deleted]),
                _block("note", text="Everything above this circle belongs to the "
                                    "previous pass. The plan on disk is the rewrite."),
            ]))

    for ev in by_event.get("ticket_status_override", []):
        nodes.append(_node(
            ev, "human", "☝",
            "Manual status override → %s" % ev["meta"].get("new_status", ""),
            _first_line(ev["summary"]),
            chips=[["", "%s → %s" % (ev["meta"].get("old_status", ""),
                                          ev["meta"].get("new_status", ""))]],
            blocks=[_block("kv", "Override", rows=[
                ["old status", str(ev["meta"].get("old_status", ""))],
                ["new status", str(ev["meta"].get("new_status", ""))],
            ])]))

    for ev in by_event.get("slack_mention_detected", []):
        nodes.append(_node(
            ev, "comment", "◓",
            "Slack message — %s" % ev["meta"].get("channel", ""),
            _first_line(ev["summary"]), chips=[["", "slack"]],
            blocks=[
                _block("quote", "Message", text=ev["meta"].get("text", "")),
                _block("kv", "Suggested response", rows=[
                    ["draft", str(ev["meta"].get("suggested_response", ""))]]),
            ]))

    for ev in by_event.get("ticket_issue_detected", []):
        nodes.append(_node(
            ev, "human", "⚠", "Bug reported on the ticket",
            _first_line(ev["summary"]), chips=[["warn", "issue detected"]],
            blocks=[_block("kv", "Trigger", rows=[
                ["source comment", str(ev["meta"].get("comment_id", ""))],
            ])]))

    for name in ("ticket_scope_review_passed",):
        for ev in by_event.get(name, []):
            verdict = ev["meta"].get("verdict") or "pass"
            votes = _last_votes(by_event.get("scope_review_fanout_complete", []), ev["at"])
            nodes.append(_node(
                ev, "ok" if verdict == "pass" else "fail", "⚖",
                "Scope review — %s" % verdict, _first_line(ev["summary"]),
                chips=[["doc", "scope-review.md"]],
                blocks=[_block("votes", "Votes", items=votes)] if votes else []))

    scan = [e for e in events if e["event"] in _SCAN_EVENTS]
    if scan:
        counts: dict[str, int] = {}
        for ev in scan:
            counts[ev["event"]] = counts.get(ev["event"], 0) + 1
        nodes.append(_node(
            scan[0], "comment", "○",
            "PR comment traffic — %d events" % len(scan),
            "grouped so the bot traffic does not bury the human review",
            chips=[["warn", "group of %d events" % len(scan)]],
            blocks=[
                _block("note", text="Bot and scanner comment events are collapsed "
                                    "into one circle. The reviewer threads below "
                                    "each keep their own circle."),
                _block("kv", "Counts", rows=[[k, str(v)] for k, v in
                                             sorted(counts.items(), key=lambda x: -x[1])]),
            ]))

    checks = [e for e in events if e["event"] in _CHECK_EVENTS]
    if checks:
        failed = sum(1 for e in checks if e["event"] == "ticket_check_error")
        nodes.append(_node(
            checks[0], "comment", "⚙",
            "CI checks — %d reports" % len(checks),
            "%d failed · %d passed" % (failed, len(checks) - failed),
            chips=[["warn", "group of %d events" % len(checks)]],
            blocks=[
                _block("kv", "Counts", rows=[
                    ["reports", str(len(checks))],
                    ["failed", str(failed)],
                    ["passed", str(len(checks) - failed)],
                ]),
                _block("pre", "Last report", text=checks[-1]["summary"][:1200]),
            ]))
    return nodes


def _last_votes(fanouts: list[dict], before: datetime) -> list[list[str]]:
    latest = None
    for ev in fanouts:
        if ev["at"] <= before:
            latest = ev
    if not latest:
        return []
    votes = latest["meta"].get("votes") or {}
    dropped = latest["meta"].get("dropped") or {}
    out = [[k, str(v)] for k, v in sorted(votes.items())]
    out += [[k, "DROPPED"] for k in sorted(dropped)]
    return out


def _node(ev: dict, tone: str, glyph: str, name: str, sub: str,
          stop: str | None = None, chips=None, blocks=None,
          lane: int = 0, repo: str | None = None) -> dict:
    return {
        "id": "ev-" + str(ev["id"]),
        "kind": "event",
        "phase": None,
        "ts": _iso(ev["at"]),
        "at": ev["at"],
        "end": ev["at"],
        "lane": lane,
        "tone": tone,
        "glyph": glyph,
        "name": name,
        "sub": sub,
        "stop": stop,
        "repo": repo,
        "live": False,
        "chips": chips or [],
        "links": ev.get("links") or {},
        "detail": {"meta": ev["event"], "blocks": blocks or []},
    }


def _comment_nodes(events: list[dict]) -> list[dict]:
    """One circle per reviewer thread, on its own stub off the spine.

    The commits a thread produced come from meta.commit and meta.files. A
    reconciliation event names every thread one commit closed."""
    by_comment: dict[str, list[dict]] = {}
    for ev in events:
        cid = ev["meta"].get("comment_id")
        if cid is None:
            if ev["event"] != "ticket_pr_comment_resolution_reconciled":
                continue
            cid = "reconcile-" + str(ev["id"])
        by_comment.setdefault(str(cid), []).append(ev)

    nodes = []
    for cid, group in by_comment.items():
        if not any(e["event"] in _COMMENT_EVENTS for e in group):
            continue
        group.sort(key=lambda e: e["at"])
        last = group[-1]
        first = group[0]
        repo = next((e["meta"].get("repo") for e in group if e["meta"].get("repo")), "")
        if not repo:
            repo = next((r for r in (_repo_from_link(e) for e in group) if r), "")
        capped = any(e["event"] == "ticket_pr_comment_fix_capped" for e in group)
        fixed = any(e["event"] == "ticket_pr_comment_fixed" for e in group)
        reconciled = [e for e in group
                      if e["event"] == "ticket_pr_comment_resolution_reconciled"]
        tone = "ok" if fixed or reconciled else ("fail" if capped else "comment")
        glyph = "✓" if tone == "ok" else ("✕" if tone == "fail" else "●")
        chips = []
        blocks = []
        quote = _comment_quote(group)
        if quote:
            blocks.append(_block("quote", "Reviewer", text=quote))
        steps = []
        seen: set[str] = set()
        for ev in group:
            line = _first_line(ev["summary"])[:160]
            token = ev["event"] + "|" + line
            if token in seen:
                continue
            seen.add(token)
            steps.append([ev["at"].strftime("%m/%d %H:%M:%S"), line])
        blocks.append(_block("steps", "What the agent did", rows=steps))
        commits = []
        for ev in group:
            sha = ev["meta"].get("commit")
            if not sha:
                continue
            commits.append({
                "sha": sha[:7],
                "msg": _first_line(ev["summary"])[:180],
                "files": ev["meta"].get("files") or [],
            })
            chips.append(["commit", sha[:7]])
            files = ev["meta"].get("files") or []
            if files:
                chips.append(["", "%d file%s" % (len(files), "" if len(files) == 1 else "s")])
        if commits:
            blocks.append(_block("commits", "Commits that resulted", items=commits))
        for ev in reconciled:
            ids = ev["meta"].get("comment_ids") or []
            chips.append(["", "%d threads reconciled" % len(ids)])
            blocks.append(_block("kv", "Reconciliation", rows=[
                ["threads closed", ", ".join(str(i) for i in ids)],
                ["at commit", str(ev["meta"].get("commit", ""))[:7]],
                ["PR", "%s #%s" % (repo or "", ev["meta"].get("pr_id", ""))],
            ]))
        if capped:
            attempts = next((e["meta"].get("attempts") for e in group
                             if e["event"] == "ticket_pr_comment_fix_capped"), "")
            chips.append(["warn", "%s of 2 attempts" % attempts])
        draft = next((e["meta"].get("draft_reply") for e in group
                      if e["meta"].get("draft_reply")), None)
        if draft:
            blocks.append(_block("pre", "Draft reply that triggered the reroute",
                                 text=draft))
            chips.insert(0, ["warn", "reclassified"])
        name = _comment_title(group, capped, bool(reconciled))
        node = _node(first, tone, glyph, name, "%s · comment %s" % (repo or "", cid),
                     chips=chips, blocks=blocks, lane=1, repo=repo)
        node["id"] = "cm-" + str(cid)
        node["ts"] = _iso(first["at"])
        node["at"] = first["at"]
        node["end"] = last["at"]
        node["detail"]["meta"] = " → ".join(dict.fromkeys(e["event"] for e in group))
        node["links"] = last.get("links") or first.get("links") or {}
        nodes.append(node)
    return nodes


def _repo_from_link(ev: dict) -> str:
    """The repo name out of a comment URL, for events whose meta omits it."""
    url = (ev.get("links") or {}).get("comment") or (ev.get("links") or {}).get("pr") or ""
    parts = [p for p in url.split("/") if p]
    for marker in ("pull-requests", "pull"):
        if marker in parts:
            index = parts.index(marker)
            if index >= 1:
                return parts[index - 1]
    return ""


def _comment_quote(group: list[dict]) -> str:
    """The reviewer's own words, dug out of the event summary."""
    for ev in group:
        summary = ev["summary"] or ""
        if '"' in summary:
            return _first_line(summary.split('"')[1])
        if "attempts: " in summary:
            return _first_line(summary.split("attempts: ", 1)[-1])
    return ""


def _comment_title(group: list[dict], capped: bool, reconciled: bool) -> str:
    quote = _comment_quote(group)
    tail = (" — " + quote[:70]) if quote else ""
    if reconciled:
        return "Threads reconciled by one commit" + tail
    if any(e["event"] == "ticket_pr_comment_fixed" for e in group):
        return "Comment fixed" + tail
    if capped:
        return "Capped after 2 attempts" + tail
    return "Comment" + tail


def _transition_nodes(transitions: list[dict], instance_key: str,
                      covered: list[dict]) -> list[dict]:
    """Circles for status changes no job circle already carries.

    A run of changes inside two minutes becomes one circle, because three
    gap pills one second apart are unreadable. A change made by a human is
    always its own circle."""
    spans = [(n["at"], n["end"]) for n in covered]
    loose = []
    for tr in transitions:
        if tr["prior_status"] is None:
            continue
        human = bool(tr["actor"]) and tr["actor"] != instance_key
        if not human and any(lo - _TRANSITION_SLACK <= tr["at"] <= hi + _TRANSITION_SLACK
                             for lo, hi in spans):
            continue
        loose.append(tr)

    nodes = []
    run: list[dict] = []

    def flush():
        if not run:
            return
        first, last = run[0], run[-1]
        human = bool(first["actor"]) and first["actor"] != instance_key
        rejected = any(t["rejected"] for t in run)
        tone = "human" if human else ("fail" if rejected or
                                      last["new_status"] == "blocked" else "ok")
        glyph = "☝" if human else ("◆" if tone == "fail" else "⚡")
        if len(run) == 1:
            name = "%s → %s" % (first["prior_status"], first["new_status"])
        else:
            name = "%s → %s in %s" % (
                first["prior_status"], last["new_status"],
                fmt_span((last["at"] - first["at"]).total_seconds()))
        if human:
            name = "Manual override — " + name
        blocks = [_block("steps", "Transitions", rows=[
            [t["at"].strftime("%H:%M:%S"),
             "%s → %s%s" % (t["prior_status"], t["new_status"],
                                 (" — " + t["reason"]) if t["reason"] else "")]
            for t in run])]
        if first["reason"]:
            blocks.append(_block("pre", "Reason", text=first["reason"]))
        nodes.append({
            "id": "tr-%s" % first["at"].timestamp(),
            "kind": "transition",
            "phase": None,
            "ts": _iso(first["at"]),
            "at": first["at"],
            "end": last["at"],
            "lane": 0,
            "tone": tone,
            "glyph": glyph,
            "name": name,
            "sub": "actor: %s" % (first["actor"] or "—"),
            "stop": _STOP_LABELS.get(last["new_status"]),
            "live": False,
            "chips": [["", "%d transition%s" % (len(run), "" if len(run) == 1 else "s")]],
            "detail": {"meta": "ticket_transitions · actor=%s" % (first["actor"] or ""),
                       "blocks": blocks},
        })
        run.clear()

    for tr in loose:
        if run and (tr["at"] - run[-1]["at"] > _COLLAPSE_WINDOW
                    or (tr["actor"] != run[-1]["actor"])):
            flush()
        run.append(tr)
    flush()
    return nodes


def _attach_docs(nodes: list[dict], docs: list[dict]) -> None:
    """Give each phase circle the files it produced.

    A file is produced by the phase whose window contains its mtime. A file
    the phase is known to write but whose mtime falls outside the window was
    rewritten by a later pass; it is still listed, marked as superseded, so
    the pass that wrote it first is not silently empty."""
    phases = [n for n in nodes
              if n["kind"] == "phase" and n["phase"] in _PHASE_DOCS]
    if not phases:
        return
    for index, node in enumerate(phases):
        start = node["at"]
        end = phases[index + 1]["at"] if index + 1 < len(phases) else None
        expected = _PHASE_DOCS.get(node["phase"], [])
        only_expected = node["phase"] in _MAINTENANCE_PHASES
        produced, superseded = [], []
        for doc in docs:
            inside = doc["mtime"] >= start and (end is None or doc["mtime"] < end)
            if inside and not (only_expected and doc["name"] not in expected):
                produced.append(_art(doc, "written in this phase"))
            elif doc["name"] in expected:
                superseded.append(_art(doc, "written in an earlier pass"
                                       if doc["mtime"] < start
                                       else "rewritten by a later pass"))
        if produced:
            node["detail"]["blocks"].insert(
                0, _block("arts", "Artifacts produced", items=produced))
            for art in produced:
                kind = "media" if art["kind"] in ("video", "image") else "doc"
                node["chips"].append([kind, art["name"]])
        if superseded:
            node["detail"]["blocks"].append(
                _block("arts", "This phase writes these, another pass owns the copy on disk",
                       items=superseded))
        for art in produced:
            if art["kind"] == "video":
                node["detail"]["blocks"].append(
                    _block("video", "Recording", name=art["name"], size=art["size"]))


def _attach_phase_events(nodes: list[dict], events: list[dict]) -> None:
    """Fold the events a phase emitted into that phase's circle.

    The plan fan-out votes and the list of repos the consensus plan touched
    belong on the planning circle, not on circles of their own."""
    phases = [n for n in nodes if n["kind"] == "phase"]
    if not phases:
        return
    folded: dict[str, list[list[str]]] = {}
    hooks: dict[str, dict[str, int]] = {}
    for index, node in enumerate(phases):
        start = node["at"]
        end = phases[index + 1]["at"] if index + 1 < len(phases) else None
        inside = [e for e in events
                  if e["at"] >= start and (end is None or e["at"] < end)]
        for ev in inside:
            if ev["event"] in ("ctp_fanout_complete", "scope_review_fanout_complete"):
                votes = ev["meta"].get("votes") or {
                    name: "PASS" for name in (ev["meta"].get("valid") or [])}
                items = [[k, str(v)] for k, v in sorted(votes.items())]
                items += [[k, "DROPPED"] for k in sorted(ev["meta"].get("dropped") or {})]
                if items:
                    node["detail"]["blocks"].append(
                        _block("votes", "Fan-out votes", items=items))
            elif ev["event"] == "ctp_complete":
                changed = ev["meta"].get("changed") or []
                if changed:
                    node["detail"]["blocks"].append(
                        _block("files", "Repos changed",
                               items=[[r, ""] for r in changed]))
                    node["chips"].append(["", "%d repos changed" % len(changed)])
            elif ev["event"] in ("git_pre_commit_unresolved", "commit_hook_failed",
                                 "commit_hook_repair_failed", "commit_hook_repair_rejected",
                                 "commit_hook_unrecognised"):
                node["detail"]["blocks"].append(
                    _block("warn", text=_first_line(ev["summary"])[:240]))
                hooks.setdefault(node["id"], {})
                hooks[node["id"]][ev["event"]] = hooks[node["id"]].get(ev["event"], 0) + 1
            elif ev["event"] in _PHASE_OWNED_EVENTS:
                folded.setdefault(node["id"], []).append(
                    [ev["at"].strftime("%H:%M:%S"), _first_line(ev["summary"])[:160]])
    for node in phases:
        rows = folded.get(node["id"])
        if rows:
            node["detail"]["blocks"].append(
                _block("steps", "Events in this phase", rows=rows[:30]))
        for name, count in sorted((hooks.get(node["id"]) or {}).items()):
            node["chips"].append(
                ["warn", name if count == 1 else "%s ×%d" % (name, count)])


def _assign_passes(nodes: list[dict], events: list[dict]) -> list[dict]:
    boundaries = [e["at"] for e in events if e["event"] == "ticket_requeued"]
    boundaries.sort()
    passes = []
    for index in range(len(boundaries) + 1):
        passes.append({
            "pass": index + 1,
            "label": "Pass %d" % (index + 1) if index else "Pass 1",
        })
    for node in nodes:
        number = 1
        for at in boundaries:
            if node["at"] > at:
                number += 1
        node["pass"] = number
    for boundary_index, at in enumerate(boundaries):
        passes[boundary_index + 1]["label"] = "Pass %d — reopened %s" % (
            boundary_index + 2, at.strftime("%Y-%m-%d"))
    return passes


def _compute_gaps(nodes: list[dict], transitions: list[dict]) -> None:
    spine = [n for n in nodes if n["lane"] == 0]
    previous = None
    for node in spine:
        if previous is None or previous["pass"] != node["pass"]:
            node["gap_ms"] = None
            node["why"] = None
            previous = node
            continue
        gap = (node["at"] - previous["at"]).total_seconds()
        gap = max(gap, 0.0)
        node["gap_ms"] = int(gap * 1000)
        node["gap_label"] = fmt_span(gap)
        midpoint = previous["at"] + (node["at"] - previous["at"]) / 2
        status = _status_at(transitions, midpoint)
        node["why"] = _WHY.get(status) if gap >= 3600 else None
        previous = node
    for node in nodes:
        node.setdefault("gap_ms", None)
        node.setdefault("gap_label", None)
        node.setdefault("why", None)


def _segments(jobs: list[dict], transitions: list[dict],
              nodes: list[dict]) -> list[dict]:
    """The time-scaled ribbon: solid where the agent worked, hatched where
    the ticket waited, keyed to the circle that opens on click."""
    spans = [(j["started"], j["finished"]) for j in jobs
             if j["finished"] and j["task"] not in _NOISE_TASKS]
    work = _merge_spans(spans, _RIBBON_BRIDGE)
    if not work:
        return []
    spine = sorted([n for n in nodes if n["lane"] == 0], key=lambda n: n["at"])
    if not spine:
        return []
    start = min(spine[0]["at"], work[0][0], min(n["at"] for n in nodes))
    end = max(spine[-1]["end"], work[-1][1], max(n["end"] for n in nodes))

    def node_at(moment: datetime) -> str:
        pick = spine[0]["id"]
        for node in spine:
            if node["at"] <= moment:
                pick = node["id"]
            else:
                break
        return pick

    out: list[dict] = []
    cursor = start
    for lo, hi in work:
        if lo > cursor:
            status = _status_at(transitions, cursor)
            out.append({
                "t0": _iso(cursor), "t1": _iso(lo),
                "kind": _RIBBON_KINDS.get(status, "idle"),
                "label": _WHY.get(status, status or "waiting"),
                "seconds": (lo - cursor).total_seconds(),
                "node": node_at(cursor),
            })
        label = _work_label(jobs, lo, hi)
        out.append({
            "t0": _iso(lo), "t1": _iso(hi), "kind": "work", "label": label,
            "seconds": (hi - lo).total_seconds(), "node": node_at(lo),
        })
        cursor = hi
    if end > cursor:
        status = _status_at(transitions, cursor)
        out.append({
            "t0": _iso(cursor), "t1": _iso(end),
            "kind": _RIBBON_KINDS.get(status, "idle"),
            "label": _WHY.get(status, status or "waiting"),
            "seconds": (end - cursor).total_seconds(),
            "node": node_at(cursor),
        })
    return out


def _work_label(jobs: list[dict], lo: datetime, hi: datetime) -> str:
    names = []
    for job in jobs:
        if job["task"] in _NOISE_TASKS or not job["finished"]:
            continue
        if job["finished"] < lo or job["started"] > hi:
            continue
        phase = _JOB_PHASES.get(job["task"])
        if phase and phase[1] not in names:
            names.append(phase[1])
    return " → ".join(names[:3]) or "agent working"


def _kpis(jobs: list[dict], nodes: list[dict]) -> dict:
    spine = sorted([n for n in nodes if n["lane"] == 0], key=lambda n: n["at"])
    if not spine:
        return {}
    ordered = sorted(nodes, key=lambda n: n["at"])
    wall = (max(n["end"] for n in ordered) - ordered[0]["at"]).total_seconds()
    work = _union_seconds([(j["started"], j["finished"]) for j in jobs
                           if j["finished"] and j["task"] not in _NOISE_TASKS])
    longest = 0.0
    longest_node = None
    for node in spine:
        if node.get("gap_ms") and node["gap_ms"] / 1000 > longest:
            longest = node["gap_ms"] / 1000
            longest_node = node
    return {
        "wall_seconds": wall, "wall_label": fmt_span(wall),
        "work_seconds": work, "work_label": fmt_span(work),
        "wait_seconds": max(wall - work, 0.0), "wait_label": fmt_span(max(wall - work, 0.0)),
        "longest_gap_seconds": longest, "longest_gap_label": fmt_span(longest),
        "longest_gap_node": longest_node["id"] if longest_node else None,
        "longest_gap_why": longest_node.get("why") if longest_node else None,
        "work_pct": round(100.0 * work / wall, 1) if wall else 0.0,
    }


def build(instance_key: str, ticket_key: str, docs_dir: Path) -> dict:
    """Return the whole timeline for one ticket, ready to render."""
    events = log.get_all_events_for_ticket(ticket_key)
    for ev in events:
        ev["at"] = _dt(ev["ts"])
    events = [e for e in events if e["at"]]
    events.sort(key=lambda e: e["at"])

    jobs = _load_jobs(instance_key, ticket_key)
    transitions = _load_transitions(instance_key, ticket_key)
    docs = _load_docs(docs_dir)

    groups = _group_jobs(jobs)
    nodes = _job_nodes([g for g in groups if g["key"] not in _MAINTENANCE_PHASES])
    nodes += _maintenance_nodes(
        [g for g in groups if g["key"] in _MAINTENANCE_PHASES], transitions)
    nodes += _event_nodes(events)
    nodes += _comment_nodes(events)
    nodes += _transition_nodes(transitions, instance_key,
                               [n for n in nodes if n["lane"] == 0])
    nodes.sort(key=lambda n: (n["at"], n["lane"]))

    passes = _assign_passes(nodes, events)
    _attach_docs(nodes, docs)
    _attach_phase_events(nodes, events)
    _compute_gaps(nodes, transitions)
    segments = _segments(jobs, transitions, nodes)
    kpis = _kpis(jobs, nodes)

    for node in nodes:
        node.pop("at", None)
        node.pop("end", None)
        node.pop("_index", None)
    return {
        "nodes": nodes,
        "segments": segments,
        "kpis": kpis,
        "passes": passes,
        "docs": [{"name": d["name"], "size": d["size"],
                  "size_label": _size_label(d["size"]),
                  "mtime": _iso(d["mtime"])} for d in docs],
    }
