import json
import re
import subprocess
import time
from pathlib import Path

import httpx

import core.log as log
import core.state as state
import core.terminal as terminal
from core.config import get_repos, ticket_worktree_path, resolve_env
from core.claude_runner import run_haiku, run_claude_code, extract_json
from core.ticket_status import TicketStatus, transition
from features.platforms import make_platform
from features.ticket_systems import make_ticket_system
import core.events as events


STATES = ["new", "planning", "reviewing", "pr_ready", "pr_created", "in_review", "merged"]


def _label(key: str, ts: dict) -> str:
    return ts.get("slug", key)

MAX_RESTARTS = 3
RESTART_COOLDOWN_SECS = 300
STUCK_SCROLLBACK_SECS = 300

_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[a-zA-Z]")
_SPINNER_LINE_RE = re.compile(r"(Cogitated|Worked|Cooked|Baked|Churned|Crunched|Thought) for \d+[ms\d\s]*")


def _scrollback_fingerprint(scrollback: str) -> str:
    import hashlib
    stripped = _ANSI_RE.sub("", scrollback)
    lines = [l for l in stripped.splitlines() if not _SPINNER_LINE_RE.search(l)]
    return hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]


def _command_for_status(status: str) -> str | None:
    if status == "planning":
        return "/confer-technical-plan docs/"
    if status == "reviewing":
        return "Run /tri-review and save the full output to docs/tri-review.md"
    return None


def restart_session(config, key, ts, base_url=""):
    from datetime import datetime, timezone
    ws = config["workspace"]
    slug = ts.get("slug", "")
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug

    terminal.kill_terminal(key)
    time.sleep(1)
    terminal.ensure_session(key, str(ticket_dir))
    time.sleep(2)
    terminal.send_keys(key, "claude --dangerously-skip-permissions")
    time.sleep(5)
    terminal.send_bare_enter(key)
    time.sleep(3)

    cmd = _command_for_status(ts.get("status", ""))
    if cmd:
        terminal.send_keys(key, cmd)

    ts["last_restart_at"] = datetime.now(timezone.utc).isoformat()
    ts["restart_count"] = ts.get("restart_count", 0) + 1
    ts.pop("stuck_logged", None)

    log.emit("ticket_session_restarted", f"Restarted session for {_label(key, ts)} (attempt {ts['restart_count']})",
        links={"detail": f"{base_url}/tickets/{key}"} if base_url else {},
        meta={"ticket": key, "status": ts.get("status", ""), "restart_count": ts["restart_count"]})


def _triage_session(config, key, ts, base_url):
    from datetime import datetime, timezone

    last = ts.get("last_triage_at", "")
    if last:
        elapsed = (datetime.now(timezone.utc) - datetime.fromisoformat(last)).total_seconds()
        if elapsed < RESTART_COOLDOWN_SECS:
            return

    health = terminal.session_healthy(key)
    scrollback = terminal.capture_pane(key, 60)
    status = ts.get("status", "")
    expected_cmd = _command_for_status(status)
    expected_file = "docs/change-manifest.md" if status == "planning" else "docs/tri-review.md"

    ws = config["workspace"]
    slug = ts.get("slug", "")
    ticket_path = ws["root"] / ws["tickets_dir"] / slug / "docs" / "ticket.md"
    ticket_desc = ""
    if ticket_path.exists():
        ticket_desc = ticket_path.read_text()[:2000]

    prompt = f"""You are a CI supervisor for an automated ticket pipeline. A Claude Code session was given a task but is now idle without producing the expected output.

Your job: decide what to do next. You have deep understanding of the pipeline:
- "planning" stage: Claude runs /confer-technical-plan docs/ which should produce docs/change-manifest.md
- "reviewing" stage: Claude runs /tri-review which should produce docs/tri-review.md
- Claude runs inside a tmux session. You can send text to it (it goes to the Claude Code prompt).
- If Claude asked a question and is waiting for input, you should answer it or instruct it to proceed.
- If Claude crashed or the session is empty, recommend a restart.
- If the ticket is genuinely impossible (missing info that only a human has), say so.
- NEVER tell Claude to git push, create a PR, or run gh pr create. The pipeline handles PRs separately. Your job is only to get the expected output file produced.

Ticket: {key}
Stage: {status}
Expected output file: {expected_file}
Command that was sent: {expected_cmd}
Session alive: {health['alive']}
Claude process running: {health['claude_running']}
Triage attempts so far: {ts.get('triage_count', 0)}

Ticket description:
{ticket_desc}

Terminal scrollback (last 60 lines):
{scrollback}

Reply with EXACTLY one JSON object, no other text:
{{"action": "send", "message": "text to send to the terminal"}}
OR {{"action": "restart"}}
OR {{"action": "skip", "reason": "still working or just needs more time"}}
OR {{"action": "stuck", "reason": "why human input is needed"}}

Prefer "send" when Claude is at a prompt and you can unblock it. Be direct and actionable in your message — tell Claude exactly what to do, don't ask questions back. If it looks like the work might already be done elsewhere or the ticket is a duplicate, tell Claude to verify and proceed with whatever makes sense."""

    result = run_haiku(prompt, timeout=60)
    if not result:
        return

    ts["last_triage_at"] = datetime.now(timezone.utc).isoformat()
    ts["triage_count"] = ts.get("triage_count", 0) + 1

    try:
        decision = json.loads(result.strip())
    except json.JSONDecodeError:
        parsed = extract_json(result)
        if not parsed:
            return
        decision = parsed

    action = decision.get("action", "")

    if action == "send":
        msg = decision.get("message", "")
        if msg:
            terminal.send_keys(key, msg)
            log.emit("ticket_session_nudged", f"Sent nudge to {_label(key, ts)}: {msg[:80]}",
                links={"detail": f"{base_url}/tickets/{key}"},
                meta={"ticket": key, "message": msg[:200]})

    elif action == "restart":
        restart_session(config, key, ts, base_url)

    elif action == "stuck":
        reason = decision.get("reason", "unknown")
        if not ts.get("stuck_logged"):
            log.emit("ticket_session_stuck", f"Session for {_label(key, ts)} needs manual intervention: {reason[:100]}",
                links={"detail": f"{base_url}/tickets/{key}"},
                meta={"ticket": key, "reason": reason})
            ts["stuck_logged"] = True


def check_health(config):
    from datetime import datetime, timezone
    ticket_state = state.load("tickets")
    base_url = config["_base_url"]
    modified_keys = {}

    active_keys = {k for k, v in ticket_state.items() if v.get("status") not in ("done", "merged")}
    for session_key in terminal.list_ticket_keys():
        if session_key not in active_keys:
            terminal.kill_terminal(session_key)
            log.emit("ticket_session_orphan_reaped", f"Killed orphan tmux session term-{session_key}",
                meta={"ticket": session_key})

    now = datetime.now(timezone.utc)
    for key, ts in ticket_state.items():
        if ts.get("status") not in ("planning", "reviewing"):
            continue
        health = terminal.session_healthy(key)
        stuck = not (health["alive"] and health["claude_running"])

        if not stuck:
            scrollback = terminal.capture_pane(key, 60)
            fp = _scrollback_fingerprint(scrollback)
            if ts.get("last_scrollback_hash") != fp:
                ts["last_scrollback_hash"] = fp
                ts["last_scrollback_change_at"] = now.isoformat()
                modified_keys[key] = ts
                continue
            last_change = ts.get("last_scrollback_change_at", "")
            if last_change:
                idle_secs = (now - datetime.fromisoformat(last_change)).total_seconds()
                if idle_secs < STUCK_SCROLLBACK_SECS:
                    continue
            else:
                ts["last_scrollback_change_at"] = now.isoformat()
                modified_keys[key] = ts
                continue

        _triage_session(config, key, ts, base_url)
        modified_keys[key] = ts

    if modified_keys:
        fresh = state.load("tickets")
        for key, ts in modified_keys.items():
            fresh[key] = ts
        state.save("tickets", fresh)


def _image_filename(alt: str, url: str, seen: set | None = None) -> str:
    filename = re.sub(r'[^\w.\-]', '_', alt) if alt else url.split("/")[-1]
    if not filename.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp')):
        filename += '.png'
    if seen is not None and filename in seen:
        base, ext = filename.rsplit('.', 1)
        i = 2
        while f"{base}_{i}.{ext}" in seen:
            i += 1
        filename = f"{base}_{i}.{ext}"
    if seen is not None:
        seen.add(filename)
    return filename


def _download_attachments(config, ticket, docs_path):
    attachments = ticket.get("attachments", [])
    att_dir = docs_path / "attachments"

    headers = {}
    auth = None
    ticket_system = config["job"].get("ticket_system", "")
    if ticket_system == "jira":
        user = resolve_env(config, "jira", "user_env")
        token = resolve_env(config, "jira", "token_env")
        if user and token:
            auth = (user, token)
    elif ticket_system == "linear":
        token = resolve_env(config, "linear", "token_env")
        if token:
            headers["Authorization"] = token

    inline_images = re.findall(r'!\[([^\]]*)\]\((https?://[^)]+)\)', ticket.get("description", ""))
    all_downloads = [(a.get("filename", ""), a.get("url", "")) for a in attachments]
    seen = set()
    for alt, url in inline_images:
        filename = _image_filename(alt, url, seen)
        all_downloads.append((filename, url))

    if not all_downloads:
        return

    att_dir.mkdir(exist_ok=True)
    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for filename, url in all_downloads:
            if not url or not filename:
                continue
            try:
                resp = client.get(url, auth=auth, headers=headers)
                if resp.status_code == 200:
                    (att_dir / filename).write_bytes(resp.content)
            except Exception:
                pass


def _localize_images(md: str, docs_path: Path) -> str:
    att_dir = docs_path / "attachments"
    seen = set()
    def _replace(m):
        alt, url = m.group(1), m.group(2)
        filename = _image_filename(alt, url, seen)
        local = att_dir / filename
        if local.exists():
            return f"![{alt}](attachments/{filename})"
        return m.group(0)
    return re.sub(r'!\[([^\]]*)\]\((https?://[^)]+)\)', _replace, md)


def _resolve_status(config: dict, external_status: str) -> str | None:
    system = config["job"].get("ticket_system", "")
    status_map = config.get(system, {}).get("status_map", {})
    if not status_map:
        return None
    return status_map.get(external_status)


def check(config: dict):
    from datetime import datetime, timezone
    assigned = _fetch_tickets(config)
    if not assigned:
        return

    ticket_state = state.load("tickets")
    base_url = config["_base_url"]
    assigned_keys = {t["key"] for t in assigned}
    discovery_only = not get_repos(config)

    for key, ts in list(ticket_state.items()):
        if key not in assigned_keys and ts.get("status") != TicketStatus.done:
            ts["status"] = transition(ts.get("status", "new"), "done")
            ts["done_at"] = datetime.now(timezone.utc).isoformat()
            ticket_state[key] = ts

    for ticket in assigned:
        key = ticket["key"]
        existing = key in ticket_state
        ts = ticket_state.get(key, {"status": "new"})
        ts["external_status"] = ticket.get("status", "")
        if ts.get("status") == "done":
            ts.pop("done_at", None)
            if ts.get("prs"):
                ts["status"] = "pr_created"
            elif ts.get("slug"):
                ts["status"] = "pr_ready"
            else:
                ts["status"] = "new"

        if discovery_only:
            if not existing:
                log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
                    links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
                    meta={"ticket": key})
                ts["slug"] = _make_slug(key, ticket["summary"])
                ts["branch"] = _make_branch(config, key, ticket)
                ts["url"] = ticket.get("url", "")
            ticket_state[key] = ts
            continue

        mapped = _resolve_status(config, ticket.get("status", ""))
        if mapped and "slug" not in ts:
            ts["slug"] = _make_slug(key, ticket["summary"])
            ts["branch"] = _make_branch(config, key, ticket)
            ts["url"] = ticket.get("url", "")
            ts["status"] = TicketStatus(mapped).value
            if mapped != "new":
                ticket_state[key] = ts
                continue

        if ts["status"] == TicketStatus.merged:
            continue

        if ts["status"] == TicketStatus.pr_failed:
            continue


        if ts["status"] == "new":
            ts = _setup_ticket(config, ticket, base_url)

        if ts["status"] == "planning":
            ts = _check_planning(config, ticket, ts, base_url)

        if ts["status"] == "reviewing":
            ts = _check_reviewing(config, ticket, ts, base_url)

        if ts["status"] == "pr_ready" and config.get("pr", {}).get("auto_pr") and not ts.get("pr_scheduled_at"):
            ts = _create_pr(config, ticket, ts, base_url)

        if ts["status"] in ("pr_created", "in_review"):
            ts = _resolve_conflicts(config, ticket, ts, base_url)

        if ts["status"] in ("pr_created", "in_review"):
            platform = make_platform(config)
            result = platform.monitor_ci(ticket, ts, base_url)
            if result.get("_ci_failed"):
                ts = _handle_ci_failure(config, platform, ticket, ts, result["pr"], result["checks"], base_url)
            else:
                ts = result

        if ts["status"] in ("pr_created", "in_review") and ts.get("ci_passed") and config.get("pr", {}).get("auto_merge"):
            ts = _merge(config, ticket, ts, base_url)

        if ts["status"] in ("pr_created", "in_review"):
            ts = _check_in_review(config, ticket, ts, base_url)

        ticket_state[key] = ts

    state.save("tickets", ticket_state)


def _fetch_tickets(config: dict) -> list[dict]:
    ts = make_ticket_system(config)
    if not ts:
        return []
    return ts.fetch_tickets()


def _setup_ticket(config, ticket, base_url) -> dict:
    from datetime import datetime, timezone
    ws = config["workspace"]
    repos = get_repos(config)
    key = ticket["key"]
    slug = _make_slug(key, ticket["summary"])
    branch = _make_branch(config, key, ticket)

    log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key})

    any_worktree = False
    for repo in repos:
        wt_path = ticket_worktree_path(config, slug, repo["name"])
        if (wt_path / ".git").is_file():
            any_worktree = True
            subprocess.run(["git", "fetch", "origin"], cwd=str(wt_path), capture_output=True, timeout=60)
            subprocess.run(["git", "checkout", branch], cwd=str(wt_path), capture_output=True, timeout=60)
            subprocess.run(["git", "reset", "--hard", f"origin/{ws['base_branch']}"], cwd=str(wt_path), capture_output=True, timeout=60)
            subprocess.run(["git", "clean", "-fd"], cwd=str(wt_path), capture_output=True, timeout=60)
            continue
        wt_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "worktree", "prune"], cwd=str(repo["path"]), capture_output=True, timeout=60)
        subprocess.run(["git", "fetch", "origin"], cwd=str(repo["path"]), capture_output=True, timeout=60)
        branches = subprocess.run(["git", "branch", "--list"], cwd=str(repo["path"]), capture_output=True, text=True, timeout=60).stdout
        if branch not in branches.replace("* ", "").replace("  ", " ").split():
            result = subprocess.run(["git", "branch", branch, f"origin/{ws['base_branch']}"], cwd=str(repo["path"]), capture_output=True, timeout=60)
            if result.returncode != 0:
                subprocess.run(["git", "branch", branch, ws["base_branch"]], cwd=str(repo["path"]), capture_output=True, timeout=60)
        wt_result = subprocess.run(["git", "worktree", "add", str(wt_path), branch], cwd=str(repo["path"]), capture_output=True, text=True, timeout=60)
        if wt_result.returncode != 0:
            log.emit("ticket_worktree_error", f"Failed to create worktree for {slug} in {repo['name']}: {wt_result.stderr.strip()}",
                meta={"ticket": key, "repo": repo["name"]})
            continue
        any_worktree = True
        subprocess.run(["chown", "-R", "1000:1000", str(wt_path)], capture_output=True, timeout=60)
        git_dir = repo["path"] / ".git"
        if git_dir.is_dir():
            subprocess.run(["chown", "-R", "1000:1000", str(git_dir)], capture_output=True, timeout=60)

        for dep in ws.get("dep_commands", []):
            if dep["match"] == repo["name"]:
                try:
                    subprocess.run(dep["cmd"].split(), cwd=str(wt_path), capture_output=True, timeout=300)
                except FileNotFoundError:
                    pass

    if not any_worktree:
        log.emit("ticket_worktree_error", f"No worktrees created for {slug}, staying at new",
            meta={"ticket": key})
        return {"status": TicketStatus.new.value, "slug": slug, "branch": branch}

    docs_path = ws["root"] / ws["tickets_dir"] / slug / "docs"
    docs_path.mkdir(parents=True, exist_ok=True)

    md = f"# {key}: {ticket['summary']}\n\n**Status:** {ticket['status']}\n\n## Description\n\n{ticket.get('description', 'No description')}\n"

    if ticket.get("related"):
        md += "\n## Related Tickets\n\n"
        for r in ticket["related"]:
            md += f"- **{r['key']}** ({r['relation']}): {r['summary']}\n"

    if ticket.get("subtasks"):
        md += "\n## Subtasks\n\n"
        for s in ticket["subtasks"]:
            md += f"- **{s['key']}**: {s['summary']}\n"

    if ticket.get("attachments"):
        md += "\n## Attachments\n\n"
        for a in ticket["attachments"]:
            md += f"- {a['filename']} ({a['url']})\n"

    if ticket.get("project"):
        p = ticket["project"]
        (docs_path / "epic.md").write_text(f"# Epic: {p['name']}\n\n{p.get('description', '')}\n")
    elif ticket.get("parent"):
        p = ticket["parent"]
        pkey = p.get("identifier") or p.get("key", "")
        ptitle = p.get("title") or p.get("summary", "")
        pdesc = p.get("description", "")
        (docs_path / "epic.md").write_text(f"# Parent: {pkey}: {ptitle}\n\n{pdesc}\n")

    _download_attachments(config, ticket, docs_path)
    md = _localize_images(md, docs_path)
    (docs_path / "ticket.md").write_text(md)
    subprocess.run(["chown", "-R", "1000:1000", str(docs_path.parent)], capture_output=True, timeout=60)

    log.emit("ticket_worktree_created", f"Workspace ready for {slug}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key, "slug": slug, "branch": branch})

    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    terminal.ensure_session(key, str(ticket_dir))
    time.sleep(2)
    terminal.send_keys(key, "claude --dangerously-skip-permissions")
    time.sleep(5)
    terminal.send_bare_enter(key)
    time.sleep(3)
    terminal.send_keys(key, _command_for_status("planning"))

    log.emit("ticket_planning_started", f"Started /confer-technical-plan for {slug}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key})

    return {"status": transition("new", "planning"), "slug": slug, "branch": branch,
            "discovered_at": datetime.now(timezone.utc).isoformat()}


def _check_planning(config, ticket, ts, base_url) -> dict:
    ws = config["workspace"]
    slug = ts["slug"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    manifest = ticket_dir / "docs" / "change-manifest.md"

    if not manifest.exists():
        return ts

    log.emit("ticket_planned", f"confer-technical-plan complete for {_label(ticket['key'], ts)}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
        meta={"ticket": ticket["key"]})

    terminal.send_keys(ticket["key"], "Run /tri-review and save the full output to docs/tri-review.md")

    log.emit("ticket_review_started", f"Started /tri-review for {_label(ticket['key'], ts)}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
        meta={"ticket": ticket["key"]})

    ts["status"] = transition(ts["status"], "reviewing")
    return ts


def _check_reviewing(config, ticket, ts, base_url) -> dict:
    ws = config["workspace"]
    slug = ts["slug"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    review_file = ticket_dir / "docs" / "tri-review.md"

    if not review_file.exists():
        return ts

    review_text = review_file.read_text()
    verdict = run_haiku(
        f"Read this code review and reply with exactly one word: PASS or FAIL.\n"
        f"PASS means safe to merge or no blocking issues. FAIL means there are blocking issues that must be fixed.\n\n"
        f"{review_text[:4000]}"
    )

    if not verdict:
        return ts

    if verdict.strip().upper().startswith("FAIL"):
        review_file.unlink(missing_ok=True)
        terminal.send_keys(ticket["key"], "Fix all blocking findings from the tri-review. Then run tests.")

        log.emit("ticket_review_fixing", f"Fixing tri-review findings for {_label(ticket['key'], ts)}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"], "verdict": verdict.strip()})
        ts["status"] = transition(ts["status"], "planning")
        return ts

    log.emit("ticket_review_passed", f"Tri-review passed for {_label(ticket['key'], ts)}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
        meta={"ticket": ticket["key"], "verdict": verdict.strip()})
    ts["status"] = transition(ts["status"], "pr_ready")

    events.dispatch("ticket_dev_complete", {
        "ticket_key": ticket["key"],
        "estimate_seconds": ticket.get("estimate_seconds", 0),
        "discovered_at": ts.get("discovered_at", ""),
        "slug": ts.get("slug", ""),
        "branch": ts.get("branch", ""),
    }, config)

    return ts


def _summarize_pr_body(raw_body: str, ticket: dict) -> str:
    if not raw_body or len(raw_body) < 200:
        return raw_body
    result = run_haiku(
        f"Summarize this PR description in 3-5 plain sentences. No bullet points, no headers, no markdown formatting. "
        f"Just say what changed and why.\n\nTicket: {ticket['key']} — {ticket['summary']}\n\n{raw_body[:3000]}"
    )
    return result if result else raw_body[:500]


def _create_pr(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    repos = get_repos(config)
    slug = ts["slug"]
    branch = ts["branch"]
    ws = config["workspace"]

    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    manifest = ticket_dir / "docs" / "change-manifest.md"
    raw_body = manifest.read_text() if manifest.exists() else ticket.get("description", "")
    pr_body = _summarize_pr_body(raw_body, ticket)

    prs = []
    any_diff = False
    for repo in repos:
        wt = ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "commit", "--no-verify", "-m", f"{ticket['key']}: {ticket['summary']}"], cwd=str(wt), capture_output=True, timeout=60)

        subprocess.run(["git", "fetch", "origin", ws["base_branch"]], cwd=str(wt), capture_output=True, timeout=60)
        diff_check = subprocess.run(
            ["git", "diff", f"origin/{ws['base_branch']}..HEAD", "--stat"],
            cwd=str(wt), capture_output=True, text=True, timeout=30)
        if not diff_check.stdout.strip():
            continue
        any_diff = True

        actual_branch = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(wt), capture_output=True, text=True, timeout=10).stdout.strip()
        push_branch = actual_branch or branch

        pushed = platform.push_branch(wt, push_branch)
        if not pushed["ok"]:
            log.emit("ticket_pr_error", f"Failed to push branch for {_label(ticket['key'], ts)} in {repo['name']}: {pushed.get('error', 'unknown')}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": repo["name"], "branch": branch, "error": pushed.get("error", "")})
            continue

        title = f"{ticket['key']}: {ticket['summary']}"
        result = platform.create_pr(repo["name"], wt, push_branch, title, pr_body, ws["base_branch"])

        if result.get("error"):
            err = result["error"]
            if "no changes to be pulled" in err.lower():
                continue
            log.emit("ticket_pr_error", f"Failed to create PR for {_label(ticket['key'], ts)} in {repo['name']}: {err}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": repo["name"], "error": err})
            continue

        pr_url = result.get("url", "")
        pr_id = result.get("id")
        pr_links = {"detail": f"{base_url}/tickets/{ticket['key']}"}
        if ticket.get("url"):
            pr_links["ticket"] = ticket["url"]
        if pr_url:
            pr_links["pr"] = pr_url
        log.emit("ticket_pr_created", f"PR created for {_label(ticket['key'], ts)} in {repo['name']}",
            links=pr_links, meta={"ticket": ticket["key"], "repo": repo["name"], "pr_url": pr_url})

        if pr_id:
            prs.append({"repo": repo["name"], "id": pr_id, "url": pr_url})

    if not any_diff:
        log.emit("ticket_no_changes", f"No code changes needed for {_label(ticket['key'], ts)}, marking as merged",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["status"] = transition(ts["status"], "merged")
        return ts

    if not prs:
        ts["pr_attempts"] = ts.get("pr_attempts", 0) + 1
        if ts["pr_attempts"] >= 3:
            log.emit("ticket_pr_error", f"No PRs created for {_label(ticket['key'], ts)} after {ts['pr_attempts']} attempts, giving up",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"]})
            ts["status"] = transition(ts["status"], "pr_failed")
        else:
            log.emit("ticket_pr_error", f"No PRs created for {_label(ticket['key'], ts)}, attempt {ts['pr_attempts']}/3",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"]})
        return ts

    ts["prs"] = prs
    ts["last_comment_ids"] = {f"{p['repo']}/{p['id']}": 0 for p in prs}
    ts["status"] = transition(ts["status"], "pr_created")
    return ts


def _pr_comments_path(config, slug):
    ws = config["workspace"]
    return ws["root"] / ws["tickets_dir"] / slug / "pr_comments.json"


def _load_pr_comments(config, slug) -> list[dict]:
    path = _pr_comments_path(config, slug)
    if path.exists():
        return json.loads(path.read_text())
    return []


def _save_pr_comments(config, slug, comments: list[dict]):
    path = _pr_comments_path(config, slug)
    path.write_text(json.dumps(comments, indent=2, default=str))


def _check_in_review(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    all_merged = True
    for pr in prs:
        pr_state = platform.get_pr_state(pr["repo"], pr["id"])
        if pr_state != "MERGED":
            all_merged = False
            break

    if all_merged:
        log.emit("ticket_merged", f"All PRs merged for {_label(ticket['key'], ts)}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["status"] = transition(ts["status"], "merged")
        return ts

    ts["status"] = transition(ts["status"], "in_review")
    platform_name = config["job"].get("platform", "")
    if platform_name == "bitbucket":
        user_id = config.get("bitbucket", {}).get("user_account_id", "")
    elif platform_name == "github":
        user_id = config.get("github", {}).get("user", "")
    else:
        user_id = ""
    slug = ts["slug"]
    last_comment_ids = ts.get("last_comment_ids", {})
    pr_comments = _load_pr_comments(config, slug)

    for pr in prs:
        pr_key = f"{pr['repo']}/{pr['id']}"
        last_seen = last_comment_ids.get(pr_key, 0)
        comments = platform.get_pr_comments(pr["repo"], pr["id"])
        new_comments = [
            c for c in comments
            if c["id"] > last_seen and c["author_id"] != user_id and not c.get("parent_id")
        ]

        if not new_comments:
            continue

        for comment in new_comments:
            classification = run_haiku(
                f"Is this PR review comment actionable (clear code change requested) or ambiguous (vague, question, opinion)?\n\n"
                f"Comment: {comment['body']}\n\n"
                f"Reply with JSON: {{\"actionable\": true/false, \"reason\": \"brief reason\"}}"
            )
            parsed = extract_json(classification) if classification else None
            actionable = parsed.get("actionable", False) if parsed else False

            entry = {
                "id": comment["id"],
                "pr_repo": pr["repo"],
                "pr_id": pr["id"],
                "body": comment["body"],
                "path": comment.get("path"),
                "line": comment.get("line"),
                "status": "new",
                "suggested_reply": "",
            }

            if actionable:
                repos = get_repos(config)
                repo_match = next((r for r in repos if r["name"] == pr["repo"]), None)
                if repo_match:
                    wt = ticket_worktree_path(config, slug, pr["repo"])
                    if wt.is_dir():
                        subprocess.run(["git", "pull", "--rebase", "origin", ts["branch"]], cwd=str(wt), capture_output=True, timeout=60)
                        context = f"File: {comment.get('path', 'unknown')}\nLine: {comment.get('line', 'unknown')}\n\nReview comment: {comment['body']}\n\nFix this review comment."
                        run_claude_code(context, wt)
                        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
                        subprocess.run(["git", "commit", "-m", f"fix: address review comment on {comment.get('path', 'unknown')}"], cwd=str(wt), capture_output=True, timeout=60)
                        platform.push_branch(wt, ts["branch"])
                        ts.pop("ci_passed", None)
                        ts.pop("checks_started_at", None)
                        platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                        entry["status"] = "addressed"
                        log.emit("ticket_pr_comment_fixed", f"{_label(ticket['key'], ts)}: Fixed {comment['body'][:80]}",
                            links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                            meta={"ticket": ticket["key"]})
            else:
                suggested = run_haiku(
                    f"A reviewer left this comment on a PR:\n\n{comment['body']}\n\n"
                    f"Write a brief, direct reply that addresses their concern. 1-2 sentences max."
                )
                entry["status"] = "needs_reply"
                entry["suggested_reply"] = suggested or ""
                log.emit("ticket_pr_comment_needs_reply", f"{_label(ticket['key'], ts)}: Reply needed {comment['body'][:80]}",
                    links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                    meta={"ticket": ticket["key"]})

            pr_comments.append(entry)

        last_comment_ids[pr_key] = max(c["id"] for c in new_comments)

    ts["last_comment_ids"] = last_comment_ids
    _save_pr_comments(config, slug, pr_comments)
    return ts


MAX_CONFLICT_ATTEMPTS = 2


def _resolve_conflicts(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    base_branch = config["workspace"].get("base_branch", "main")

    for pr in prs:
        info = platform.get_pr_info(pr["repo"], pr["id"])
        if info.get("mergeable") != "CONFLICTING":
            continue

        attempts = ts.get("conflict_resolution_attempts", 0)
        if attempts >= MAX_CONFLICT_ATTEMPTS:
            log.emit("ticket_conflict_failed", f"Conflict resolution failed for {_label(ticket['key'], ts)} PR #{pr['id']} after {attempts} attempts",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})
            ts["status"] = transition(ts["status"], "pr_failed")
            return ts

        wt = ticket_worktree_path(config, ts["slug"], pr["repo"])
        if not wt.is_dir():
            continue

        result = platform.merge_base(wt, base_branch)
        if not result["ok"]:
            log.emit("ticket_conflict_failed", f"Merge failed for {_label(ticket['key'], ts)} PR #{pr['id']}: {result.get('error', '')[:100]}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"], "error": result.get("error", "")})
            ts["conflict_resolution_attempts"] = attempts + 1
            if attempts + 1 >= MAX_CONFLICT_ATTEMPTS:
                ts["status"] = transition(ts["status"], "pr_failed")
            return ts

        pushed = platform.push_branch(wt, ts["branch"])
        if not pushed["ok"]:
            log.emit("ticket_conflict_push_failed", f"Push failed for {_label(ticket['key'], ts)}: {pushed.get('error', '')[:100]}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": pr["repo"]})
            ts["conflict_resolution_attempts"] = attempts + 1
            return ts

        ts.pop("ci_passed", None)
        ts.pop("checks_started_at", None)
        ts["conflict_resolution_attempts"] = attempts + 1

        log.emit("ticket_conflict_resolved", f"Merged {base_branch} into {_label(ticket['key'], ts)} PR #{pr['id']}",
            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
            meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})

    return ts


MAX_CI_FIX_ATTEMPTS = 2


def _merge(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    prs = ts.get("prs", [])
    if not prs:
        return ts

    all_merged = True
    for pr in prs:
        result = platform.merge_pr(pr["repo"], pr["id"])
        if result.get("error"):
            log.emit("ticket_merge_error", f"Failed to merge PR #{pr['id']} in {pr['repo']}: {result['error']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"], "error": result["error"]})
            all_merged = False
        else:
            log.emit("ticket_pr_merged", f"Merged PR #{pr['id']} in {pr['repo']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
                meta={"ticket": ticket["key"], "repo": pr["repo"], "pr_id": pr["id"]})

    if all_merged:
        ts["status"] = transition(ts["status"], "merged")
        ts.pop("ci_passed", None)
    return ts


def _handle_ci_failure(config, platform, ticket, ts, pr, checks, base_url) -> dict:
    failed_names = [c["name"] for c in checks if c["state"].upper() in ("FAILURE", "FAILED")]
    fix_attempts = ts.get("ci_fix_attempts", 0)

    if fix_attempts >= MAX_CI_FIX_ATTEMPTS:
        log.emit("ticket_checks_failed", f"CI failed for {_label(ticket['key'], ts)} after {fix_attempts} fix attempts: {', '.join(failed_names)}",
            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
            meta={"ticket": ticket["key"], "failed_checks": failed_names})
        ts["status"] = transition(ts["status"], "pr_failed")
        return ts

    failure_logs = platform.get_failed_logs(pr["repo"], pr["id"])
    pr_diff = platform.get_pr_diff(pr["repo"], pr["id"]) or ""

    prompt = f"""CI checks failed on a PR created by our automated pipeline. Determine if this is caused by our changes or is pre-existing/unrelated.

Ticket: {ticket['key']} — {ticket['summary']}
Failed checks: {', '.join(failed_names)}
Fix attempt: {fix_attempts + 1}/{MAX_CI_FIX_ATTEMPTS}

PR diff (what we changed):
{pr_diff[:4000]}

Failure logs:
{failure_logs[:4000]}

Analyze causality:
1. Could our diff have caused these failures? Consider both direct changes and indirect effects (e.g. we changed a function that these tests depend on).
2. Or are these pre-existing failures, flaky tests, or infra issues unrelated to our changes?

Reply with EXACTLY one JSON object:
{{"caused_by_us": true/false, "reason": "brief explanation", "fix_hint": "what to change if caused_by_us"}}"""

    result = run_haiku(prompt, timeout=120)
    if not result:
        return ts

    try:
        analysis = extract_json(result) or json.loads(result.strip())
    except (json.JSONDecodeError, TypeError):
        return ts

    caused = analysis.get("caused_by_us", False)
    reason = analysis.get("reason", "")

    if not caused:
        log.emit("ticket_checks_unrelated", f"CI failure for {_label(ticket['key'], ts)} not caused by our changes: {reason[:100]}",
            links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
            meta={"ticket": ticket["key"], "failed_checks": failed_names, "reason": reason})
        return ts

    fix_hint = analysis.get("fix_hint", "")

    terminal.send_keys(ticket["key"],
        f"CI checks failed: {', '.join(failed_names)}. This is caused by our changes. "
        f"Fix the issue: {fix_hint}. Then commit with --no-verify and push.")

    ts["ci_fix_attempts"] = fix_attempts + 1
    ts.pop("checks_started_at", None)

    log.emit("ticket_ci_fix_sent", f"Sent CI fix to {_label(ticket['key'], ts)} (attempt {ts['ci_fix_attempts']}): {fix_hint[:80]}",
        links={"detail": f"{base_url}/tickets/{ticket['key']}", "pr": pr.get("url", "")},
        meta={"ticket": ticket["key"], "failed_checks": failed_names, "fix_hint": fix_hint})

    return ts


def _make_slug(key: str, summary: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", summary.lower()).strip("-")
    words = slug.split("-")[:7]
    slug = "-".join(words)
    return f"{key}-{slug}" if slug else key


def _make_branch(config, key: str, ticket: dict) -> str:
    ws = config["workspace"]
    prefix = ws.get("branch_prefix", "")
    slug = _make_slug(key, ticket["summary"])

    if prefix:
        branch_type = run_haiku(
            f"Ticket summary: {ticket['summary']}\nDescription: {ticket.get('description', '')[:500]}\n\n"
            "Is this a bugfix or a feature? Reply with exactly one word: bugfix or feature"
        )
        bt = "bugfix" if branch_type and "bugfix" in branch_type.lower() else "feature"
        return f"{prefix}/{bt}/{slug}"
    return slug


