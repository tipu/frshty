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
from features.platforms import make_platform


STATES = ["new", "planning", "reviewing", "pr_ready", "pr_created", "in_review", "merged"]


def _download_attachments(config, ticket, docs_path):
    attachments = ticket.get("attachments", [])
    if not attachments:
        return
    att_dir = docs_path / "attachments"
    att_dir.mkdir(exist_ok=True)

    auth = None
    if config["job"].get("ticket_system") == "jira":
        user = resolve_env(config, "jira", "user_env")
        token = resolve_env(config, "jira", "token_env")
        if user and token:
            auth = (user, token)

    with httpx.Client(timeout=60, follow_redirects=True) as client:
        for a in attachments:
            url = a.get("url", "")
            filename = a.get("filename", "")
            if not url or not filename:
                continue
            try:
                resp = client.get(url, auth=auth)
                if resp.status_code == 200:
                    (att_dir / filename).write_bytes(resp.content)
            except Exception:
                pass


def _resolve_status(config: dict, external_status: str) -> str | None:
    system = config["job"].get("ticket_system", "")
    status_map = config.get(system, {}).get("status_map", {})
    if not status_map:
        return None
    return status_map.get(external_status)


def check(config: dict):
    assigned = _fetch_tickets(config)
    if not assigned:
        return

    ticket_state = state.load("tickets")
    base_url = config["_base_url"]

    for ticket in assigned:
        key = ticket["key"]
        ts = ticket_state.get(key, {"status": "new"})
        ts["external_status"] = ticket.get("status", "")

        mapped = _resolve_status(config, ticket.get("status", ""))
        if mapped:
            if "slug" not in ts:
                ts["slug"] = _make_slug(key, ticket["summary"])
                ts["branch"] = _make_branch(config, key, ticket)
                ts["url"] = ticket.get("url", "")
            ts["status"] = mapped
            ticket_state[key] = ts
            continue

        if ts["status"] == "merged":
            continue

        if ts["status"] == "new":
            ts = _setup_ticket(config, ticket, base_url)

        if ts["status"] == "planning":
            ts = _check_planning(config, ticket, ts, base_url)

        if ts["status"] == "reviewing":
            ts = _check_reviewing(config, ticket, ts, base_url)

        if ts["status"] == "pr_ready":
            ts = _create_pr(config, ticket, ts, base_url)

        if ts["status"] == "pr_created" and config.get("pr", {}).get("auto_merge"):
            ts = _merge(config, ticket, ts, base_url)

        if ts["status"] in ("pr_created", "in_review"):
            ts = _check_in_review(config, ticket, ts, base_url)

        ticket_state[key] = ts

    state.save("tickets", ticket_state)


def _fetch_tickets(config: dict) -> list[dict]:
    system = config["job"].get("ticket_system", "")
    if system == "jira":
        return _fetch_jira(config)
    if system == "linear":
        return _fetch_linear(config)
    return []


def _fetch_jira(config: dict) -> list[dict]:
    jira = config.get("jira", {})
    base_url = jira.get("base_url", "")
    user = resolve_env(config, "jira", "user_env")
    token = resolve_env(config, "jira", "token_env")
    if not base_url or not user or not token:
        return []
    board_id = jira.get("board_id")
    account_id = jira.get("user_account_id", "")
    jql = jira.get("jql", "")
    if not board_id and not jql:
        return []
    with httpx.Client(auth=(user, token), timeout=30) as client:
        if board_id:
            url = f"{base_url}/rest/agile/1.0/board/{board_id}/issue?maxResults=100"
            resp = client.get(url)
            if resp.status_code != 200:
                return []
            issues = resp.json().get("issues", [])
            if account_id:
                issues = [i for i in issues if (i.get("fields", {}).get("assignee") or {}).get("accountId") == account_id]
        else:
            url = f"{base_url}/rest/api/3/search/jql?jql={jql}&maxResults=20&fields=key,summary,status,description,attachment,issuelinks,parent,subtasks"
            resp = client.get(url)
            if resp.status_code != 200:
                return []
            issues = resp.json().get("issues", [])
        results = []
        for i in issues:
            fields = i.get("fields", {})
            if not fields:
                continue
            status = fields.get("status", {})

            attachments = []
            for a in fields.get("attachment", []):
                attachments.append({"filename": a.get("filename", ""), "url": a.get("content", ""), "mime": a.get("mimeType", "")})

            related = []
            for link in fields.get("issuelinks", []):
                rel_type = link.get("type", {}).get("outward", "relates to")
                if link.get("outwardIssue"):
                    ri = link["outwardIssue"]
                    related.append({"key": ri.get("key", ""), "summary": ri.get("fields", {}).get("summary", ""), "relation": rel_type})
                elif link.get("inwardIssue"):
                    ri = link["inwardIssue"]
                    rel_type = link.get("type", {}).get("inward", "relates to")
                    related.append({"key": ri.get("key", ""), "summary": ri.get("fields", {}).get("summary", ""), "relation": rel_type})

            parent = fields.get("parent")
            parent_info = None
            if parent:
                parent_info = {"key": parent.get("key", ""), "summary": parent.get("fields", {}).get("summary", "")}

            subtasks = []
            for st_item in fields.get("subtasks", []):
                subtasks.append({"key": st_item.get("key", ""), "summary": st_item.get("fields", {}).get("summary", "")})

            results.append({
                "key": i.get("key", ""),
                "summary": fields.get("summary", ""),
                "status": status.get("name", "") if isinstance(status, dict) else str(status),
                "description": _adf_to_text(fields.get("description")),
                "url": f"{base_url.split('/rest')[0]}/browse/{i.get('key', '')}",
                "attachments": attachments,
                "related": related,
                "parent": parent_info,
                "subtasks": subtasks,
            })
        return results


def _fetch_linear(config: dict) -> list[dict]:
    linear = config.get("linear", {})
    token = resolve_env(config, "linear", "token_env")
    email = linear.get("assignee_email", "")
    if not token or not email:
        return []
    query = '''
    query {
      issues(
        filter: { assignee: { email: { eq: "%s" } } state: { name: { in: ["In Progress", "Prioritized"] } } }
        first: 20 orderBy: updatedAt
      ) { nodes { identifier title state { name } description url
          project { name description }
          parent { identifier title description }
          attachments { nodes { title url } }
          relations { nodes { type relatedIssue { identifier title description url } } }
          children { nodes { identifier title state { name } } }
      } }
    }
    ''' % email
    with httpx.Client(timeout=30) as client:
        resp = client.post("https://api.linear.app/graphql",
            json={"query": query},
            headers={"Authorization": token, "Content-Type": "application/json"})
        if resp.status_code != 200:
            return []
        nodes = resp.json().get("data", {}).get("issues", {}).get("nodes", [])
        results = []
        for n in nodes:
            attachments = [{"filename": a.get("title", ""), "url": a.get("url", "")} for a in n.get("attachments", {}).get("nodes", [])]
            related = [{"key": r["relatedIssue"]["identifier"], "summary": r["relatedIssue"]["title"], "relation": r.get("type", "related")} for r in n.get("relations", {}).get("nodes", []) if r.get("relatedIssue")]
            subtasks = [{"key": c["identifier"], "summary": c["title"]} for c in n.get("children", {}).get("nodes", [])]
            results.append({
                "key": n["identifier"],
                "summary": n["title"],
                "status": n["state"]["name"],
                "description": n.get("description", ""),
                "url": n.get("url", ""),
                "project": n.get("project"),
                "parent": n.get("parent"),
                "attachments": attachments,
                "related": related,
                "subtasks": subtasks,
            })
        return results


def _setup_ticket(config, ticket, base_url) -> dict:
    ws = config["workspace"]
    repos = get_repos(config)
    key = ticket["key"]
    slug = _make_slug(key, ticket["summary"])
    branch = _make_branch(config, key, ticket)

    log.emit("ticket_found", f"New ticket: {key} — {ticket['summary']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key})

    for repo in repos:
        wt_path = ticket_worktree_path(config, slug, repo["name"])
        if (wt_path / ".git").is_file():
            continue
        wt_path.parent.mkdir(parents=True, exist_ok=True)
        subprocess.run(["git", "fetch", "origin"], cwd=str(repo["path"]), capture_output=True, timeout=60)
        branches = subprocess.run(["git", "branch", "--list"], cwd=str(repo["path"]), capture_output=True, text=True, timeout=60).stdout
        if branch not in branches.replace("* ", "").replace("  ", " ").split():
            result = subprocess.run(["git", "branch", branch, f"origin/{ws['base_branch']}"], cwd=str(repo["path"]), capture_output=True, timeout=60)
            if result.returncode != 0:
                subprocess.run(["git", "branch", branch, ws["base_branch"]], cwd=str(repo["path"]), capture_output=True, timeout=60)
        subprocess.run(["git", "worktree", "add", str(wt_path), branch], cwd=str(repo["path"]), capture_output=True, timeout=60)
        subprocess.run(["chown", "-R", "1000:1000", str(wt_path)], capture_output=True, timeout=60)

        for dep in ws.get("dep_commands", []):
            if dep["match"] == repo["name"]:
                try:
                    subprocess.run(dep["cmd"].split(), cwd=str(wt_path), capture_output=True, timeout=300)
                except FileNotFoundError:
                    pass

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

    (docs_path / "ticket.md").write_text(md)

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
    subprocess.run(["chown", "-R", "1000:1000", str(docs_path.parent)], capture_output=True, timeout=60)

    log.emit("ticket_worktree_created", f"Workspace ready for {key}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key, "slug": slug, "branch": branch})

    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    terminal.ensure_session(key, str(ticket_dir))
    time.sleep(2)
    terminal.send_keys(key, "claude --dangerously-skip-permissions")
    time.sleep(3)
    terminal.send_keys(key, "/confer-technical-plan docs/")

    log.emit("ticket_planning_started", f"Started /confer-technical-plan for {key}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{key}"},
        meta={"ticket": key})

    return {"status": "planning", "slug": slug, "branch": branch}


def _check_planning(config, ticket, ts, base_url) -> dict:
    ws = config["workspace"]
    slug = ts["slug"]
    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    manifest = ticket_dir / "docs" / "change-manifest.md"

    if not manifest.exists():
        return ts

    log.emit("ticket_planned", f"confer-technical-plan complete for {ticket['key']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
        meta={"ticket": ticket["key"]})

    terminal.send_keys(ticket["key"], "Run /tri-review and save the full output to docs/tri-review.md")

    log.emit("ticket_review_started", f"Started /tri-review for {ticket['key']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
        meta={"ticket": ticket["key"]})

    ts["status"] = "reviewing"
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

    if verdict and "FAIL" in verdict.upper():
        terminal.send_keys(ticket["key"], "Fix all blocking findings in docs/tri-review.md. Then run tests. When done, delete docs/tri-review.md.")

        log.emit("ticket_review_fixing", f"Fixing tri-review findings for {ticket['key']}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["status"] = "planning"
        return ts

    log.emit("ticket_review_passed", f"Tri-review passed for {ticket['key']}",
        links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
        meta={"ticket": ticket["key"]})
    ts["status"] = "pr_ready"
    return ts


def _create_pr(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    repos = get_repos(config)
    slug = ts["slug"]
    branch = ts["branch"]
    ws = config["workspace"]

    ticket_dir = ws["root"] / ws["tickets_dir"] / slug
    manifest = ticket_dir / "docs" / "change-manifest.md"
    pr_body = manifest.read_text() if manifest.exists() else ticket.get("description", "")

    prs = []
    for repo in repos:
        wt = ticket_worktree_path(config, slug, repo["name"])
        if not wt.is_dir():
            continue
        subprocess.run(["git", "add", "-A"], cwd=str(wt), capture_output=True, timeout=60)
        subprocess.run(["git", "commit", "-m", f"{ticket['key']}: {ticket['summary']}"], cwd=str(wt), capture_output=True, timeout=60)

        pushed = platform.push_branch(wt, branch)
        if not pushed:
            log.emit("ticket_pr_error", f"Failed to push branch for {ticket['key']} in {repo['name']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": repo["name"], "branch": branch})
            continue

        title = f"{ticket['key']}: {ticket['summary']}"
        result = platform.create_pr(repo["name"], wt, branch, title, pr_body, ws["base_branch"])

        if result.get("error"):
            log.emit("ticket_pr_error", f"Failed to create PR for {ticket['key']} in {repo['name']}: {result['error']}",
                links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                meta={"ticket": ticket["key"], "repo": repo["name"], "error": result["error"]})
            continue

        pr_url = result.get("url", "")
        pr_id = result.get("id")
        pr_links = {"detail": f"{base_url}/tickets/{ticket['key']}"}
        if ticket.get("url"):
            pr_links["ticket"] = ticket["url"]
        if pr_url:
            pr_links["pr"] = pr_url
        log.emit("ticket_pr_created", f"PR created for {ticket['key']} in {repo['name']}",
            links=pr_links, meta={"ticket": ticket["key"], "repo": repo["name"], "pr_url": pr_url})

        if pr_id:
            prs.append({"repo": repo["name"], "id": pr_id, "url": pr_url})

    if not prs:
        log.emit("ticket_pr_error", f"No PRs created for {ticket['key']}, staying at pr_ready",
            links={"detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        return ts

    ts["prs"] = prs
    ts["last_comment_ids"] = {f"{p['repo']}/{p['id']}": 0 for p in prs}
    ts["status"] = "pr_created"
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
        log.emit("ticket_merged", f"All PRs merged for {ticket['key']}",
            links={"ticket": ticket.get("url", ""), "detail": f"{base_url}/tickets/{ticket['key']}"},
            meta={"ticket": ticket["key"]})
        ts["status"] = "merged"
        return ts

    ts["status"] = "in_review"
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
                        platform.resolve_comment(pr["repo"], pr["id"], comment["id"])
                        entry["status"] = "addressed"
                        log.emit("ticket_pr_comment_fixed", f"Fixed: {comment['body'][:80]}",
                            links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                            meta={"ticket": ticket["key"]})
            else:
                suggested = run_haiku(
                    f"A reviewer left this comment on a PR:\n\n{comment['body']}\n\n"
                    f"Write a brief, direct reply that addresses their concern. 1-2 sentences max."
                )
                entry["status"] = "needs_reply"
                entry["suggested_reply"] = suggested or ""
                log.emit("ticket_pr_comment_needs_reply", f"Reply needed: {comment['body'][:80]}",
                    links={"detail": f"{base_url}/tickets/{ticket['key']}"},
                    meta={"ticket": ticket["key"]})

            pr_comments.append(entry)

        last_comment_ids[pr_key] = max(c["id"] for c in new_comments)

    ts["last_comment_ids"] = last_comment_ids
    _save_pr_comments(config, slug, pr_comments)
    return ts


def _merge(config, ticket, ts, base_url) -> dict:
    platform = make_platform(config)
    pr_config = config.get("pr", {})
    if not pr_config.get("auto_merge"):
        return ts

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
        ts["status"] = "merged"
    return ts


def _make_slug(key: str, summary: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", summary.lower()).strip("-")[:50]
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


def _adf_to_text(adf) -> str:
    if not adf or not isinstance(adf, dict):
        return ""
    parts = []
    for node in adf.get("content", []):
        if node.get("type") == "paragraph":
            text = "".join(
                c.get("text", "") for c in node.get("content", []) if c.get("type") == "text"
            )
            parts.append(text)
    return "\n\n".join(parts)
