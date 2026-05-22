import json
import urllib.error
import urllib.request
from datetime import datetime, timezone

from fastapi import APIRouter
from fastapi.responses import JSONResponse

import core.db as db
import core.llm as llm
import core.log as log
import manager.staleness as staleness
from features.tickets import _load_pr_comments
from web.state import _config


router = APIRouter()


_SLACK_BRIDGE_URL = "http://127.0.0.1:8900/send"
_COMMENT_DRAFT_CAP = 3


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolve_slack_target(login: str) -> dict | None:
    targets = _config.get("slack_targets") or {}
    target = targets.get(login)
    if not target:
        return None
    if not target.get("workspace") or not target.get("dm_channel_id"):
        return None
    return target


def _slack_ping_action(login: str, ctx: dict) -> dict:
    target = _resolve_slack_target(login)
    if not target:
        return {
            "action_type": "request_re_review",
            "label": f"gh pr review --request {login}",
            "payload": {
                "cli_hint": f"gh pr review --request {login} (no slack mapping)",
                "github_login": login,
            },
        }
    return {
        "action_type": "slack_ping",
        "label": f"Ping {login} on Slack",
        "payload": {
            "github_login": login,
            "draft_text": "",
            "target": {"workspace": target["workspace"], "channel": target["dm_channel_id"]},
            "thread_ts": None,
            "context": {
                "pr_url": ctx.get("pr_url", ""),
                "pr_title": ctx.get("pr_title", ""),
                "ticket_key": ctx.get("ticket_key"),
            },
        },
    }


def _merge_item(ticket: dict, pr: dict) -> dict:
    key = ticket["ticket_key"]
    return {
        "id": f"{pr.get('repo', '')}#{pr.get('id', '')}",
        "repo": pr.get("repo"),
        "pr_id": pr.get("id"),
        "url": pr.get("url"),
        "title": ticket.get("summary") or key,
        "ticket_key": key,
        "block_reason": "awaiting_merge",
        "block_detail": {"approvers": pr.get("approvers") or []},
        "priority": 0,
        "source_loop": "merge_ready",
        "snooze_entity_id": key,
        "actions": [{
            "action_type": "merge",
            "label": "Merge",
            "payload": {
                "endpoint": f"/api/tickets/{key}/merge",
                "method": "POST",
                "ticket_key": key,
            },
        }],
    }


def _failed_item(ticket: dict, pr: dict) -> dict:
    key = ticket["ticket_key"]
    actions = [{
        "action_type": "rerun_ci",
        "label": "Rerun ticket review",
        "payload": {
            "endpoint": f"/api/reviews/rerun-ticket/{key}",
            "method": "POST",
            "ticket_key": key,
        },
    }]
    return {
        "id": f"{pr.get('repo', '')}#{pr.get('id', '')}",
        "repo": pr.get("repo"),
        "pr_id": pr.get("id"),
        "url": pr.get("url"),
        "title": ticket.get("summary") or key,
        "ticket_key": key,
        "block_reason": "ci_failing",
        "block_detail": {
            "ci_fix_attempts": ticket.get("ci_fix_attempts", 0),
            "pr_failed_reason": ticket.get("pr_failed_reason", ""),
            "sort_key": ticket.get("discovered_at", ""),
        },
        "priority": 1,
        "source_loop": "pr_failed_tickets",
        "snooze_entity_id": key,
        "actions": actions,
    }


def _reply_items(ticket: dict) -> list[dict]:
    key = ticket["ticket_key"]
    grouped: dict[tuple[str, int], list[dict]] = {}
    for c in ticket.get("comments") or []:
        repo = c.get("pr_repo")
        pr_id = c.get("pr_id")
        if not repo or pr_id is None:
            continue
        grouped.setdefault((repo, pr_id), []).append(c)

    items: list[dict] = []
    for (repo, pr_id), comments in grouped.items():
        capped = comments[:_COMMENT_DRAFT_CAP]
        pr_url = capped[0].get("pr_url", "") if capped else ""
        actions = []
        for c in capped:
            cid = c.get("id")
            actions.append({
                "action_type": "post_reply",
                "label": f"Reply to comment #{cid}",
                "payload": {
                    "endpoint": f"/api/tickets/{key}/pr-comments/{cid}/reply",
                    "method": "POST",
                    "ticket_key": key,
                    "comment_id": cid,
                    "comment_body": (c.get("body") or "")[:600],
                    "draft_text": (c.get("suggested_reply") or "")[:600],
                },
            })
        min_cid = min((c.get("id") for c in capped if c.get("id") is not None), default=0)
        items.append({
            "id": f"{repo}#{pr_id}",
            "repo": repo,
            "pr_id": pr_id,
            "url": pr_url,
            "title": ticket.get("summary") or key,
            "ticket_key": key,
            "block_reason": "needs_review_response",
            "block_detail": {
                "comment_ids": [c.get("id") for c in capped],
                "total_needs_reply": ticket.get("count", len(comments)),
                "sort_key": f"{key}:{min_cid:08d}",
            },
            "priority": 2,
            "source_loop": "pr_comments_needs_reply",
            "snooze_entity_id": key,
            "actions": actions,
        })
    return items


def _approval_item(ticket: dict) -> dict:
    key = ticket["ticket_key"]
    return {
        "id": f"ticket:{key}",
        "repo": None,
        "pr_id": None,
        "url": None,
        "title": ticket.get("summary") or key,
        "ticket_key": key,
        "block_reason": "awaiting_approval",
        "block_detail": {
            "source": ticket.get("source", ""),
            "sort_key": ticket.get("discovered_at", ""),
        },
        "priority": 3,
        "source_loop": "pending_approvals_stuck",
        "snooze_entity_id": key,
        "actions": [],
    }


def _stale_item(pr: dict) -> dict:
    repo = pr.get("repo", "")
    pr_id = pr.get("pr_id", "")
    actions: list[dict] = []
    seen_logins: set[str] = set()
    for rev in (pr.get("approvers") or []):
        if rev and rev not in seen_logins:
            seen_logins.add(rev)
            actions.append(_slack_ping_action(rev, {
                "pr_url": pr.get("url", ""),
                "pr_title": pr.get("title", ""),
                "ticket_key": None,
            }))
    return {
        "id": f"{repo}#{pr_id}",
        "repo": repo,
        "pr_id": pr_id,
        "url": pr.get("url", ""),
        "title": pr.get("title", "") or f"{repo}#{pr_id}",
        "ticket_key": None,
        "block_reason": "stale_no_activity",
        "block_detail": {
            "review_state": pr.get("review_state", ""),
            "ci_state": pr.get("ci_state", ""),
            "sort_key": pr.get("created_on", ""),
        },
        "priority": 4,
        "source_loop": "stale_own_prs",
        "snooze_entity_id": f"{repo}/{pr_id}",
        "actions": actions,
    }


def _build_queue(instance_key: str, config: dict, thresholds: dict) -> list[dict]:
    loops = staleness.aggregate_all(instance_key, config=config, thresholds=thresholds)
    items: list[dict] = []

    for ticket in loops.get("merge_ready") or []:
        for pr in ticket.get("prs") or []:
            items.append(_merge_item(ticket, pr))

    for ticket in loops.get("pr_failed_tickets") or []:
        prs = ticket.get("prs") or []
        if not prs:
            items.append(_failed_item(ticket, {"repo": None, "id": None, "url": ticket.get("url")}))
        for pr in prs:
            items.append(_failed_item(ticket, pr))

    for ticket in loops.get("pr_comments_needs_reply") or []:
        items.extend(_reply_items(ticket))

    for ticket in loops.get("pending_approvals_stuck") or []:
        items.append(_approval_item(ticket))

    for pr in loops.get("stale_own_prs") or []:
        items.append(_stale_item(pr))

    return sorted(items, key=lambda i: (
        i["priority"],
        i["block_detail"].get("sort_key", ""),
        i["id"],
    ))


@router.get("/api/wizard/queue")
def api_wizard_queue():
    instance_key = _config.get("job", {}).get("key", "")
    if not instance_key:
        return JSONResponse({"error": "no instance"}, status_code=400)
    thresholds = (_config.get("manager") or {}).get("thresholds") or {}
    try:
        items = _build_queue(instance_key, _config, thresholds)
    except Exception as e:
        log.emit("wizard_queue_failed",
                 f"[{instance_key}] _build_queue crashed: {type(e).__name__}: {e}")
        return JSONResponse({"error": f"queue build failed: {e}"}, status_code=500)
    return {
        "items": items,
        "generated_at": _now_iso(),
        "address_book_logins": sorted((_config.get("slack_targets") or {}).keys()),
    }


def _reply_prompt(comment_body: str, ticket_key: str | None) -> str:
    ctx = f"Ticket {ticket_key}. " if ticket_key else ""
    return (
        f"{ctx}A reviewer left this comment on a pull request:\n\n"
        f"{comment_body}\n\n"
        "Draft a short, professional reply (under 80 words). Address the point directly. "
        "Do not start with 'Thanks' or pleasantries. No sign-off. Plain text only."
    )


def _ping_prompt(login: str, pr_url: str, pr_title: str, context: str) -> str:
    extra = f"\nExtra context: {context}" if context else ""
    return (
        f"Draft a brief friendly Slack DM to {login} asking them to take a look at this PR. "
        f"Mention the PR title and link. Under 30 words. Plain text only.\n\n"
        f"PR: {pr_title}\nURL: {pr_url}{extra}"
    )


@router.post("/api/wizard/draft_reply")
def api_wizard_draft_reply(body: dict):
    repo = (body.get("repo") or "").strip()
    pr_id = body.get("pr_id")
    comment_id = body.get("comment_id")
    if not repo or pr_id is None or comment_id is None:
        return JSONResponse({"error": "repo, pr_id, comment_id required"}, status_code=400)

    instance_key = _config.get("job", {}).get("key", "")
    rows = db.query_all(
        "SELECT ticket_key, slug FROM tickets WHERE instance_key=? AND status='in_review'"
        " AND COALESCE(obsolete_at, '') = ''",
        (instance_key,),
    )
    comment = None
    ticket_key = None
    for r in rows:
        slug = r["slug"]
        if not slug:
            continue
        comments = _load_pr_comments(_config, slug)
        for c in comments:
            if (c.get("pr_repo") == repo
                    and c.get("pr_id") == pr_id
                    and c.get("id") == comment_id):
                comment = c
                ticket_key = r["ticket_key"]
                break
        if comment:
            break

    if not comment:
        return JSONResponse({"error": "comment not found"}, status_code=404)

    suggested = (comment.get("suggested_reply") or "").strip()
    if suggested:
        return {"text": suggested, "comment_body": comment.get("body", "")}

    drafted = llm.run_fast(_reply_prompt(comment.get("body", ""), ticket_key))
    return {"text": (drafted or "").strip(), "comment_body": comment.get("body", "")}


@router.post("/api/wizard/draft_ping")
def api_wizard_draft_ping(body: dict):
    login = (body.get("github_login") or "").strip()
    if not login:
        return JSONResponse({"error": "github_login required"}, status_code=400)
    target = _resolve_slack_target(login)
    drafted = llm.run_fast(_ping_prompt(
        login,
        body.get("pr_url", ""),
        body.get("pr_title", ""),
        body.get("context", ""),
    ))
    return {
        "text": (drafted or "").strip(),
        "target": ({"workspace": target["workspace"], "channel": target["dm_channel_id"]}
                   if target else None),
    }


def _post_slack_bridge(payload: dict) -> dict:
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        _SLACK_BRIDGE_URL,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            body = json.loads(e.read().decode("utf-8"))
        except Exception:
            body = {"ok": False, "error": str(e)}
        body["_status"] = e.code
        return body
    except (urllib.error.URLError, TimeoutError, OSError) as e:
        return {"ok": False, "error": f"slack bridge unavailable: {type(e).__name__}: {e}",
                "_status": 502}


@router.post("/api/wizard/slack_ping")
def api_wizard_slack_ping(body: dict):
    login = (body.get("github_login") or "").strip()
    text = (body.get("text") or "").strip()
    if not login:
        return JSONResponse({"error": "github_login required"}, status_code=400)
    if not text:
        return JSONResponse({"error": "text required"}, status_code=400)

    target = _resolve_slack_target(login)
    if not target:
        return JSONResponse(
            {"ok": False, "error": f"no slack target for {login}"},
            status_code=400,
        )

    payload = {
        "workspace": target["workspace"],
        "channel": target["dm_channel_id"],
        "text": text,
    }
    thread_ts = body.get("thread_ts")
    if thread_ts:
        payload["thread_ts"] = thread_ts

    if body.get("dry_run"):
        return {"would_send": payload}

    result = _post_slack_bridge(payload)
    status = result.pop("_status", 200 if result.get("ok") else 502)
    if status == 502:
        log.emit("wizard_slack_bridge_unavailable",
                 f"slack bridge unreachable: {result.get('error', '')}")
    return JSONResponse(result, status_code=status)
