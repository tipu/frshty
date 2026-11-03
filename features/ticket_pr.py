"""The ticket layer on the own-PR engine.

The own-PR engine drives every pull request: comments, base sync, conflicts
and CI fixes. A pull request a ticket opened gets a few more steps from this
module: a drafted reply for a comment that asks for no change, the check that
a comment needs no reply at all, the evidence run on a drafted reply, one CI
fix budget for all the PRs of a ticket, and the merge gate that reads
comment_state."""
import json
from pathlib import Path

import core.comments as comments
import core.log as log
import core.queue as q
import core.state as state
import features.defence as defence
from core.claude_runner import run_haiku, run_balanced, extract_json, parse_structured, strict_object
from core.config import ticket_worktree_path
from core.ticket_status import BASE_SYNC
from features.platforms import make_platform

NEEDS_REPLY = "needs a reply"
FIX_BUDGET_SPENT = "fix budget spent"
MAX_LEDGER_FIX_ATTEMPTS = 2
_NO_REPLY_SCHEMA = strict_object({
    "needs_reply": {"type": "boolean"},
})
_LEDGER_STATES = {
    "addressed": ("processed", 1),
    "replied": ("processed", 0),
    "no_reply_needed": ("processed", 0),
    "needs_reply": ("manual", 0),
}


def ticket_pr_seen(ts: dict) -> dict:
    """pr key -> the own-PR engine's record for each PR of the ticket."""
    pr_state = state.load("own_prs") or {}
    return {f"{p['repo']}/{p['id']}": pr_state.get(f"{p['repo']}/{p['id']}")
            for p in ts.get("prs", [])}


def draft_comment_reply(config, ts: dict, comment: dict, pr: dict, worktree: Path | None) -> str:
    """Draft a reply to a PR review comment grounded in the ticket, the actual
    branch code the comment points at, and the comment itself — so the reply is
    specific instead of "I'd need to see the code"."""
    ws = config["workspace"]
    slug = ts.get("slug") or ""
    ticket_md = ws["root"] / ws["tickets_dir"] / slug / "docs" / "ticket.md"
    try:
        ticket_ctx = ticket_md.read_text()[:2000]
    except OSError:
        ticket_ctx = f"{ts.get('summary', '')}\n{ts.get('description', '')}"[:2000]

    path = comment.get("path")
    code_ctx = "(no file path on this comment)"
    if path:
        wt = worktree if worktree is not None and worktree.is_dir() else ticket_worktree_path(config, slug, pr["repo"])
        try:
            lines = (wt / path).read_text().splitlines()
            ln = comment.get("line")
            if isinstance(ln, int) and ln > 0:
                lo, hi = max(0, ln - 40), min(len(lines), ln + 20)
            else:
                lo, hi = 0, min(len(lines), 120)
            snippet = "\n".join(f"{i + 1}: {lines[i]}" for i in range(lo, hi))
            code_ctx = f"{path} ({pr['repo']}):\n{snippet}"
        except OSError:
            code_ctx = f"(could not read {path} in {pr['repo']})"

    hunk = comment.get("diff_hunk") or ""
    prompt = (
        "You are the PR author replying to a code-review comment. Write a SPECIFIC, "
        "accurate reply grounded in the ticket and the actual code below. If the comment "
        "requests or suggests a change, state clearly whether you will make it and why; if "
        "it is a question, answer it directly from the code. 2-4 sentences. Write in "
        "Caterpillar Technical English: short sentences, active voice, one idea per "
        "sentence. Do NOT say you need more context.\n\n"
        f"TICKET:\n{ticket_ctx}\n\n"
        f"CODE:\n{code_ctx}\n\n"
        + (f"REVIEWED HUNK:\n{hunk}\n\n" if hunk else "")
        + f"REVIEWER COMMENT (on {path}:{comment.get('line')}):\n{comment['body']}\n\n"
        "Reply:"
    )
    return run_balanced(prompt) or ""


def reply_commits_to_change(reply: str) -> bool:
    """A drafted reply that promises a code change ('this is a regression, I
    will restore the count') proves the comment was actionable after all.
    Posting the promise without the change leaves the reviewer waiting forever,
    so such a comment must route to the fix path instead of reply-only."""
    if not reply.strip():
        return False
    prompt = (
        "Does this PR review reply COMMIT to making a code change, e.g. 'I will "
        "restore X', 'I will rename Y before merging', 'This is a regression. I "
        "will fix it.'? Answer false when the reply only explains the code, "
        "answers a question, or declines the change.\n\n"
        f"REPLY:\n{reply}\n\n"
        'Reply with JSON: {"commits_to_change": true|false}'
    )
    raw = run_balanced(prompt)
    parsed = extract_json(raw) if raw else None
    return isinstance(parsed, dict) and bool(parsed.get("commits_to_change"))


def comment_needs_no_reply(body: str) -> bool:
    """Whether a review comment is a statement that asks the author for nothing.

    An approval, a sign-off, thanks or a status report needs no answer. Such a
    comment held the merge as owed for the life of the PR, because nobody
    replies to it. A failed or unclear answer counts as a reply needed."""
    prompt = (
        "A reviewer left this comment on a pull request. Does it need a reply "
        "from the PR author? needs_reply=false ONLY when the comment asks for "
        "nothing: an approval, a sign-off, 'LGTM', thanks, or a report such as "
        "'review done, no issues found, approved'. needs_reply=true when it asks "
        "a question, requests or suggests a change, raises a concern, or you are "
        "not sure.\n\n"
        f"COMMENT:\n{body}\n\n"
        'Reply with JSON: {"needs_reply": true|false}'
    )
    parsed = parse_structured(run_haiku(prompt, schema=_NO_REPLY_SCHEMA),
                              _NO_REPLY_SCHEMA, fn_name="ticket_comment_no_reply")
    return parsed is not None and parsed["needs_reply"] is False


def substantiate_reply(config, ticket_key: str, pr: dict, comment_id, suggested: str) -> dict:
    """Queue the evidence run for a drafted reply and return its pending marker.

    The check runs two test suites, each with a ten-minute ceiling. Doing that
    inline would hold the comment poll for as long as it takes. The work goes
    on the queue instead and writes its verdict onto the comment_state row when
    it finishes. The caller stores the drafted reply on that row first."""
    if not config.get("features", {}).get("defence"):
        return {"verdict": defence.INCONCLUSIVE, "reason": "features.defence disabled"}
    if not suggested.strip():
        return {"verdict": defence.INCONCLUSIVE, "reason": "no drafted reply to substantiate"}
    instance_key = (config.get("job") or {}).get("key") or ""
    if not instance_key:
        return {"verdict": defence.INCONCLUSIVE, "reason": "no instance key to queue against"}
    try:
        q.enqueue_job(instance_key, "substantiate_reply",
                      payload={"repo": pr["repo"], "pr_id": pr["id"], "comment_id": str(comment_id)},
                      ticket_key=ticket_key)
    except Exception as e:
        log.emit("defence_enqueue_failed",
                 f"{ticket_key}: could not queue substantiation: {type(e).__name__}: {e}",
                 meta={"ticket": ticket_key, "repo": pr["repo"]})
        return {"verdict": defence.INCONCLUSIVE, "reason": f"could not queue: {type(e).__name__}"}
    return {"verdict": defence.PENDING, "reason": "evidence run queued"}


def _ledger_path(config, ts: dict) -> Path:
    ws = config["workspace"]
    return ws["root"] / ws["tickets_dir"] / (ts.get("slug") or "") / "pr_comments.json"


def _kind_class(kind) -> str:
    return "issue_comment" if kind == "issue_comment" else "review"


def _ledger_entries(config, ts: dict, pr: dict) -> list[dict] | None:
    """The newest pr_comments.json row per comment of this PR, or None when
    the file cannot be read."""
    path = _ledger_path(config, ts)
    if not path.exists():
        return []
    try:
        rows = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(rows, list):
        return None
    latest: dict = {}
    for e in rows:
        if isinstance(e, dict) and e.get("pr_repo") == pr["repo"] and e.get("pr_id") == pr["id"]:
            latest[(e.get("comment_kind"), e.get("id"), e.get("created_at"))] = e
    return list(latest.values())


def _ledger_match(entry: dict, live: list[dict]) -> dict | None:
    if entry.get("comment_kind") is not None:
        hits = [c for c in live if c["id"] == entry.get("id")
                and _kind_class(c.get("comment_kind")) == _kind_class(entry["comment_kind"])]
    else:
        hits = [c for c in live if c["id"] == entry.get("id")
                and (c.get("body") or "") == (entry.get("body") or "")]
    return hits[0] if len(hits) == 1 else None


def import_ledger(config, instance_key: str, ts: dict, pr: dict, live: list[dict]) -> int | None:
    """Copy the ticket engine's record of this PR into comment_state, once.

    Each pr_comments.json row the platform still reports becomes a
    comment_state row: an answered comment is processed, a comment that waits
    for a reply goes to the operator, and every other open comment goes back
    to the fixer. A comment the ticket cursor already passed without a row was
    settled by that engine, so it is recorded as processed. Returns how many
    rows were written, or None when the file cannot be read."""
    entries = _ledger_entries(config, ts, pr)
    if entries is None:
        return None
    pr_key = f"{pr['repo']}/{pr['id']}"
    written = 0
    matched: set = set()
    for entry in entries:
        c = _ledger_match(entry, live)
        if c is None:
            continue
        matched.add(str(c["id"]))
        status = entry.get("status") or ""
        attempts = int(entry.get("attempts") or 0)
        row_state, fix_count = _LEDGER_STATES.get(status, ("new", 0))
        last_error = NEEDS_REPLY if row_state == "manual" else None
        if status == "fix_failed" and attempts >= MAX_LEDGER_FIX_ATTEMPTS:
            row_state, last_error = "manual", FIX_BUDGET_SPENT
        if status in ("fix_failed", "fix_unpushed", "fix_unresolved"):
            fix_count = 1 if status != "fix_failed" else 0
        if comments.import_comment(instance_key, "pr", pr_key, {
                "comment_id": c["id"],
                "comment_edited_at": c.get("updated_at") or c.get("created_at"),
                "state": row_state, "fix_count": fix_count,
                "error_count": attempts if row_state == "new" else 0,
                "last_error": last_error,
                "body": c.get("body") or entry.get("body") or "",
                "path": entry.get("path"), "line": entry.get("line"),
                "url": c.get("html_url") or "", "comment_kind": c.get("comment_kind"),
                "diff_hunk": entry.get("diff_hunk") or "",
                "created_at": entry.get("created_at") or c.get("created_at") or "",
                "suggested_reply": entry.get("suggested_reply") or "",
                "defence": entry.get("defence")}):
            written += 1
    review_cursor = (ts.get("last_comment_ids") or {}).get(pr_key, 0)
    issue_cursor = (ts.get("last_issue_comment_ids") or {}).get(pr_key, 0)
    for c in live:
        if str(c["id"]) in matched:
            continue
        cursor = issue_cursor if _kind_class(c.get("comment_kind")) == "issue_comment" else review_cursor
        if not isinstance(c["id"], int) or c["id"] > cursor:
            continue
        if comments.import_comment(instance_key, "pr", pr_key, {
                "comment_id": c["id"],
                "comment_edited_at": c.get("updated_at") or c.get("created_at"),
                "state": "processed"}):
            written += 1
    return written


def ledger_status(row: dict) -> str:
    st = row.get("state")
    if st == "manual":
        return "needs_reply"
    if st in ("processed", "deleted"):
        if row.get("fix_count"):
            return "addressed"
        if (row.get("suggested_reply") or "").strip():
            return "replied"
        return "no_reply_needed"
    if row.get("error_count"):
        return "fix_failed"
    return "new"


def comment_rows(config, ts: dict) -> list[dict]:
    """The comments on the ticket's PRs, in the row shape the ticket pages read."""
    instance_key = (config.get("job") or {}).get("key") or ""
    out = []
    for pr in ts.get("prs", []):
        for row in comments.get_comment_rows(instance_key, "pr", f"{pr['repo']}/{pr['id']}"):
            if row.get("body") is None:
                continue
            cid = str(row["comment_id"])
            try:
                defence_result = json.loads(row["defence"]) if row.get("defence") else None
            except ValueError:
                defence_result = None
            out.append({
                "id": int(cid) if cid.isdigit() else cid,
                "comment_kind": row.get("comment_kind"),
                "pr_repo": pr["repo"],
                "pr_id": pr["id"],
                "pr_url": pr.get("url", ""),
                "html_url": row.get("url") or "",
                "body": row.get("body") or "",
                "path": row.get("path"),
                "line": row.get("line"),
                "diff_hunk": row.get("diff_hunk") or "",
                "created_at": row.get("created_at") or "",
                "status": ledger_status(row),
                "suggested_reply": row.get("suggested_reply") or "",
                "defence": defence_result,
                "attempts": row.get("error_count") or 0,
                "reason": row.get("last_error") or "",
            })
    return out


def _owed_rows(instance_key: str, pr_key: str) -> list[dict]:
    return [r for r in comments.get_comment_rows(instance_key, "pr", pr_key)
            if r["state"] in comments.OWED_STATES]


def owed_comment_rows(config, pr: dict) -> list[dict]:
    """The comments on one PR that still owe work or a reply, for the PR board."""
    instance_key = (config.get("job") or {}).get("key") or ""
    return [{"url": r.get("url") or "",
             "loc": f"{r.get('path') or ''}:{r.get('line') or ''}".strip(":"),
             "author": "",
             "snippet": (r.get("body") or "")[:280]}
            for r in _owed_rows(instance_key, f"{pr['repo']}/{pr['id']}")]


def merge_hold_reason(config, ts: dict) -> str:
    """Why the merge must wait for the comments, or "".

    The own-PR engine records on every poll whether it could read the
    comments of each PR. A PR it has not read yet is held: no record is not
    evidence that nothing is owed. Every comment_state row that still owes
    work or a reply holds the merge."""
    instance_key = (config.get("job") or {}).get("key") or ""
    owed = 0
    for pr_key, seen in ticket_pr_seen(ts).items():
        if not seen or "comments_read_ok" not in seen:
            return "comments not reconciled"
        if not seen["comments_read_ok"]:
            return "comment read failed"
        owed += len(_owed_rows(instance_key, pr_key))
    if owed:
        return f"{owed} comment(s) owed an answer"
    return ""


def _unresolved_comment_row(c: dict) -> dict:
    return {
        "url": c.get("html_url", ""),
        "loc": f"{c.get('path', '')}:{c.get('line', '')}".strip(":"),
        "author": c.get("author_name") or c.get("author_id", ""),
        "snippet": (c.get("body", "") or "")[:280],
    }


def reconcile_comments_now(config, ts: dict) -> tuple[str, list[dict]]:
    """Read the comments again and say what is owed, changing nothing.

    The merge cannot trust the last poll: a reviewer can write a comment
    between that poll and the merge. This reads the platform once per pull
    request. A comment counts as owed when it is open, frshty did not write
    it, and comment_state either has no row for it or a row that still owes
    work or a reply. A read that fails, or any error at all, holds the merge."""
    instance_key = (config.get("job") or {}).get("key") or ""
    try:
        platform = make_platform(config)
        user_id = platform.self_id()
        owed_rows = []
        for pr in ts.get("prs", []):
            fetched = platform.get_pr_comments(pr["repo"], pr["id"])
            if fetched is None:
                return "comment read failed", []
            pr_key = f"{pr['repo']}/{pr['id']}"
            rows = {str(r["comment_id"]): r
                    for r in comments.get_comment_rows(instance_key, "pr", pr_key)}
            for c in fetched:
                if c.get("resolved") or c.get("author_id") == user_id:
                    continue
                row = rows.get(str(c["id"]))
                if row is None or row["state"] in comments.OWED_STATES:
                    owed_rows.append(_unresolved_comment_row(c))
    except Exception as e:
        return f"comment read failed: {type(e).__name__}", []
    if not owed_rows:
        return "", []
    return f"{len(owed_rows)} comment(s) owed an answer", owed_rows


def ci_capped(ts: dict) -> list[str]:
    """The ticket PRs whose CI fix budget the own-PR engine spent."""
    return [k for k, seen in ticket_pr_seen(ts).items() if seen and seen.get("ci_cap_emitted")]


def conflict_capped(ts: dict, pr_info_map: dict) -> list[str]:
    """The ticket PRs that conflict with their base after the own-PR engine
    spent its base sync budget on them."""
    conflicting = [pr for pr in ts.get("prs", [])
                   if (pr_info_map.get((pr["repo"], pr["id"])) or {}).get("mergeable") == "CONFLICTING"]
    if not conflicting:
        return []
    seen_by_key = ticket_pr_seen(ts)
    return [f"{pr['repo']}/{pr['id']}" for pr in conflicting
            if not BASE_SYNC.may_continue(
                (seen_by_key.get(f"{pr['repo']}/{pr['id']}") or {}).get("base_sync_attempts", 0))]
