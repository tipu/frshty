"""The tickets a task names, and the tasks that name a ticket.

A task is coupled to a ticket by the ticket key its objective names. Nothing
is recorded when the task is dispatched: the couple is derived from the
objective and from the ticket rows every time it is read. So a task written
before its ticket existed couples the moment the ticket lands, and a ticket
that is deleted stops claiming tasks it no longer has.

Recognition reads the ticket keys that exist rather than a pattern a key is
expected to follow. The instances here hold DEV-635 and NEC-12, and they also
hold PRD-6_FUNCTIONAL_REQUIREMENTS-3, which no project-plus-number pattern
matches. A key counts when it stands on its own in the text, so DEV-63 is not
found inside DEV-635 and PRD-6 is not found inside PRD-6_FUNCTIONAL_REQUIREMENTS-3.

A follow-up the board wrote by itself names no key at all: it names a pull
request address. merge_scopes couples that text to a ticket the same derived
way, through the pull request addresses the ticket rows hold.
"""
import json
import re

import core.db as db
import core.runtime as runtime

BOUNDARY = r"[0-9A-Za-z_-]"
OPEN_PR_STATE = "OPEN"
_PR_URL_RE = re.compile(
    r"https?://([^/\s]+)/([^/\s]+)/([^/\s]+)/pull(?:-requests)?/(\d+)", re.I)


def _matcher(keys) -> re.Pattern | None:
    """One regex that finds any known ticket key standing on its own.

    The longest key is tried first, so PRD-6_FUNCTIONAL_REQUIREMENTS-3 wins
    over PRD-6 where both exist and the text names the long one."""
    ordered = sorted(keys, key=len, reverse=True)
    if not ordered:
        return None
    return re.compile(
        f"(?<!{BOUNDARY})(" + "|".join(re.escape(k) for k in ordered) + f")(?!{BOUNDARY})")


def index(rows: list[dict] | None = None) -> dict:
    """The lookup one read of the couple needs: every ticket row grouped by
    key, the matcher that finds those keys in a task objective, and the
    instances this process loaded.

    Every row counts, including a row on an instance that is not loaded. The
    loaded set only breaks a tie between instances that hold the same key;
    dropping the unloaded rows outright would hand a task that selected the
    unloaded project the other instance's ticket. It is None when no registry
    is loaded at all, which is how a process that never started the event
    system still reports the couples it can see."""
    if rows is None:
        rows = db.query_all(
            "SELECT instance_key, ticket_key, status, slug FROM tickets")
    by_key: dict[str, list[dict]] = {}
    for r in rows:
        by_key.setdefault(r["ticket_key"], []).append(r)
    for rows_of_key in by_key.values():
        rows_of_key.sort(key=lambda r: r["instance_key"])
    instances = runtime.instances()
    loaded = set(instances.keys()) if instances is not None else None
    return {"by_key": by_key, "matcher": _matcher(by_key), "loaded": loaded}


def keys_in(text: str, idx: dict) -> list[str]:
    """Every known ticket key a piece of text names, in order and once each."""
    matcher = idx["matcher"]
    if matcher is None:
        return []
    out: list[str] = []
    for key in matcher.findall(text or ""):
        if key not in out:
            out.append(key)
    return out


def item_keys(item: dict, idx: dict) -> list[str]:
    """Every ticket key one task names.

    The objective is what the operator wrote, so it is the first source. A
    task the today board opened for a ticket carries that ticket in scope_ref
    and can have a title that never spells the key out, so scope_ref counts
    too."""
    keys = keys_in(item.get("objective") or "", idx)
    ref = (item.get("scope_ref") or "").strip()
    if item.get("scope") == "ticket" and ref in idx["by_key"] and ref not in keys:
        keys.insert(0, ref)
    return keys


def _owners(rows: list[dict], contexts: set[str], loaded: set[str] | None) -> list[dict]:
    """The instances that own one key for one task.

    A project the task selected settles which instance is meant, and every
    holding instance is a candidate when the task selected none of them. The
    loaded set is applied last, to whichever instances that left: an instance
    this process did not load serves no ticket page, so a link to it would
    land on another instance's ticket of the same key. Selecting first and
    narrowing second is what keeps a task that named an unloaded project from
    being handed a loaded instance's ticket instead."""
    owned = [r for r in rows if r["instance_key"] in contexts] or rows
    if loaded is None:
        return owned
    return [r for r in owned if r["instance_key"] in loaded]


def _detail_url(instance_key: str, ticket_key: str) -> str:
    """The address of one ticket on the instance that holds it.

    A ticket page resolves its instance from the Host header, so
    /tickets/KEY on the board's own host would answer with another instance's
    tickets. The absolute address on the owning instance's host is the only
    link that lands on the ticket."""
    instances = runtime.instances()
    entry = instances.get(instance_key) if instances else None
    base = (entry.config.get("_base_url") or "").rstrip("/") if entry else ""
    return f"{base}/tickets/{ticket_key}"


def links_for(item: dict, idx: dict) -> list[dict]:
    """The tickets one task is coupled to.

    One ticket key can exist on more than one instance, and _owners settles
    which of them the task means."""
    contexts = {c.strip() for c in (item.get("contexts") or "").split(",") if c.strip()}
    out: list[dict] = []
    for key in item_keys(item, idx):
        rows = idx["by_key"].get(key) or []
        for r in _owners(rows, contexts, idx["loaded"]):
            out.append({
                "key": r["ticket_key"],
                "instance": r["instance_key"],
                "status": r["status"] or "",
                "slug": r["slug"] or "",
                "url": _detail_url(r["instance_key"], r["ticket_key"]),
            })
    return out


def for_items(items: list[dict]) -> dict[int, list[dict]]:
    """The tickets every task in a board page is coupled to, keyed by task id.

    One index serves the whole page, so a board of a hundred tasks costs one
    read of the tickets table rather than one per task."""
    idx = index()
    return {it["id"]: links_for(it, idx) for it in items}


def _like_term(ticket_key: str) -> str:
    escaped = ticket_key.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


def tasks_for(instance_key: str, ticket_key: str) -> list[dict]:
    """Every task coupled to one ticket, newest first.

    The query only narrows the candidates to the tasks whose text holds the
    key at all. links_for decides every one of them, so the ticket page lists
    exactly the tasks whose cards carry this ticket's pill on the board. The
    query must never decide, which is why it matches scope_ref with the same
    LIKE as the objective rather than comparing it: SQLite TRIM strips spaces
    where Python strip takes every whitespace character, and a query that
    decided would disagree with the board on a tab. There is no cap either: a
    cap would let the board show a pill the ticket page does not list back."""
    if not ticket_key:
        return []
    idx = index()
    if ticket_key not in idx["by_key"]:
        return []
    rows = db.query_all(
        "SELECT i.id, i.objective, i.state, i.stop_reason, i.contexts, i.scope,"
        " i.scope_ref,"
        " i.critical, i.created_at, i.updated_at, i.archived_at, i.summary,"
        " i.current_checkpoint,"
        " (SELECT provider FROM work_runs r WHERE r.work_item_id = i.id"
        "  ORDER BY r.id DESC LIMIT 1) AS last_provider"
        " FROM work_items i"
        " WHERE i.objective LIKE ? ESCAPE '\\'"
        "    OR (i.scope = 'ticket' AND i.scope_ref LIKE ? ESCAPE '\\')"
        " ORDER BY i.id DESC",
        (_like_term(ticket_key), _like_term(ticket_key)))
    out: list[dict] = []
    for r in rows:
        if not any(link["key"] == ticket_key and link["instance"] == instance_key
                   for link in links_for(r, idx)):
            continue
        out.append({
            "id": r["id"],
            "objective": r["objective"],
            "state": r["state"],
            "stop_reason": r["stop_reason"] or "",
            "critical": bool(r["critical"]),
            "archived": bool(r["archived_at"]),
            "projects": [c.strip() for c in (r["contexts"] or "").split(",")
                         if c.strip() and c.strip() != "slack_int"],
            "provider": r["last_provider"] or "",
            "note": (r["summary"] or r["current_checkpoint"] or "").strip(),
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "url": f"/tasks/{r['id']}",
        })
    return out


def pr_refs_in(text: str) -> list[dict]:
    """Every pull request one piece of text names by address, once each.

    A follow-up draft names its pull request by URL rather than by ticket
    key, so the address is what couples it to a ticket. The address of a pull
    request on Bitbucket carries a trailing page ('/198/overview') and the
    one on GitHub does not, so only the part up to the number identifies it.
    """
    out: list[dict] = []
    seen: set[str] = set()
    for m in _PR_URL_RE.finditer(text or ""):
        host, owner, repo, number = (g.lower() for g in m.groups())
        key = f"{host}/{owner}/{repo}/{number}"
        if key in seen:
            continue
        seen.add(key)
        out.append({"key": key, "repo": repo, "id": number})
    return out


def pr_address(url: str) -> str:
    """The host, owner, repository and number one pull request URL names, or
    "" when the text is no pull request address.

    It is the same identity pr_refs_in reports, so a cached URL and an
    address read out of a draft compare directly."""
    m = _PR_URL_RE.search(url or "")
    return "/".join(g.lower() for g in m.groups()) if m else ""


def _pr_identity(pr: dict) -> tuple[str, str]:
    """One tracked pull request as its address key and its repo-and-number key.

    The address key is empty when the ticket holds no usable URL for the pull
    request, and the repo-and-number key is what answers then."""
    return (pr_address(pr.get("url") or ""),
            f"{str(pr.get('repo') or '').lower()}/{pr.get('id')}")


def merge_scopes(text: str) -> list[dict]:
    """Every ticket behind the pull requests one piece of text names.

    A merge follow-up names the one pull request the run it came from left
    open. That pull request can be one of several under a ticket, and merging
    one of them on its own delivers a part of the ticket. So the ticket is
    resolved from the address the text names, and every open pull request it
    holds is reported with it: 'named' are the ones the text already names,
    'siblings' are the rest, and 'unapproved' are the siblings nobody has
    approved yet.

    A text can name pull requests of more than one ticket, and each of them
    is resolved on its own pull requests. They come back in instance and
    ticket order, so the first is the same ticket a caller that wants one
    already had.

    Only a ticket still in_review is resolved. That is the status whose poll
    refreshes ts['prs'][i]['approvers'] (features/tickets._cache_pr_health),
    so it is the only status whose approver cache answers for now rather than
    for whenever the ticket was last polled. A ticket that left in_review is
    no longer waiting to be merged anyway. Text that names no pull request a
    tracked in_review ticket holds resolves nothing, which is every follow-up
    that has nothing to do with a ticket.

    The named pull request is never counted as unapproved. Its own approval
    is what the run that opened the follow-up established, and the cache that
    would answer for it here can be older than that run.

    A named pull request that is already merged still resolves the ticket. It
    drops out of 'named', because there is nothing left to merge in it, but
    the ticket it belongs to is what the follow-up is held against: merging
    the named pull request by hand while the follow-up waits must not release
    the follow-up over the siblings that are still unapproved.

    'covered' is the identity of every pull request the ticket holds, open or
    not, in the one form that identifies it: its address where it has a
    usable URL, and its repository and number where it has none. That is the
    test _pr_identity already applies, and recording the repository and
    number of a pull request that does have an address would make a pull
    request of another host or another owner with the same repository name
    and number read as this ticket's. A caller that has to account for every
    pull request a text names reads the rest of them out of the text itself,
    and 'covered' is how it tells those apart from the ones these tickets
    already answered for.
    """
    refs = pr_refs_in(text)
    if not refs:
        return []
    named_keys = {r["key"] for r in refs}
    named_pairs = {f"{r['repo']}/{r['id']}" for r in refs}
    rows = db.query_all(
        "SELECT instance_key, ticket_key, data FROM tickets"
        " WHERE status = 'in_review' AND COALESCE(obsolete_at, '') = ''"
        " ORDER BY instance_key, ticket_key")
    out: list[dict] = []
    for row in rows:
        try:
            data = json.loads(row["data"]) if row["data"] else {}
        except (json.JSONDecodeError, ValueError):
            continue
        holds_named = False
        named: list[dict] = []
        siblings: list[dict] = []
        covered: set[str] = set()
        for pr in data.get("prs") or []:
            key, pair = _pr_identity(pr)
            matched = key in named_keys if key else pair in named_pairs
            holds_named = holds_named or matched
            covered.add(key or pair)
            if (pr.get("pr_state") or OPEN_PR_STATE).upper() != OPEN_PR_STATE:
                continue
            entry = {"repo": pr.get("repo"), "id": pr.get("id"),
                     "url": pr.get("url") or "",
                     "approvers": list(pr.get("approvers") or [])}
            if matched:
                named.append(entry)
            else:
                siblings.append(entry)
        if not holds_named:
            continue
        out.append({
            "ticket_key": row["ticket_key"],
            "instance_key": row["instance_key"],
            "named": named,
            "siblings": siblings,
            "unapproved": [p for p in siblings if not p["approvers"]],
            "covered": covered,
        })
    return out

