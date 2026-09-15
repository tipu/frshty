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
"""
import re

import core.db as db
import core.runtime as runtime

BOUNDARY = r"[0-9A-Za-z_-]"


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
        "SELECT i.id, i.objective, i.state, i.contexts, i.scope, i.scope_ref,"
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
