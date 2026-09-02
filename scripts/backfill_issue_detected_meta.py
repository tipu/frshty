"""Fill the comment detail into old ticket_issue_detected events.

Events written before the emitter stored the comment show only a comment id
on the ticket timeline. This script fetches each comment from the tracker the
instance uses and writes the author, the timestamps and a clipped body into
the event meta. It changes nothing that is already filled.

Run a dry run first. Narrow the run with --instance and --ticket; without
them every instance and every ticket is fetched, which is slow.

    python3 scripts/backfill_issue_detected_meta.py --instance aimyable
    python3 scripts/backfill_issue_detected_meta.py --instance aimyable --apply
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core.db as db
import core.state as state
from core.config import load_config
from features.tickets import ISSUE_COMMENT_EXCERPT_CHARS, clip_text
from features.ticket_systems import make_ticket_system

PROJECT_DIR = Path(__file__).resolve().parent.parent


def _config_for(instance_key: str) -> dict | None:
    path = PROJECT_DIR / "config" / f"{instance_key}.toml"
    if not path.exists():
        return None
    return load_config(str(path))


def _flag(name: str) -> str:
    if name not in sys.argv:
        return ""
    index = sys.argv.index(name)
    return sys.argv[index + 1] if index + 1 < len(sys.argv) else ""


def main() -> int:
    apply = "--apply" in sys.argv
    only_instance = _flag("--instance")
    only_ticket = _flag("--ticket")
    state.init("backfill")
    sql = ("SELECT id, instance_key, meta FROM log_events"
           " WHERE event = 'ticket_issue_detected'")
    params: tuple = ()
    if only_instance:
        sql += " AND instance_key = ?"
        params = (only_instance,)
    rows = db.query_all(sql + " ORDER BY ts", params)
    todo = []
    for row in rows:
        meta = json.loads(row["meta"] or "{}")
        if meta.get("comment_excerpt"):
            continue
        if not meta.get("ticket") or not meta.get("comment_id"):
            continue
        if only_ticket and meta["ticket"] != only_ticket:
            continue
        todo.append((row, meta))

    print(f"{len(rows)} events, {len(todo)} without the comment text")
    by_instance: dict[str, dict[str, list]] = {}
    for row, meta in todo:
        by_instance.setdefault(row["instance_key"], {}).setdefault(meta["ticket"], []).append((row, meta))

    filled = 0
    for instance_key, tickets_map in by_instance.items():
        config = _config_for(instance_key)
        if not config:
            print(f"{instance_key}: no config file, skipped")
            continue
        state.init(instance_key)
        system = make_ticket_system(config)
        if not system:
            print(f"{instance_key}: no ticket system, skipped")
            continue
        for ticket_key, entries in tickets_map.items():
            try:
                comments = system.fetch_comments(ticket_key)
            except Exception as exc:
                print(f"{instance_key} {ticket_key}: fetch failed: {exc}")
                continue
            by_id = {str(c.get("id")): c for c in comments}
            for row, meta in entries:
                comment = by_id.get(str(meta["comment_id"]))
                if not comment:
                    print(f"{instance_key} {ticket_key}: comment {meta['comment_id']} is gone")
                    continue
                excerpt, chars = clip_text(comment.get("body", ""), ISSUE_COMMENT_EXCERPT_CHARS)
                meta.update({
                    "comment_author": comment.get("author_name", ""),
                    "comment_created_at": comment.get("created_at", ""),
                    "comment_updated_at": comment.get("updated_at", ""),
                    "comment_excerpt": excerpt,
                    "comment_chars": chars,
                    "comment_excerpt_chars": len(excerpt),
                    "triggers": meta.get("triggers") or "fix_reported_bug",
                })
                print(f"{instance_key} {ticket_key} comment {meta['comment_id']}: "
                      f"{comment.get('author_name', '?')}, {chars} chars")
                if apply:
                    db.execute("UPDATE log_events SET meta = ? WHERE id = ?",
                               (json.dumps(meta), row["id"]))
                filled += 1

    print(f"{filled} events filled" if apply else f"{filled} events would be filled (dry run)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
