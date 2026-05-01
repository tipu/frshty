"""Diff parsed sections against persisted prd_section rows."""
import core.db as db


def load_persisted(prd_id: int) -> list[dict]:
    rows = db.query_all(
        "SELECT id, stable_key, header, content, content_hash FROM prd_section"
        " WHERE prd_id=? AND deleted_at IS NULL",
        (prd_id,),
    )
    return [dict(r) for r in rows]


def diff(parsed: list[dict], persisted: list[dict]) -> dict:
    """Returns {new: [..], modified: [(old, new), ..], deleted: [..], unchanged: int}."""
    p_by_key = {p["stable_key"]: p for p in persisted}
    new_keys = {n["stable_key"]: n for n in parsed}
    new_sections: list[dict] = []
    modified: list[tuple[dict, dict]] = []
    unchanged = 0
    for n in parsed:
        old = p_by_key.get(n["stable_key"])
        if old is None:
            new_sections.append(n)
        elif old["content_hash"] != n["content_hash"]:
            modified.append((old, n))
        else:
            unchanged += 1
    deleted = [p for k, p in p_by_key.items() if k not in new_keys]
    return {
        "new": new_sections,
        "modified": modified,
        "deleted": deleted,
        "unchanged": unchanged,
    }
