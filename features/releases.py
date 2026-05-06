"""Release domain. Tag tickets to a release; run a whole-release LLM inspection
when every member ticket reaches a terminal status (merged/validation/done).

Storage: `tickets.release_key` column (per-ticket assignment), `releases` table
(per-release metadata + idempotency hash), `release_review` table (append-only
verdict + findings, mirroring `pm_review`).

The trigger flow lives in `maybe_trigger_inspect`, which is called from
`core.state.update_ticket` after a status change.
"""
from __future__ import annotations
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import core.db as db


TERMINAL_STATUSES = ("merged", "validation", "done")
RELEASES_DIRNAME = "releases"
RELEASE_KEY_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_valid_release_key(release_key: str) -> bool:
    return isinstance(release_key, str) and bool(RELEASE_KEY_RE.match(release_key))


# --- data access ----------------------------------------------------------

def upsert_release(instance_key: str, release_key: str,
                   title: str | None = None) -> dict:
    if not is_valid_release_key(release_key):
        raise ValueError(f"invalid release_key: {release_key!r}")
    existing = get_release_by_key(instance_key, release_key)
    if existing:
        if title is not None and title != existing.get("title"):
            db.execute(
                "UPDATE releases SET title=? WHERE id=?",
                (title, existing["id"]),
            )
            existing["title"] = title
        return existing
    db.execute(
        "INSERT INTO releases(instance_key, release_key, title, status, created_at)"
        " VALUES (?, ?, ?, 'open', ?)",
        (instance_key, release_key, title, _now()),
    )
    return get_release_by_key(instance_key, release_key) or {}


def get_release_by_key(instance_key: str, release_key: str) -> dict | None:
    return db.query_one(
        "SELECT id, instance_key, release_key, title, status, ticket_set_hash,"
        " last_inspected_at, created_at"
        " FROM releases WHERE instance_key=? AND release_key=?",
        (instance_key, release_key),
    )


def get_release_by_id(instance_key: str, release_id: int) -> dict | None:
    return db.query_one(
        "SELECT id, instance_key, release_key, title, status, ticket_set_hash,"
        " last_inspected_at, created_at"
        " FROM releases WHERE instance_key=? AND id=?",
        (instance_key, release_id),
    )


def list_releases(instance_key: str) -> list[dict]:
    return db.query_all(
        "SELECT id, instance_key, release_key, title, status, ticket_set_hash,"
        " last_inspected_at, created_at"
        " FROM releases WHERE instance_key=?"
        " ORDER BY created_at DESC, id DESC",
        (instance_key,),
    )


def list_release_tickets(instance_key: str, release_id: int) -> list[dict]:
    """Tickets in a release, ordered by ticket_key for deterministic hashing."""
    rel = get_release_by_id(instance_key, release_id)
    if not rel:
        return []
    rows = db.query_all(
        "SELECT ticket_key, status, slug, data FROM tickets"
        " WHERE instance_key=? AND release_key=?"
        " ORDER BY ticket_key",
        (instance_key, rel["release_key"]),
    )
    out: list[dict] = []
    for r in rows:
        try:
            d = json.loads(r["data"]) if r["data"] else {}
        except (json.JSONDecodeError, ValueError):
            d = {}
        out.append({
            "ticket_key": r["ticket_key"],
            "status": r["status"],
            "slug": r["slug"],
            "summary": d.get("summary") or "",
            "description": (d.get("description") or "")[:1000],
        })
    return out


def all_terminal(instance_key: str, release_id: int) -> bool:
    """True iff release has >= 1 member and all members are terminal."""
    rel = get_release_by_id(instance_key, release_id)
    if not rel:
        return False
    row = db.query_one(
        "SELECT COUNT(*) AS total,"
        " SUM(CASE WHEN status IN ('merged','validation','done') THEN 1 ELSE 0 END) AS terminal"
        " FROM tickets WHERE instance_key=? AND release_key=?",
        (instance_key, rel["release_key"]),
    )
    if not row or not row["total"]:
        return False
    return int(row["total"]) == int(row["terminal"] or 0)


def list_summaries(instance_key: str) -> list[dict]:
    out: list[dict] = []
    for rel in list_releases(instance_key):
        counts = db.query_one(
            "SELECT COUNT(*) AS total,"
            " SUM(CASE WHEN status IN ('merged','validation','done') THEN 1 ELSE 0 END) AS terminal"
            " FROM tickets WHERE instance_key=? AND release_key=?",
            (instance_key, rel["release_key"]),
        ) or {"total": 0, "terminal": 0}
        latest = latest_review(rel["id"])
        out.append({
            "id": rel["id"],
            "release_key": rel["release_key"],
            "title": rel.get("title"),
            "status": rel["status"],
            "ticket_count": int(counts["total"] or 0),
            "terminal_count": int(counts["terminal"] or 0),
            "latest_verdict": latest.get("verdict") if latest else None,
            "last_inspected_at": rel.get("last_inspected_at"),
        })
    return out


def latest_review(release_id: int) -> dict | None:
    row = db.query_one(
        "SELECT id, release_id, instance_key, verdict, findings, ticket_set_hash,"
        " release_md_hash, created_at FROM release_review"
        " WHERE release_id=? ORDER BY created_at DESC, id DESC LIMIT 1",
        (release_id,),
    )
    if not row:
        return None
    try:
        findings = json.loads(row["findings"]) if row["findings"] else []
    except (json.JSONDecodeError, ValueError):
        findings = []
    if not isinstance(findings, list):
        findings = []
    return {
        "id": row["id"],
        "release_id": row["release_id"],
        "instance_key": row["instance_key"],
        "verdict": row["verdict"],
        "findings": findings,
        "ticket_set_hash": row["ticket_set_hash"],
        "release_md_hash": row["release_md_hash"],
        "created_at": row["created_at"],
    }


def insert_release_review(*, release_id: int, instance_key: str, verdict: str,
                          findings: list, ticket_set_hash: str,
                          release_md_hash: str | None) -> dict:
    db.execute(
        "INSERT INTO release_review"
        "(release_id, instance_key, verdict, findings, ticket_set_hash,"
        " release_md_hash, created_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?)",
        (release_id, instance_key, verdict, json.dumps(findings or []),
         ticket_set_hash, release_md_hash, _now()),
    )
    return latest_review(release_id) or {}


def mark_release_inspected(instance_key: str, release_id: int,
                           ticket_set_hash: str) -> None:
    db.execute(
        "UPDATE releases SET status='inspected', ticket_set_hash=?, last_inspected_at=?"
        " WHERE instance_key=? AND id=?",
        (ticket_set_hash, _now(), instance_key, release_id),
    )


def assign_ticket(instance_key: str, ticket_key: str,
                  release_key: str | None) -> dict | None:
    """Assign (or unassign with release_key=None) a ticket to a release.
    Returns the updated ticket dict, or None if the ticket doesn't exist.

    Caller is responsible for ensuring core.state is operating against
    `instance_key` (i.e. either `state.init(<key>)` was called or the request
    context middleware has switched via `state.use(<key>)`)."""
    if release_key is not None and not is_valid_release_key(release_key):
        raise ValueError(f"invalid release_key: {release_key!r}")
    if release_key is not None:
        upsert_release(instance_key, release_key)

    import core.state as state

    def _mut(current: dict) -> dict | None:
        if not current:
            return None
        merged = dict(current)
        if release_key is None:
            merged.pop("release_key", None)
        else:
            merged["release_key"] = release_key
        return merged

    return state.update_ticket(ticket_key, _mut)


def ticket_release_key(instance_key: str, ticket_key: str) -> str | None:
    row = db.query_one(
        "SELECT release_key FROM tickets WHERE instance_key=? AND ticket_key=?",
        (instance_key, ticket_key),
    )
    return row.get("release_key") if row else None


# --- release.md handling --------------------------------------------------

def release_md_path(config: dict, release_key: str) -> Path | None:
    ws = config.get("workspace") or {}
    root = ws.get("root")
    if not root:
        return None
    root_path = Path(root) if not isinstance(root, Path) else root
    return root_path / RELEASES_DIRNAME / release_key / "release.md"


def read_release_md(config: dict, release_key: str) -> tuple[str | None, str | None]:
    """Returns (text, sha256_hex) or (None, None) if missing/unreadable."""
    p = release_md_path(config, release_key)
    if p is None or not p.is_file():
        return None, None
    try:
        text = p.read_text()
    except OSError:
        try:
            import core.log as log
            log.emit("release_md_unreadable",
                     f"could not read {p}", meta={"path": str(p)})
        except Exception:
            pass
        return None, None
    h = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return text, h


def compute_ticket_set_hash(tickets: Iterable[dict],
                            release_md_hash: str | None) -> str:
    keys = sorted(t["ticket_key"] for t in tickets if t.get("ticket_key"))
    h = hashlib.sha256()
    h.update("\n".join(keys).encode("utf-8"))
    if release_md_hash:
        h.update(b"\n--release.md--\n")
        h.update(release_md_hash.encode("utf-8"))
    return h.hexdigest()


# --- trigger --------------------------------------------------------------

def maybe_trigger_inspect(instance_key: str, ticket_key: str,
                          new_status: str | None,
                          old_status: str | None) -> None:
    """Called from core.state.update_ticket after a status transition. If the
    new status is terminal AND the ticket is assigned to a release AND that
    release is fully terminal AND the feature flag is on, enqueue a
    `release_inspect` job. Cheap no-op otherwise.

    Safe to call when runtime/config/queue isn't initialized — returns silently.
    """
    if new_status not in TERMINAL_STATUSES:
        return
    try:
        import core.runtime as rt
    except Exception:
        return
    instances = rt.instances() if hasattr(rt, "instances") else None
    if instances is None:
        return
    reg = instances.get(instance_key)
    if reg is None:
        return
    cfg = getattr(reg, "config", None) or {}
    if not (cfg.get("features") or {}).get("releases"):
        return
    rk = ticket_release_key(instance_key, ticket_key)
    if not rk:
        return
    rel = get_release_by_key(instance_key, rk)
    if not rel:
        return
    if not all_terminal(instance_key, rel["id"]):
        return
    try:
        import core.queue as q
        q.enqueue_job(instance_key, "release_inspect",
                      payload={"release_id": rel["id"]})
    except Exception as e:
        try:
            import core.log as log
            log.emit("release_inspect_enqueue_error",
                     f"could not enqueue release_inspect for {rk}: {type(e).__name__}: {e}",
                     meta={"release_key": rk})
        except Exception:
            pass


# --- payload helpers (consumed by pm/release_runner) ----------------------

def _truncate(text: str, n: int) -> str:
    if len(text) <= n:
        return text
    return text[:n] + "\n... [truncated]"


def build_inspection_payload(config: dict, release: dict,
                             tickets: list[dict], release_md: str | None) -> str:
    parts: list[str] = []
    parts.append(f"RELEASE: {release['release_key']}")
    if release.get("title"):
        parts.append(f"TITLE: {release['title']}")
    if release_md:
        parts.append("\nRELEASE.MD (focus):")
        parts.append(_truncate(release_md, 6000))
    parts.append("\nTICKETS:")
    ws = config.get("workspace") or {}
    root = ws.get("root")
    tickets_dir = ws.get("tickets_dir") or "tickets"
    root_path = Path(root) if root and not isinstance(root, Path) else root
    for t in tickets:
        parts.append(f"\n## {t['ticket_key']} [{t['status']}] — {t.get('summary','')}")
        if t.get("description"):
            parts.append(_truncate(t["description"], 1500))
        slug = t.get("slug")
        if slug and root_path:
            manifest = Path(root_path) / tickets_dir / slug / "docs" / "change-manifest.md"
            if manifest.is_file():
                try:
                    parts.append("change-manifest.md:")
                    parts.append(_truncate(manifest.read_text(), 4000))
                except OSError:
                    pass
    return "\n".join(parts)
