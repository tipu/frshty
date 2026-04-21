#!/usr/bin/env python3
"""One-shot JSON -> SQLite kv migration.

Walks ~/.frshty/<instance>/*.json and stores each file as a blob row in the
shared `kv` table of ~/.frshty/frshty.db, keyed by (instance_key, module).

The feature code (via core/state.py) reads/writes the same blobs after this
migration runs, so existing JSON files are superseded. Idempotent:
re-running overwrites the same rows.

Usage:
    python scripts/migrate_state.py [--root ~/.frshty] [--db ~/.frshty/frshty.db]
"""
from __future__ import annotations
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402


KNOWN_MODULES = [
    "own_prs", "scheduler", "slack", "reviews", "timesheet_fill",
    "billing_entries", "billing_invoices", "billing_autogen",
]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load(path: Path):
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def migrate_instance(instance_key: str, instance_dir: Path) -> int:
    n = 0
    for module in KNOWN_MODULES:
        data = _load(instance_dir / f"{module}.json")
        if data is None:
            continue
        db.execute(
            "INSERT INTO kv(instance_key, key, data, updated_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(instance_key, key) DO UPDATE SET data=excluded.data, updated_at=excluded.updated_at",
            (instance_key, module, json.dumps(data, default=str), _now()),
        )
        n += 1
    return n


def _insert_ticket_rows(instance_key: str, tickets: dict) -> int:
    now = _now()
    inserted = 0
    for k, v in tickets.items():
        if not isinstance(v, dict):
            continue
        auto_pr = v.get("auto_pr")
        result = db.query_one(
            "SELECT 1 AS x FROM tickets WHERE instance_key=? AND ticket_key=?",
            (instance_key, k),
        )
        if result:
            continue
        db.execute(
            "INSERT INTO tickets"
            "(instance_key, ticket_key, status, slug, branch, url, external_status, auto_pr, data, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (instance_key, k, v.get("status", "new"), v.get("slug"), v.get("branch"),
             v.get("url"), v.get("external_status"),
             (1 if auto_pr else 0) if auto_pr is not None else None,
             json.dumps(v, default=str), now),
        )
        inserted += 1
    return inserted


def migrate_tickets_kv_to_rows(instance_key: str) -> int:
    """Promote kv['tickets'] blob into per-row entries in the tickets table.
    Idempotent: skips tickets that already exist as rows. Leaves the kv blob
    in place for rollback safety."""
    row = db.query_one(
        "SELECT data FROM kv WHERE instance_key=? AND key='tickets'", (instance_key,)
    )
    if not row or not row.get("data"):
        return 0
    try:
        existing = json.loads(row["data"])
    except json.JSONDecodeError:
        return 0
    if not isinstance(existing, dict) or not existing:
        return 0
    return _insert_ticket_rows(instance_key, existing)


def migrate_tickets_json_to_rows(instance_key: str, instance_dir: Path) -> int:
    """Promote ~/.frshty/<inst>/tickets.json directly into the tickets table,
    skipping the kv staging. Idempotent."""
    data = _load(instance_dir / "tickets.json")
    if not isinstance(data, dict) or not data:
        return 0
    return _insert_ticket_rows(instance_key, data)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=str(Path.home() / ".frshty"))
    ap.add_argument("--db", default=str(Path.home() / ".frshty" / "frshty.db"))
    args = ap.parse_args()

    root = Path(args.root).expanduser()
    db_path = Path(args.db).expanduser()
    migrations_dir = ROOT / "migrations"

    print(f"Initializing {db_path} from migrations in {migrations_dir}")
    db.init(db_path, migrations_dir)

    total = 0
    instance_dirs: dict[str, Path] = {}
    for instance_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        instance_key = instance_dir.name
        if instance_key.startswith(".") or instance_key == "__pycache__":
            continue
        instance_dirs[instance_key] = instance_dir
        n = migrate_instance(instance_key, instance_dir)
        if n:
            print(f"  {instance_key}: {n} modules")
            total += n
    print(f"Done. Inserted/updated {total} kv rows.")

    # Include any instances that already have a kv['tickets'] blob but no on-disk dir
    # (e.g. from a prior migration run) so the rows promotion still covers them.
    extra_keys: list[str] = []
    for r in db.query_all("SELECT DISTINCT instance_key FROM kv WHERE key='tickets'"):
        ik = r["instance_key"]
        if ik and ik not in instance_dirs:
            extra_keys.append(ik)

    print("Promoting tickets -> per-row storage...")
    promoted_total = 0
    for ik, idir in instance_dirs.items():
        n = migrate_tickets_json_to_rows(ik, idir) + migrate_tickets_kv_to_rows(ik)
        if n:
            print(f"  {ik}: {n} tickets")
            promoted_total += n
    for ik in extra_keys:
        n = migrate_tickets_kv_to_rows(ik)
        if n:
            print(f"  {ik}: {n} tickets")
            promoted_total += n
    print(f"Done. Promoted {promoted_total} tickets to per-row storage.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
