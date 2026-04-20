#!/usr/bin/env python3
"""One-shot JSON -> SQLite migration for frshty state.

Reads every ~/.frshty/<instance>/*.json and inserts rows into the consolidated
~/.frshty/frshty.db. Idempotent: re-running is safe (INSERT OR REPLACE).

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


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load(path: Path) -> dict | list | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return None


def migrate_tickets(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    n = 0
    for key, ts in data.items():
        if not isinstance(ts, dict):
            continue
        core_fields = {"status", "slug", "branch", "url", "external_status"}
        status = ts.get("status", "new")
        slug = ts.get("slug")
        branch = ts.get("branch")
        url = ts.get("url")
        ext = ts.get("external_status")
        auto_pr = ts.get("auto_pr")
        auto_pr_int = None
        if isinstance(auto_pr, bool):
            auto_pr_int = 1 if auto_pr else 0
        extra = {k: v for k, v in ts.items() if k not in core_fields and k != "auto_pr"}
        db.execute(
            "INSERT OR REPLACE INTO tickets(instance_key, ticket_key, status, slug, branch, url, external_status, auto_pr, data, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (instance_key, key, status, slug, branch, url, ext, auto_pr_int, db.dump_json(extra), _now()),
        )
        n += 1
    return n


def migrate_own_prs(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    n = 0
    for composite, payload in data.items():
        if not isinstance(payload, dict):
            continue
        repo = payload.get("repo") or composite.split("#")[0]
        pr_id_raw = payload.get("id") or composite.rsplit("#", 1)[-1]
        try:
            pr_id = int(pr_id_raw)
        except (TypeError, ValueError):
            continue
        db.execute(
            "INSERT OR REPLACE INTO own_prs(instance_key, repo, pr_id, data, updated_at) VALUES (?, ?, ?, ?, ?)",
            (instance_key, repo, pr_id, db.dump_json(payload), _now()),
        )
        n += 1
    return n


def migrate_scheduler(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    n = 0
    for key, payload in data.items():
        if not isinstance(payload, dict):
            continue
        run_at = payload.get("run_at")
        db.execute(
            "INSERT OR REPLACE INTO scheduler(instance_key, key, run_at, data) VALUES (?, ?, ?, ?)",
            (instance_key, key, run_at, db.dump_json(payload)),
        )
        n += 1
    return n


def migrate_slack(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    db.execute(
        "INSERT OR REPLACE INTO slack_state(instance_key, namespace, key, data, updated_at) VALUES (?, 'root', 'state', ?, ?)",
        (instance_key, db.dump_json(data), _now()),
    )
    return 1


def migrate_billing_entries(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    n = 0
    for date_str, entry in data.items():
        if not isinstance(entry, dict):
            continue
        db.execute(
            "INSERT OR REPLACE INTO billing_entries(instance_key, date, type, hours) VALUES (?, ?, ?, ?)",
            (instance_key, date_str, entry.get("type", "work"), float(entry.get("hours", 0) or 0)),
        )
        n += 1
    return n


def migrate_billing_invoices(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    n = 0
    for inv_id, payload in data.items():
        if not isinstance(payload, dict):
            continue
        db.execute(
            "INSERT OR REPLACE INTO billing_invoices(instance_key, id, number, status, start_date, end_date, hours, amount, date, source, data)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (instance_key, inv_id, payload.get("number"), payload.get("status"),
             payload.get("start"), payload.get("end"), payload.get("hours"),
             payload.get("amount"), payload.get("date"), payload.get("source"),
             db.dump_json(payload)),
        )
        n += 1
    return n


def migrate_billing_autogen(instance_key: str, data: dict | None) -> int:
    if not data:
        return 0
    n = 0
    for marker, payload in data.items():
        if not isinstance(payload, dict):
            continue
        db.execute(
            "INSERT OR REPLACE INTO billing_autogen(instance_key, marker, data) VALUES (?, ?, ?)",
            (instance_key, marker, db.dump_json(payload)),
        )
        n += 1
    return n


MIGRATIONS = [
    ("tickets.json", migrate_tickets),
    ("own_prs.json", migrate_own_prs),
    ("scheduler.json", migrate_scheduler),
    ("slack.json", migrate_slack),
    ("billing_entries.json", migrate_billing_entries),
    ("billing_invoices.json", migrate_billing_invoices),
    ("billing_autogen.json", migrate_billing_autogen),
]


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
    for instance_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        instance_key = instance_dir.name
        if instance_key.startswith("."):
            continue
        print(f"Migrating instance: {instance_key}")
        for fname, fn in MIGRATIONS:
            data = _load(instance_dir / fname)
            n = fn(instance_key, data if isinstance(data, dict) else None)
            if n:
                print(f"  {fname}: {n} rows")
                total += n
    print(f"Done. Inserted/updated {total} rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
