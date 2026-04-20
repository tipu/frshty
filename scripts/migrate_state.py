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
    "tickets", "own_prs", "scheduler", "slack", "reviews", "timesheet_fill",
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
        if instance_key.startswith(".") or instance_key == "__pycache__":
            continue
        n = migrate_instance(instance_key, instance_dir)
        if n:
            print(f"  {instance_key}: {n} modules")
            total += n
    print(f"Done. Inserted/updated {total} kv rows.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
