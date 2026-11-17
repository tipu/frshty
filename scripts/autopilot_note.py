"""Append one line to an autopilot run's log on the /autopilot page.

Usage: python3 scripts/autopilot_note.py <run_id> "<what you did>"

Writes straight to the instance database, the path core.paths.db_path names,
so it applies no migration and needs no running board."""
import os
import sqlite3
import sys
from datetime import datetime, timezone


def main(argv: list[str]) -> int:
    if len(argv) != 3 or not argv[1].isdigit() or not argv[2].strip():
        print('usage: autopilot_note.py <run_id> "<what you did>"', file=sys.stderr)
        return 2
    root = os.environ.get("FRSHTY_ROOT") or os.path.join(os.path.expanduser("~"), ".frshty")
    path = os.environ.get("FRSHTY_DB") or os.path.join(root, "frshty.db")
    run_id = int(argv[1])
    conn = sqlite3.connect(path, timeout=30)
    try:
        if conn.execute("SELECT 1 FROM autopilot_runs WHERE id=?", (run_id,)).fetchone() is None:
            print(f"no autopilot run {run_id}", file=sys.stderr)
            return 1
        with conn:
            conn.execute("INSERT INTO autopilot_notes(run_id, ts, text) VALUES (?, ?, ?)",
                         (run_id, datetime.now(timezone.utc).isoformat(), argv[2].strip()))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
