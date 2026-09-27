"""Record the unlisted files in the artifact folder of every closed task.

A task that wrote a file without an ARTIFACT: line shows no artifact on the
board. `record_artifacts` now scans the folder when a run stops, but a task
that closed before that change needs this one pass. The pass inserts only the
missing rows, so a second run adds nothing.

    python3 scripts/backfill_folder_artifacts.py
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import core.db as db
from services import work_store

DB_PATH = os.environ.get("FRSHTY_DB") or os.path.join(os.environ.get("FRSHTY_ROOT") or os.path.expanduser("~/.frshty"), "frshty.db")


def main() -> int:
    db._DB_PATH = Path(DB_PATH)
    report = work_store.backfill_folder_artifacts()
    for row in report:
        print(f"work-{row['id']}: {row['added']} rows added")
    print(f"{len(report)} tasks, {sum(r['added'] for r in report)} rows added")
    return 0


if __name__ == "__main__":
    sys.exit(main())
