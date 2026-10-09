"""Add a client, company or person name to the forbidden list.

tests/test_no_org_names.py fails when a tracked file holds a forbidden name.
The list stores only a SHA-256 hash and the length of each name, so the
repository never holds the names it forbids.

Usage:
    python3 scripts/forbid_name.py <name> [<name> ...]
"""
import hashlib
import re
import sys
from pathlib import Path

LIST = Path(__file__).resolve().parent.parent / "tests" / "forbidden_names.sha256"


def entry(name: str) -> str:
    token = name.lower()
    if not re.fullmatch(r"[a-z0-9]+", token):
        raise SystemExit(f"a name is one run of letters and digits; split {name!r} into its words")
    return f"{hashlib.sha256(token.encode()).hexdigest()} {len(token)}"


def main(names: list[str]) -> int:
    if not names:
        raise SystemExit(__doc__)
    lines = {line for line in LIST.read_text().splitlines() if line and not line.startswith("#")} if LIST.exists() else set()
    header = [line for line in LIST.read_text().splitlines() if line.startswith("#")] if LIST.exists() else []
    lines.update(entry(name) for name in names)
    LIST.write_text("\n".join(header + sorted(lines)) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
