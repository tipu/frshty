#!/usr/bin/env python3
"""Print one secret of this instance, or list the names it holds.

    secret.py            list every secret name
    secret.py <name>     print that secret

Agents asked the operator for a password or a key in close to half of their
questions, and each answer then sat in the board's database. The instance's
read-only ~/.secrets (mounted from ~/.frshty-containers/<key>/secrets/ by
scripts/instance.py) holds one file per secret, named by the secret, so an
agent reads the value itself and nothing passes through the board.
FRSHTY_SECRETS_DIR names another directory."""

import os
import sys
from pathlib import Path


def secrets_dir() -> Path:
    return Path(os.environ.get("FRSHTY_SECRETS_DIR") or Path.home() / ".secrets")


def names(root: Path) -> list[str]:
    if not root.is_dir():
        return []
    return sorted(p.name for p in root.iterdir() if p.is_file() and not p.name.startswith("."))


def main(argv: list[str]) -> int:
    root = secrets_dir()
    known = names(root)
    if not argv:
        if not known:
            print(f"no secrets in {root}", file=sys.stderr)
            return 1
        print("\n".join(known))
        return 0
    name = argv[0]
    if name not in known:
        print(f"no secret named '{name}' in {root}. available:", file=sys.stderr)
        for other in known:
            print(f"  {other}", file=sys.stderr)
        return 1
    print((root / name).read_text().rstrip("\n"))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
