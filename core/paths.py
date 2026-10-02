"""Where frshty keeps its state.

FRSHTY_ROOT moves the whole tree. An instance container sets it to a host
directory mounted at the same path, so a worktree it registers in a shared
repository names a path the host can see too, and the host never mistakes it
for a missing worktree.
"""
import os
from pathlib import Path


def frshty_root() -> Path:
    return Path(os.environ.get("FRSHTY_ROOT") or Path.home() / ".frshty")


def db_path() -> Path:
    """FRSHTY_DB moves the database out of the state tree. An instance
    container on a macOS host sets it to a Docker volume, because file locks
    do not hold across processes on a macOS bind mount and SQLite needs them."""
    return Path(os.environ.get("FRSHTY_DB") or frshty_root() / "frshty.db")
