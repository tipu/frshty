"""Read the PRD markdown file from disk."""
import hashlib
from pathlib import Path


def read(path: str | Path) -> tuple[str, str] | None:
    """Returns (text, file_hash) or None if file missing/unreadable."""
    p = Path(path)
    if not p.is_file():
        return None
    try:
        text = p.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    return text, hashlib.sha256(text.encode("utf-8")).hexdigest()
