"""Durable per-item storage for the files a work session hands the operator.

A session writes its reports, pages, images and videos into
~/.frshty/artifacts/work-<item id>/ instead of the Claude Code scratchpad,
/tmp, or the hosted Claude artifact publisher. The board serves an artifact
from disk long after the session's tmux pane is gone, so the file has to
survive a reboot and stay on this machine. `gc_artifacts` bounds the growth:
it deletes an item's folder once nothing inside it was written for
MAX_AGE_DAYS, forgets the rows that pointed into that folder, and forgets the
older rows that still point into /tmp at a file a reboot has wiped.
"""
import base64
import binascii
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path

import core.db as db
import core.log as log

MAX_AGE_DAYS = 30
GC_INTERVAL_S = 86400
SCRATCH_PREFIX = "/tmp/"
ROOT_ENV = "FRSHTY_ARTIFACT_ROOT"
INTAKE_DIR = "intake"
INTAKE_IMAGE_TYPES = {"image/png": ".png", "image/jpeg": ".jpg",
                      "image/gif": ".gif", "image/webp": ".webp"}
MAX_INTAKE_IMAGES = 8
MAX_INTAKE_IMAGE_BYTES = 10 * 1024 * 1024


def root() -> Path:
    """The artifact store. ROOT_ENV relocates it, which is how the test suite
    keeps a launch out of the operator's real store even after a test reloads
    this module."""
    override = os.environ.get(ROOT_ENV)
    return Path(override) if override else Path.home() / ".frshty" / "artifacts"


def item_dir(item_id: int) -> Path:
    """The artifact folder of one work item, created if it does not exist."""
    folder = root() / f"work-{item_id}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def decode_intake_images(images) -> tuple[list[tuple[bytes, str]], str]:
    """The images one launch carries, decoded, or an empty list and an error.

    The board posts an image the operator pasted into the compose box as
    base64 beside its media type. Every check runs before the work item is
    created, so a paste the board cannot store refuses the launch instead of
    leaving a task that names a file nothing wrote."""
    if not images:
        return [], ""
    if not isinstance(images, list):
        return [], "images must be a list"
    if len(images) > MAX_INTAKE_IMAGES:
        return [], (f"too many images: {len(images)}; "
                    f"at most {MAX_INTAKE_IMAGES} per task")
    decoded: list[tuple[bytes, str]] = []
    for index, image in enumerate(images, 1):
        if not isinstance(image, dict):
            return [], f"image {index} is not an object"
        media_type = str(image.get("type") or "").strip().lower()
        suffix = INTAKE_IMAGE_TYPES.get(media_type)
        if suffix is None:
            return [], (f"image {index} has unsupported type "
                        f"{media_type or '(none)'}; paste a png, jpeg, gif or webp")
        try:
            data = base64.b64decode(str(image.get("data") or ""), validate=True)
        except (ValueError, binascii.Error):
            return [], f"image {index} is not valid base64"
        if not data:
            return [], f"image {index} is empty"
        if len(data) > MAX_INTAKE_IMAGE_BYTES:
            return [], (f"image {index} is {len(data)} bytes, over the "
                        f"{MAX_INTAKE_IMAGE_BYTES} byte limit")
        decoded.append((data, suffix))
    return decoded, ""


def save_intake_images(item_id: int, decoded: list[tuple[bytes, str]]) -> list[str]:
    """Write the decoded images of one launch into the item's folder.

    The agent runs in a tmux pane and reads an image from a path, so a pasted
    image has to outlive the request that carried it. It lands under the
    item's own folder, where the same gc reclaims it."""
    if not decoded:
        return []
    folder = item_dir(item_id) / INTAKE_DIR
    folder.mkdir(parents=True, exist_ok=True)
    paths = []
    for index, (data, suffix) in enumerate(decoded, 1):
        path = folder / f"pasted-{index}{suffix}"
        path.write_bytes(data)
        paths.append(str(path))
    return paths


def _newest_mtime(folder: Path) -> float:
    """Newest mtime anywhere in the tree. A folder's own mtime only tracks its
    direct children, so a file written deep inside it would otherwise read as
    old and the whole item would be deleted while still current."""
    newest = folder.stat().st_mtime
    for dirpath, dirnames, filenames in os.walk(folder):
        for name in dirnames + filenames:
            try:
                newest = max(newest, os.lstat(os.path.join(dirpath, name)).st_mtime)
            except OSError:
                continue
    return newest


def _forget_rows(folder: Path) -> int:
    prefix = str(folder) + os.sep
    rows = db.query_all("SELECT id, path FROM work_artifacts")
    stale = [r["id"] for r in rows
             if r["path"] == str(folder) or r["path"].startswith(prefix)]
    if not stale:
        return 0
    with db.tx() as c:
        c.executemany("DELETE FROM work_artifacts WHERE id = ?",
                      [(i,) for i in stale])
    return len(stale)


def _forget_lost_scratch_rows(cutoff: float) -> int:
    """Forget artifact rows older than the cutoff that point into the scratch
    prefix at a file that is gone.

    Sessions used to write reports into /tmp, which a reboot wipes. Such a row
    outlives its file and leaves a dead link on the board forever."""
    stamp = datetime.fromtimestamp(cutoff, timezone.utc).isoformat()
    rows = db.query_all(
        "SELECT id, path FROM work_artifacts WHERE created_at < ?", (stamp,))
    stale = [r["id"] for r in rows
             if r["path"].startswith(SCRATCH_PREFIX) and not os.path.exists(r["path"])]
    if not stale:
        return 0
    with db.tx() as c:
        c.executemany("DELETE FROM work_artifacts WHERE id = ?",
                      [(i,) for i in stale])
    return len(stale)


def gc_artifacts(max_age_days: int = MAX_AGE_DAYS, now: float | None = None,
                 force: bool = False) -> dict:
    """Delete artifact folders untouched for `max_age_days`. Self-throttled to
    one sweep per GC_INTERVAL_S through a stamp file, so the scan loop can call
    this on every tick."""
    base = root()
    removed: list[str] = []
    forgotten = 0
    try:
        base.mkdir(parents=True, exist_ok=True)
    except OSError:
        return {"removed": removed, "forgotten": forgotten}
    now = time.time() if now is None else now
    stamp = base / ".gc-stamp"
    if not force and stamp.exists() and now - stamp.stat().st_mtime < GC_INTERVAL_S:
        return {"removed": removed, "forgotten": forgotten, "throttled": True}
    stamp.touch()
    cutoff = now - max_age_days * 86400
    for entry in sorted(base.iterdir()):
        if entry.name.startswith(".") or not entry.is_dir():
            continue
        try:
            if _newest_mtime(entry) >= cutoff:
                continue
            shutil.rmtree(entry)
        except OSError:
            continue
        removed.append(entry.name)
        forgotten += _forget_rows(entry)
    forgotten += _forget_lost_scratch_rows(cutoff)
    if removed or forgotten:
        log.emit("work_artifact_gc",
                 f"Deleted {len(removed)} artifact folder(s) and {forgotten} "
                 f"artifact row(s) older than {max_age_days} days",
                 meta={"removed": removed, "forgotten_rows": forgotten})
    return {"removed": removed, "forgotten": forgotten}
