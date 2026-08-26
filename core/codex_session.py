import base64
import binascii
import glob
import json
import os
import re
from datetime import datetime, timedelta, timezone

HOME_DIR = os.environ.get("CODEX_HOME") or os.path.expanduser("~/.codex")
_ROLLOUT_PREFIX = "rollout-"
_DATA_IMAGE_RE = re.compile(r"^data:(image/[a-z0-9.+-]+);base64,(.+)$", re.IGNORECASE | re.DOTALL)
_IMAGE_ID_RE = re.compile(r"^(\d+)-(\d+)$")
_ROLLOUT_NAME_RE = re.compile(
    r"^" + _ROLLOUT_PREFIX + r"\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-(.+)\.jsonl$")
_MAX_IMAGE_BASE64 = 64 * 1024 * 1024
LAUNCH_WINDOW_SECONDS = 600


def is_rollout(path: str) -> bool:
    """True when `path` names a codex rollout transcript.

    The work layer stores one transcript path per run and reads it back
    without carrying the provider along, so the reader is chosen from the
    file name codex itself writes."""
    base = os.path.basename(path or "")
    return base.startswith(_ROLLOUT_PREFIX) and base.endswith(".jsonl")


def rollout_path(thread_id: str, home: str = "") -> str:
    """The rollout transcript codex wrote for one thread id, or "" when no
    file exists yet. Codex files sessions by date, so the id is matched
    against every day directory."""
    thread_id = (thread_id or "").strip()
    if not thread_id:
        return ""
    root = home or HOME_DIR
    matches = glob.glob(os.path.join(root, "sessions", "*", "*", "*",
                                     f"{_ROLLOUT_PREFIX}*-{thread_id}.jsonl"))
    return sorted(matches)[-1] if matches else ""


def _instant(value: str) -> datetime | None:
    """One ISO timestamp as an aware datetime, or None when it cannot be read.

    Codex stamps a rollout in UTC with a trailing Z and the work store stamps
    a run with an explicit offset, so the two are compared as instants rather
    than as strings."""
    try:
        parsed = datetime.fromisoformat((value or "").strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def session_meta(path: str) -> dict:
    """The session_meta payload codex writes as the first line of a rollout."""
    try:
        with open(path, "rb") as f:
            record = json.loads(f.readline().decode("utf-8", errors="replace"))
    except (OSError, ValueError):
        return {}
    if record.get("type") != "session_meta":
        return {}
    payload = record.get("payload")
    return payload if isinstance(payload, dict) else {}


def find_rollout(cwd: str, started_after: str, home: str = "",
                 within_seconds: int = LAUNCH_WINDOW_SECONDS) -> str:
    """The rollout of the codex session started in `cwd` at `started_after`.

    A run learns its rollout path from the notify program, and that program
    runs only after codex completes a turn. A long first turn therefore leaves
    the run with no transcript to read. Codex writes the working directory and
    the start time into the first record of every rollout, so the file is
    found from the two facts the launch already recorded. A pane opens its
    rollout within seconds of the launch, so a session that starts later than
    `within_seconds` belongs to something else and is never taken. The
    earliest match inside that window wins. Files untouched since the run
    started are skipped without being opened."""
    root = home or HOME_DIR
    target = os.path.realpath(cwd) if cwd else ""
    start = _instant(started_after)
    if not target or start is None:
        return ""
    deadline = start + timedelta(seconds=within_seconds)
    best_at, best_path = None, ""
    for path in glob.glob(os.path.join(root, "sessions", "*", "*", "*",
                                       f"{_ROLLOUT_PREFIX}*.jsonl")):
        try:
            if datetime.fromtimestamp(os.stat(path).st_mtime, timezone.utc) < start:
                continue
        except OSError:
            continue
        meta = session_meta(path)
        at = _instant(str(meta.get("timestamp") or ""))
        if at is None or at < start or at > deadline:
            continue
        if os.path.realpath(str(meta.get("cwd") or "")) != target:
            continue
        if best_at is None or at < best_at:
            best_at, best_path = at, path
    return best_path


def rollout_thread_id(path: str) -> str:
    """The codex thread id of a rollout, taken from its file name.

    Codex names every rollout `rollout-<start time>-<thread id>.jsonl`, and
    the start time is one fixed-width field, so the id is what follows it."""
    match = _ROLLOUT_NAME_RE.match(os.path.basename(path or ""))
    return match.group(1) if match else ""


def _tail_records(path: str, max_bytes: int):
    """JSONL records from the tail, with each record's byte offset.

    Include the complete line that crosses the byte boundary. Inline images
    can make one rollout record larger than ``max_bytes``; dropping that line
    would hide the prompt that the limit was meant to retain.
    """
    try:
        with open(path, "rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            start = max(0, size - max_bytes)
            cursor = start
            while cursor > 0:
                chunk_start = max(0, cursor - 65536)
                f.seek(chunk_start)
                chunk = f.read(cursor - chunk_start)
                newline = chunk.rfind(b"\n")
                if newline >= 0:
                    start = chunk_start + newline + 1
                    break
                cursor = chunk_start
            else:
                start = 0
            f.seek(start)
            while True:
                offset = f.tell()
                line = f.readline()
                if not line:
                    break
                try:
                    yield json.loads(line.decode("utf-8", errors="replace")), offset
                except ValueError:
                    continue
    except OSError:
        return


def _items(path: str, max_bytes: int):
    """The completed conversation items in a rollout, oldest first.

    A rollout holds the raw model exchange as well, including the developer
    preamble and the environment block. The item_completed events are the
    conversation as codex itself shows it, so only those are read."""
    for d, _ in _tail_records(path, max_bytes):
        payload = d.get("payload") or {}
        if payload.get("type") != "item_completed":
            continue
        item = payload.get("item")
        if isinstance(item, dict):
            yield item, d.get("timestamp", "")


def _item_text(item: dict) -> str:
    parts = []
    for block in item.get("content") or []:
        if isinstance(block, dict) and block.get("text"):
            parts.append(str(block["text"]))
    return "\n".join(parts).strip()


def _command_arg(item: dict) -> str:
    command = item.get("command")
    if isinstance(command, list) and command:
        return str(command[-1]).replace("\n", " ")[:120]
    return str(command or "").replace("\n", " ")[:120]


def _image_refs(content: list, offset: int) -> list[dict]:
    refs = []
    for block_index, block in enumerate(content):
        if not isinstance(block, dict) or block.get("type") != "input_image":
            continue
        match = _DATA_IMAGE_RE.match(str(block.get("image_url") or ""))
        if not match:
            continue
        refs.append({"id": f"{offset}-{block_index}", "media_type": match.group(1).lower()})
    return refs


def _response_text(content: list) -> str:
    return "\n".join(
        str(block.get("text") or "") for block in content
        if isinstance(block, dict) and block.get("type") == "input_text" and block.get("text")
    ).strip()


def timeline(path: str, max_bytes: int = 4194304) -> list[dict]:
    """A codex rollout rendered in the same shape as a Claude transcript
    timeline: prompts, agent text and tool calls, oldest first."""
    if not path or not os.path.isfile(path):
        return []
    out: list[dict] = []
    pending_images: list[dict] = []
    pending_text = ""
    pending_at = ""
    for record, offset in _tail_records(path, max_bytes):
        payload = record.get("payload") or {}
        if record.get("type") == "response_item" and payload.get("type") == "message" \
                and payload.get("role") == "user":
            content = payload.get("content") or []
            if isinstance(content, list):
                pending_images = _image_refs(content, offset)
                pending_text = _response_text(content)
                pending_at = record.get("timestamp", "")
            continue
        if payload.get("type") != "item_completed":
            continue
        item = payload.get("item")
        if not isinstance(item, dict):
            continue
        at = record.get("timestamp", "")
        kind = item.get("type")
        if kind == "UserMessage":
            text = _item_text(item)
            if text or pending_images:
                entry = {"kind": "prompt", "text": text[:500], "at": at}
                if pending_images:
                    entry["images"] = pending_images
                out.append(entry)
            pending_images = []
            pending_text = ""
            pending_at = ""
        elif kind == "AgentMessage":
            text = _item_text(item)
            if text:
                out.append({"kind": "text", "text": text[:2000], "at": at})
        elif kind == "CommandExecution":
            out.append({"kind": "tool", "name": "exec", "arg": _command_arg(item), "at": at})
        elif kind == "Extension":
            out.append({"kind": "tool", "name": str(item.get("kind") or "extension"),
                        "arg": str(item.get("query") or "").replace("\n", " ")[:120], "at": at})
    # Older rollouts can contain raw user messages without item_completed.
    if pending_images:
        out.append({"kind": "prompt", "text": pending_text[:500], "at": pending_at,
                    "images": pending_images})
    return out


def embedded_image(path: str, image_id: str) -> tuple[bytes, str] | None:
    """Return one inline user image addressed by ``byte-offset-block-index``."""
    match = _IMAGE_ID_RE.fullmatch(image_id or "")
    if not match or not path or not os.path.isfile(path):
        return None
    offset, block_index = (int(part) for part in match.groups())
    try:
        with open(path, "rb") as f:
            if offset < 0 or offset >= os.fstat(f.fileno()).st_size:
                return None
            f.seek(offset)
            record = json.loads(f.readline().decode("utf-8", errors="replace"))
    except (OSError, ValueError):
        return None
    payload = record.get("payload") or {}
    if record.get("type") != "response_item" or payload.get("type") != "message" \
            or payload.get("role") != "user":
        return None
    content = payload.get("content") or []
    if not isinstance(content, list) or block_index >= len(content):
        return None
    block = content[block_index]
    if not isinstance(block, dict) or block.get("type") != "input_image":
        return None
    data_match = _DATA_IMAGE_RE.match(str(block.get("image_url") or ""))
    if not data_match or len(data_match.group(2)) > _MAX_IMAGE_BASE64:
        return None
    try:
        return base64.b64decode(data_match.group(2), validate=True), data_match.group(1).lower()
    except (ValueError, binascii.Error):
        return None


def assistant_texts(path: str, max_bytes: int = 4194304) -> list[str]:
    if not path or not os.path.isfile(path):
        return []
    texts = []
    for item, _ in _items(path, max_bytes):
        if item.get("type") != "AgentMessage":
            continue
        text = _item_text(item)
        if text:
            texts.append(text)
    return texts


def last_assistant_text(path: str, max_bytes: int = 262144) -> str:
    texts = assistant_texts(path, max_bytes)
    return texts[-1] if texts else ""
