"""Point frshty at the filtered Slack capture and read it as capture records.

slack_int writes two logs into one workspace directory. `messages.jsonl` holds
every intercepted frame, and almost all of it is transport noise: pings,
typing, presence, reconnects, and REST responses that carry no message.
`filtered.jsonl` beside it holds only the records that carry content, one
normalized line each, and it is well under one percent of the raw log. Reading
the filtered log is what the two scanners in `features/` do, and this module is
where that choice is made.

A filtered line is normalized, not raw, and both scanners parse the raw shape.
`as_capture_record` converts one filtered line back into the capture record it
was distilled from rather than adding a second parser, so the scanners keep one
code path and both logs feed it.

A capture slack_int has not filtered still has to work, so `live_path` falls
back to the raw log when there is no filtered log beside it. The fallback is
decided once per scan, from the directory, not per line.
"""
import json
import os
from pathlib import Path

RAW_FILE = "messages.jsonl"
FILTERED_FILE = "filtered.jsonl"
USERS_FILE = "users.json"


def capture_dir(messages_dir: str, raw_path: str = "") -> str:
    """The workspace directory one instance's capture lives in.

    core.config derives one of messages_dir and raw_path from the other, so
    either key names the same directory. A config read straight from tomllib
    in a test has not been through that, so the raw path is honoured too."""
    if messages_dir:
        return os.path.expanduser(messages_dir)
    if raw_path:
        return str(Path(os.path.expanduser(raw_path)).parent)
    return ""


def live_path(messages_dir: str, raw_path: str = "") -> str:
    """The capture file a scan reads: the filtered log when slack_int wrote
    one, the raw log otherwise."""
    folder = capture_dir(messages_dir, raw_path)
    if not folder:
        return ""
    filtered = os.path.join(folder, FILTERED_FILE)
    if os.path.exists(filtered):
        return filtered
    if raw_path:
        return os.path.expanduser(raw_path)
    return os.path.join(folder, RAW_FILE)


def is_filtered(path: str) -> bool:
    """Whether a path names the filtered log or one of its rotated siblings."""
    name = Path(path).name
    return name == FILTERED_FILE or name.startswith(FILTERED_FILE + ".")


def is_filtered_record(record: dict) -> bool:
    """Whether one parsed line came from the filtered log.

    A raw capture record always carries `payload`, the intercepted body. A
    filtered record carries no payload and names the workspace it came from,
    so the two shapes never look alike."""
    return (isinstance(record, dict) and "payload" not in record
            and bool(record.get("ws")))


def as_capture_record(record: dict) -> dict | None:
    """One filtered line as the capture record it was distilled from.

    Only the fields the scanners read are rebuilt. A file record carries no
    message, so it returns None.

    An edit and a deletion keep the raw wrapper shape, with the text under
    `message` or `previous_message` and never on the wrapper itself. That is
    what the raw log does and it is load-bearing. features/slack_monitor.py
    reads the wrapper's text, finds none, and leaves edits alone;
    features/slack_conversations.py unwraps them and rewrites the message they
    change. Text on the wrapper would surface every edit a second time as a
    new mention of the message it edits.

    thread_ts is set only when the message really is a reply, for the same
    reason: a root message with a thread_ts equal to its own ts would make
    slack_monitor gather a thread's replies as the context before it instead
    of the messages that came before it in the channel.

    A bot message is given back the `bot_message` subtype that the filter
    replaced with a `bot` name. Both scanners skip that subtype, and without
    it a bot would enter the conversation index as a person. It goes on the
    message, not on the wrapper, because an edit is read from the message
    under the wrapper and a bot that edits its own message would otherwise
    arrive with nothing left to say it is a bot."""
    if not isinstance(record, dict) or record.get("kind") == "file":
        return None
    ts = str(record.get("ts") or "")
    if not ts:
        return None
    channel = str(record.get("ch") or "")
    subtype = str(record.get("subtype") or "")
    edited = bool(record.get("edited")) or subtype == "message_changed"
    deleted = bool(record.get("deleted")) or subtype == "message_deleted"
    message = {"type": "message", "ts": ts, "user": str(record.get("user") or ""),
               "text": record.get("text") or ""}
    if record.get("bot"):
        message["subtype"] = "bot_message"
    elif subtype and not edited and not deleted:
        message["subtype"] = subtype
    thread_ts = str(record.get("thread_ts") or "")
    if thread_ts:
        message["thread_ts"] = thread_ts
    name = str(record.get("name") or "")
    if name:
        message["user_profile"] = {"real_name": name}

    if deleted:
        payload = {"type": "message", "subtype": "message_deleted",
                   "channel": channel, "deleted_ts": ts,
                   "previous_message": message}
    elif edited:
        payload = {"type": "message", "subtype": "message_changed",
                   "channel": channel, "message": message}
    else:
        payload = dict(message)
        payload["channel"] = channel
    return {"dt": str(record.get("dt") or ""),
            "source": str(record.get("src") or ""),
            "workspace": str(record.get("ws") or ""),
            "payload": payload}


def user_names(messages_dir: str, raw_path: str = "") -> dict[str, str]:
    """The display names slack_int indexed beside a filtered capture.

    The raw log carried Slack's own user lists inside the boot and roster
    payloads, and slack_monitor scraped names out of them. The filter drops
    those payloads because they hold no message, and writes users.json beside
    the filtered log instead. Reading it is what replaces the scrape, so a
    name still renders for somebody who never spoke inside the scanned
    window."""
    folder = capture_dir(messages_dir, raw_path)
    if not folder:
        return {}
    try:
        with open(os.path.join(folder, USERS_FILE)) as f:
            users = json.load(f)
    except (OSError, ValueError):
        return {}
    if not isinstance(users, dict):
        return {}
    out = {}
    for uid, info in users.items():
        if isinstance(info, dict) and info.get("name"):
            out[str(uid)] = str(info["name"])
    return out
