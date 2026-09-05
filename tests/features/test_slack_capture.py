"""Tests for core.slack_capture and the two scanners reading a filtered
capture.

slack_int writes filtered.jsonl beside messages.jsonl, holding the same
messages with the transport noise removed. These tests assert that frshty
reads the filtered log when it is there, that a filtered line reaches the
scanners as the capture record it was distilled from, and that a capture
slack_int has not filtered still works.

The model call is patched everywhere. Nothing here asserts that a real model
answered."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import core.db as db
import core.slack_capture as slack_capture
import core.state as state
from features import slack_conversations as sc
from features import slack_monitor

NOW = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
OPERATOR = "U0OPERATOR"
ERIK = "U0ERIK"
CHANNEL = "C0PLATFORM"
WORKSPACE = "atropos-workspace"
ROOT_TS = "1788458400.000100"
REPLY_TS = "1788458500.000200"


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "atropos"
    state._instance_key_cv.set("atropos")
    state.save("slack", {"user_id": OPERATOR, "names": {OPERATOR: "Danial"}})
    slack_monitor._last_channel_resolve = ""
    yield


def _config(tmp_path, **slack):
    settings = {"messages_dir": str(tmp_path / "capture"),
                "workspace": WORKSPACE, "user_id": OPERATOR}
    settings.update(slack)
    return {"job": {"key": "atropos"}, "features": {"slack": True},
            "slack": settings, "_base_url": "http://localhost:7100"}


def _folder(tmp_path):
    folder = tmp_path / "capture"
    folder.mkdir(exist_ok=True)
    return folder


def _write(tmp_path, name, lines):
    path = _folder(tmp_path) / name
    with open(path, "a") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
    return path


def _filtered(ts, user, text, thread_ts=None, channel=CHANNEL, **extra):
    record = {"dt": "2026-09-03T19:00:00.000000+00:00", "ts": ts,
              "time": "2026-09-03T19:00:00+00:00", "ws": WORKSPACE,
              "ch": channel, "user": user, "text": text, "src": "ws"}
    if thread_ts:
        record["thread_ts"] = thread_ts
    record.update(extra)
    return record


def _raw(ts, user, text, channel=CHANNEL):
    return {"dt": "2026-09-03T19:00:00.000000+00:00", "source": "ws",
            "endpoint": "https://wss-primary.slack.com/?x=1",
            "payload": {"type": "message", "channel": channel, "ts": ts,
                        "user": user, "text": text, "team": "T019"}}


# --- which log is read -----------------------------------------------------

def test_the_filtered_log_is_read_when_it_is_there(tmp_path):
    _write(tmp_path, "messages.jsonl", [_raw(ROOT_TS, ERIK, "raw")])
    _write(tmp_path, "filtered.jsonl", [_filtered(ROOT_TS, ERIK, "filtered")])

    assert sc.capture_path(_config(tmp_path)).endswith("/filtered.jsonl")


def test_the_raw_log_is_read_when_there_is_no_filtered_log(tmp_path):
    _write(tmp_path, "messages.jsonl", [_raw(ROOT_TS, ERIK, "raw")])

    assert sc.capture_path(_config(tmp_path)).endswith("/messages.jsonl")


def test_the_raw_path_alone_still_names_the_filtered_log(tmp_path):
    """A config that predates messages_dir names the raw file. The filtered
    log lives in the same directory, so it is still the one that is read."""
    _write(tmp_path, "filtered.jsonl", [_filtered(ROOT_TS, ERIK, "filtered")])
    config = {"slack": {"raw_path": str(_folder(tmp_path) / "messages.jsonl"),
                        "workspace": WORKSPACE}}

    assert sc.capture_path(config).endswith("/filtered.jsonl")


def test_the_rotated_siblings_of_the_filtered_log_are_read_too(tmp_path):
    _write(tmp_path, "filtered.jsonl", [_filtered(REPLY_TS, ERIK, "newest")])
    _write(tmp_path, "filtered.jsonl.1", [_filtered(ROOT_TS, ERIK, "older")])
    _write(tmp_path, "messages.jsonl.1", [_raw(ROOT_TS, ERIK, "raw")])

    names = [path.rsplit("/", 1)[-1] for path in sc.capture_files(_config(tmp_path))]
    assert names == ["filtered.jsonl", "filtered.jsonl.1"]


def test_a_directory_with_no_capture_names_nothing():
    assert slack_capture.live_path("", "") == ""


# --- the shape a filtered line reaches the scanners in ---------------------

def test_a_raw_record_is_not_a_filtered_one():
    assert not slack_capture.is_filtered_record(_raw(ROOT_TS, ERIK, "hi"))
    assert slack_capture.is_filtered_record(_filtered(ROOT_TS, ERIK, "hi"))


def test_a_filtered_line_becomes_the_message_it_was_distilled_from():
    record = slack_capture.as_capture_record(
        _filtered(REPLY_TS, ERIK, "move WB-412 to PLT", thread_ts=ROOT_TS,
                  name="Erik"))

    payload = record["payload"]
    assert payload["type"] == "message"
    assert payload["ts"] == REPLY_TS
    assert payload["user"] == ERIK
    assert payload["text"] == "move WB-412 to PLT"
    assert payload["channel"] == CHANNEL
    assert payload["thread_ts"] == ROOT_TS
    assert payload["user_profile"]["real_name"] == "Erik"
    assert record["workspace"] == WORKSPACE


def test_a_top_level_message_carries_no_thread_ts():
    """slack_monitor gathers the messages before a mention differently for a
    reply and for a top level message. A thread_ts equal to the message's own
    ts would make every top level mention look like a reply."""
    payload = slack_capture.as_capture_record(
        _filtered(ROOT_TS, ERIK, "standup in five"))["payload"]

    assert "thread_ts" not in payload


def test_an_edit_keeps_its_text_off_the_wrapper():
    """The raw log puts an edit's text under `message`, never on the record
    itself. slack_monitor reads the record's text and so ignores edits; an
    edit whose text sat on the wrapper would raise a second mention for the
    message it edits."""
    payload = slack_capture.as_capture_record(
        _filtered(REPLY_TS, ERIK, "move WB-412 to PLT today",
                  thread_ts=ROOT_TS, subtype="message_changed",
                  edited=True))["payload"]

    assert payload["subtype"] == "message_changed"
    assert "text" not in payload
    assert payload["message"]["ts"] == REPLY_TS
    assert payload["message"]["text"] == "move WB-412 to PLT today"


def test_a_deletion_names_the_message_it_removed():
    payload = slack_capture.as_capture_record(
        _filtered(REPLY_TS, ERIK, "move WB-412 to PLT", thread_ts=ROOT_TS,
                  subtype="message_deleted", deleted=True))["payload"]

    assert payload["subtype"] == "message_deleted"
    assert payload["deleted_ts"] == REPLY_TS
    assert payload["previous_message"]["thread_ts"] == ROOT_TS
    assert "text" not in payload


def test_a_bot_message_gets_back_the_subtype_that_skips_it():
    """The filter keeps bot messages and records the bot's name instead of the
    bot_message subtype. Both scanners skip that subtype, so without it a bot
    would enter the conversation index as a person."""
    payload = slack_capture.as_capture_record(
        _filtered(ROOT_TS, "B0DEPLOY", "deploy finished", bot="Deploybot"))["payload"]

    assert payload["subtype"] == "bot_message"


def test_an_edited_bot_message_is_still_a_bot_message():
    """An edit is read from the message under the wrapper. A bot marker left
    on the wrapper alone would be dropped there, and the bot would enter the
    conversation index as a person."""
    payload = slack_capture.as_capture_record(
        _filtered(ROOT_TS, "B0DEPLOY", "deploy finished in 4m", bot="Deploybot",
                  subtype="message_changed", edited=True))["payload"]

    assert payload["subtype"] == "message_changed"
    assert payload["message"]["subtype"] == "bot_message"


def test_an_edited_bot_message_opens_no_conversation(tmp_path):
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, "B0DEPLOY", "deploy finished", bot="Deploybot"),
        _filtered(ROOT_TS, "B0DEPLOY", "deploy finished in 4m", bot="Deploybot",
                  subtype="message_changed", edited=True),
    ])

    counts = sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    assert counts["messages"] == 0
    assert _conversations() == []


def test_a_file_record_is_not_a_message():
    record = {"dt": "2026-09-03T19:00:00+00:00", "ts": "1788458400.000100",
              "ws": WORKSPACE, "ch": "", "user": ERIK, "kind": "file",
              "file": {"id": "F1", "name": "plan.pdf"}, "src": "files.info"}

    assert slack_capture.is_filtered_record(record)
    assert slack_capture.as_capture_record(record) is None


def test_a_line_with_no_timestamp_is_not_a_message():
    assert slack_capture.as_capture_record(
        {"ws": WORKSPACE, "ts": "", "text": "hi"}) is None


# --- the user index written beside the filtered log ------------------------

def test_names_come_from_the_index_beside_the_filtered_log(tmp_path):
    """The filter drops the boot payloads that carried Slack's user lists and
    writes users.json instead. Reading it is what replaces the scrape."""
    (_folder(tmp_path) / "users.json").write_text(json.dumps(
        {ERIK: {"name": "Erik Lund", "email": "erik@acme.test"},
         "U0EMPTY": {"email": "nobody@acme.test"}}))

    names = slack_capture.user_names(str(_folder(tmp_path)))
    assert names == {ERIK: "Erik Lund"}


def test_a_broken_user_index_names_nobody(tmp_path):
    (_folder(tmp_path) / "users.json").write_text("{not json")

    assert slack_capture.user_names(str(_folder(tmp_path))) == {}


# --- the conversation index over a filtered capture ------------------------

def _conversations():
    return db.query_all("SELECT * FROM slack_conversations ORDER BY id")


def _messages(conversation_id):
    return db.query_all(
        "SELECT * FROM slack_conversation_messages WHERE conversation_id = ?"
        " AND deleted = 0 ORDER BY ts", (conversation_id,))


def test_a_filtered_thread_lands_in_one_conversation(tmp_path):
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, OPERATOR, "raised WB-412 for the duplicate export"),
        _filtered(REPLY_TS, ERIK, "please move it to the PLT board",
                  thread_ts=ROOT_TS, name="Erik"),
    ])

    counts = sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    rows = _conversations()
    assert len(rows) == 1
    assert rows[0]["thread_ts"] == ROOT_TS
    assert rows[0]["channel_id"] == CHANNEL
    assert counts["messages"] == 2
    assert [m["text"] for m in _messages(rows[0]["id"])] == [
        "raised WB-412 for the duplicate export",
        "please move it to the PLT board"]


def test_a_filtered_edit_rewrites_the_message_it_changes(tmp_path):
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, ERIK, "move WB-412 to PLT")])
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, ERIK, "move WB-412 to PLT and to TRIAGE",
                  subtype="message_changed", edited=True)])

    sc.ingest(_config(tmp_path), instance_key="atropos",
              now=NOW + timedelta(minutes=1))

    rows = _conversations()
    assert len(rows) == 1
    assert [m["text"] for m in _messages(rows[0]["id"])] == [
        "move WB-412 to PLT and to TRIAGE"]


def test_a_filtered_deletion_removes_the_message(tmp_path):
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, ERIK, "move WB-412 to PLT")])
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, ERIK, "move WB-412 to PLT",
                  subtype="message_deleted", deleted=True)])

    sc.ingest(_config(tmp_path), instance_key="atropos",
              now=NOW + timedelta(minutes=1))

    assert _messages(_conversations()[0]["id"]) == []


def test_a_filtered_bot_message_opens_no_conversation(tmp_path):
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, "B0DEPLOY", "deploy finished", bot="Deploybot")])

    counts = sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    assert counts["messages"] == 0
    assert _conversations() == []


def test_a_conversation_name_comes_from_the_index_beside_the_capture(tmp_path):
    """slack_monitor stores the names it resolves in state, and this instance
    has none for Erik. The index slack_int wrote is what names him."""
    (_folder(tmp_path) / "users.json").write_text(
        json.dumps({ERIK: {"name": "Erik Lund", "email": ""}}))
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ROOT_TS, ERIK, "move WB-412 to PLT")])

    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    assert _messages(_conversations()[0]["id"])[0]["user_name"] == "Erik Lund"


# --- the mention scanner over a filtered capture ---------------------------

def _recent_ts(minutes_ago=1):
    when = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    return f"{when.timestamp():.6f}"


def _armed_state(**extra):
    blob = {"user_id": OPERATOR, "last_dt": "2026-01-01T00:00:00+00:00",
            "names": {OPERATOR: "Danial", CHANNEL: "#platform"}}
    blob.update(extra)
    state.save("slack", blob)


def _events(name):
    """The events straight out of the table. log.get_events needs an active
    job key, and this scan runs outside one."""
    return db.query_all(
        "SELECT event, summary, meta FROM log_events WHERE event = ? ORDER BY ts",
        (name,))


def _mention_events():
    return _events("slack_mention_detected")


def test_a_mention_in_the_filtered_log_raises_one_event(tmp_path):
    _armed_state()
    ts = _recent_ts()
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ts, ERIK, f"<@{OPERATOR}> can you look at WB-412", name="Erik")])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    events = _mention_events()
    assert len(events) == 1
    assert "WB-412" in json.loads(events[0]["meta"])["text"]


def test_one_message_captured_twice_raises_one_event(tmp_path):
    """A message reaches the capture live and again in a REST history pull.
    The raw log hid the second copy inside a batch this scan never unpacked.
    The filtered log unpacks it, so the scan has to drop the repeat itself."""
    _armed_state()
    ts = _recent_ts()
    text = f"<@{OPERATOR}> can you look at WB-412"
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ts, ERIK, text, name="Erik"),
        dict(_filtered(ts, ERIK, text, name="Erik"), src="conversations.history"),
    ])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert len(_mention_events()) == 1


def test_an_edit_in_the_filtered_log_raises_no_second_mention(tmp_path):
    _armed_state()
    ts = _recent_ts()
    text = f"<@{OPERATOR}> can you look at WB-412"
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ts, ERIK, text, name="Erik"),
        _filtered(ts, ERIK, text + " today", subtype="message_changed",
                  edited=True, name="Erik"),
    ])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert len(_mention_events()) == 1


def test_a_message_that_names_no_channel_raises_no_event(tmp_path):
    """A message pulled over REST names no channel. An event for it would say
    the mention was in no channel, and it would store a reply context that
    chat.postMessage rejects with channel_not_found. 1595 of the 3934 messages
    in the live filtered archive name no channel."""
    _armed_state()
    _write(tmp_path, "filtered.jsonl", [
        dict(_filtered(_recent_ts(), ERIK, f"<@{OPERATOR}> look at WB-412",
                       name="Erik"), ch="", src="conversations.history")])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert _mention_events() == []
    assert state.load("slack").get("replies", {}) == {}


def test_the_copy_that_names_the_channel_is_the_one_that_is_used(tmp_path):
    """The same message reaches the capture live, naming its channel, and
    again in a REST pull that names none. Exactly one event is raised, and it
    carries the real channel and a reply context that can be replied to."""
    _armed_state()
    ts = _recent_ts()
    text = f"<@{OPERATOR}> can you look at WB-412"
    _write(tmp_path, "filtered.jsonl", [
        dict(_filtered(ts, ERIK, text, name="Erik"), ch="",
             src="conversations.history"),
        _filtered(ts, ERIK, text, name="Erik"),
    ])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    events = _mention_events()
    assert len(events) == 1
    assert json.loads(events[0]["meta"])["channel"] == "#platform"
    assert state.load("slack")["replies"][ts]["channel"] == CHANNEL


def test_a_message_from_another_workspace_is_not_read(tmp_path):
    _armed_state()
    ts = _recent_ts()
    other = _filtered(ts, ERIK, f"<@{OPERATOR}> look at WB-412")
    other["ws"] = "some-other-workspace"
    _write(tmp_path, "filtered.jsonl", [other])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert _mention_events() == []


def test_the_position_of_each_capture_file_is_kept_apart(tmp_path):
    """A position is a byte count in one file. The raw log and the filtered
    log hold different bytes, so a position taken in one names nothing in the
    other. Applied to the filtered log, a raw position lands in the middle of
    it and the messages before that byte are never read.

    The mention here sits at the start of the filtered log, and the carried
    position is the raw log's. A shared position skips the mention. A position
    kept per file has none for the filtered log and reads it from the start."""
    mention = _filtered(_recent_ts(), ERIK,
                        f"<@{OPERATOR}> can you look at WB-412", name="Erik")
    later = _filtered(_recent_ts(), ERIK, "and the deploy is green", name="Erik")
    carried = len(json.dumps(mention)) + 1
    _armed_state(file_offset=carried)
    path = _write(tmp_path, "filtered.jsonl", [mention, later])
    assert carried < path.stat().st_size, "the carried position is inside the file"

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert len(_mention_events()) == 1
    offsets = state.load("slack")["offsets"]
    assert offsets["messages.jsonl"] == carried, "the raw log keeps its own position"
    assert offsets["filtered.jsonl"] == path.stat().st_size


def test_a_second_scan_reads_only_what_arrived_after_the_first(tmp_path):
    _armed_state()
    _write(tmp_path, "filtered.jsonl", [
        _filtered(_recent_ts(minutes_ago=2), ERIK,
                  f"<@{OPERATOR}> can you look at WB-412", name="Erik")])
    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))
    _write(tmp_path, "filtered.jsonl", [
        _filtered(_recent_ts(minutes_ago=1), ERIK,
                  f"<@{OPERATOR}> and WB-500 as well", name="Erik")])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    texts = [json.loads(e["meta"])["text"] for e in _mention_events()]
    assert len(texts) == 2
    assert sum("WB-500" in t for t in texts) == 1


def test_no_operator_id_is_reported_once(tmp_path):
    """Without an id nothing is a mention and nothing is a direct message, so
    the scan reads the whole capture and raises nothing. The filtered log
    drops the boot payloads the id used to be discovered from, so silence
    here would be the whole feature failing quietly."""
    _armed_state(user_id="")
    config = _config(tmp_path, user_id="")
    _write(tmp_path, "filtered.jsonl", [
        _filtered(_recent_ts(), ERIK, "anyone around?", name="Erik")])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(config)
        slack_monitor.check(config)

    assert len(_events("slack_user_id_missing")) == 1


def test_the_configured_operator_id_wins_over_a_stale_stored_one(tmp_path):
    _armed_state(user_id="U0STALE")
    ts = _recent_ts()
    _write(tmp_path, "filtered.jsonl", [
        _filtered(ts, ERIK, f"<@{OPERATOR}> can you look at WB-412", name="Erik")])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert len(_mention_events()) == 1


def test_a_raw_capture_still_raises_its_mentions(tmp_path):
    """slack_int on another host may not write a filtered log yet."""
    _armed_state()
    _write(tmp_path, "messages.jsonl", [
        _raw(_recent_ts(), ERIK, f"<@{OPERATOR}> can you look at WB-412")])

    with patch.object(slack_monitor, "run_haiku", return_value="REPLY"):
        slack_monitor.check(_config(tmp_path))

    assert len(_mention_events()) == 1
