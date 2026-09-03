"""Tests for features.slack_conversations — the Slack conversation index and
the proposals it opens.

The model call is patched everywhere. These tests assert which conversation is
built from the capture and what decides whether a proposal is opened, never
that a real model answered."""
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import core.config as cfg
import core.db as db
import core.state as state
from features import slack_conversations as sc
from services import work_store

NOW = datetime(2026, 9, 3, 20, 0, tzinfo=timezone.utc)
OPERATOR = "U0OPERATOR"
ERIK = "U0ERIK"
CHANNEL = "C0PLATFORM"
ROOT_TS = "1788458400.000100"


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "atropos"
    state._instance_key_cv.set("atropos")
    state.save("slack", {"user_id": OPERATOR,
                         "names": {OPERATOR: "Danial", ERIK: "Erik",
                                   CHANNEL: "#platform"}})
    yield


def _config(tmp_path, **slack):
    settings = {"messages_dir": str(tmp_path / "capture"),
                "workspace": "atropos-workspace", "user_id": OPERATOR}
    settings.update(slack)
    return {"job": {"key": "atropos"}, "features": {"slack": True},
            "slack": settings, "_base_url": "http://localhost:7100"}


def _capture(tmp_path, lines):
    folder = tmp_path / "capture"
    folder.mkdir(exist_ok=True)
    path = folder / "messages.jsonl"
    with open(path, "a") as f:
        for line in lines:
            f.write(json.dumps(line) + "\n")
    return path


def _ws(ts, user, text, thread_ts=None, channel=CHANNEL):
    payload = {"type": "message", "channel": channel, "ts": ts,
               "user": user, "text": text, "team": "T019"}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    return {"dt": "2026-09-03T19:00:00+00:00", "source": "ws",
            "endpoint": "https://wss-primary.slack.com/?x=1", "payload": payload}


def _rest(messages):
    return {"dt": "2026-09-03T19:00:00+00:00", "source": "rest",
            "endpoint": "https://atropos-workspace.slack.com/api/conversations.replies?x=1",
            "payload": {"ok": True, "messages": messages}}


def _erik_thread():
    """The conversation that motivated this feature, as the capture holds it:
    the operator opens a thread, the REST pull carries Erik's reply with no
    channel field of its own."""
    return [
        _ws(ROOT_TS, OPERATOR, "raised WB-412 for the duplicate cohort export"),
        _rest([
            {"type": "message", "ts": ROOT_TS, "user": OPERATOR,
             "text": "raised WB-412 for the duplicate cohort export",
             "thread_ts": ROOT_TS},
            {"type": "message", "ts": "1788458500.000200", "user": ERIK,
             "text": "Please move this ticket to the PLT board, no one is using "
                     "the WB board anymore and it will get lost. Please assign it "
                     "to the TRIAGE sprint so we can scope and schedule it.",
             "thread_ts": ROOT_TS},
        ]),
    ]


def _conversations():
    return db.query_all("SELECT * FROM slack_conversations ORDER BY id")


def _messages(conversation_id):
    return db.query_all(
        "SELECT * FROM slack_conversation_messages WHERE conversation_id = ?"
        " ORDER BY ts", (conversation_id,))


# --- the index -------------------------------------------------------------

def test_thread_replies_land_in_one_conversation(tmp_path):
    _capture(tmp_path, _erik_thread())
    counts = sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    rows = _conversations()
    assert len(rows) == 1, "the root and its reply are one conversation"
    assert rows[0]["thread_ts"] == ROOT_TS
    assert rows[0]["channel_id"] == CHANNEL, "the channel comes from the ws record"
    assert rows[0]["message_count"] == 2
    assert counts["messages"] == 2
    texts = [m["text"] for m in _messages(rows[0]["id"])]
    assert "PLT board" in texts[1]


def test_two_top_level_messages_are_two_conversations(tmp_path):
    _capture(tmp_path, [
        _ws("1788458400.000100", ERIK, "standup in five"),
        _ws("1788700500.000100", ERIK, "unrelated: the deploy is green"),
    ])
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)
    assert len(_conversations()) == 2


def _rewind(offset=0):
    blob = state.load(sc.STATE_MODULE)
    blob["offsets"] = {key: offset for key in blob.get("offsets", {})}
    state.save(sc.STATE_MODULE, blob)


def test_reingest_writes_nothing_new(tmp_path):
    _capture(tmp_path, _erik_thread())
    config = _config(tmp_path)
    sc.ingest(config, instance_key="atropos", now=NOW)
    before = _conversations()[0]
    _rewind()

    second = sc.ingest(config, instance_key="atropos", now=NOW)
    assert second["messages"] == 0, "a message row is keyed by (conversation, ts)"
    after = _conversations()[0]
    assert after["message_count"] == 2
    assert after["updated_at"] == before["updated_at"], (
        "a re-read that changes nothing must not touch the row")


def test_channel_noise_is_skipped(tmp_path):
    _capture(tmp_path, [
        {"dt": "x", "source": "ws", "endpoint": "e",
         "payload": {"type": "message", "subtype": "channel_join",
                     "channel": CHANNEL, "ts": "1788458400.000100",
                     "user": ERIK, "text": "has joined the channel"}},
        {"dt": "x", "source": "ws", "endpoint": "e", "payload": {"type": "ping", "id": 1}},
    ])
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)
    assert _conversations() == []


def test_operator_involvement_is_recorded(tmp_path):
    _capture(tmp_path, [
        _ws("1788458400.000100", ERIK, "who owns the exporter?"),
        _ws("1788458500.000100", ERIK, f"<@{OPERATOR}> can you look at the exporter?"),
    ])
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)
    involved = {r["thread_ts"]: r["involves_operator"] for r in _conversations()}
    assert involved["1788458400.000100"] == 0
    assert involved["1788458500.000100"] == 1


# --- the proposal ----------------------------------------------------------

def _verdict(actionable=True, objective="Move WB-412 to the PLT board and assign "
                                        "it to the TRIAGE sprint.",
             reason="Erik asked for the board move"):
    return json.dumps({"actionable": actionable, "reason": reason,
                       "objective": objective if actionable else ""})


def _run(tmp_path, verdict, now=None, **slack):
    config = _config(tmp_path, propose_tasks=True, **slack)
    with patch.object(sc, "run_haiku", return_value=verdict) as haiku, \
         patch.object(sc.work_launch, "project_entries", return_value=[]):
        opened = sc.check(config, instance_key="atropos", now=now or NOW)
    return opened, haiku


def test_a_settled_request_opens_a_proposal(tmp_path):
    _capture(tmp_path, _erik_thread())
    opened, _ = _run(tmp_path, _verdict())

    assert opened["proposed"] == 1
    item = db.query_one("SELECT * FROM work_items ORDER BY id DESC LIMIT 1")
    assert item["state"] == work_store.PROPOSED_STATE
    assert "WB-412" in item["objective"]
    assert item["contexts"] == "atropos,slack"
    assert "PLT board" in item["launch_brief"], "the brief carries the thread"
    assert "#platform" in item["current_checkpoint"]
    conversation = _conversations()[0]
    assert conversation["work_item_id"] == item["id"]
    assert conversation["proposed_at"]


def test_the_same_capture_judged_as_chatter_opens_nothing(tmp_path):
    """The oracle for the test above: identical input, a false verdict, and
    no task appears. Without this the assertion above would pass on any code
    that opened a task unconditionally."""
    _capture(tmp_path, _erik_thread())
    opened, _ = _run(tmp_path, _verdict(actionable=False))

    assert opened["proposed"] == 0
    assert db.query_all("SELECT id FROM work_items") == []
    assert _conversations()[0]["judged_ts"] == "1788458500.000200"


def test_a_conversation_still_moving_is_not_judged(tmp_path):
    _capture(tmp_path, _erik_thread())
    just_now = datetime.fromtimestamp(1788458500.0002, tz=timezone.utc) + timedelta(minutes=1)
    opened, haiku = _run(tmp_path, _verdict(), now=just_now)

    assert haiku.call_count == 0, "the settle window has not passed"
    assert opened["proposed"] == 0


def test_an_ancient_conversation_is_not_judged(tmp_path):
    _capture(tmp_path, _erik_thread())
    much_later = datetime.fromtimestamp(1788458500.0002, tz=timezone.utc) + timedelta(days=5)
    _, haiku = _run(tmp_path, _verdict(), now=much_later)
    assert haiku.call_count == 0


def test_a_conversation_without_the_operator_is_not_judged(tmp_path):
    _capture(tmp_path, [
        _ws(ROOT_TS, ERIK, "Please move WB-412 to the PLT board."),
    ])
    _, haiku = _run(tmp_path, _verdict())
    assert haiku.call_count == 0, "nobody asked the operator"


def test_a_proposal_is_never_opened_twice_for_one_conversation(tmp_path):
    _capture(tmp_path, _erik_thread())
    _run(tmp_path, _verdict())
    opened, haiku = _run(tmp_path, _verdict())

    assert haiku.call_count == 0
    assert opened["proposed"] == 0
    assert len(db.query_all("SELECT id FROM work_items")) == 1


def test_the_daily_budget_caps_proposals(tmp_path):
    _capture(tmp_path, [
        _ws("1788458400.000100", OPERATOR, "first thing"),
        _ws("1788458500.000100", OPERATOR, "second thing"),
        _ws("1788458600.000100", OPERATOR, "third thing"),
    ])
    opened, _ = _run(tmp_path, _verdict(), propose_max_per_day=2)
    assert opened["proposed"] == 2
    assert len(db.query_all("SELECT id FROM work_items")) == 2


def test_indexing_runs_with_proposals_switched_off(tmp_path):
    _capture(tmp_path, _erik_thread())
    config = _config(tmp_path)
    with patch.object(sc, "run_haiku") as haiku:
        out = sc.check(config, instance_key="atropos", now=NOW)
    assert haiku.call_count == 0
    assert out["messages"] == 2
    assert out["proposed"] == 0
    assert len(_conversations()) == 1


def test_a_model_that_answers_nothing_leaves_the_conversation_unjudged(tmp_path):
    _capture(tmp_path, _erik_thread())
    _run(tmp_path, "")
    assert _conversations()[0]["judged_ts"] == "", "the judgement never happened"


# --- config ----------------------------------------------------------------

def test_messages_dir_supplies_raw_path():
    slack = {"messages_dir": "/srv/slack/messages/atropos"}
    cfg._resolve_slack_paths(slack)
    assert slack["raw_path"] == "/srv/slack/messages/atropos/messages.jsonl"


def test_raw_path_supplies_messages_dir():
    slack = {"raw_path": "/srv/slack/messages/atropos/messages.jsonl"}
    cfg._resolve_slack_paths(slack)
    assert slack["messages_dir"] == "/srv/slack/messages/atropos"


def test_an_empty_slack_block_stays_empty():
    slack = {}
    cfg._resolve_slack_paths(slack)
    assert slack == {}


# --- the capture reader ----------------------------------------------------

def test_a_half_written_last_line_is_read_once_it_is_finished(tmp_path):
    """The writer appends. Consuming a partial line and saving the offset past
    it would lose that message for good."""
    folder = tmp_path / "capture"
    folder.mkdir()
    path = folder / "messages.jsonl"
    whole = json.dumps(_ws(ROOT_TS, OPERATOR, "the whole message"))
    path.write_text(whole[:40])
    config = _config(tmp_path)

    first = sc.ingest(config, instance_key="atropos", now=NOW)
    assert first["messages"] == 0
    with open(path, "w") as f:
        f.write(whole + "\n")

    second = sc.ingest(config, instance_key="atropos", now=NOW)
    assert second["messages"] == 1
    assert [m["text"] for m in _messages(_conversations()[0]["id"])] == ["the whole message"]


def test_a_rotated_capture_is_finished_before_the_new_one(tmp_path):
    """slack_int renames messages.jsonl to messages.jsonl.1 and starts a new
    file. Everything written to the old file after the last scan must still
    be read."""
    folder = tmp_path / "capture"
    folder.mkdir()
    live = folder / "messages.jsonl"
    live.write_text(json.dumps(_ws(ROOT_TS, OPERATOR, "before the scan")) + "\n")
    config = _config(tmp_path)
    sc.ingest(config, instance_key="atropos", now=NOW)

    with open(live, "a") as f:
        f.write(json.dumps(_ws("1788458450.000100", OPERATOR, "after the scan")) + "\n")
    live.rename(folder / "messages.jsonl.1")
    live.write_text(json.dumps(_ws("1788458500.000100", OPERATOR, "in the new file")) + "\n")

    sc.ingest(config, instance_key="atropos", now=NOW)
    texts = sorted(r["thread_ts"] for r in _conversations())
    assert texts == ["1788458400.000100", "1788458450.000100", "1788458500.000100"]


def test_a_capture_line_that_is_not_an_object_does_not_wedge_the_scan(tmp_path):
    """A valid json line that is not a record used to raise inside the write
    transaction, so the offset was never saved and every later scan crashed on
    the same line."""
    folder = tmp_path / "capture"
    folder.mkdir()
    (folder / "messages.jsonl").write_text(
        "[]\n" + json.dumps(_ws(ROOT_TS, OPERATOR, "still read")) + "\n")

    counts = sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)
    assert counts["messages"] == 1


def test_an_edit_rewrites_the_message_it_edits(tmp_path):
    _capture(tmp_path, [_ws(ROOT_TS, OPERATOR, "please delete production")])
    config = _config(tmp_path)
    sc.ingest(config, instance_key="atropos", now=NOW)

    _capture(tmp_path, [{
        "dt": "x", "source": "ws", "endpoint": "e",
        "payload": {"type": "message", "subtype": "message_changed",
                    "channel": CHANNEL,
                    "message": {"type": "message", "ts": ROOT_TS, "user": OPERATOR,
                                "text": "never mind, ignore that"}}}])
    sc.ingest(config, instance_key="atropos", now=NOW)

    assert [m["text"] for m in _messages(_conversations()[0]["id"])] == [
        "never mind, ignore that"]


def test_a_deleted_message_leaves_the_index(tmp_path):
    _capture(tmp_path, [_ws(ROOT_TS, OPERATOR, "please delete production")])
    config = _config(tmp_path)
    sc.ingest(config, instance_key="atropos", now=NOW)

    _capture(tmp_path, [{
        "dt": "x", "source": "ws", "endpoint": "e",
        "payload": {"type": "message", "subtype": "message_deleted",
                    "channel": CHANNEL, "deleted_ts": ROOT_TS}}])
    sc.ingest(config, instance_key="atropos", now=NOW)

    assert _messages(_conversations()[0]["id"]) == []


def test_a_dm_seen_only_through_a_rest_pull_still_counts(tmp_path):
    """A REST batch carries no channel on the message. The endpoint names it,
    and a direct message is addressed to the operator whoever wrote it."""
    _capture(tmp_path, [{
        "dt": "x", "source": "rest",
        "endpoint": "https://atropos-workspace.slack.com/api/conversations.history"
                    "?channel=D0ERIK&limit=28",
        "payload": {"ok": True, "messages": [
            {"type": "message", "ts": ROOT_TS, "user": ERIK,
             "text": "can you move WB-412 to the PLT board?"}]}}])
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    row = _conversations()[0]
    assert row["channel_id"] == "D0ERIK"
    assert row["involves_operator"] == 1


def test_a_long_thread_keeps_its_root(tmp_path):
    """The root names the ticket the rest of the thread calls "this"."""
    lines = [_ws(ROOT_TS, OPERATOR, "raised WB-412 for the cohort export")]
    for i in range(sc.MAX_TRANSCRIPT_MESSAGES + 5):
        lines.append(_ws(f"17884585{i:02d}.000100", ERIK, f"reply {i}",
                         thread_ts=ROOT_TS))
    _capture(tmp_path, lines)
    sc.ingest(_config(tmp_path), instance_key="atropos", now=NOW)

    transcript, _ = sc._transcript(_conversations()[0]["id"], {})
    assert "raised WB-412 for the cohort export" in transcript
    assert transcript.count("\n") + 1 == sc.MAX_TRANSCRIPT_MESSAGES
