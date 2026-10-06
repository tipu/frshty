"""Tests for features.direct_inbox — the follow-up tasks proposed from the
operator's direct emails and texts.

The Gmail connector run, the gvoice CLI and the text judge are patched
everywhere. These tests assert what reaches the models, what is recorded and
what opens a task, never that Gmail or Google Voice answered.
"""
import json
import subprocess
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import core.db as db
import core.log as log
import core.state as state
from core.tasks.routes import _cron_routes
from features import direct_inbox as di
from services import work_store

NOW = datetime(2026, 10, 6, 18, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    log.init(tmp_path, "personal")
    with patch.object(di, "_cwd_for", return_value=""):
        yield


def _config(**settings):
    return {"job": {"key": "personal"}, "features": {"direct_inbox": True},
            "direct_inbox": {"operator_name": "Danial Jaffry", **settings}}


def _email(thread_id="t1", last_at="2026-10-06T17:00:00Z", needs_reply=True):
    return {"thread_id": thread_id, "last_at": last_at,
            "from": "Ada Lovelace <ada@example.com>", "subject": "Invoice",
            "needs_reply": needs_reply, "reason": "Ada asks for the invoice.",
            "objective": "Send Ada the September invoice.",
            "summary": "Ada asks for the September invoice."}


def _texts(*messages):
    return SimpleNamespace(returncode=0, stderr="", stdout=json.dumps(
        {"count": len(messages), "messages": list(messages)}))


def _text(direction, minutes_ago, participant="Bob", text="are you free?"):
    return {"direction": direction, "participant": participant, "text": text,
            "thread": 1,
            "timestamp": (NOW - timedelta(minutes=minutes_ago)).isoformat()}


def _run(config=None, emails=None, gvoice=None, judge=None, now=NOW):
    config = config if config is not None else _config()
    gmail_out = json.dumps({"threads": emails or []})
    gvoice_result = gvoice if gvoice is not None else _texts()
    judge_out = json.dumps(judge) if judge is not None else None
    with patch.object(di, "run_agentic", return_value=gmail_out) as agent, \
            patch.object(di.subprocess, "run", return_value=gvoice_result) as cli, \
            patch.object(di, "run_haiku", return_value=judge_out) as haiku:
        report = di.check(config, instance_key="personal", now=now)
    return report, agent, cli, haiku


def _proposals():
    return db.query_all("SELECT * FROM work_items WHERE scope = 'proposal'"
                        " ORDER BY id")


def test_an_email_that_waits_opens_one_proposal():
    report, _, _, _ = _run(emails=[_email()])
    rows = _proposals()
    assert report["proposed"] == 1
    assert len(rows) == 1
    assert rows[0]["state"] == work_store.PROPOSED_STATE
    assert rows[0]["objective"] == ("Follow up with Ada Lovelace <ada@example.com>"
                                    " on email: Send Ada the September invoice.")
    assert "personal,followup" == rows[0]["contexts"]
    assert "Gmail thread t1" in rows[0]["launch_brief"]


def test_the_gmail_run_may_only_read():
    with patch.object(di, "run_agentic", return_value='{"threads": []}') as agent:
        di.read_emails(_config(), "personal", NOW)
    kwargs = agent.call_args.kwargs
    assert all(t.startswith("mcp__claude_ai_Gmail__") for t in kwargs["tools"])
    for verb in ("send_message", "reply", "forward", "create_draft", "trash_thread"):
        assert f"mcp__claude_ai_Gmail__{verb}" in kwargs["denied_tools"]
        assert f"mcp__claude_ai_Gmail__{verb}" not in kwargs["tools"]
    assert "Bash" in kwargs["denied_tools"]


def test_the_same_email_message_opens_no_second_task():
    _run(emails=[_email()])
    report, _, _, _ = _run(emails=[_email()], now=NOW + timedelta(hours=1))
    assert report["proposed"] == 0
    assert len(_proposals()) == 1


def test_a_closed_task_does_not_reopen_for_the_same_message():
    _run(emails=[_email()])
    db.execute("UPDATE work_items SET state = ?", (work_store.CANCELED_STATE,))
    report, _, _, _ = _run(emails=[_email()], now=NOW + timedelta(hours=1))
    assert report["proposed"] == 0
    assert len(_proposals()) == 1


def test_a_new_message_waits_while_the_earlier_task_is_open():
    _run(emails=[_email()])
    later = _email(last_at="2026-10-06T18:30:00Z")
    report, _, _, _ = _run(emails=[later], now=NOW + timedelta(hours=1))
    assert report["proposed"] == 0
    first = _proposals()[0]["id"]
    db.execute("UPDATE work_items SET state = ? WHERE id = ?",
               (work_store.CANCELED_STATE, first))
    report, _, _, _ = _run(emails=[later], now=NOW + timedelta(hours=2))
    assert report["proposed"] == 1
    assert len(_proposals()) == 2


def test_an_email_that_asks_nothing_opens_nothing():
    report, _, _, _ = _run(emails=[_email(needs_reply=False)])
    assert report["proposed"] == 0
    assert _proposals() == []
    assert db.query_one("SELECT needs_reply FROM direct_followups")["needs_reply"] == 0


def test_a_text_that_ends_on_the_other_person_reaches_the_judge_and_opens_a_task():
    texts = _texts(_text("outgoing", 60, text="call you later"),
                   _text("incoming", 10, text="are you free at 5?"))
    judge = {"conversations": [{"index": 1, "needs_reply": True,
                                "reason": "Bob asks if the operator is free at 5.",
                                "objective": "Tell Bob whether 5pm works."}]}
    report, _, cli, haiku = _run(config=_config(gmail=False), gvoice=texts, judge=judge)
    assert cli.call_args.args[0][:3] == ["gvoice", "recent", "72h"]
    assert "are you free at 5?" in haiku.call_args.args[0]
    assert report["proposed"] == 1
    row = _proposals()[0]
    assert row["objective"] == "Follow up with Bob on text message: Tell Bob whether 5pm works."
    assert "them: are you free at 5?" in row["launch_brief"]


def test_a_text_the_operator_already_answered_never_reaches_the_judge():
    texts = _texts(_text("incoming", 60), _text("outgoing", 10, text="yes"))
    report, _, _, haiku = _run(config=_config(gmail=False), gvoice=texts)
    haiku.assert_not_called()
    assert report["proposed"] == 0


def test_a_text_already_judged_is_not_judged_again():
    texts = _texts(_text("incoming", 10))
    judge = {"conversations": [{"index": 1, "needs_reply": False,
                                "reason": "nothing asked", "objective": ""}]}
    _run(config=_config(gmail=False), gvoice=texts, judge=judge)
    _, _, _, haiku = _run(config=_config(gmail=False), gvoice=texts,
                          now=NOW + timedelta(hours=1))
    haiku.assert_not_called()


def test_a_missing_gvoice_is_reported_and_email_still_runs():
    with patch.object(di, "run_agentic", return_value=json.dumps({"threads": [_email()]})), \
            patch.object(di.subprocess, "run", side_effect=FileNotFoundError), \
            patch.object(di.log, "emit") as emit:
        report = di.check(_config(), instance_key="personal", now=NOW)
    assert report["proposed"] == 1
    assert report["errors"] == ["the Google Voice CLI `gvoice` is not installed"]
    assert "direct_inbox_gvoice_failed" in [c.args[0] for c in emit.call_args_list]


def test_a_signed_out_gvoice_names_the_login():
    signed_out = SimpleNamespace(returncode=2, stdout="", stderr="Not signed in")
    report, _, _, _ = _run(config=_config(gmail=False), gvoice=signed_out)
    assert "instance.py gvoice-login" in report["errors"][0]


def test_a_failed_recent_asks_status_whether_the_session_is_signed_out():
    crashed = SimpleNamespace(returncode=1, stdout="", stderr="browser has been closed")
    status = SimpleNamespace(returncode=2, stdout="", stderr="Not signed in")
    config = _config(gmail=False, gvoice_profile_dir="/state/gvoice-profile",
                     gvoice_chrome_channel="chrome")
    with patch.object(di.subprocess, "run", side_effect=[crashed, status]) as cli:
        report = di.check(config, instance_key="personal", now=NOW)
    assert "gvoice-login <config>` on the host to sign /state/gvoice-profile in" in report["errors"][0]
    assert cli.call_args_list[1].args[0][1:] == ["status", "--json"]
    env = cli.call_args_list[0].kwargs["env"]
    assert env["GVOICE_PROFILE_DIR"] == "/state/gvoice-profile"
    assert env["GVOICE_CHROME_CHANNEL"] == "chrome"


def test_a_failed_recent_with_a_live_session_names_no_login():
    crashed = SimpleNamespace(returncode=1, stdout="", stderr="selector changed")
    status = SimpleNamespace(returncode=0, stdout="{}", stderr="")
    with patch.object(di.subprocess, "run", side_effect=[crashed, status]):
        report = di.check(_config(gmail=False), instance_key="personal", now=NOW)
    assert "gvoice-login" not in report["errors"][0]
    assert "selector changed" in report["errors"][0]


def test_a_gvoice_timeout_is_reported():
    with patch.object(di.subprocess, "run",
                      side_effect=subprocess.TimeoutExpired("gvoice", 1)):
        report = di.check(_config(gmail=False), instance_key="personal", now=NOW)
    assert report["errors"] and "ran past" in report["errors"][0]


def test_an_empty_gmail_answer_is_reported():
    with patch.object(di, "run_agentic", return_value=None), \
            patch.object(di.log, "emit") as emit:
        report = di.check(_config(texts=False), instance_key="personal", now=NOW)
    assert report["errors"] == ["the Gmail connector run returned no thread list"]
    assert "direct_inbox_gmail_failed" in [c.args[0] for c in emit.call_args_list]


def test_the_pending_cap_holds_the_rest_back():
    emails = [_email(thread_id=f"t{i}") for i in range(3)]
    report, _, _, _ = _run(config=_config(texts=False, max_pending=2), emails=emails)
    assert report["proposed"] == 2
    assert db.query_one("SELECT COUNT(*) AS n FROM direct_followups")["n"] == 3
    first = _proposals()[0]["id"]
    db.execute("UPDATE work_items SET state = ? WHERE id = ?",
               (work_store.CANCELED_STATE, first))
    report, _, _, _ = _run(config=_config(texts=False, max_pending=2), emails=[],
                           now=NOW + timedelta(hours=1))
    assert report["proposed"] == 1
    assert len(_proposals()) == 3


def test_a_held_back_message_opens_its_task_after_it_leaves_the_window():
    _run(emails=[_email()])
    later = _email(last_at="2026-10-06T18:30:00Z")
    _run(emails=[later], now=NOW + timedelta(hours=1))
    assert len(_proposals()) == 1
    db.execute("UPDATE work_items SET state = ?", (work_store.CANCELED_STATE,))
    report, _, _, _ = _run(emails=[], now=NOW + timedelta(days=5))
    assert report["proposed"] == 1
    assert "2026-10-06T18:30:00+00:00" in _proposals()[1]["launch_brief"]


def test_a_reply_by_the_operator_supersedes_a_held_back_message():
    texts = _texts(_text("incoming", 30, text="invoice?"))
    judge = {"conversations": [{"index": 1, "needs_reply": True,
                                "reason": "Bob asks for the invoice.",
                                "objective": "Send Bob the invoice."}]}
    _run(config=_config(gmail=False, max_pending=0), gvoice=texts, judge=judge)
    assert _proposals() == []
    answered = _texts(_text("incoming", 30, text="invoice?"),
                      _text("outgoing", 5, text="sent"))
    report, _, _, haiku = _run(config=_config(gmail=False), gvoice=answered,
                               now=NOW + timedelta(hours=1))
    haiku.assert_not_called()
    assert report["proposed"] == 0
    assert _proposals() == []


def test_a_reply_to_one_of_two_people_of_one_name_hides_nothing():
    a = dict(_text("incoming", 30, participant="Alex", text="invoice?"), thread=1)
    b = dict(_text("outgoing", 5, participant="Alex", text="see you"), thread=2)
    judge = {"conversations": [{"index": 1, "needs_reply": True,
                                "reason": "Alex asks for the invoice.",
                                "objective": "Send Alex the invoice."}]}
    report, _, _, haiku = _run(config=_config(gmail=False), gvoice=_texts(a, b),
                               judge=judge)
    assert "invoice?" in haiku.call_args.args[0]
    assert "see you" not in haiku.call_args.args[0]
    assert report["proposed"] == 1
    keys = {r["thread_key"] for r in db.query_all("SELECT thread_key FROM direct_followups")}
    assert keys == {"Alex"}


def test_a_nameless_text_key_holds_when_the_threads_move():
    judge = {"conversations": [{"index": 1, "needs_reply": True,
                                "reason": "someone asks for a call",
                                "objective": "Call back."}]}
    lone = dict(_text("incoming", 30, participant=None, text="call me"), thread=1)
    _run(config=_config(gmail=False), gvoice=_texts(lone), judge=judge)
    report, _, _, _ = _run(config=_config(gmail=False),
                           gvoice=_texts(dict(lone, thread=3)), judge=judge,
                           now=NOW + timedelta(hours=1))
    assert report["proposed"] == 0
    assert len(_proposals()) == 1


def test_a_text_key_holds_when_the_threads_move():
    judge = {"conversations": [{"index": 1, "needs_reply": True,
                                "reason": "Alex asks for the invoice.",
                                "objective": "Send Alex the invoice."}]}
    alex = dict(_text("incoming", 30, participant="Alex", text="invoice?"), thread=1)
    _run(config=_config(gmail=False), gvoice=_texts(alex), judge=judge)
    moved = dict(alex, thread=2)
    other = dict(_text("outgoing", 5, participant="Alex", text="see you"), thread=1)
    report, _, _, _ = _run(config=_config(gmail=False), gvoice=_texts(moved, other),
                           judge=judge, now=NOW + timedelta(hours=1))
    assert report["proposed"] == 0
    assert len(_proposals()) == 1


def test_a_gvoice_that_cannot_start_keeps_the_email_verdicts():
    with patch.object(di, "run_agentic", return_value=json.dumps({"threads": [_email()]})), \
            patch.object(di.subprocess, "run", side_effect=PermissionError("denied")):
        report = di.check(_config(), instance_key="personal", now=NOW)
    assert report["proposed"] == 1
    assert "could not start" in report["errors"][0]


@pytest.mark.parametrize("llm", [
    {"provider": "opencode"},
    {"provider": "claude", "claude": {"args": ["--dangerously-skip-permissions"]}},
])
def test_gmail_is_not_read_by_an_unconfined_model_run(llm):
    config = dict(_config(texts=False), llm=llm)
    with patch.object(di, "run_agentic") as agent:
        report = di.check(config, instance_key="personal", now=NOW)
    agent.assert_not_called()
    assert "read only" in report["errors"][0]


def test_a_scan_inside_the_interval_reads_nothing():
    _run(emails=[])
    report, agent, cli, _ = _run(emails=[], now=NOW + timedelta(minutes=5))
    assert report["skipped"] == "interval has not passed"
    agent.assert_not_called()
    cli.assert_not_called()


def test_the_feature_flag_routes_the_scan():
    config = _config()
    registries = {"personal": SimpleNamespace(config=config)}
    tasks = [j["task"] for j in _cron_routes({"instance_key": "personal"}, registries)]
    assert "direct_inbox_scan" in tasks
    config["features"]["direct_inbox"] = False
    tasks = [j["task"] for j in _cron_routes({"instance_key": "personal"}, registries)]
    assert "direct_inbox_scan" not in tasks
