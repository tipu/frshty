"""The board wakes an agent whose background-task wait outlived its snooze."""
import json
from datetime import datetime, timedelta, timezone

import pytest

import core.db as db
from services import work_store


def _transcript(tmp_path, task_id="b123"):
    path = tmp_path / "t.jsonl"
    lines = [
        {"type": "user", "message": {"content": [{"type": "tool_result",
         "content": f"Command running in background with ID: {task_id}"}]}},
        {"type": "assistant", "message": {"content": [
            {"type": "text", "text": "Waiting on the review."}]}},
    ]
    path.write_text("".join(json.dumps(line) + "\n" for line in lines))
    return str(path)


def _parked(tmp_path):
    item_id = work_store.create_item("bg wait item")
    sid = f"sid-bg-wait-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    work_store.record_event(sid, "Stop", {"transcript_path": _transcript(tmp_path),
                                          "last_assistant_message": "Waiting on the review."})
    return item_id


def _expire(item_id):
    past = (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat()
    db.execute("UPDATE work_items SET snoozed_until = ? WHERE id = ?", (past, item_id))


def _item(item_id):
    return db.query_one("SELECT state, stop_reason, snoozed_until FROM work_items "
                        "WHERE id = ?", (item_id,))


def _events(item_id, kind):
    return [json.loads(r["payload"]) for r in db.query_all(
        "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = ? ORDER BY id",
        (item_id, kind))]


@pytest.fixture
def board(monkeypatch):
    state = {"sent": [], "send_ok": True, "running": True}
    monkeypatch.setattr(work_store, "tmux_send",
                        lambda key, text: state["sent"].append((key, text)) or state["send_ok"])
    monkeypatch.setattr(work_store, "agent_running", lambda key, agent="claude": state["running"])
    return state


def _sent(board, item_id):
    return [text for key, text in board["sent"] if key == f"work-{item_id}"]


def _sweep(item_id):
    return [a for a in work_store.sweep_background_waits() if a["id"] == item_id]


class TestSweepBackgroundWaits:
    def test_a_stop_on_a_background_task_parks_the_item_for_thirty_minutes(self, tmp_path):
        item_id = _parked(tmp_path)
        item = _item(item_id)
        assert item["state"] == "waiting_external"
        assert item["stop_reason"].startswith(work_store.BG_WAIT_REASON)
        left = datetime.fromisoformat(item["snoozed_until"]) - datetime.now(timezone.utc)
        assert timedelta(minutes=29) < left <= timedelta(minutes=30)

    def test_an_expired_wait_asks_the_agent_to_check_its_tasks(self, tmp_path, board):
        item_id = _parked(tmp_path)
        _expire(item_id)
        actions = _sweep(item_id)
        assert [a["tasks"] for a in actions] == [["b123"]]
        sent = _sent(board, item_id)
        assert len(sent) == 1 and "b123" in sent[0]
        item = _item(item_id)
        assert item["state"] == "agent_working"
        assert item["snoozed_until"] is None
        assert [e["tasks"] for e in _events(item_id, "bg_wait_nudged")] == [["b123"]]
        assert _sweep(item_id) == []

    def test_a_wait_inside_its_snooze_is_left_alone(self, tmp_path, board):
        item_id = _parked(tmp_path)
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []
        assert _item(item_id)["state"] == "waiting_external"

    def test_a_gone_agent_is_left_for_the_operator(self, tmp_path, board):
        board["running"] = False
        item_id = _parked(tmp_path)
        _expire(item_id)
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []
        assert _item(item_id)["state"] == "waiting_external"

    def test_a_run_past_its_budget_is_left_for_the_operator(self, tmp_path, board):
        item_id = _parked(tmp_path)
        _expire(item_id)
        started = (datetime.now(timezone.utc)
                   - timedelta(minutes=work_store.RUN_BUDGET_MINUTES + 5)).isoformat()
        db.execute("UPDATE work_runs SET started_at = ? WHERE work_item_id = ?",
                   (started, item_id))
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []

    def test_a_failed_send_is_tried_again_on_the_next_scan(self, tmp_path, board):
        board["send_ok"] = False
        item_id = _parked(tmp_path)
        _expire(item_id)
        assert _sweep(item_id) == []
        assert _item(item_id)["state"] == "waiting_external"
        assert _events(item_id, "bg_wait_nudged") == []
        board["send_ok"] = True
        assert len(_sweep(item_id)) == 1

    def test_a_ci_wait_is_not_nudged(self, tmp_path, board):
        item_id = work_store.create_item("ci wait item")
        sid = f"sid-bg-ci-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
        assert work_store.maybe_autocontinue(
            sid, "", tail="Pushed.\nwaiting on PR #42 checks") == "ci_wait"
        _expire(item_id)
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []


class TestMissedDecisionRetry:
    def test_a_recorded_stop_decision_is_not_run_again(self, board):
        item_id = work_store.create_item("budget spent item")
        sid = f"sid-bg-budget-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
        started = (datetime.now(timezone.utc)
                   - timedelta(minutes=work_store.RUN_BUDGET_MINUTES + 5)).isoformat()
        db.execute("UPDATE work_runs SET started_at = ? WHERE work_item_id = ?",
                   (started, item_id))
        work_store.record_event(sid, "Stop", {"last_assistant_message": "Working."})
        assert work_store.maybe_autocontinue(sid, "", tail="Working.") == "continued"
        work_store.record_event(sid, "Stop", {"last_assistant_message": "Stopped."})
        assert work_store.maybe_autocontinue(sid, "", tail="Stopped.") == "budget_spent"
        assert len(_events(item_id, "autocontinue_stopped")) == 1
        db.execute("UPDATE work_items SET updated_at = ? WHERE id = ?",
                   ("2000-01-01T00:00:00+00:00", item_id))
        cutoff = datetime.now(timezone.utc).isoformat()
        retried = [a for a in work_store.retry_missed_autocontinues(cutoff) if a["id"] == item_id]
        assert retried == []
        assert len(_events(item_id, "autocontinue_stopped")) == 1

    def test_a_disabled_stop_is_retried_once_autocontinue_is_back_on(self, board):
        item_id = work_store.create_item("disabled item")
        sid = f"sid-bg-disabled-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
        db.execute("UPDATE work_items SET autocontinue = 0 WHERE id = ?", (item_id,))
        work_store.record_event(sid, "Stop", {"last_assistant_message": "Stopped."})
        assert work_store.maybe_autocontinue(sid, "", tail="Stopped.") == "disabled"
        db.execute("UPDATE work_items SET autocontinue = 1, updated_at = ? WHERE id = ?",
                   ("2000-01-01T00:00:00+00:00", item_id))
        cutoff = datetime.now(timezone.utc).isoformat()
        retried = [a for a in work_store.retry_missed_autocontinues(cutoff) if a["id"] == item_id]
        assert len(retried) == 1


class TestRecheckBeforeSend:
    def test_an_item_that_left_the_wait_after_the_select_gets_no_nudge(
            self, tmp_path, board, monkeypatch):
        item_id = _parked(tmp_path)
        _expire(item_id)
        real = work_store.pending_background_tasks

        def woke_meanwhile(path):
            db.execute("UPDATE work_items SET state = 'needs_you', pending_question = 'q' "
                       "WHERE id = ?", (item_id,))
            return real(path)

        monkeypatch.setattr(work_store, "pending_background_tasks", woke_meanwhile)
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []
        assert _item(item_id)["state"] == "needs_you"


class TestBudgetStopAfterReopen:
    def test_a_reopen_after_a_budget_stop_is_retried(self, board):
        item_id = work_store.create_item("reopened item")
        sid = f"sid-bg-reopen-{item_id}"
        work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
        started = (datetime.now(timezone.utc)
                   - timedelta(minutes=work_store.RUN_BUDGET_MINUTES + 5)).isoformat()
        db.execute("UPDATE work_runs SET started_at = ? WHERE work_item_id = ?",
                   (started, item_id))
        work_store.record_event(sid, "Stop", {"last_assistant_message": "Working."})
        assert work_store.maybe_autocontinue(sid, "", tail="Working.") == "continued"
        work_store.record_event(sid, "Stop", {"last_assistant_message": "Stopped."})
        assert work_store.maybe_autocontinue(sid, "", tail="Stopped.") == "budget_spent"
        db.execute("UPDATE work_items SET state = 'needs_ack' WHERE id = ?", (item_id,))
        work_store.apply_action(item_id, "reopen")
        db.execute("UPDATE work_items SET updated_at = ? WHERE id = ?",
                   ("2000-01-01T00:00:00+00:00", item_id))
        assert db.query_one("SELECT state FROM work_items WHERE id = ?",
                            (item_id,))["state"] == "needs_you"
        cutoff = datetime.now(timezone.utc).isoformat()
        retried = [a for a in work_store.retry_missed_autocontinues(cutoff) if a["id"] == item_id]
        assert retried == [{"id": item_id, "action": "autocontinue_retry:continued"}]
