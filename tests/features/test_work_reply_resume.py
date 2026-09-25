import threading
from unittest.mock import MagicMock, patch

import pytest

import core.db as db
import core.state as state
from services import work_launch, work_store
from web import work as work_routes


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    yield


def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    app = FastAPI()
    app.include_router(work_routes.router)
    return TestClient(app)


def _waiting_item(provider="claude"):
    item_id = work_store.create_item("answer my question")
    work_store.add_run(item_id, f"sid-rr-{item_id}", f"work-{item_id}", "/tmp",
                       provider=provider)
    db.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
    return item_id


_RealThread = threading.Thread


class _Inline:
    def __init__(self, target, args, daemon):
        self.thread = _RealThread(target=target, args=args, daemon=daemon)

    def start(self):
        self.thread.start()
        self.thread.join()


class TestReplyRestartsAGoneSession:
    def test_live_session_is_answered_directly(self, monkeypatch):
        item_id = _waiting_item()
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        with patch.object(work_launch, "resume_session") as resume:
            r = _client().post(f"/api/work/items/{item_id}/reply", json={"text": "leave it"})
        assert r.status_code == 200, r.text
        assert r.json() == {"id": item_id, "action": "reply"}
        resume.assert_not_called()
        sender.assert_called_once_with(f"work-{item_id}", "leave it")

    def test_gone_session_is_restarted_and_then_answered(self, monkeypatch):
        item_id = _waiting_item()
        alive = {"up": False}
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": alive["up"])
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)

        def resume(i):
            alive["up"] = True
            return True

        monkeypatch.setattr(work_launch, "resume_session", resume)
        monkeypatch.setattr(work_launch.terminal, "answer_trust", lambda k, a="claude": False)
        monkeypatch.setattr(work_launch.terminal, "session_healthy",
                            lambda k, agent="claude": {"agent_running": alive["up"]})
        monkeypatch.setattr(work_launch.time, "sleep", lambda s: None)
        monkeypatch.setattr(work_launch.threading, "Thread", _Inline)
        r = _client().post(f"/api/work/items/{item_id}/reply", json={"text": "leave it"})
        assert r.status_code == 200, r.text
        assert r.json() == {"id": item_id, "action": "reply", "resumed": True}
        sender.assert_called_once_with(f"work-{item_id}", "leave it")
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"

    def test_session_that_cannot_restart_reports_why(self, monkeypatch):
        item_id = _waiting_item()
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        monkeypatch.setattr(work_launch, "resume_session", lambda i: False)
        r = _client().post(f"/api/work/items/{item_id}/reply", json={"text": "leave it"})
        assert r.status_code == 409
        assert "could not be restarted" in r.json()["error"]

    def test_agent_that_never_comes_up_is_reported(self, monkeypatch):
        item_id = _waiting_item()
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        monkeypatch.setattr(work_launch, "resume_session", lambda i: True)
        monkeypatch.setattr(work_launch.terminal, "answer_trust", lambda k, a="claude": False)
        monkeypatch.setattr(work_launch.terminal, "session_healthy",
                            lambda k, agent="claude": {"agent_running": False})
        monkeypatch.setattr(work_launch.time, "sleep", lambda s: None)
        monkeypatch.setattr(work_launch.threading, "Thread", _Inline)
        state._default_instance_key = "primary"
        emitted = []
        monkeypatch.setattr(work_launch.log, "emit",
                            lambda *a, **k: emitted.append((a[0], state._instance_key_cv.get())))
        r = _client().post(f"/api/work/items/{item_id}/reply", json={"text": "leave it"})
        assert r.status_code == 200, r.text
        assert emitted == [("work_reply_failed", "personal")]
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"

    def test_finished_item_is_not_restarted(self, monkeypatch):
        item_id = _waiting_item()
        work_store.apply_action(item_id, "done")
        with patch.object(work_launch, "resume_session") as resume:
            r = _client().post(f"/api/work/items/{item_id}/reply", json={"text": "leave it"})
        assert r.status_code == 409
        assert "reopen" in r.json()["error"]
        resume.assert_not_called()
