import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest

import core.db as db
import core.state as state
import core.log as log


def _agent_started(session_id):
    """The SessionStart the launched Claude would have delivered."""
    run = db.query_one("SELECT id, work_item_id FROM work_runs WHERE session_id = ?",
                       (session_id,))
    db.execute(
        "INSERT INTO work_events(work_item_id, work_run_id, kind, payload, created_at)"
        " VALUES (?, ?, 'SessionStart', '{}', ?)",
        (run["work_item_id"], run["id"], datetime.now(timezone.utc).isoformat()))


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    saved = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        import frshty
    finally:
        sys.argv = saved
    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets", "ticket_layout": "flat", "base_branch": "main"},
        "features": {}, "pr": {}, "slack": {},
        "_config_path": tmp_path / "config.toml", "_state_dir": tmp_path, "_base_url": "http://localhost:8000",
        "repos": [],
    })
    (tmp_path / "config.toml").write_text("[job]\nkey='test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False), tmp_path


def test_launch_creates_session_and_resumes(client):
    c, tmp = client
    slug = "frg-186-x"
    wt = tmp / "tickets" / slug
    wt.mkdir(parents=True)
    state.save_ticket("FRG-186", {
        "status": "in_review", "slug": slug, "summary": "Clinician review",
        "prs": [{"repo": "analysis_dev", "id": 551, "url": "http://x/551", "approvers": ["cody"]}],
    })
    # pr_comments.json with a needs_reply comment
    (wt / "pr_comments.json").write_text('[{"id":1,"pr_repo":"analysis_dev","pr_id":551,"body":"Sanitize?","path":"a.py","line":5,"diff_hunk":"@@","status":"needs_reply","suggested_reply":"done"}]')

    calls = []
    def fake_launch(key, cwd, sid, ctx, first_run, config=None):
        calls.append({"key": key, "cwd": cwd, "sid": sid, "ctx": ctx,
                      "first_run": first_run, "config": config})
    with patch("core.terminal.launch_claude", side_effect=fake_launch), \
         patch("core.terminal.session_healthy", return_value={"alive": False, "agent_running": False}):
        r1 = c.post("/api/today/launch", json={"loop_type": "pr_comments_needs_reply", "ticket_key": "FRG-186"})
        assert r1.status_code == 200, r1.text
        key = r1.json()["key"]
        assert key == "today-pr_comments_needs_reply-FRG-186"
        assert calls[0]["first_run"] is True
        assert "Sanitize?" in calls[0]["ctx"] and "done" in calls[0]["ctx"]
        assert calls[0]["cwd"] == str(wt)
        assert calls[0]["config"] is not None  # pane inherits the instance's claude auth
        sid = r1.json()["session_id"]
        _agent_started(sid)

        # second call by key -> should resume (first_run False, no context)
        r2 = c.post("/api/today/launch", json={"key": key})
        assert r2.status_code == 200, r2.text
        assert calls[1]["first_run"] is False
        assert calls[1]["ctx"] == ""
        assert calls[1]["sid"] == sid  # SAME session id => --resume <id>

    # if claude already running -> attach only, no relaunch
    with patch("core.terminal.launch_claude", side_effect=fake_launch), \
         patch("core.terminal.session_healthy", return_value={"alive": True, "agent_running": True}):
        r3 = c.post("/api/today/launch", json={"key": key})
        assert r3.status_code == 200
        assert r3.json()["status"] == "running"
    assert len(calls) == 2  # no third launch
    print("SMOKE OK key=", key, "sid=", sid)


def test_launch_reseeds_when_the_agent_never_started(client):
    """A Claude that quit on its folder-trust question leaves `seeded` set and
    no conversation, so every later launch resumed a session id that does not
    exist. The launch is seeded again instead."""
    c, tmp = client
    slug = "frg-187-x"
    wt = tmp / "tickets" / slug
    wt.mkdir(parents=True)
    state.save_ticket("FRG-187", {
        "status": "in_review", "slug": slug, "summary": "Never started",
        "prs": [{"repo": "analysis_dev", "id": 552, "url": "http://x/552", "approvers": ["cody"]}],
    })
    (wt / "pr_comments.json").write_text(
        '[{"id":2,"pr_repo":"analysis_dev","pr_id":552,"body":"Why?","path":"a.py","line":5,'
        '"diff_hunk":"@@","status":"needs_reply","suggested_reply":"because"}]')

    calls = []

    def fake_launch(key, cwd, sid, ctx, first_run, config=None):
        calls.append({"sid": sid, "ctx": ctx, "first_run": first_run})

    with patch("core.terminal.launch_claude", side_effect=fake_launch), \
         patch("core.terminal.session_healthy", return_value={"alive": True, "agent_running": False}):
        r1 = c.post("/api/today/launch",
                    json={"loop_type": "pr_comments_needs_reply", "ticket_key": "FRG-187"})
        assert r1.status_code == 200, r1.text
        key = r1.json()["key"]
        assert calls[0]["first_run"] is True
        r2 = c.post("/api/today/launch", json={"key": key})
        assert r2.status_code == 200, r2.text
    assert calls[1]["first_run"] is True
    assert calls[1]["sid"] == calls[0]["sid"]
    assert "Why?" in calls[1]["ctx"]
