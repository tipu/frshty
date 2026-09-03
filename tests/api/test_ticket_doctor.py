import sys
from unittest.mock import patch

import pytest

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
from services import ticket_doctor


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        import frshty
    finally:
        sys.argv = saved_argv

    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config(_config_for(tmp_path))
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


def _config_for(tmp_path):
    return {
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": tmp_path,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
            "repos": ["repo_a"],
        },
        "features": {"tickets": True},
        "pr": {"auto_pr": True},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    }


def _seed_ticket(tmp_path, key="DEV-901", slug="dev-901-stuck"):
    state.init(tmp_path)
    state._default_instance_key = "test"
    log.init(tmp_path, "test")
    docs = tmp_path / "tickets" / slug / "docs"
    docs.mkdir(parents=True)
    (docs / "plan.md").write_text("the plan")
    state.save_ticket(key, {"status": "reviewing", "slug": slug,
                            "summary": "Stuck ticket", "discovered_at": "2026-09-01T00:00:00Z"})
    return key, slug


def test_brief_carries_state_jobs_events_and_pipeline_map(tmp_path):
    key, slug = _seed_ticket(tmp_path)
    config = _config_for(tmp_path)
    job_id = q.enqueue_job("test", "start_reviewing", {}, ticket_key=key)
    q.mark_done(job_id, "failed", {"error": "review_worktree_missing"})
    log.emit("ticket_review_failed", f"{key}: reviewer never started", meta={"ticket": key})

    brief = ticket_doctor.brief(config, key, state.load_ticket(key))

    assert '"status": "reviewing"' in brief
    assert f"- ticket: {key}" in brief
    assert "- instance: test" in brief
    assert f"job {job_id} start_reviewing status=failed" in brief
    assert "review_worktree_missing" in brief
    assert "reviewer never started" in brief
    assert f"repo_a: {tmp_path / 'tickets' / slug / 'repo_a'} (MISSING)" in brief
    assert "plan.md" in brief
    assert "features/ticket_states.py" in brief
    assert str(db.path()) in brief


def test_brief_excludes_another_tickets_job(tmp_path):
    """Negative control: the snapshot must not sweep in unrelated pipeline rows."""
    key, _ = _seed_ticket(tmp_path, key="DEV-903", slug="dev-903-quiet")
    config = _config_for(tmp_path)
    other = q.enqueue_job("test", "create_pr", {}, ticket_key="DEV-902")

    brief = ticket_doctor.brief(config, key, state.load_ticket(key))

    assert f"job {other} create_pr" not in brief
    assert "no jobs recorded for this ticket" in brief


def test_doctor_route_launches_task_tagged_with_instance_and_frshty(client, tmp_path):
    key, _ = _seed_ticket(tmp_path)
    with patch("services.work_launch.launch",
               return_value={"item_id": 77, "state": "agent_working"}) as launched:
        resp = client.post(f"/api/tickets/{key}/doctor",
                           json={"description": "sat in reviewing for two days"})

    assert resp.status_code == 200, resp.text
    assert resp.json()["item_id"] == 77
    kwargs = launched.call_args.kwargs
    assert launched.call_args.args[0].startswith(f"Doctor ticket {key} (test, status reviewing)")
    assert "sat in reviewing for two days" in launched.call_args.args[0]
    assert kwargs["contexts"] == ["test", "frshty"]
    assert kwargs["cwd"] == ticket_doctor.FRSHTY_ROOT
    assert '"status": "reviewing"' in kwargs["brief"]


def test_doctor_route_rejects_empty_description(client, tmp_path):
    key, _ = _seed_ticket(tmp_path)
    with patch("services.work_launch.launch") as launched:
        resp = client.post(f"/api/tickets/{key}/doctor", json={"description": "   "})
    assert resp.status_code == 400
    assert launched.call_count == 0


def test_doctor_route_unknown_ticket(client, tmp_path):
    _seed_ticket(tmp_path)
    resp = client.post("/api/tickets/DEV-000/doctor", json={"description": "stuck"})
    assert resp.status_code == 404


def test_doctor_route_reports_work_layer_down(client, tmp_path):
    key, _ = _seed_ticket(tmp_path)
    with patch("services.work_launch.launch",
               return_value={"error": "personal instance not loaded; work layer is read-only"}):
        resp = client.post(f"/api/tickets/{key}/doctor", json={"description": "stuck"})
    assert resp.status_code == 503
    assert "personal instance" in resp.json()["error"]


def _seed_work_item(item_id, item_state, summary=""):
    db.execute(
        "INSERT INTO work_items(id, objective, state, summary, created_at, updated_at)"
        " VALUES (?, 'Doctor ticket', ?, ?, '2026-09-01T00:00:00Z', '2026-09-01T01:00:00Z')",
        (item_id, item_state, summary))


def test_launch_records_the_run_and_history_reports_it_running(tmp_path):
    key, _ = _seed_ticket(tmp_path, key="DEV-910", slug="dev-910-stuck")
    config = _config_for(tmp_path)
    _seed_work_item(910, "agent_working")
    with patch("services.work_launch.launch",
               return_value={"item_id": 910, "state": "agent_working"}):
        ticket_doctor.launch(config, key, "stuck in reviewing")

    runs = ticket_doctor.history(config, key)

    assert len(runs) == 1
    assert runs[0]["item_id"] == 910
    assert runs[0]["description"] == "stuck in reviewing"
    assert runs[0]["state"] == "agent_working"
    assert runs[0]["running"] is True
    assert runs[0]["url"] == "/tasks/910"


def test_history_reports_a_finished_run_with_its_outcome(tmp_path):
    key, _ = _seed_ticket(tmp_path, key="DEV-911", slug="dev-911-stuck")
    config = _config_for(tmp_path)
    _seed_work_item(911, "done", summary="worktree was missing; recreated it")
    with patch("services.work_launch.launch",
               return_value={"item_id": 911, "state": "agent_working"}):
        ticket_doctor.launch(config, key, "no jobs since Monday")

    runs = ticket_doctor.history(config, key)

    assert runs[0]["running"] is False
    assert runs[0]["state"] == "done"
    assert runs[0]["outcome"] == "worktree was missing; recreated it"


def test_history_of_one_ticket_excludes_another_tickets_run(tmp_path):
    """Negative control: history is per ticket, not per instance."""
    key, _ = _seed_ticket(tmp_path, key="DEV-912", slug="dev-912-stuck")
    other, _ = _seed_ticket(tmp_path, key="DEV-913", slug="dev-913-quiet")
    config = _config_for(tmp_path)
    _seed_work_item(912, "agent_working")
    with patch("services.work_launch.launch",
               return_value={"item_id": 912, "state": "agent_working"}):
        ticket_doctor.launch(config, key, "stuck")

    assert len(ticket_doctor.history(config, key)) == 1
    assert ticket_doctor.history(config, other) == []


def test_failed_launch_records_no_run(tmp_path):
    key, _ = _seed_ticket(tmp_path, key="DEV-914", slug="dev-914-stuck")
    config = _config_for(tmp_path)
    with patch("services.work_launch.launch",
               return_value={"error": "launch failed: tmux session did not start",
                             "item_id": 914}):
        ticket_doctor.launch(config, key, "stuck")

    assert ticket_doctor.history(config, key) == []


def test_doctor_post_returns_history_so_the_page_stays_put(client, tmp_path):
    key, _ = _seed_ticket(tmp_path, key="DEV-915", slug="dev-915-stuck")
    _seed_work_item(915, "agent_working")
    with patch("services.work_launch.launch",
               return_value={"item_id": 915, "state": "agent_working"}):
        resp = client.post(f"/api/tickets/{key}/doctor", json={"description": "stuck"})

    assert resp.status_code == 200, resp.text
    runs = resp.json()["runs"]
    assert [r["item_id"] for r in runs] == [915]
    assert runs[0]["running"] is True


def test_doctor_get_returns_history(client, tmp_path):
    key, _ = _seed_ticket(tmp_path, key="DEV-916", slug="dev-916-stuck")
    _seed_work_item(916, "needs_ack", summary="the precondition never cleared")
    with patch("services.work_launch.launch",
               return_value={"item_id": 916, "state": "agent_working"}):
        client.post(f"/api/tickets/{key}/doctor", json={"description": "stuck"})

    resp = client.get(f"/api/tickets/{key}/doctor")

    assert resp.status_code == 200, resp.text
    runs = resp.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["running"] is False
    assert runs[0]["outcome"] == "the precondition never cleared"


def test_second_request_while_a_run_is_live_goes_to_that_run(tmp_path):
    """Two doctor runs on one ticket asked the operator the same question.

    The second request belongs in the live run's thread, not in a new item."""
    key, _ = _seed_ticket(tmp_path, key="DEV-917", slug="dev-917-stuck")
    config = _config_for(tmp_path)
    _seed_work_item(917, "agent_working")
    with patch("services.work_launch.launch",
               return_value={"item_id": 917, "state": "agent_working"}):
        ticket_doctor.launch(config, key, "first report")

    with patch("services.work_store.reply",
               return_value={"id": 917, "action": "reply"}) as replied, \
            patch("services.work_launch.launch") as launched:
        result = ticket_doctor.launch(config, key, "second report")

    assert launched.call_count == 0
    assert replied.call_args.args == (917, "second report")
    assert result == {"item_id": 917, "state": "agent_working", "action": "reply"}
    assert [r["description"] for r in ticket_doctor.history(config, key)] == [
        "second report", "first report"]


def test_second_request_after_the_run_finished_starts_a_new_task(tmp_path):
    """Negative control: a finished run is not replied to, it is followed."""
    key, _ = _seed_ticket(tmp_path, key="DEV-918", slug="dev-918-stuck")
    config = _config_for(tmp_path)
    _seed_work_item(918, "agent_working")
    with patch("services.work_launch.launch",
               return_value={"item_id": 918, "state": "agent_working"}):
        ticket_doctor.launch(config, key, "first report")
    db.execute("UPDATE work_items SET state = 'done' WHERE id = ?", (918,))

    with patch("services.work_store.reply") as replied, \
            patch("services.work_launch.launch",
                  return_value={"item_id": 919, "state": "agent_working"}) as launched:
        ticket_doctor.launch(config, key, "second report")

    assert replied.call_count == 0
    assert launched.call_args.kwargs["source_item_id"] == 918


def test_first_request_for_a_ticket_follows_nothing(tmp_path):
    key, _ = _seed_ticket(tmp_path, key="DEV-920", slug="dev-920-stuck")
    config = _config_for(tmp_path)
    with patch("services.work_launch.launch",
               return_value={"item_id": 920, "state": "agent_working"}) as launched:
        ticket_doctor.launch(config, key, "first report")

    assert launched.call_args.kwargs["source_item_id"] is None


def test_a_dead_session_falls_back_to_a_new_task(tmp_path):
    """A live item whose terminal is gone must not swallow the request."""
    key, _ = _seed_ticket(tmp_path, key="DEV-921", slug="dev-921-stuck")
    config = _config_for(tmp_path)
    _seed_work_item(921, "agent_working")
    with patch("services.work_launch.launch",
               return_value={"item_id": 921, "state": "agent_working"}):
        ticket_doctor.launch(config, key, "first report")

    with patch("services.work_store.reply",
               return_value={"error": "tmux session gone"}), \
            patch("services.work_launch.launch",
                  return_value={"item_id": 922, "state": "agent_working"}) as launched:
        result = ticket_doctor.launch(config, key, "second report")

    assert result["item_id"] == 922
    assert launched.call_args.kwargs["source_item_id"] == 921
