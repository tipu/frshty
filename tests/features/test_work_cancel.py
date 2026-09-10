from unittest.mock import MagicMock, patch

import pytest

import core.db as db
import core.state as state
from services import work_launch, work_store


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    yield


def _client():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from web.work import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app)


def _item(item_id):
    return db.query_one("SELECT * FROM work_items WHERE id = ?", (item_id,))


def _with_run(objective="cancel me", session="sess-cancel"):
    item_id = work_store.create_item(objective)
    run_id = work_store.add_run(item_id, session, f"work-{item_id}", "/tmp")
    return item_id, run_id


class TestAction:
    def test_cancel_sets_the_state_and_the_reason(self):
        item_id, _ = _with_run()
        assert work_store.apply_action(item_id, "cancel") == {
            "id": item_id, "action": "cancel"}
        item = _item(item_id)
        assert item["state"] == "canceled"
        assert item["stop_reason"] == work_store.CANCELED_REASON

    def test_cancel_records_an_event(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "cancel")
        kinds = [r["kind"] for r in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))]
        assert "operator_cancel" in kinds

    def test_cancel_finishes_the_open_run(self):
        item_id, run_id = _with_run()
        work_store.apply_action(item_id, "cancel")
        run = db.query_one("SELECT status, finished_at FROM work_runs WHERE id = ?", (run_id,))
        assert run["status"] == "stopped"
        assert run["finished_at"]

    def test_cancel_clears_a_pending_question(self):
        item_id, _ = _with_run()
        db.execute("UPDATE work_items SET state = 'needs_you', pending_question = '{}' "
                   "WHERE id = ?", (item_id,))
        work_store.apply_action(item_id, "cancel")
        assert _item(item_id)["pending_question"] == ""

    def test_a_canceled_task_cannot_be_canceled_again(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "cancel")
        assert "error" in work_store.apply_action(item_id, "cancel")

    def test_a_completed_task_cannot_be_canceled(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "done")
        assert "error" in work_store.apply_action(item_id, "cancel")

    def test_a_proposal_cannot_be_canceled(self):
        item_id = work_store.create_proposal("frshty proposed this")
        assert "error" in work_store.apply_action(item_id, "cancel")
        assert _item(item_id)["state"] == work_store.PROPOSED_STATE

    def test_a_canceled_task_reopens(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "cancel")
        work_store.apply_action(item_id, "reopen")
        assert _item(item_id)["state"] == "needs_you"

    def test_a_canceled_task_archives(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "cancel")
        assert "error" not in work_store.apply_action(item_id, "archive")
        item = _item(item_id)
        assert item["archived_at"] and item["state"] == "canceled"


class TestBoard:
    def test_the_board_files_it_in_its_own_group(self):
        item_id, _ = _with_run(objective="group me")
        work_store.apply_action(item_id, "cancel")
        groups = work_store.grouped_items()
        assert item_id in {r["id"] for r in groups["canceled"]}
        assert item_id not in {r["id"] for r in groups["done"]}

    def test_an_archived_canceled_task_leaves_the_board(self):
        item_id, _ = _with_run(objective="archive me")
        work_store.apply_action(item_id, "cancel")
        work_store.apply_action(item_id, "archive")
        assert work_store.grouped_items()["canceled"] == []
        assert item_id in {r["id"] for r in
                           work_store.grouped_items(archived=True)["canceled"]}

    def test_it_does_not_count_towards_the_attention_badge(self):
        item_id, _ = _with_run()
        db.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
        assert work_store.attention_count() == 1
        work_store.apply_action(item_id, "cancel")
        assert work_store.attention_count() == 0


class TestClosedGuards:
    def test_a_hook_event_does_not_move_a_canceled_task(self):
        item_id, _ = _with_run(session="sess-guard")
        work_store.apply_action(item_id, "cancel")
        work_store.record_event("sess-guard", "SessionStart", {})
        assert _item(item_id)["state"] == "canceled"

    def test_a_question_is_not_recorded_on_a_canceled_task(self):
        item_id, _ = _with_run(session="sess-question")
        work_store.apply_action(item_id, "cancel")
        assert work_store.record_question(
            "sess-question", {"questions": [{"question": "which one?"}]}) == ""
        assert _item(item_id)["pending_question"] == ""

    def test_a_reply_is_refused_on_a_canceled_task(self):
        item_id, _ = _with_run(session="sess-reply")
        work_store.apply_action(item_id, "cancel")
        assert "error" in work_store.reply(item_id, "carry on")

    def test_a_reply_that_lands_after_a_cancel_does_not_revive_the_task(self):
        """reply() sends into the pane between two transactions. A cancel in
        that window has already killed the pane, so the write that would put
        the item back to agent_working has to be refused."""
        item_id, _ = _with_run(session="sess-late-reply")
        db.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))

        def send(key, text):
            with patch("services.work_launch.terminal.kill_terminal"):
                work_launch.cancel(item_id)
            return True

        with patch("services.work_store.agent_running", return_value=True), \
             patch("services.work_store.tmux_send", side_effect=send):
            result = work_store.reply(item_id, "carry on")
        assert "error" in result, result
        assert _item(item_id)["state"] == "canceled"
        assert _item(item_id)["stop_reason"] == work_store.CANCELED_REASON

    def test_a_late_kickoff_failure_does_not_revive_a_canceled_task(self):
        item_id, run_id = _with_run(session="sess-kickoff")
        work_store.apply_action(item_id, "cancel")
        work_store.mark_launch_failed(run_id, "kickoff never delivered")
        assert _item(item_id)["state"] == "canceled"

    def test_a_late_kickoff_failure_still_lands_on_an_open_task(self):
        item_id, run_id = _with_run(session="sess-open")
        work_store.mark_launch_failed(run_id, "kickoff never delivered")
        assert _item(item_id)["state"] == "failed_stale"


class TestSession:
    def test_cancel_kills_the_tmux_session(self):
        item_id, _ = _with_run(objective="kill my pane")
        with patch("services.work_launch.terminal.kill_terminal") as kill:
            result = work_launch.cancel(item_id)
        assert "error" not in result
        assert kill.call_args_list == [(("work-%d" % item_id,), {})]
        assert _item(item_id)["state"] == "canceled"

    def test_a_refused_cancel_keeps_the_session(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.kill_terminal") as kill:
            result = work_launch.cancel(item_id)
        assert "error" in result
        assert kill.call_args_list == []

    def test_cancel_supersedes_a_running_kickoff(self):
        item_id, run_id = _with_run(objective="supersede my kickoff")
        key = f"work-{item_id}"
        with patch("services.work_launch.terminal.kill_terminal"), \
             patch("services.work_launch.threading.Thread"):
            generation = work_launch.start_kickoff(key, run_id)
            work_launch.cancel(item_id)
        assert work_launch._superseded(key, generation)

    def test_a_canceled_task_is_not_resumed(self):
        item_id, _ = _with_run(objective="stay dead")
        with patch("services.work_launch.terminal.kill_terminal"):
            work_launch.cancel(item_id)
        with patch("services.work_launch.personal_config",
                   return_value={"workspace": {"root": "/tmp"}}), \
             patch("services.work_launch.terminal.session_healthy") as healthy:
            assert work_launch.resume_session(item_id) is False
        assert healthy.call_args_list == []


class TestLaunchRace:
    """A launch resolves its directory outside launch_lock, and that takes
    minutes when it fetches a repository and installs dependencies. A cancel
    that lands inside that window has already killed a pane that does not
    exist yet, so the launch itself has to stop."""

    def _patches(self, tmp_path):
        reg = MagicMock()
        reg.config = {"workspace": {"root": str(tmp_path)}}
        instances = MagicMock()
        instances.get.return_value = reg
        return (patch("services.work_launch.runtime.instances", return_value=instances),
                patch("services.work_launch.terminal.session_healthy",
                      return_value={"alive": True, "agent_running": False}),
                patch("services.work_launch.start_kickoff"),
                patch("services.work_launch.terminal.kill_terminal"))

    def test_a_cancel_during_the_launch_starts_no_agent(self, tmp_path):
        instances, healthy, kickoff, kill = self._patches(tmp_path)
        canceled = {}

        def materialize(item_id, plan):
            canceled["result"] = work_launch.cancel(item_id)
            canceled["id"] = item_id
            return str(tmp_path), {}

        with instances, healthy, kickoff, kill, \
             patch("services.work_launch.terminal.launch_agent") as launch, \
             patch("services.work_launch._materialize", side_effect=materialize):
            result = work_launch.launch("cancel me mid launch")
        assert "error" not in canceled["result"], canceled["result"]
        assert "error" in result, result
        assert launch.call_args_list == []
        assert _item(canceled["id"])["state"] == "canceled"
        assert db.query_all("SELECT id FROM work_runs WHERE work_item_id = ?",
                            (canceled["id"],)) == []

    def test_a_cancel_during_a_resume_starts_no_agent(self, tmp_path):
        item_id, _ = _with_run(objective="cancel me mid resume", session="sess-race")
        work_store.record_event("sess-race", "SessionStart", {})
        instances, healthy, kickoff, kill = self._patches(tmp_path)
        canceled = {}

        def plan(*args, **kwargs):
            canceled["result"] = work_launch.cancel(item_id)
            return {"cwd": str(tmp_path)}

        with instances, healthy, kickoff, kill, \
             patch("services.work_launch.personal_config",
                   return_value={"workspace": {"root": str(tmp_path)}}), \
             patch("services.work_launch.project_entries", return_value=[]), \
             patch("services.work_launch.work_worktree.plan", side_effect=plan), \
             patch("services.work_launch.terminal.launch_agent") as launch:
            resumed = work_launch.resume_session(item_id)
        assert "error" not in canceled["result"], canceled["result"]
        assert resumed is False
        assert launch.call_args_list == []
        assert _item(item_id)["state"] == "canceled"


class TestRoute:
    def test_the_action_endpoint_cancels_and_kills(self):
        item_id, _ = _with_run(objective="cancel over http")
        with patch("services.work_launch.terminal.kill_terminal") as kill:
            r = _client().post(f"/api/work/items/{item_id}/action",
                               json={"action": "cancel"})
        assert r.status_code == 200, r.text
        assert kill.call_args_list == [(("work-%d" % item_id,), {})]
        assert _item(item_id)["state"] == "canceled"

    def test_the_action_endpoint_reports_a_refused_cancel(self):
        item_id, _ = _with_run()
        work_store.apply_action(item_id, "done")
        with patch("services.work_launch.terminal.kill_terminal"):
            r = _client().post(f"/api/work/items/{item_id}/action",
                               json={"action": "cancel"})
        assert r.status_code == 400, r.text
