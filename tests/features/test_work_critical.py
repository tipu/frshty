import pathlib
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


def _critical(item_id):
    return db.query_one("SELECT critical FROM work_items WHERE id = ?", (item_id,))["critical"]


def _launch_patches(tmp_path):
    reg = MagicMock()
    reg.config = {"workspace": {"root": tmp_path}}
    instances = MagicMock()
    instances.get.return_value = reg
    return (patch("services.work_launch.runtime.instances", return_value=instances),
            patch("services.work_launch.terminal.launch_claude"),
            patch("services.work_launch.terminal.session_healthy",
                  return_value={"alive": True, "agent_running": True}))


class TestStore:
    def test_a_task_is_not_critical_by_default(self):
        assert _critical(work_store.create_item("ordinary work")) == 0

    def test_create_item_records_the_mark(self):
        assert _critical(work_store.create_item("critical work", critical=True)) == 1

    def test_the_board_reports_the_mark(self):
        item_id = work_store.create_item("board critical work", critical=True)
        rows = work_store.grouped_items(q="board critical work")["agent_working"]
        assert [r["critical"] for r in rows if r["id"] == item_id] == [1]

    def test_the_thread_page_reports_the_mark(self):
        root = work_store.create_item("thread root")
        member = work_store.create_item("thread member", source_item_id=root, critical=True)
        tasks = work_store.thread_detail(root)["tasks"]
        assert {t["id"]: t["critical"] for t in tasks} == {root: 0, member: 1}


class TestToggle:
    def test_marking_sets_the_flag(self):
        item_id = work_store.create_item("mark me")
        assert work_store.apply_action(item_id, "critical_on") == {
            "id": item_id, "action": "critical_on"}
        assert _critical(item_id) == 1

    def test_unmarking_clears_the_flag(self):
        item_id = work_store.create_item("unmark me", critical=True)
        work_store.apply_action(item_id, "critical_off")
        assert _critical(item_id) == 0

    def test_marking_does_not_refresh_the_staleness_clock(self):
        item_id = work_store.create_item("stale but critical")
        db.execute("UPDATE work_items SET updated_at = '2001-01-01T00:00:00+00:00' WHERE id = ?",
                   (item_id,))
        work_store.apply_action(item_id, "critical_on")
        row = db.query_one("SELECT updated_at FROM work_items WHERE id = ?", (item_id,))
        assert row["updated_at"] == "2001-01-01T00:00:00+00:00"

    def test_a_proposal_can_be_marked_before_it_is_approved(self):
        item_id = work_store.create_proposal("frshty proposed this")
        assert "error" not in work_store.apply_action(item_id, "critical_on")
        assert _critical(item_id) == 1
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == work_store.PROPOSED_STATE

    def test_a_proposal_still_refuses_every_other_action(self):
        item_id = work_store.create_proposal("leave me proposed")
        assert "error" in work_store.apply_action(item_id, "done")

    def test_the_action_endpoint_marks_the_task(self):
        item_id = work_store.create_item("mark me over http")
        r = _client().post(f"/api/work/items/{item_id}/action", json={"action": "critical_on"})
        assert r.status_code == 200, r.text
        assert _critical(item_id) == 1


class TestLaunch:
    def test_intake_marks_the_task(self, tmp_path):
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            r = _client().post("/api/work/intake",
                               json={"text": "ship the critical widget", "critical": True})
        assert r.status_code == 200, r.text
        assert _critical(r.json()["item_id"]) == 1

    def test_intake_leaves_an_unmarked_task_unmarked(self, tmp_path):
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            r = _client().post("/api/work/intake", json={"text": "ship the ordinary widget"})
        assert r.status_code == 200, r.text
        assert _critical(r.json()["item_id"]) == 0

    def test_a_follow_up_inherits_the_mark(self, tmp_path):
        source = work_store.create_item("critical source", critical=True)
        db.execute("UPDATE work_items SET state = 'done' WHERE id = ?", (source,))
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            result = work_launch.launch_followup(source, "critical follow-up")
        assert "error" not in result, result
        assert _critical(result["item_id"]) == 1

    def test_a_follow_up_of_ordinary_work_stays_unmarked(self, tmp_path):
        source = work_store.create_item("ordinary source")
        db.execute("UPDATE work_items SET state = 'done' WHERE id = ?", (source,))
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            result = work_launch.launch_followup(source, "ordinary follow-up")
        assert "error" not in result, result
        assert _critical(result["item_id"]) == 0

    def test_a_follow_up_can_drop_the_mark(self, tmp_path):
        source = work_store.create_item("critical source to drop", critical=True)
        db.execute("UPDATE work_items SET state = 'done' WHERE id = ?", (source,))
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            result = work_launch.launch_followup(source, "calmer follow-up", critical=False)
        assert "error" not in result, result
        assert _critical(result["item_id"]) == 0

    def test_an_approved_proposal_keeps_its_mark(self, tmp_path):
        item_id = work_store.create_proposal("proposed critical work")
        work_store.apply_action(item_id, "critical_on")
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            result = work_launch.launch_proposed(item_id)
        assert "error" not in result, result
        assert _critical(item_id) == 1


class TestBoardTemplate:
    def test_the_card_carries_the_critical_class(self):
        page = pathlib.Path("templates/work.html").read_text()
        assert ':class="[g, { critical: it.critical }]"' in page
        assert 'critical: this.intakeCritical' in page

    def test_the_stylesheet_paints_a_gold_border(self):
        css = pathlib.Path("static/frshty-v2.css").read_text()
        assert ".ln-task.critical { border-color: var(--ln-gold);" in css
