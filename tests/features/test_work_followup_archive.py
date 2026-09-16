import pathlib
from unittest.mock import MagicMock, patch

import pytest

import core.db as db
import core.state as state
from services import work_store


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


def _launch_patches(tmp_path):
    reg = MagicMock()
    reg.config = {"workspace": {"root": tmp_path}}
    instances = MagicMock()
    instances.get.return_value = reg
    return (patch("services.work_launch.runtime.instances", return_value=instances),
            patch("services.work_launch.terminal.launch_claude"),
            patch("services.work_launch.terminal.session_healthy",
                  return_value={"alive": True, "agent_running": True}))


def _archived_task(objective="archived source task"):
    item_id = work_store.create_item(objective)
    work_store.apply_action(item_id, "done")
    work_store.apply_action(item_id, "archive")
    return item_id


class TestArchivedSource:
    def test_the_archive_view_lists_the_task(self):
        item_id = _archived_task("archive view source task")
        rows = work_store.grouped_items(q="archive view source task", archived=True)["done"]
        assert item_id in {r["id"] for r in rows}

    def test_a_follow_up_launches_from_an_archived_task(self, tmp_path):
        item_id = _archived_task()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            r = _client().post(f"/api/work/items/{item_id}/followup",
                               json={"text": "carry on from the archived task"})
        assert r.status_code == 200, r.text
        child = r.json()["item_id"]
        assert db.query_one("SELECT source_item_id FROM work_items WHERE id = ?",
                            (child,))["source_item_id"] == item_id

    def test_the_follow_up_leaves_the_source_archived(self, tmp_path):
        item_id = _archived_task()
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            r = _client().post(f"/api/work/items/{item_id}/followup",
                               json={"text": "carry on from the archived task"})
        assert r.status_code == 200, r.text
        row = db.query_one("SELECT state, archived_at FROM work_items WHERE id = ?", (item_id,))
        assert row["state"] == "done"
        assert row["archived_at"]

    def test_a_follow_up_is_refused_on_a_canceled_task(self, tmp_path):
        item_id = work_store.create_item("canceled source task")
        work_store.apply_action(item_id, "cancel")
        work_store.apply_action(item_id, "archive")
        instances, launch, healthy = _launch_patches(tmp_path)
        with instances, launch, healthy:
            r = _client().post(f"/api/work/items/{item_id}/followup",
                               json={"text": "carry on from the canceled task"})
        assert r.status_code == 400, r.text
        assert "not done" in r.json()["error"]


class TestBoardTemplate:
    def _board(self):
        return pathlib.Path("templates/work.html").read_text()

    def test_a_completed_row_offers_follow_up(self):
        board = self._board()
        done_branch = board.split("v-else-if=\"g === 'done'\"")[1].split("</template>")[0]
        assert 'toggleFollowup(it)' in done_branch

    def test_the_follow_up_box_opens_for_a_completed_row(self):
        board = self._board()
        assert 'v-if="canFollowUp(g) && fupOpen[rowKey(it)]"' in board
        assert 'canFollowUp(g) {\n      return g === "needs_ack" || g === "done";' in board

    def test_the_board_names_the_task_the_follow_up_launched(self):
        board = self._board()
        assert 'if (d.item_id) this.fupLaunched[key] = d.item_id;' in board
        assert 'v-if="fupLaunched[rowKey(it)]"' in board
        assert 'followupHref(it)' in board
