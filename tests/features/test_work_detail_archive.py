import pathlib

import pytest

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


def _page():
    return pathlib.Path("templates/work_detail.html").read_text()


def _section(state_value):
    page = _page()
    head = f"""<div class="sec" v-if="item.state === '{state_value}'">"""
    assert head in page, state_value
    return page.split(head)[1].split('<div class="sec"')[0]


def _completed_task(objective="completed detail task"):
    item_id = work_store.create_item(objective)
    work_store.apply_action(item_id, "done")
    return item_id


class TestDetailTemplate:
    def test_a_completed_task_offers_archive(self):
        section = _section("done")
        assert "act('archive')" in section
        assert 'v-else title="take this completed task off the board"' in section

    def test_an_archived_completed_task_offers_unarchive(self):
        section = _section("done")
        assert 'v-if="item.archived_at"' in section
        assert "act('unarchive')" in section

    def test_a_canceled_task_offers_reopen_and_nothing_else(self):
        """A cancel already filed the task in the archive. Unarchive would put a
        stopped task back on the board, which is the clutter the archive removes.
        Reopen is the one way back, and it makes the task live again."""
        section = _section("canceled")
        assert "act('reopen')" in section
        assert "act('unarchive')" not in section
        assert "act('archive')" not in section

    def test_the_header_marks_an_archived_task(self):
        assert 'v-if="item.archived_at"\n          title="this task is in the archive, not on the board"' in _page()


class TestDetailPayload:
    def test_the_detail_call_reports_the_archive_mark(self):
        item_id = _completed_task("archive mark detail task")
        work_store.apply_action(item_id, "archive")
        r = _client().get(f"/api/work/items/{item_id}/detail")
        assert r.status_code == 200, r.text
        assert r.json()["item"]["archived_at"]

    def test_the_detail_call_leaves_the_mark_empty_on_the_board(self):
        item_id = _completed_task("board detail task")
        r = _client().get(f"/api/work/items/{item_id}/detail")
        assert r.status_code == 200, r.text
        assert not r.json()["item"]["archived_at"]


class TestDetailActions:
    def test_archive_files_a_completed_task(self):
        item_id = _completed_task("archive action task")
        r = _client().post(f"/api/work/items/{item_id}/action", json={"action": "archive"})
        assert r.status_code == 200, r.text
        assert work_store.item_detail(item_id)["item"]["archived_at"]

    def test_unarchive_puts_it_back_on_the_board(self):
        item_id = _completed_task("unarchive action task")
        work_store.apply_action(item_id, "archive")
        r = _client().post(f"/api/work/items/{item_id}/action", json={"action": "unarchive"})
        assert r.status_code == 200, r.text
        item = work_store.item_detail(item_id)["item"]
        assert item["state"] == "done"
        assert not item["archived_at"]


class TestRenderedPage:
    def test_the_rendered_task_page_carries_the_archive_control(self):
        item_id = _completed_task("rendered archive task")
        r = _client().get(f"/tasks/{item_id}")
        assert r.status_code == 200
        assert "act('archive')" in r.text
        assert "act('unarchive')" in r.text
