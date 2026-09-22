import pathlib
from unittest.mock import patch

import pytest

import core.state as state
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


def _page(name="work_detail.html"):
    return pathlib.Path("templates", name).read_text()


def _section(state_value):
    page = _page()
    head = f"""<div class="sec" v-if="item.state === '{state_value}'">"""
    assert head in page, state_value
    return page.split(head)[1].split('<div class="sec"')[0]


class TestContinueButton:
    @pytest.mark.parametrize("state_value", ["needs_ack", "done"])
    def test_a_finished_task_offers_continue(self, state_value):
        assert '@click="continueTask"' in _section(state_value)

    def test_continue_reopens_restarts_and_focuses_correct(self):
        page = _page()
        method = page.split("async continueTask() {")[1].split("async debrief() {")[0]
        assert 'action: "reopen"' in method
        assert "/resume`" in method
        assert "this.session && this.session.agent_running" in method
        assert "this.$refs.correctBox" in method
        assert "scrollIntoView" in method
        assert "focus(" in method
        assert 'ref="correctBox" v-model="btwText"' in page


class TestResumeRoute:
    def test_resume_restarts_the_session(self):
        with patch.object(work_routes.work_launch, "resume_session",
                          return_value=True) as resume:
            r = _client().post("/api/work/items/42/resume")
        assert r.status_code == 200, r.text
        assert r.json() == {"resumed": True}
        resume.assert_called_once_with(42)

    def test_resume_reports_a_session_it_cannot_restart(self):
        with patch.object(work_routes.work_launch, "resume_session", return_value=False):
            r = _client().post("/api/work/items/42/resume")
        assert r.status_code == 409
        assert "could not be restarted" in r.json()["error"]


class TestQuestionWarning:
    def test_the_shell_defines_the_warning(self):
        shell = pathlib.Path("static/frshty-shell.js").read_text()
        assert "globalProperties.$questionWarning = questionWarning" in shell
        assert "stops without making changes" in shell

    @pytest.mark.parametrize("page,model", [
        ("work.html", "intakeText"),
        ("work.html", "fupText[rowKey(it)]"),
        ("thread_detail.html", "launchText"),
        ("work_detail.html", "fupText"),
        ("work_detail.html", "fuText[f.id]"),
    ])
    def test_every_launch_box_shows_the_warning(self, page, model):
        assert f"$questionWarning({model})" in _page(page)
