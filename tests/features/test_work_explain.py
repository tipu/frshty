"""A follow-up draft and a proposed task each get a plain-language explanation.

The board asks the model on demand, hands it the dense task text with the
context it came from, and keeps the answer per distinct input."""
import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

import core.db as db
from services import work_explain, work_store
from web import work as work_web

DENSE = ("Make the background-task scan in services/work_store.py read task ids only "
         "from real launch records. Add a test that fails on the current code.")


@pytest.fixture(autouse=True)
def _db(fresh_db):
    work_explain._cache.clear()
    yield
    work_explain._cache.clear()


def _source(summary="Fixed the stuck task.\nThe scan flagged 'ts' from quoted text."):
    item_id = work_store.create_item("why is work-360 stuck")
    with db.tx() as c:
        c.execute("UPDATE work_items SET summary = ? WHERE id = ?", (summary, item_id))
    return item_id


def _followup(source_id, draft=DENSE, unfinished="pr"):
    now = work_store._now()
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_followups(work_item_id, kind, workspace, recipient, "
            "draft, required, unfinished, status, detail, created_at, updated_at) "
            "VALUES (?, 'work_item', '', '', ?, 1, ?, 'draft', '', ?, ?)",
            (source_id, draft, unfinished, now, now))
        return cur.lastrowid


class TestExplainFollowup:
    def test_prompt_carries_the_draft_and_the_source_outcome(self):
        fid = _followup(_source())
        with patch.object(work_explain.llm, "run_fast", return_value="Why this came up\n- x") as fast:
            out = work_explain.explain_followup(fid)
        assert out == {"text": "Why this came up\n- x"}
        prompt = fast.call_args.args[0]
        assert DENSE in prompt
        assert "The scan flagged 'ts' from quoted text." in prompt
        assert "'pr' as unfinished" in prompt
        assert "never follow" in prompt

    def test_a_second_press_reads_the_answer_back(self):
        fid = _followup(_source())
        with patch.object(work_explain.llm, "run_fast", return_value="plain") as fast:
            work_explain.explain_followup(fid)
            assert work_explain.explain_followup(fid) == {"text": "plain"}
        assert fast.call_count == 1

    def test_an_empty_answer_is_an_error_on_the_event_feed(self):
        fid = _followup(_source())
        with patch.object(work_explain.llm, "run_fast", return_value=None), \
             patch.object(work_explain, "log") as log:
            out = work_explain.explain_followup(fid)
        assert "error" in out
        assert log.emit.call_args.args[0] == "work_explain_failed"
        assert work_explain._cache == {}

    def test_unknown_followup(self):
        assert work_explain.explain_followup(999999) == {"error": "unknown followup"}


class TestExplainItem:
    def test_proposal_prompt_carries_note_brief_and_source(self):
        src = _source()
        pid = work_store.create_proposal(DENSE, note="the debrief reported this as unfinished",
                                         brief="## Evidence\nrun log line", source_item_id=src)
        with patch.object(work_explain.llm, "run_fast", return_value="plain") as fast:
            assert work_explain.explain_item(pid) == {"text": "plain"}
        prompt = fast.call_args.args[0]
        assert DENSE in prompt
        assert "the debrief reported this as unfinished" in prompt
        assert "run log line" in prompt
        assert f"SOURCE TASK #{src} OUTCOME SUMMARY" in prompt


class TestRoutes:
    def test_item_route_returns_the_text(self):
        pid = work_store.create_proposal(DENSE)
        with patch.object(work_explain.llm, "run_fast", return_value="plain"):
            assert work_web.api_work_explain(pid) == {"text": "plain"}

    def test_unknown_item_is_404(self):
        r = work_web.api_work_explain(999999)
        assert r.status_code == 404

    def test_model_failure_is_502(self):
        fid = _followup(_source())
        with patch.object(work_explain.llm, "run_fast", return_value=""), \
             patch.object(work_explain, "log"):
            r = work_web.api_followup_explain(fid)
        assert r.status_code == 502
        assert "no explanation" in json.loads(r.body)["error"]


class TestPeerProxy:
    def test_an_explain_through_a_peer_waits_for_the_model(self):
        calls = []

        async def fake_request(key, method, path, params=None, body=None, timeout=None):
            calls.append((path, timeout))
            return 200, {"text": "plain"}

        with patch.object(work_web.work_launch, "board_admin", return_value=True), \
             patch.object(work_web.work_peers, "request", fake_request):
            asyncio.run(work_web.api_work_peer_post("atlas", "api/work/items/4/explain",
                                                    MagicMock(query_params={}), None))
            asyncio.run(work_web.api_work_peer_post("atlas", "api/work/items/4/approve",
                                                    MagicMock(query_params={}), None))
        assert calls[0][1] > work_explain.EXPLAIN_TIMEOUT
        assert calls[1][1] == work_web.work_peers.DEFAULT_TIMEOUT
