"""The today-agent: the planner, the daily tick, and the /api/today endpoints.

The planner reads the same staleness buckets /today already renders and turns
them into an ordered list of goals. The tick stores one plan per day in KV and
prunes goals whose tickets reached their target state. The endpoints render the
plan, park questions the pipeline could not answer, and let the operator steer.
"""
import json
import sys
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import core.db as db
import core.log as log
import core.queue as q
import core.state as state
import manager.planner as planner
from core.tasks.autonomy import KV_KEY, today_agent_tick
from core.tasks.registry import TaskContext


def _ctx(instance_key="test", config=None):
    return TaskContext(
        instance_key=instance_key,
        ticket_key=None,
        task="today_agent_tick",
        payload={},
        job_id=1,
        triggering_event_id=None,
        config=config if config is not None else {"today_agent": {"enabled": True}},
        registry=None,
        now=datetime.now(timezone.utc),
    )


class TestRankCandidates:
    def test_bucket_priority_orders_the_candidates(self):
        ranked = planner._rank_candidates({
            "pickup_new": [{"ticket_key": "PROJ-3"}],
            "merge_ready": [{"ticket_key": "PROJ-1"}],
            "ready_to_submit": [{"ticket_key": "PROJ-2"}],
        }, [])
        assert [c["ticket_key"] for c in ranked] == ["PROJ-1", "PROJ-2", "PROJ-3"]

    def test_a_ticket_in_two_buckets_is_kept_once(self):
        ranked = planner._rank_candidates({
            "merge_ready": [{"ticket_key": "PROJ-1"}],
            "pickup_new": [{"ticket_key": "PROJ-1"}],
        }, [])
        assert [c["ticket_key"] for c in ranked] == ["PROJ-1"]
        assert ranked[0]["source_bucket"] == "merge_ready"

    def test_a_ticket_touched_recently_leads_its_bucket(self):
        ranked = planner._rank_candidates({
            "merge_ready": [{"ticket_key": "PROJ-1"}, {"ticket_key": "PROJ-2"}],
        }, ["PROJ-2"])
        assert [c["ticket_key"] for c in ranked] == ["PROJ-2", "PROJ-1"]
        assert ranked[0]["continuity"] is True

    def test_each_bucket_carries_its_target_state(self):
        ranked = planner._rank_candidates({
            "merge_ready": [{"ticket_key": "PROJ-1"}],
            "pickup_new": [{"ticket_key": "PROJ-2"}],
        }, [])
        targets = {c["ticket_key"]: c["target_state"] for c in ranked}
        assert targets == {"PROJ-1": "merged", "PROJ-2": "pr_ready"}


class TestRecentTicketKeys:
    def test_only_tickets_inside_the_window_are_recent(self, tmp_state):
        now = datetime.now(timezone.utc)
        state.save("tickets", {
            "PROJ-1": {"last_activity_at": now.isoformat()},
            "PROJ-2": {"last_activity_at": (now - timedelta(days=5)).isoformat()},
            "PROJ-3": {"last_activity_at": "not-a-date"},
            "PROJ-4": {},
        })
        assert planner._recent_ticket_keys("test") == ["PROJ-1"]


class TestBuildPlan:
    def _buckets(self):
        return {
            "merge_ready": [{"ticket_key": "PROJ-1", "summary": "one"}],
            "ready_to_submit": [{"ticket_key": "PROJ-2", "summary": "two"}],
            "pickup_new": [{"ticket_key": "PROJ-3", "summary": "three"},
                           {"ticket_key": "PROJ-4", "summary": "four"}],
        }

    def test_without_the_llm_the_top_three_ranked_candidates_become_goals(self, tmp_state):
        with patch.object(planner.staleness, "aggregate_all", return_value=self._buckets()):
            plan = planner.build_plan("test", {}, use_llm=False)
        assert [g["ticket_key"] for g in plan["goals"]] == ["PROJ-1", "PROJ-2", "PROJ-3"]
        assert plan["goals"][0]["rationale"] == "one"
        assert plan["bucket_counts"] == {"merge_ready": 1, "ready_to_submit": 1,
                                         "pickup_new": 2}
        assert plan["date"] == datetime.now(timezone.utc).date().isoformat()

    def test_no_candidates_yields_an_empty_plan(self, tmp_state):
        with patch.object(planner.staleness, "aggregate_all", return_value={}):
            plan = planner.build_plan("test", {}, use_llm=False)
        assert plan["goals"] == []
        assert plan["bucket_counts"] == {}

    def test_the_llm_pick_replaces_the_deterministic_order(self, tmp_state):
        answer = json.dumps({"goals": [
            {"ticket_key": "PROJ-3", "rationale": "unblocks the release"},
        ]})
        with patch.object(planner.staleness, "aggregate_all", return_value=self._buckets()), \
             patch.object(planner, "_load_priorities", return_value=("ship it", "h")), \
             patch.object(planner.llm, "run_fast", return_value=answer):
            plan = planner.build_plan("test", {}, use_llm=True)
        assert [g["ticket_key"] for g in plan["goals"]] == ["PROJ-3"]
        assert plan["goals"][0]["rationale"] == "unblocks the release"

    def test_a_ticket_the_llm_invented_is_dropped(self, tmp_state):
        answer = json.dumps({"goals": [{"ticket_key": "NOPE-9", "rationale": "x"}]})
        with patch.object(planner.staleness, "aggregate_all", return_value=self._buckets()), \
             patch.object(planner, "_load_priorities", return_value=("", "")), \
             patch.object(planner.llm, "run_fast", return_value=answer):
            plan = planner.build_plan("test", {}, use_llm=True)
        assert [g["ticket_key"] for g in plan["goals"]] == ["PROJ-1", "PROJ-2", "PROJ-3"]

    def test_an_llm_failure_falls_back_to_the_deterministic_pick(self, tmp_state):
        with patch.object(planner.staleness, "aggregate_all", return_value=self._buckets()), \
             patch.object(planner, "_load_priorities", return_value=("", "")), \
             patch.object(planner.llm, "run_fast", side_effect=RuntimeError("no llm")):
            plan = planner.build_plan("test", {}, use_llm=True)
        assert [g["ticket_key"] for g in plan["goals"]] == ["PROJ-1", "PROJ-2", "PROJ-3"]


class TestTodayAgentTick:
    def test_a_disabled_instance_does_nothing(self, tmp_state):
        result = today_agent_tick(_ctx(config={"today_agent": {"enabled": False}}))
        assert result.status == "ok"
        assert result.artifacts["skipped"] == "today_agent disabled"
        assert state.load(KV_KEY) == {}

    def test_an_instance_without_the_block_is_off(self, tmp_state):
        result = today_agent_tick(_ctx(config={}))
        assert result.artifacts["skipped"] == "today_agent disabled"

    def test_the_first_tick_of_the_day_builds_and_stores_a_plan(self, tmp_state, tmp_log):
        plan = {"date": datetime.now(timezone.utc).date().isoformat(),
                "goals": [{"ticket_key": "PROJ-1", "target_state": "merged"}],
                "bucket_counts": {}}
        with patch("core.tasks.autonomy.build_plan", return_value=plan) as build:
            result = today_agent_tick(_ctx())
        build.assert_called_once()
        assert result.artifacts["rebuild"] is True
        stored = state.load(KV_KEY)["plan"]
        assert [g["ticket_key"] for g in stored["goals"]] == ["PROJ-1"]
        assert stored["last_tick_at"]

    def test_a_second_tick_the_same_day_does_not_rebuild(self, tmp_state, tmp_log):
        today = datetime.now(timezone.utc).date().isoformat()
        state.save(KV_KEY, {"plan": {"date": today, "goals": []}})
        with patch("core.tasks.autonomy.build_plan") as build:
            result = today_agent_tick(_ctx())
        build.assert_not_called()
        assert result.artifacts["rebuild"] is False

    def test_a_paused_plan_is_not_rebuilt(self, tmp_state, tmp_log):
        state.save(KV_KEY, {"paused": True,
                            "plan": {"date": "2020-01-01", "goals": []}})
        with patch("core.tasks.autonomy.build_plan") as build:
            today_agent_tick(_ctx())
        build.assert_not_called()

    def test_a_goal_that_reached_its_target_moves_to_completed(self, tmp_state, tmp_log):
        state.save_ticket("PROJ-1", {"status": "merged",
                                     "merged_external_status": "Done"})
        state.save_ticket("PROJ-2", {"status": "planning"})
        today = datetime.now(timezone.utc).date().isoformat()
        state.save(KV_KEY, {"plan": {"date": today, "goals": [
            {"ticket_key": "PROJ-1", "target_state": "merged"},
            {"ticket_key": "PROJ-2", "target_state": "merged"},
        ]}})
        result = today_agent_tick(_ctx())
        assert result.artifacts == {"active": 1, "completed": 1, "rebuild": False}
        stored = state.load(KV_KEY)["plan"]
        assert [g["ticket_key"] for g in stored["goals"]] == ["PROJ-2"]
        assert [g["ticket_key"] for g in stored["completed"]] == ["PROJ-1"]

    def test_a_skipped_ticket_leaves_the_plan(self, tmp_state, tmp_log):
        state.save_ticket("PROJ-1", {"status": "planning"})
        today = datetime.now(timezone.utc).date().isoformat()
        state.save(KV_KEY, {"skipped": ["PROJ-1"], "plan": {
            "date": today,
            "goals": [{"ticket_key": "PROJ-1", "target_state": "merged"}]}})
        result = today_agent_tick(_ctx())
        assert result.artifacts["active"] == 0

    def test_a_planner_failure_is_reported_not_swallowed(self, tmp_state, tmp_log):
        with patch("core.tasks.autonomy.build_plan", side_effect=RuntimeError("boom")):
            result = today_agent_tick(_ctx())
        assert result.status == "failed"
        assert "RuntimeError" in result.reason
        events = [e["event"] for e in log.get_events(limit=20)]
        assert "today_planner_error" in events


@pytest.fixture()
def client(tmp_path):
    state.init(tmp_path)
    log.init(tmp_path, "test")

    saved_argv = sys.argv[:]
    sys.argv = ["frshty"]
    try:
        if "frshty" in sys.modules:
            frshty = sys.modules["frshty"]
        else:
            import frshty
    finally:
        sys.argv = saved_argv

    from fastapi.testclient import TestClient
    from web.state import set_primary_config
    set_primary_config({
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {"root": tmp_path, "tickets_dir": "tickets",
                      "ticket_layout": "flat", "base_branch": "main"},
        "features": {},
        "pr": {},
        "slack": {},
        "_config_path": tmp_path / "config.toml",
        "_state_dir": tmp_path,
        "_base_url": "http://localhost:8000",
    })
    (tmp_path / "config.toml").write_text("[job]\nkey = 'test'\n")
    return TestClient(frshty.app, raise_server_exceptions=False)


class TestTodayPageRenders:
    def test_the_page_carries_the_plan_and_question_regions(self, client):
        resp = client.get("/today")
        assert resp.status_code == 200
        html = resp.text
        for needle in ("Today's plan", "Questions waiting on you",
                       "/api/today/plan", "loadPlan", "answerQuestion", "skipGoal"):
            assert needle in html, needle


class TestPlanEndpoint:
    def test_no_plan_reads_as_empty(self, client):
        body = client.get("/api/today/plan").json()
        assert body["empty"] is True

    def test_a_goal_whose_ticket_reached_its_target_reads_as_completed(self, client):
        state.save_ticket("PROJ-1", {"status": "merged",
                                     "merged_external_status": "Done"})
        state.save_ticket("PROJ-2", {"status": "planning"})
        state.save(KV_KEY, {"plan": {"goals": [
            {"ticket_key": "PROJ-1", "target_state": "merged"},
            {"ticket_key": "PROJ-2", "target_state": "merged"},
        ]}})
        body = client.get("/api/today/plan").json()
        assert body["empty"] is False
        by_key = {g["ticket_key"]: g for g in body["goals"]}
        assert by_key["PROJ-1"]["completed"] is True
        assert by_key["PROJ-2"]["completed"] is False
        assert by_key["PROJ-2"]["current_state"] == "planning"

    def test_a_skipped_ticket_is_not_returned(self, client):
        state.save_ticket("PROJ-1", {"status": "planning"})
        state.save(KV_KEY, {"skipped": ["PROJ-1"], "plan": {
            "goals": [{"ticket_key": "PROJ-1", "target_state": "merged"}]}})
        body = client.get("/api/today/plan").json()
        assert body["goals"] == []
        assert body["skipped_keys"] == ["PROJ-1"]


class TestQuestionsAndAnswer:
    def _blocked_job(self, task="prove", ticket_key="PROJ-1"):
        job_id = q.enqueue_job("test", task, {"seed": 1}, ticket_key=ticket_key)
        db.execute(
            "UPDATE jobs SET status='blocked', response=?, finished_at=? WHERE id=?",
            (json.dumps({"reason": "Which environment?",
                         "artifacts": {"kind": "ambiguity_blocking",
                                       "expected_input": "text",
                                       "deferred_payload": {"stage": 2}}}),
             datetime.now(timezone.utc).isoformat(), job_id),
        )
        return job_id

    def test_a_blocked_job_is_surfaced_as_a_question(self, client):
        job_id = self._blocked_job()
        body = client.get("/api/today/questions").json()
        assert [x["job_id"] for x in body["questions"]] == [job_id]
        asked = body["questions"][0]
        assert asked["question"] == "Which environment?"
        assert asked["kind"] == "ambiguity_blocking"
        assert asked["ticket_key"] == "PROJ-1"

    def test_a_queued_job_is_not_a_question(self, client):
        job_id = q.enqueue_job("test", "prove", {}, ticket_key="PROJ-2")
        body = client.get("/api/today/questions").json()
        assert job_id not in [x["job_id"] for x in body["questions"]]

    def test_answering_consumes_the_job_and_enqueues_a_new_one(self, client):
        job_id = self._blocked_job()
        resp = client.post("/api/today/answer",
                           json={"job_id": job_id, "answer": "staging"})
        assert resp.status_code == 200
        new_id = resp.json()["new_job_id"]
        old = db.query_one("SELECT status, response FROM jobs WHERE id=?", (job_id,))
        assert old["status"] == "answered"
        assert "staging" not in old["response"]
        new = db.query_one("SELECT task, ticket_key, payload, status FROM jobs WHERE id=?",
                           (new_id,))
        assert new["task"] == "prove"
        assert new["ticket_key"] == "PROJ-1"
        assert new["status"] == "queued"
        payload = json.loads(new["payload"])
        assert payload["_answer"] == "staging"
        assert payload["_resume_from_job"] == job_id
        assert payload["stage"] == 2
        assert payload["seed"] == 1

    def test_answering_a_job_that_is_not_blocked_is_refused(self, client):
        job_id = q.enqueue_job("test", "prove", {}, ticket_key="PROJ-3")
        resp = client.post("/api/today/answer", json={"job_id": job_id, "answer": "x"})
        assert resp.status_code == 409

    def test_answering_an_unknown_job_is_a_404(self, client):
        resp = client.post("/api/today/answer", json={"job_id": 999999, "answer": "x"})
        assert resp.status_code == 404

    def test_answering_without_a_job_id_is_a_400(self, client):
        assert client.post("/api/today/answer", json={"answer": "x"}).status_code == 400


class TestSteerEndpoint:
    def test_pause_then_resume(self, client):
        assert client.post("/api/today/steer", json={"action": "pause"}).status_code == 200
        assert state.load(KV_KEY)["paused"] is True
        client.post("/api/today/steer", json={"action": "resume"})
        assert state.load(KV_KEY)["paused"] is False

    def test_skip_records_the_ticket_and_the_reason(self, client):
        client.post("/api/today/steer",
                    json={"action": "skip", "ticket_key": "PROJ-1", "reason": "blocked on ops"})
        blob = state.load(KV_KEY)
        assert blob["skipped"] == ["PROJ-1"]
        assert blob["skip_reasons"]["PROJ-1"] == "blocked on ops"

    def test_skip_without_a_ticket_key_is_a_400(self, client):
        assert client.post("/api/today/steer",
                           json={"action": "skip"}).status_code == 400

    def test_replan_stores_a_fresh_plan_and_clears_the_pause(self, client):
        state.save(KV_KEY, {"paused": True})
        plan = {"date": "2026-09-01", "goals": [{"ticket_key": "PROJ-7"}]}
        with patch("web.today.build_plan", return_value=plan) as build:
            resp = client.post("/api/today/steer", json={"action": "replan"})
        assert resp.status_code == 200
        build.assert_called_once()
        blob = state.load(KV_KEY)
        assert blob["plan"] == plan
        assert blob["paused"] is False

    def test_an_unknown_action_is_a_400(self, client):
        assert client.post("/api/today/steer",
                           json={"action": "explode"}).status_code == 400
