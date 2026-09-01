import json
from datetime import datetime, timedelta, timezone

import pytest

import core.db as db
import core.log as log
import core.state as state
import features.ticket_timeline as tl


INSTANCE = "tl-test"
TICKET = "DEV-900"
T0 = datetime(2026, 5, 1, 10, 0, 0, tzinfo=timezone.utc)


def _at(**kw) -> str:
    return (T0 + timedelta(**kw)).isoformat()


def _job(job_id, task, status, start, end):
    db.execute(
        "INSERT INTO jobs (id, instance_key, ticket_key, task, status,"
        " enqueued_at, started_at, finished_at, response)"
        " VALUES (?,?,?,?,?,?,?,?,'')",
        (job_id, INSTANCE, TICKET, task, status, start, start, end),
    )


def _event(event, summary, at, meta=None, links=None):
    db.execute(
        "INSERT INTO log_events (id, instance_key, job, event, summary, links, meta, ts)"
        " VALUES (?,?,?,?,?,?,?,?)",
        (f"{event}-{at}", INSTANCE, INSTANCE, event, summary,
         json.dumps(links or {}), json.dumps(meta or {"ticket": TICKET}), at),
    )


def _transition(prior, new, at, actor=INSTANCE, reason=""):
    db.execute(
        "INSERT INTO ticket_transitions (instance_key, ticket_key, prior_status,"
        " new_status, rejected, actor, reason, ts) VALUES (?,?,?,?,0,?,?,?)",
        (INSTANCE, TICKET, prior, new, actor, reason, at),
    )


@pytest.fixture()
def record(fresh_db, tmp_path):
    """One ticket that went through planning, blocked, was unblocked by a
    human, then had a PR comment fixed. Every table the timeline reads."""
    state.init(INSTANCE)
    log.init(tmp_path, INSTANCE)
    docs = tmp_path / "docs"
    docs.mkdir()
    plan = docs / "technical-plan.md"
    plan.write_text("# plan\n")
    import os
    written = (T0 + timedelta(minutes=20)).timestamp()
    os.utime(plan, (written, written))

    _event("ticket_found", f"New ticket: {TICKET}", _at(minutes=0),
           links={"ticket": "https://tracker/DEV-900"})
    _transition(None, "new", _at(minutes=0))
    _transition("new", "planning", _at(minutes=10))
    _job(1, "start_planning", "ok", _at(minutes=10), _at(minutes=30))
    _event("ctp_complete", "consensus plan implemented", _at(minutes=29),
           meta={"ticket": TICKET, "changed": ["repo-a", "repo-b"]})
    _transition("planning", "blocked", _at(minutes=31), reason="task failed")
    _transition("blocked", "reviewing", _at(hours=12), actor="danial",
                reason="manual override")
    _job(2, "sync_pr_base", "ok", _at(hours=13), _at(hours=13, minutes=1))
    _job(3, "sync_pr_base", "ok", _at(hours=14), _at(hours=14, minutes=1))
    _job(4, "sync_pr_base", "ok", _at(hours=15), _at(hours=15, minutes=1))
    _event("ticket_pr_comment_fixed",
           'repo-a: Fixed "lock is too wide" — changed src/a.py (abc1234)',
           _at(hours=16),
           meta={"ticket": TICKET, "repo": "repo-a", "comment_id": 55,
                 "commit": "abc1234def", "files": ["src/a.py"]})
    yield docs


def _build(docs):
    return tl.build(INSTANCE, TICKET, docs)


class TestFmtSpan:
    @pytest.mark.parametrize("seconds,expected", [
        (5, "5s"), (95, "1m 35s"), (120, "2m"),
        (3600, "1h"), (3900, "1h 05m"), (86400, "1d"), (156600, "1d 19h"),
    ])
    def test_spans(self, seconds, expected):
        assert tl.fmt_span(seconds) == expected


class TestBuild:
    def test_orders_nodes_oldest_first(self, record):
        nodes = _build(record)["nodes"]
        stamps = [n["ts"] for n in nodes]
        assert stamps == sorted(stamps)

    def test_phase_circle_carries_the_artifact_it_wrote(self, record):
        plan = next(n for n in _build(record)["nodes"] if n["name"] == "Planning")
        arts = [b for b in plan["detail"]["blocks"] if b["k"] == "arts"]
        assert arts, "the planning circle has no artifact block"
        assert [a["name"] for a in arts[0]["items"]] == ["technical-plan.md"]
        assert ["doc", "technical-plan.md"] in plan["chips"]

    def test_phase_circle_carries_the_repos_the_plan_touched(self, record):
        plan = next(n for n in _build(record)["nodes"] if n["name"] == "Planning")
        files = [b for b in plan["detail"]["blocks"] if b["k"] == "files"]
        assert [f[0] for f in files[0]["items"]] == ["repo-a", "repo-b"]

    def test_gap_pill_measures_the_wait_and_names_its_reason(self, record):
        nodes = _build(record)["nodes"]
        override = next(n for n in nodes if n["name"].startswith("Manual override"))
        assert override["gap_label"] == "11h 50m"
        assert override["why"] == "blocked, nobody was watching"
        assert override["tone"] == "human"

    def test_repeated_maintenance_runs_fold_into_one_circle(self, record):
        nodes = _build(record)["nodes"]
        syncs = [n for n in nodes if n["name"].startswith("Base sync")]
        assert len(syncs) == 1
        assert syncs[0]["name"] == "Base sync — 3 runs"
        assert ["", "3 runs"] in syncs[0]["chips"]

    def test_comment_circle_hangs_off_the_spine_with_its_commit(self, record):
        comment = next(n for n in _build(record)["nodes"] if n["lane"] == 1)
        assert comment["repo"] == "repo-a"
        commits = [b for b in comment["detail"]["blocks"] if b["k"] == "commits"]
        assert commits[0]["items"][0]["sha"] == "abc1234"
        assert commits[0]["items"][0]["files"] == ["src/a.py"]
        assert ["commit", "abc1234"] in comment["chips"]

    def test_kpis_split_agent_time_from_waiting(self, record):
        kpis = _build(record)["kpis"]
        assert kpis["work_label"] == "23m"
        assert kpis["wall_label"] == "16h"
        assert kpis["wait_seconds"] > kpis["work_seconds"]

    def test_ribbon_covers_the_whole_span_without_gaps(self, record):
        out = _build(record)
        segments = out["segments"]
        assert segments
        for before, after in zip(segments, segments[1:]):
            assert before["t1"] == after["t0"]
        assert {s["kind"] for s in segments} <= {"work", "idle", "block", "review"}
        ids = {n["id"] for n in out["nodes"]}
        assert all(s["node"] in ids for s in segments)

    def test_a_ticket_with_no_record_returns_empty(self, fresh_db, tmp_path):
        state.init(INSTANCE)
        log.init(tmp_path, INSTANCE)
        out = tl.build(INSTANCE, "DEV-NOTHING", tmp_path / "missing")
        assert out["nodes"] == []
        assert out["segments"] == []
        assert out["kpis"] == {}
