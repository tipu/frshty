import json
import os
import re
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


class TestArtifactChips:
    """A chip on a phase circle opens the file it names. The chip only offers
    to open what the docs endpoint serves, so a file type the board cannot
    serve carries the inert kind."""

    def _write(self, docs, name, minutes):
        path = docs / name
        path.write_text("x")
        written = (T0 + timedelta(minutes=minutes)).timestamp()
        os.utime(path, (written, written))

    def test_a_text_document_gets_an_openable_chip(self, record):
        plan = next(n for n in _build(record)["nodes"] if n["name"] == "Planning")
        assert ["doc", "technical-plan.md"] in plan["chips"]

    def test_a_video_gets_a_media_chip(self, record):
        self._write(record, "walkthrough.webm", 21)
        plan = next(n for n in _build(record)["nodes"] if n["name"] == "Planning")
        assert ["media", "walkthrough.webm"] in plan["chips"]

    def test_a_file_type_the_board_cannot_serve_gets_an_inert_chip(self, record):
        self._write(record, "proof.js", 22)
        plan = next(n for n in _build(record)["nodes"] if n["name"] == "Planning")
        assert ["file", "proof.js"] in plan["chips"]
        assert ["doc", "proof.js"] not in plan["chips"]


class TestIssueReportedCircle:
    """The circle a human's bug report puts on the spine. The panel has to
    name the comment, quote it, and say what the report started."""

    def _issue_event(self, meta):
        _event("ticket_issue_detected",
               "Issue detected in comment for %s: the export button times out" % TICKET,
               _at(hours=17), meta={"ticket": TICKET, **meta})

    def test_panel_quotes_the_comment_and_names_the_trigger(self, record):
        self._issue_event({
            "comment_id": "17285",
            "comment_author": "Sam Reviewer",
            "comment_created_at": "2026-09-01T18:20:00+00:00",
            "comment_change": "new",
            "comment_excerpt": "the export button times out after the merge",
            "comment_chars": 43,
            "comment_excerpt_chars": 43,
            "issue_reason": "the export times out after the merge",
            "ticket_summary": "Export report to CSV",
            "triggers": "fix_reported_bug",
        })
        node = next(n for n in _build(record)["nodes"] if n["glyph"] == "⚠")
        blocks = node["detail"]["blocks"]
        quote = next(b for b in blocks if b["k"] == "quote")
        assert quote["text"] == "the export button times out after the merge"
        notes = [b["text"] for b in blocks if b["k"] == "note"]
        assert "the export times out after the merge" in notes
        rows = dict(next(b for b in blocks if b["k"] == "kv")["rows"])
        assert rows["source comment"] == "17285"
        assert rows["reported by"] == "Sam Reviewer"
        assert rows["ticket"] == "Export report to CSV"
        assert rows["started"] == "fix_reported_bug"
        assert node["name"] == "Bug reported by Sam Reviewer"
        assert ["warn", "issue detected"] in node["chips"]

    def test_a_long_comment_says_how_much_was_cut(self, record):
        self._issue_event({
            "comment_id": "17285",
            "comment_excerpt": "x" * 1200 + " …",
            "comment_chars": 4300,
            "comment_excerpt_chars": 1202,
        })
        node = next(n for n in _build(record)["nodes"] if n["glyph"] == "⚠")
        notes = " ".join(b["text"] for b in node["detail"]["blocks"] if b["k"] == "note")
        assert "4300 characters" in notes
        assert "first 1202" in notes

    def test_an_event_recorded_before_the_comment_was_stored_still_renders(self, record):
        self._issue_event({"comment_id": "17285"})
        node = next(n for n in _build(record)["nodes"] if n["glyph"] == "⚠")
        assert node["name"] == "Bug reported on the ticket"
        notes = " ".join(b["text"] for b in node["detail"]["blocks"] if b["k"] == "note")
        assert "17285" in notes
        rows = dict(next(b for b in node["detail"]["blocks"] if b["k"] == "kv")["rows"])
        assert rows["started"] == "fix_reported_bug"


class TestEveryTimestampLeavesAsAnInstant:
    """The browser renders wall-clock time in the reader's zone, so the API
    must never hand it a pre-formatted clock. Every timestamp in the payload
    leaves as a tz-aware ISO instant."""

    CLOCK = re.compile(r"^\d{2}[:/]\d{2}")

    def _instant(self, value):
        parsed = datetime.fromisoformat(value)
        assert parsed.tzinfo is not None, f"{value} carries no offset"
        return parsed

    def test_step_rows_start_with_an_instant_not_a_clock(self, record):
        checked = 0
        for node in _build(record)["nodes"]:
            for block in node["detail"]["blocks"]:
                if block["k"] != "steps":
                    continue
                for row in block["rows"]:
                    assert not self.CLOCK.match(row[0]), (
                        f"{node['id']} step row is a formatted clock: {row[0]}")
                    self._instant(row[0])
                    checked += 1
        assert checked, "no step rows were checked"

    def test_totals_rows_carry_instants(self, record):
        sync = next(n for n in _build(record)["nodes"]
                    if n["name"].startswith("Base sync"))
        rows = dict(next(b for b in sync["detail"]["blocks"]
                         if b.get("title") == "Totals")["rows"])
        self._instant(rows["first"])
        self._instant(rows["last"])

    def test_node_and_segment_timestamps_carry_an_offset(self, record):
        built = _build(record)
        for node in built["nodes"]:
            self._instant(node["ts"])
        for segment in built["segments"]:
            self._instant(segment["t0"])
            self._instant(segment["t1"])
        for doc in built["docs"]:
            self._instant(doc["mtime"])

    def test_a_reopened_pass_reports_the_instant_not_a_date(self, record):
        _event("ticket_requeued", "reopened", _at(hours=20))
        passes = _build(record)["passes"]
        second = next(p for p in passes if p["pass"] == 2)
        assert second["label"] == "Pass 2"
        assert "reopened" not in second["label"]
        self._instant(second["reopened_at"])
