import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import core.db as db
import core.discovery as discovery
import core.runtime as runtime
import core.scheduler as scheduler
import core.state as state
from services import global_watch, pending_work

ROOT = Path(__file__).resolve().parent.parent
NOW = datetime(2026, 10, 6, 18, 0, tzinfo=timezone.utc)
SINCE = (NOW - timedelta(minutes=15)).isoformat()
STALE = (NOW - timedelta(hours=2)).isoformat()
RECENT = (NOW - timedelta(minutes=5)).isoformat()
OLD = (NOW - timedelta(minutes=60)).isoformat()


def _ev(key, ev_id, event="job_started", ts=RECENT):
    return {"id": ev_id, "event": event, "ts": ts, "instance_key": key}


def test_discovery_reads_peers_when_only_own_config_is_mounted(tmp_path, monkeypatch):
    (tmp_path / "personal.toml").write_text('[job]\nkey = "personal"\nport = 7110\n')
    (tmp_path / "peers.toml").write_text(
        '[[peers]]\nkey = "frshty"\nbase_url = "http://127.0.0.1:7131/"\n'
        '[[peers]]\nkey = "personal"\nbase_url = "http://127.0.0.1:7135"\n'
        '[[peers]]\nkey = "aimyable"\nbase_url = "http://127.0.0.1:7132"\n')
    monkeypatch.setattr(discovery, "CONFIG_DIR", tmp_path)

    found = {i["key"]: i["base_url"] for i in discovery.discover_instances()}

    assert found == {"personal": "http://localhost:7110",
                     "frshty": "http://127.0.0.1:7131",
                     "aimyable": "http://127.0.0.1:7132"}


def test_discovery_without_peers_finds_only_own_config(tmp_path, monkeypatch):
    (tmp_path / "personal.toml").write_text('[job]\nkey = "personal"\nport = 7110\n')
    monkeypatch.setattr(discovery, "CONFIG_DIR", tmp_path)

    assert [i["key"] for i in discovery.discover_instances()] == ["personal"]


def test_every_minutes_cadence_advances_past_now():
    prev = NOW - timedelta(minutes=50)

    nxt = scheduler._advance_recurring("every_15m", prev, NOW)

    assert nxt == NOW + timedelta(minutes=10)


def test_every_minutes_cadence_steps_once_when_on_time():
    prev = NOW - timedelta(seconds=30)

    assert scheduler._advance_recurring("every_15m", prev, NOW) == prev + timedelta(minutes=15)


def test_evaluate_healthy_feed_has_no_findings():
    events = [_ev("personal", "a"), _ev("frshty", "b")]

    assert global_watch.evaluate(events, {}, ["frshty", "personal"], SINCE, STALE) == []


def test_evaluate_flags_a_feed_that_shows_only_one_instance():
    events = [_ev("personal", "a")]

    findings = global_watch.evaluate(events, {}, ["aimyable", "frshty", "personal"], SINCE, STALE)

    assert [(f["kind"], f["instance"]) for f in findings] == [
        ("silent", "aimyable"), ("silent", "frshty")]


def test_evaluate_flags_unreachable_relabelled_and_error_events():
    events = [_ev("aimyable", "x"), _ev("bh", "x"),
              _ev("frshty", "y", event="ticket_check_error"),
              _ev("frshty", "z", event="pr_base_sync_failed", ts=OLD)]

    findings = global_watch.evaluate(events, {"quill": "timed out"},
                                     ["aimyable", "bh", "frshty", "quill"], SINCE, STALE)

    assert [(f["kind"], f["instance"]) for f in findings] == [
        ("unreachable", "quill"), ("relabelled", "bh"), ("error_events", "frshty")]
    assert findings[2]["detail"] == "ticket_check_error x1"


def _config():
    return {"job": {"key": "personal"}, "_base_url": "https://personal.frshty.localhost",
            "global_watch": {"enabled": True, "agent": False}}


def _response(events, errors=None):
    return events, errors or {}


def test_run_alerts_once_per_finding_set_and_reports_recovery(tmp_path):
    state.init(tmp_path)
    instances = [{"key": "personal", "base_url": "http://127.0.0.1:7135"},
                 {"key": "frshty", "base_url": "http://127.0.0.1:7131"}]
    broken = _response([_ev("personal", "a")])
    healthy = _response([_ev("personal", "a"), _ev("frshty", "b")])
    with patch.object(global_watch, "discover_instances", return_value=instances), \
         patch.object(global_watch, "read_feed", side_effect=[broken, broken, healthy]) as feed, \
         patch.object(global_watch.log, "emit") as emit:
        first = global_watch.run(_config(), now=NOW)
        global_watch.run(_config(), now=NOW + timedelta(minutes=15))
        last = global_watch.run(_config(), now=NOW + timedelta(minutes=30))

    assert feed.call_args_list[0].args == (2, NOW)
    assert [f["kind"] for f in first["findings"]] == ["silent"]
    assert last["findings"] == []
    assert [c.args[0] for c in emit.call_args_list] == ["global_watch_alert", "global_watch_ok"]


def test_read_feed_keeps_every_instance_past_the_merged_cut():
    busy = [_ev("atropos", str(i)) for i in range(global_watch.FETCH_LIMIT - 1)]
    busy.append(_ev("atropos", "old", ts=OLD))
    quiet = [_ev("personal", "p", event="preflight_fail")]

    async def _remote(**_kwargs):
        return busy, {}

    with patch.object(global_watch, "_fetch_local_global_events", return_value=quiet), \
         patch.object(global_watch, "_fetch_remote_global_events", side_effect=_remote):
        events, errors = global_watch.read_feed(2, NOW)

    assert len(events) == global_watch.FETCH_LIMIT + 1
    findings = global_watch.evaluate(events, errors, ["atropos", "personal"], SINCE, STALE)
    assert [(f["kind"], f["instance"]) for f in findings] == [("error_events", "personal")]


def test_run_reads_back_to_the_last_run_when_it_is_older_than_the_stale_window(tmp_path):
    state.init(tmp_path)
    state.save("global_watch", {"last_run_at": (NOW - timedelta(minutes=180)).isoformat(),
                                "fingerprint": []})
    events = [_ev("personal", "a"),
              _ev("personal", "b", event="ticket_check_error",
                  ts=(NOW - timedelta(minutes=150)).isoformat())]
    with patch.object(global_watch, "discover_instances",
                      return_value=[{"key": "personal", "base_url": "x"}]), \
         patch.object(global_watch, "read_feed", return_value=(events, {})) as feed, \
         patch.object(global_watch.log, "emit"):
        out = global_watch.run(_config(), now=NOW)

    assert feed.call_args.args == (3, NOW)
    assert [(f["kind"], f["detail"]) for f in out["findings"]] == [
        ("error_events", "ticket_check_error x1")]


def test_evaluate_flags_an_instance_whose_page_starts_after_the_last_run():
    events = [_ev("aimyable", str(i)) for i in range(global_watch.FETCH_LIMIT)]

    findings = global_watch.evaluate(events, {}, ["aimyable"], SINCE, STALE)

    assert [(f["kind"], f["instance"]) for f in findings] == [("truncated", "aimyable")]


def test_evaluate_accepts_a_full_page_that_reaches_back_to_the_last_run():
    events = [_ev("aimyable", str(i)) for i in range(global_watch.FETCH_LIMIT - 1)]
    events.append(_ev("aimyable", "old", ts=OLD))

    assert global_watch.evaluate(events, {}, ["aimyable"], SINCE, STALE) == []


def _agent_config():
    return {"job": {"key": "personal"}, "global_watch": {"enabled": True}}


def test_build_digest_lists_every_instance_and_only_events_since_the_last_run():
    events = [_ev("personal", "a", event="upwork_scan_done"),
              _ev("personal", "b", event="job_started", ts=OLD),
              {**_ev("frshty", "c"), "meta": {"category": "noise"}}]
    findings = [{"kind": "silent", "instance": "quill", "detail": "no event in the stale window"}]

    digest = global_watch.build_digest(events, ["frshty", "personal", "quill"], findings, SINCE)

    assert "- quill silent: no event in the stale window" in digest
    assert "INSTANCE frshty: 1 events, 1 noise" in digest
    assert "INSTANCE personal: 1 events, 0 noise" in digest
    assert "INSTANCE quill: 0 events, 0 noise" in digest
    assert "upwork_scan_done" in digest and "job_started" not in digest.split("INSTANCE personal")[1]


def _codex(text, code=0):
    return patch.object(global_watch, "run_external_model", return_value=(text, code))


def test_ask_agent_runs_codex_only_on_the_cheap_model_in_a_read_only_sandbox():
    with _codex('{"status": "ok", "problems": []}') as call:
        verdict = global_watch.ask_agent(_agent_config(), "DIGEST", SINCE, NOW)

    cmd = call.call_args.args[0]
    assert cmd[:2] == ["codex", "exec"]
    assert cmd[cmd.index("-m") + 1] == "gpt-6-luna"
    assert "model_reasoning_effort=low" in cmd
    assert cmd[cmd.index("--sandbox") + 1] == "read-only"
    assert "--dangerously-bypass-approvals-and-sandbox" not in cmd
    assert "DIGEST" in call.call_args.kwargs["stdin_text"]
    assert verdict == {"status": "ok", "problems": []}


def test_ask_agent_reports_a_failed_or_unparseable_call():
    with _codex(None, None):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)["status"] == "failed"
    with _codex("I think things look fine"):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)["status"] == "failed"
    with _codex('{"status": "problem", "problems": []}'):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)["status"] == "failed"
    with _codex('{"status": "problem", "problems": 1}'):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)["status"] == "failed"
    with _codex('{"status": "ok"}'):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)["status"] == "failed"


def test_ask_agent_asks_for_a_problem_list_and_no_separate_status():
    with _codex('{"problems": []}') as call:
        global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)

    prompt = call.call_args.kwargs["stdin_text"]
    assert '{"problems": [{"instance"' in prompt
    assert '"status"' not in prompt


def test_ask_agent_derives_the_status_from_the_problem_list():
    with _codex('{"problems": []}'):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW) == {"status": "ok", "problems": []}
    problem = {"instance": "aimyable", "what": "scan_tickets repeats"}
    with _codex(json.dumps({"problems": [problem]})):
        assert global_watch.ask_agent(_agent_config(), "D", SINCE, NOW) == {
            "status": "problem", "problems": [problem]}


def test_ask_agent_fails_a_problem_without_details():
    for problems in ([{"instance": "aimyable", "what": " "}], [{"what": "scan_tickets repeats"}],
                     ["scan_tickets repeats"]):
        with _codex(json.dumps({"problems": problems})):
            verdict = global_watch.ask_agent(_agent_config(), "D", SINCE, NOW)
        assert verdict["status"] == "failed"
        assert verdict["reason"].startswith("a problem names no instance or detail")


def test_run_posts_the_agent_verdict_only_when_it_changes(tmp_path):
    state.init(tmp_path)
    db.init(tmp_path / "t.db", ROOT / "migrations")
    problem = ('{"status": "problem", "problems": [{"instance": "aimyable", "what": "scan_tickets'
               ' repeats with no progress", "evidence": "scan_tickets x40", "likely_cause": "wedged job"}]}')
    healthy = '{"status": "ok", "problems": []}'
    with patch.object(global_watch, "discover_instances",
                      return_value=[{"key": "personal", "base_url": "x"}]), \
         patch.object(global_watch, "read_feed", return_value=([_ev("personal", "a")], {})), \
         patch.object(global_watch, "run_external_model",
                      side_effect=[(problem, 0), (problem, 0), (None, None), (None, None), (healthy, 0)]), \
         patch.object(global_watch.log, "emit") as emit:
        for i in range(5):
            out = global_watch.run(_agent_config(), now=NOW + timedelta(minutes=15 * i))

    assert [c.args[0] for c in emit.call_args_list] == [
        "global_watch_agent_alert", "global_watch_agent_failed", "global_watch_agent_ok"]
    assert "aimyable: scan_tickets repeats with no progress" in emit.call_args_list[0].args[1]
    assert out["agent"] == {"status": "ok", "problems": []}


def _problem(instance, task_key, task="Repair the broken worktrees."):
    return {"instance": instance, "what": "git status fails in every worktree",
            "evidence": "work_worktree_broken x9", "likely_cause": "stale worktree metadata",
            "task_key": task_key, "task": task}


def _verdict(*problems):
    return json.dumps({"status": "problem", "problems": list(problems)})


def _run_agent(tmp_path, replies, config=None):
    state.init(tmp_path)
    db.init(tmp_path / "t.db", ROOT / "migrations")
    entries = [{"key": "frshty", "root": str(tmp_path), "repos": [], "primary": True}]
    with patch.object(global_watch, "discover_instances",
                      return_value=[{"key": "personal", "base_url": "x"}]), \
         patch.object(global_watch, "read_feed", return_value=([_ev("personal", "a")], {})), \
         patch.object(global_watch.work_launch, "project_entries", return_value=entries), \
         patch.object(global_watch, "run_external_model",
                      side_effect=[(r, 0) for r in replies]) as model, \
         patch.object(global_watch.log, "emit") as emit:
        outs = [global_watch.run(config or _agent_config(), now=NOW + timedelta(minutes=15 * i))
                for i in range(len(replies))]
    return outs, model, emit


def test_run_proposes_a_task_for_a_problem_once_while_it_persists(tmp_path):
    reply = _verdict(_problem("aimyable", "Broken Worktrees!"))

    outs, model, emit = _run_agent(tmp_path, [reply, reply])

    rows = db.query_all("SELECT * FROM work_items")
    assert len(rows) == 1
    row = rows[0]
    assert row["state"] == "proposed"
    assert row["proposal_key"] == "global_watch:broken-worktrees"
    assert row["objective"] == "[aimyable] Repair the broken worktrees."
    assert row["contexts"] == "frshty" and row["launch_cwd"] == str(tmp_path)
    assert "work_worktree_broken x9" in row["launch_brief"]
    assert outs[0]["proposed_tasks"] == [{"work_item_id": row["id"],
                                          "task_key": "broken-worktrees", "instance": "aimyable"}]
    assert outs[1]["proposed_tasks"] == []
    assert "- broken-worktrees (proposed): [aimyable] Repair the broken worktrees." in \
        model.call_args_list[1].kwargs["stdin_text"]
    assert [c.args[0] for c in emit.call_args_list].count("global_watch_task_proposed") == 1


def test_run_does_not_propose_a_declined_task_again(tmp_path):
    reply = _verdict(_problem("aimyable", "broken-worktrees"))
    _run_agent(tmp_path, [reply])
    item_id = db.query_one("SELECT id FROM work_items")["id"]
    db.execute("UPDATE work_items SET state = 'canceled', stop_reason = ? WHERE id = ?",
               (global_watch.work_store.DECLINED_REASON, item_id))

    outs, _, _ = _run_agent(tmp_path, [reply])

    assert outs[0]["proposed_tasks"] == []
    assert db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"] == 1


def test_run_stops_proposing_while_max_open_tasks_wait(tmp_path):
    config = {"job": {"key": "personal"}, "global_watch": {"enabled": True, "max_open_tasks": 1}}
    reply = _verdict(_problem("aimyable", "broken-worktrees"),
                     _problem("personal", "preflight-github-repo", "Fix the github.repo preflight."))

    outs, _, emit = _run_agent(tmp_path, [reply, reply], config)

    assert [t["task_key"] for t in outs[0]["proposed_tasks"]] == ["broken-worktrees"]
    assert outs[1]["proposed_tasks"] == []
    assert [c.args[0] for c in emit.call_args_list].count("global_watch_task_capped") == 1
    assert db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"] == 1


def test_the_cap_counts_waiting_proposals_beyond_the_known_tasks_limit(tmp_path):
    config = {"job": {"key": "personal"}, "global_watch": {"enabled": True, "max_open_tasks": 1}}
    _run_agent(tmp_path, [_verdict(_problem("aimyable", "old-fault"))], config)
    for i in range(global_watch.KNOWN_TASKS_LIMIT):
        global_watch.work_store.create_proposal(f"declined {i}", proposal_key=f"global_watch:d{i}")
    db.execute("UPDATE work_items SET state = 'canceled' WHERE proposal_key LIKE 'global_watch:d%'")

    outs, _, _ = _run_agent(tmp_path, [_verdict(_problem("aimyable", "new-fault"))], config)

    assert outs[0]["proposed_tasks"] == []
    assert db.query_one("SELECT COUNT(*) AS n FROM work_items WHERE state = 'proposed'")["n"] == 1


def test_run_proposes_nothing_when_propose_tasks_is_off_or_the_task_is_missing(tmp_path):
    off = {"job": {"key": "personal"}, "global_watch": {"enabled": True, "propose_tasks": False}}
    _run_agent(tmp_path, [_verdict(_problem("aimyable", "broken-worktrees"))], off)
    _run_agent(tmp_path, [_verdict(_problem("aimyable", "broken-worktrees", task=""))])

    assert db.query_one("SELECT COUNT(*) AS n FROM work_items")["n"] == 0


def _seed_config():
    return {"job": {"key": "seedtest"}, "features": {}, "global_watch": {"enabled": True}}


def test_a_restart_keeps_a_global_watch_run_that_is_already_due_soon(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")
    soon = datetime.now(timezone.utc) + timedelta(minutes=2)
    scheduler.upsert_recurring("seedtest", "global_watch", "global_watch",
                               cadence="every_15m", next_run_at=soon)

    runtime._seed_recurring_schedules([_seed_config()])

    assert scheduler.run_at("seedtest", "global_watch") == soon


def test_a_first_start_schedules_global_watch_one_interval_out(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")
    before = datetime.now(timezone.utc)

    runtime._seed_recurring_schedules([_seed_config()])

    first = scheduler.run_at("seedtest", "global_watch")
    assert before + timedelta(minutes=15) <= first <= datetime.now(timezone.utc) + timedelta(minutes=15)


def _seed_pending():
    for key, status in [("A-1", "planning"), ("A-2", "in_review"), ("A-3", "in_review"),
                        ("A-4", "done"), ("A-5", "pr_ready")]:
        db.execute("INSERT INTO tickets (instance_key, ticket_key, status, updated_at) VALUES (?, ?, ?, ?)",
                   ("aimyable", key, status, RECENT))
    db.execute("INSERT INTO tickets (instance_key, ticket_key, status, updated_at) VALUES (?, ?, ?, ?)",
               ("other", "B-1", "planning", RECENT))
    for state_name in ("agent_working", "needs_you", "done", "canceled", "needs_ack"):
        db.execute("INSERT INTO work_items (objective, state, instance_key, created_at, updated_at)"
                   " VALUES (?, ?, ?, ?, ?)", ("x", state_name, "aimyable", RECENT, RECENT))
    db.execute("INSERT INTO work_items (objective, state, instance_key, created_at, updated_at)"
               " VALUES (?, ?, ?, ?, ?)", ("x", "agent_working", "other", RECENT, RECENT))
    for status in ("queued", "running", "ok", "failed"):
        db.execute("INSERT INTO jobs (instance_key, task, status, enqueued_at) VALUES (?, ?, ?, ?)",
                   ("aimyable", "scan_tickets", status, RECENT))


def test_pending_work_splits_unfinished_work_by_who_acts_next(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")
    _seed_pending()

    assert pending_work.snapshot("aimyable") == {
        "agent": {"tickets": {"planning": 1}, "work_items": {"agent_working": 1},
                  "jobs": {"queued": 1, "running": 1}},
        "person": {"tickets": {"in_review": 2, "pr_ready": 1}, "work_items": {"needs_you": 1}}}


def test_pending_work_of_an_idle_instance_is_empty(tmp_path):
    db.init(tmp_path / "t.db", ROOT / "migrations")

    snapshot = pending_work.snapshot("clarivis")

    assert snapshot == {"agent": {"tickets": {}, "work_items": {}, "jobs": {}},
                        "person": {"tickets": {}, "work_items": {}}}
    assert global_watch._format_pending(snapshot) == "none"


def test_read_pending_asks_remote_instances_and_marks_the_ones_that_do_not_answer():
    instances = [{"key": "personal", "base_url": "http://p"}, {"key": "aimyable", "base_url": "http://a"},
                 {"key": "atropos", "base_url": "http://t"}, {"key": "quill", "base_url": "http://q"}]
    idle = {"agent": {"tickets": {}, "work_items": {}, "jobs": {}}, "person": {"tickets": {"in_review": 2}}}
    answers = {"http://a": idle, "http://t": {"error": "timed out"}, "http://q": {"detail": "Not Found"}}

    async def _call(base_url, method, path, timeout):
        assert (method, path) == ("GET", "/api/work/pending")
        return answers[base_url]

    with patch.object(global_watch, "call_instance", side_effect=_call), \
         patch.object(global_watch.pending_work, "snapshot", return_value=idle) as local:
        out = global_watch.read_pending(["aimyable", "atropos", "clarivis", "personal", "quill"],
                                        instances, "personal")

    local.assert_called_once_with("personal")
    assert out["personal"] == idle and out["aimyable"] == idle
    assert out["atropos"] == {"error": "timed out"}
    assert out["quill"]["error"].startswith("unexpected response")
    assert out["clarivis"] == {"error": "not discovered"}
    assert global_watch._format_pending(out["aimyable"]) == "person: tickets in_review x2"
    assert global_watch._format_pending(out["atropos"]) == "unknown (timed out)"


def test_build_digest_shows_pending_work_and_leaves_out_its_own_events():
    events = [_ev("personal", "a", event="global_watch_agent_alert"),
              _ev("personal", "b", event="global_watch_task_proposed"),
              _ev("personal", "c", event="upwork_scan_done")]
    pending = {"personal": {"agent": {"tickets": {}, "work_items": {"agent_working": 1}, "jobs": {}},
                            "person": {"tickets": {}, "work_items": {}}},
               "frshty": {"agent": {"tickets": {}, "work_items": {}, "jobs": {}},
                          "person": {"tickets": {}, "work_items": {}}}}

    findings = [{"kind": "error_events", "instance": "personal",
                 "detail": "global_watch_agent_failed x1", "event": "global_watch_agent_failed"}]

    digest = global_watch.build_digest(events, ["frshty", "personal"], findings, SINCE, pending)

    assert "global_watch" not in digest
    assert "INSTANCE personal: 1 events, 0 noise" in digest
    assert "pending work: agent: work_items agent_working x1" in digest
    assert "INSTANCE frshty: 0 events, 0 noise\ntop: none\npending work: none" in digest


def test_run_digest_covers_a_full_poll_cycle_when_the_last_run_was_recent(tmp_path):
    state.init(tmp_path)
    db.init(tmp_path / "t.db", ROOT / "migrations")
    state.save("global_watch", {"last_run_at": SINCE, "fingerprint": []})
    cycle = (NOW - timedelta(minutes=25)).isoformat()
    events = [_ev("atropos", "a", event="job_started", ts=cycle),
              _ev("personal", "p", event="upwork_scan_done")]
    instances = [{"key": "personal", "base_url": "x"}, {"key": "atropos", "base_url": "http://t"}]

    async def _call(*_args, **_kwargs):
        return {"agent": {"tickets": {}, "work_items": {}, "jobs": {}},
                "person": {"tickets": {}, "work_items": {}}}

    with patch.object(global_watch, "discover_instances", return_value=instances), \
         patch.object(global_watch, "read_feed", return_value=(events, {})), \
         patch.object(global_watch, "call_instance", side_effect=_call), \
         patch.object(global_watch, "run_external_model", return_value=('{"problems": []}', 0)) as model, \
         patch.object(global_watch.log, "emit"):
        out = global_watch.run(_agent_config(), now=NOW)

    prompt = model.call_args.kwargs["stdin_text"]
    assert f"from {(NOW - timedelta(minutes=30)).isoformat()} up to now" in prompt
    assert "INSTANCE atropos: 1 events, 0 noise\ntop: job_started x1\npending work: none" in prompt
    assert out["agent"]["status"] == "ok"


def test_run_digest_reaches_back_to_the_last_run_when_it_is_older_than_a_poll_cycle(tmp_path):
    state.init(tmp_path)
    db.init(tmp_path / "t.db", ROOT / "migrations")
    last = (NOW - timedelta(minutes=45)).isoformat()
    state.save("global_watch", {"last_run_at": last, "fingerprint": []})
    with patch.object(global_watch, "discover_instances",
                      return_value=[{"key": "personal", "base_url": "x"}]), \
         patch.object(global_watch, "read_feed", return_value=([_ev("personal", "a")], {})), \
         patch.object(global_watch, "run_external_model", return_value=('{"problems": []}', 0)) as model, \
         patch.object(global_watch.log, "emit"):
        global_watch.run(_agent_config(), now=NOW)

    assert f"from {last} up to now" in model.call_args.kwargs["stdin_text"]
