import base64
import json
import os
import subprocess
import sys
from unittest.mock import MagicMock, patch

import core.codex_session as codex_session
import core.db as db
import core.terminal as terminal
from services import work_launch, work_store


def _rollout(dirpath, thread_id, items, cwd="", at="2026-08-25T03:51:41.043Z"):
    """A codex rollout file holding `items` as item_completed events, plus the
    developer preamble a real rollout carries and the timeline must drop."""
    day = dirpath / "sessions" / "2026" / "08" / "24"
    day.mkdir(parents=True, exist_ok=True)
    path = day / f"rollout-2026-08-24T20-51-40-{thread_id}.jsonl"
    meta = {"session_id": thread_id}
    if cwd:
        meta["cwd"] = cwd
        meta["timestamp"] = at
    lines = [json.dumps({"timestamp": at, "type": "session_meta", "payload": meta}),
             json.dumps({"timestamp": "2026-08-25T03:51:41.796Z", "type": "response_item",
                         "payload": {"type": "message", "role": "developer",
                                     "content": [{"type": "input_text", "text": "PREAMBLE"}]}})]
    for n, item in enumerate(items):
        lines.append(json.dumps({
            "timestamp": f"2026-08-25T03:51:4{n}.000Z", "type": "event_msg",
            "payload": {"type": "item_completed", "thread_id": thread_id, "item": item}}))
    path.write_text("\n".join(lines) + "\n")
    return str(path)


def _mkrun(objective, provider="codex"):
    item_id = work_store.create_item(objective)
    sid = f"sid-codex-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp", provider=provider)
    return item_id, sid


class TestCodexCommand:
    def test_first_run_seeds_context_and_notify(self, tmp_path, monkeypatch):
        sent = []
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": False})
        monkeypatch.setattr(terminal, "launch_pane_command",
                            lambda k, cwd, cmd: sent.append(cmd))
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path))
        terminal.launch_codex("work-1", "/tmp", "sess-uuid-1", "the context", True)
        cmd = sent[0]
        assert "codex --dangerously-bypass-approvals-and-sandbox" in cmd
        assert "scripts/codex_notify.py" in cmd
        assert "sess-uuid-1" in cmd
        assert f"$(cat {tmp_path}/sess-uuid-1.md)" in cmd
        assert (tmp_path / "sess-uuid-1.md").read_text() == "the context"

    def test_resume_uses_recorded_codex_thread(self, monkeypatch):
        sent = []
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": False})
        monkeypatch.setattr(terminal, "launch_pane_command",
                            lambda k, cwd, cmd: sent.append(cmd))
        terminal.launch_codex("work-1", "/tmp", "sess-uuid-2", "", False,
                              agent_session_id="thread-abc")
        assert "codex resume --dangerously-bypass-approvals-and-sandbox" in sent[0]
        assert sent[0].endswith("thread-abc")

    def test_resume_without_thread_falls_back_to_last(self, monkeypatch):
        sent = []
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": False})
        monkeypatch.setattr(terminal, "launch_pane_command",
                            lambda k, cwd, cmd: sent.append(cmd))
        terminal.launch_codex("work-1", "/tmp", "sess-uuid-3", "", False)
        assert sent[0].endswith("--last")

    def test_running_codex_is_not_relaunched(self, monkeypatch):
        sent = []
        monkeypatch.setattr(terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": True})
        monkeypatch.setattr(terminal, "launch_pane_command",
                            lambda k, cwd, cmd: sent.append(cmd))
        terminal.launch_codex("work-1", "/tmp", "sess-uuid-4", "", True)
        assert sent == []


class TestCodexLaunch:
    def test_launch_records_codex_provider(self, tmp_path, monkeypatch):
        monkeypatch.setattr(work_launch, "personal_config",
                            lambda: {"workspace": {"root": tmp_path}})
        launcher = MagicMock()
        with patch("services.work_launch.terminal.launch_agent", launcher), \
             patch("services.work_launch.terminal.session_healthy",
                   return_value={"alive": True, "agent_running": True}), \
             patch("services.work_launch.threading.Thread"):
            out = work_launch.launch("run this on codex", agent="codex")
        assert out["agent"] == "codex"
        assert launcher.call_args.kwargs["agent"] == "codex"
        run = db.query_one("SELECT provider FROM work_runs WHERE session_id = ?",
                           (out["session_id"],))
        assert run["provider"] == "codex"

    def test_unknown_agent_is_rejected(self, tmp_path, monkeypatch):
        monkeypatch.setattr(work_launch, "personal_config",
                            lambda: {"workspace": {"root": tmp_path}})
        out = work_launch.launch("run this somewhere", agent="gemini")
        assert out == {"error": "unknown agent: gemini"}

    def test_codex_kickoff_sends_no_prompt(self, monkeypatch):
        sender = MagicMock(return_value=True)
        monkeypatch.setattr(work_store, "tmux_send", sender)
        monkeypatch.setattr(work_launch.terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": True})
        monkeypatch.setattr(work_launch.time, "sleep", lambda s: None)
        work_launch._kickoff("work-1", 1, "codex")
        sender.assert_not_called()

    def test_algotrader2_project_is_offered(self, monkeypatch):
        monkeypatch.setattr(work_launch.runtime, "instances", lambda: {})
        root = os.path.expanduser("~/Documents/dev/algotrader2/implementation")
        entries = {e["key"]: e["root"] for e in work_launch.project_entries()}
        if os.path.isdir(root):
            assert entries["algotrader2"] == root
        else:
            assert "algotrader2" not in entries


class TestCodexNotify:
    def _notify(self, sid, payload, codex_home=""):
        env = {**os.environ, "FRSHTY_DB": str(db._DB_PATH)}
        if codex_home:
            env["CODEX_HOME"] = str(codex_home)
        return subprocess.run(
            [sys.executable, "scripts/codex_notify.py", sid, json.dumps(payload)],
            capture_output=True, text=True, timeout=15, env=env,
        )

    def test_turn_complete_records_thread_and_needs_you(self):
        item_id, sid = _mkrun("codex notify turn")
        r = self._notify(sid, {"type": "agent-turn-complete", "thread-id": "thread-1",
                               "last-assistant-message": "Which repo should I touch?"})
        assert r.returncode == 0, r.stderr
        run = db.query_one("SELECT agent_session_id FROM work_runs WHERE session_id = ?", (sid,))
        assert run["agent_session_id"] == "thread-1"
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "needs_you"
        assert "Which repo should I touch?" in item["stop_reason"]
        kinds = [e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ? ORDER BY id", (item_id,))]
        assert "question_detected" in kinds

    def test_done_marker_finishes_the_item(self):
        item_id, sid = _mkrun("codex notify done")
        r = self._notify(sid, {"type": "agent-turn-complete", "thread-id": "thread-2",
                               "last-assistant-message": "Shipped it.\nWORK_DONE"})
        assert r.returncode == 0, r.stderr
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "done"

    def test_artifact_line_is_recorded(self):
        item_id, sid = _mkrun("codex notify artifact")
        r = self._notify(sid, {"type": "agent-turn-complete", "thread-id": "thread-3",
                               "last-assistant-message": "ARTIFACT: /tmp/report.html - the report"})
        assert r.returncode == 0, r.stderr
        art = db.query_one("SELECT path, note FROM work_artifacts WHERE work_item_id = ?",
                           (item_id,))
        assert art["path"] == "/tmp/report.html"
        assert art["note"] == "the report"

    def test_foreign_session_writes_nothing(self):
        before = db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"]
        r = self._notify("sid-not-a-work-session",
                         {"type": "agent-turn-complete", "last-assistant-message": "hi"})
        assert r.returncode == 0
        assert db.query_one("SELECT COUNT(*) AS n FROM work_events")["n"] == before

    def test_other_notification_types_are_ignored(self):
        item_id, sid = _mkrun("codex notify other type")
        r = self._notify(sid, {"type": "agent-turn-started", "last-assistant-message": ""})
        assert r.returncode == 0
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"


class TestCodexSweep:
    def test_pane_activity_keeps_a_live_codex_item_working(self, monkeypatch):
        from datetime import datetime, timedelta, timezone
        item_id, sid = _mkrun("codex long turn")
        stale = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET updated_at = ? WHERE id = ?", (stale, item_id))
        fresh = datetime.now(timezone.utc).isoformat()
        monkeypatch.setattr(work_store, "pane_activity", lambda k: fresh)
        monkeypatch.setattr(work_store, "agent_running",
                            lambda k, a="claude": (_ for _ in ()).throw(AssertionError))
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "refreshed"} in actions
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"

    def test_dead_codex_pane_fails_the_item(self, monkeypatch):
        from datetime import datetime, timedelta, timezone
        item_id, sid = _mkrun("codex dead pane")
        stale = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET updated_at = ? WHERE id = ?", (stale, item_id))
        monkeypatch.setattr(work_store, "pane_activity", lambda k: "")
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        actions = work_store.sweep_stale_items()
        assert {"id": item_id, "action": "failed"} in actions

    def test_pane_activity_reads_the_format_tmux_advances_on_output(self):
        """tmux freezes session_activity at the time the session was created,
        so the pane freshness signal has to read window_activity."""
        captured = {}

        def fake_run(argv, **kwargs):
            captured["argv"] = argv
            return subprocess.CompletedProcess(argv, 0, stdout="1787792497\n", stderr="")

        with patch("services.work_store.subprocess.run", fake_run):
            stamp = work_store.pane_activity("work-42")
        assert "#{window_activity}" in captured["argv"]
        assert "#{session_activity}" not in captured["argv"]
        assert stamp.startswith("2026-")

    def test_a_resumed_agent_revives_a_failed_item(self, monkeypatch, tmp_path):
        from datetime import datetime, timedelta, timezone
        item_id, sid = _mkrun("codex resumed in the same pane")
        transcript = tmp_path / "rollout.jsonl"
        transcript.write_text("{}\n")
        failed_at = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'failed_stale', "
                      "stop_reason = 'Agent process gone without a Stop event', "
                      "updated_at = ? WHERE id = ?", (failed_at, item_id))
            c.execute("UPDATE work_runs SET status = 'stopped', transcript_path = ? "
                      "WHERE work_item_id = ?", (str(transcript), item_id))
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        assert {"id": item_id, "action": "revived"} in work_store.sweep_stale_items()
        item = db.query_one("SELECT state, stop_reason FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "agent_working"
        assert item["stop_reason"] == ""
        run = db.query_one("SELECT status FROM work_runs WHERE work_item_id = ?", (item_id,))
        assert run["status"] == "running"

    def test_a_dead_pane_leaves_a_failed_item_alone(self, monkeypatch, tmp_path):
        from datetime import datetime, timedelta, timezone
        item_id, sid = _mkrun("codex still dead")
        transcript = tmp_path / "dead.jsonl"
        transcript.write_text("{}\n")
        failed_at = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'failed_stale', updated_at = ? WHERE id = ?",
                      (failed_at, item_id))
            c.execute("UPDATE work_runs SET status = 'stopped', transcript_path = ? "
                      "WHERE work_item_id = ?", (str(transcript), item_id))
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": False)
        assert {"id": item_id, "action": "revived"} not in work_store.revive_resumed_runs()
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"

    def test_a_transcript_older_than_the_failure_leaves_the_item_alone(self, monkeypatch, tmp_path):
        from datetime import datetime, timezone
        item_id, sid = _mkrun("codex idle pane")
        transcript = tmp_path / "old.jsonl"
        transcript.write_text("{}\n")
        failed_at = datetime.now(timezone.utc).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'failed_stale', updated_at = ? WHERE id = ?",
                      (failed_at, item_id))
            c.execute("UPDATE work_runs SET status = 'stopped', transcript_path = ? "
                      "WHERE work_item_id = ?", (str(transcript), item_id))
        os.utime(transcript, (0, 0))
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        assert {"id": item_id, "action": "revived"} not in work_store.revive_resumed_runs()
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "failed_stale"

    def test_reply_needs_a_live_codex(self, monkeypatch):
        item_id, sid = _mkrun("codex reply")
        seen = {}

        def fake_running(key, agent="claude"):
            seen["agent"] = agent
            return False

        monkeypatch.setattr(work_store, "agent_running", fake_running)
        out = work_store.reply(item_id, "carry on")
        assert seen["agent"] == "codex"
        assert "no live Codex" in out["error"]

    def test_side_question_refuses_a_codex_run(self):
        item_id, sid = _mkrun("codex btw")
        out = work_store.side_question(item_id, "what is left?")
        assert "side questions need a claude session" in out["error"]


USER_ITEM = {"type": "UserMessage", "content": [{"type": "text", "text": "count the files"}]}
AGENT_ITEM = {"type": "AgentMessage", "phase": "final_answer",
              "content": [{"type": "Text", "text": "Top-level files: 32"}]}
EXEC_ITEM = {"type": "CommandExecution", "command": ["/usr/bin/zsh", "-lc", "ls | wc -l"]}
NOISE_ITEMS = [{"type": "Reasoning", "summary_text": []},
               {"type": "ContextCompaction", "id": "x"}]


class TestCodexRollout:
    def test_rollout_path_finds_the_thread_file(self, tmp_path):
        path = _rollout(tmp_path, "thread-find", [USER_ITEM])
        assert codex_session.rollout_path("thread-find", str(tmp_path)) == path
        assert codex_session.rollout_path("thread-missing", str(tmp_path)) == ""
        assert codex_session.rollout_path("", str(tmp_path)) == ""

    def test_is_rollout_only_matches_codex_files(self, tmp_path):
        assert codex_session.is_rollout(_rollout(tmp_path, "thread-kind", []))
        assert not codex_session.is_rollout("/home/x/.claude/projects/p/abc.jsonl")
        assert not codex_session.is_rollout("")

    def test_timeline_reads_prompts_text_and_commands(self, tmp_path):
        path = _rollout(tmp_path, "thread-tl",
                        [USER_ITEM, EXEC_ITEM, AGENT_ITEM] + NOISE_ITEMS)
        tl = codex_session.timeline(path)
        assert [e["kind"] for e in tl] == ["prompt", "tool", "text"]
        assert tl[0]["text"] == "count the files"
        assert tl[1] == {"kind": "tool", "name": "exec", "arg": "ls | wc -l",
                         "at": "2026-08-25T03:51:41.000Z"}
        assert tl[2]["text"] == "Top-level files: 32"

    def test_timeline_drops_the_developer_preamble(self, tmp_path):
        path = _rollout(tmp_path, "thread-pre", [USER_ITEM])
        assert all("PREAMBLE" not in e.get("text", "") for e in codex_session.timeline(path))

    def test_timeline_exposes_inline_image_as_lazy_reference(self, tmp_path):
        image_bytes = b"\x89PNG\r\n\x1a\ntranscript image"
        path = _rollout(tmp_path, "thread-image", [USER_ITEM])
        with open(path) as f:
            lines = f.read().splitlines()
        lines.insert(2, json.dumps({
            "timestamp": "2026-08-25T03:51:39.000Z", "type": "response_item",
            "payload": {"type": "message", "role": "user", "content": [
                {"type": "input_text", "text": "count the files"},
                {"type": "input_image", "image_url": "data:image/png;base64," +
                 base64.b64encode(image_bytes).decode()},
            ]},
        }))
        with open(path, "w") as f:
            f.write("\n".join(lines) + "\n")

        prompt = codex_session.timeline(path)[0]
        assert len(prompt["images"]) == 1
        assert prompt["images"][0]["media_type"] == "image/png"
        assert codex_session.embedded_image(path, prompt["images"][0]["id"]) == \
            (image_bytes, "image/png")
        assert codex_session.embedded_image(path, "not-an-image") is None

    def test_last_assistant_text_is_the_final_agent_message(self, tmp_path):
        path = _rollout(tmp_path, "thread-last", [USER_ITEM, AGENT_ITEM])
        assert codex_session.last_assistant_text(path) == "Top-level files: 32"
        assert codex_session.assistant_texts(path) == ["Top-level files: 32"]

    def test_missing_file_reads_empty(self, tmp_path):
        assert codex_session.timeline(str(tmp_path / "rollout-nope.jsonl")) == []
        assert codex_session.last_assistant_text(str(tmp_path / "rollout-nope.jsonl")) == ""


class TestCodexTranscriptInWorkStore:
    def test_work_store_readers_dispatch_on_a_rollout(self, tmp_path):
        art = {"type": "AgentMessage",
               "content": [{"type": "Text", "text": "ARTIFACT: /tmp/r.html - the report"}]}
        path = _rollout(tmp_path, "thread-ws", [USER_ITEM, EXEC_ITEM, AGENT_ITEM, art])
        assert [e["kind"] for e in work_store.transcript_timeline(path)] == \
            ["prompt", "tool", "text", "text"]
        assert work_store.last_assistant_text(path) == "ARTIFACT: /tmp/r.html - the report"
        assert work_store._assistant_texts(path) == ["Top-level files: 32",
                                                     "ARTIFACT: /tmp/r.html - the report"]
        assert work_store.pending_tool_calls(path) is False

    def test_notify_records_the_rollout_as_the_transcript(self, tmp_path):
        item_id, sid = _mkrun("codex notify transcript")
        path = _rollout(tmp_path, "thread-notify", [USER_ITEM, EXEC_ITEM, AGENT_ITEM])
        r = TestCodexNotify()._notify(
            sid, {"type": "agent-turn-complete", "thread-id": "thread-notify",
                  "last-assistant-message": "Top-level files: 32"}, codex_home=tmp_path)
        assert r.returncode == 0, r.stderr
        run = db.query_one("SELECT transcript_path FROM work_runs WHERE session_id = ?", (sid,))
        assert run["transcript_path"] == path

    def test_debrief_reads_a_codex_rollout(self, tmp_path, monkeypatch):
        from services import work_debrief
        item_id, sid = _mkrun("codex debrief")
        path = _rollout(tmp_path, "thread-debrief", [USER_ITEM, EXEC_ITEM, AGENT_ITEM])
        with db.tx() as c:
            c.execute("UPDATE work_runs SET transcript_path = ? WHERE session_id = ?",
                      (path, sid))
        work_store.apply_action(item_id, "done")
        seen = {}

        def fake_llm(prompt, **kw):
            seen["prompt"] = prompt
            return '{"summary": "counted the files", "followups": []}'

        monkeypatch.setattr(work_debrief.llm, "run_balanced", fake_llm)
        out = work_debrief.run_debrief(item_id)
        assert out["summary"] == "counted the files"
        assert "OPERATOR: count the files" in seen["prompt"]
        assert "TOOL: exec ls | wc -l" in seen["prompt"]
        assert "AGENT: Top-level files: 32" in seen["prompt"]


class TestRunningCodexTranscript:
    """A codex run has no transcript path until its notify program runs, and
    that program runs only after the first turn completes. These cover the
    discovery that fills the gap so a working item still shows its timeline."""

    def _times(self):
        """Three instants in the past: before the run started, the moment the
        run started, and the moment codex opened its rollout."""
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        return tuple((now - timedelta(minutes=m)).isoformat().replace("+00:00", "Z")
                     for m in (15, 10, 5))

    def test_find_rollout_matches_the_cwd_and_the_start(self, tmp_path):
        earlier, started, opened = self._times()
        old = _rollout(tmp_path, "thread-earlier", [USER_ITEM], cwd="/tmp/work-a", at=earlier)
        wanted = _rollout(tmp_path, "thread-live", [USER_ITEM], cwd="/tmp/work-a", at=opened)
        _rollout(tmp_path, "thread-other", [USER_ITEM], cwd="/tmp/work-b", at=opened)
        assert codex_session.find_rollout("/tmp/work-a", started, str(tmp_path)) == wanted
        assert codex_session.find_rollout("/tmp/work-a", earlier, str(tmp_path)) == old
        assert codex_session.find_rollout("/tmp/work-c", started, str(tmp_path)) == ""
        assert codex_session.find_rollout("/tmp/work-a", "", str(tmp_path)) == ""

    def test_a_session_opened_long_after_the_launch_is_not_taken(self, tmp_path):
        """A run whose codex never started must not adopt the next run's
        rollout in the same directory."""
        from datetime import datetime, timedelta, timezone
        started = datetime.now(timezone.utc) - timedelta(hours=1)
        late = started + timedelta(seconds=codex_session.LAUNCH_WINDOW_SECONDS + 60)
        _rollout(tmp_path, "thread-someone-else", [USER_ITEM], cwd="/tmp/work-a",
                 at=late.isoformat().replace("+00:00", "Z"))
        start = started.isoformat().replace("+00:00", "Z")
        assert codex_session.find_rollout("/tmp/work-a", start, str(tmp_path)) == ""
        assert codex_session.find_rollout("/tmp/work-a", start, str(tmp_path),
                                          within_seconds=7200) != ""

    def test_rollout_thread_id_reads_the_file_name(self, tmp_path):
        path = _rollout(tmp_path, "01a03798-7aa7-72a0-be04-7f6dc18fe0a7", [])
        assert codex_session.rollout_thread_id(path) == \
            "01a03798-7aa7-72a0-be04-7f6dc18fe0a7"
        assert codex_session.rollout_thread_id("/x/claude.jsonl") == ""

    def test_detail_shows_a_working_codex_run_before_the_first_notify(
            self, tmp_path, monkeypatch):
        _, started, opened = self._times()
        item_id, sid = _mkrun("codex still on its first turn")
        path = _rollout(tmp_path, "thread-first-turn", [USER_ITEM, EXEC_ITEM, AGENT_ITEM],
                        cwd="/tmp", at=opened)
        with db.tx() as c:
            c.execute("UPDATE work_runs SET started_at = ? WHERE session_id = ?",
                      (started, sid))
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))
        detail = work_store.item_detail(item_id)
        assert [e["kind"] for e in detail["timeline"]] == ["prompt", "tool", "text"]
        run = db.query_one(
            "SELECT transcript_path, agent_session_id FROM work_runs WHERE session_id = ?",
            (sid,))
        assert run["transcript_path"] == path
        assert run["agent_session_id"] == "thread-first-turn"

    def test_a_recorded_thread_id_is_used_before_any_search(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun("codex thread already known")
        path = _rollout(tmp_path, "thread-known", [USER_ITEM, AGENT_ITEM])
        with db.tx() as c:
            c.execute("UPDATE work_runs SET agent_session_id = 'thread-known' "
                      "WHERE session_id = ?", (sid,))
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))
        monkeypatch.setattr(codex_session, "find_rollout",
                            lambda *a, **k: (_ for _ in ()).throw(AssertionError))
        assert [e["kind"] for e in work_store.item_detail(item_id)["timeline"]] == \
            ["prompt", "text"]

    def test_a_missing_recorded_thread_falls_back_to_the_matching_run(
            self, tmp_path, monkeypatch):
        _, started, opened = self._times()
        item_id, sid = _mkrun("codex recorded a phantom thread")
        path = _rollout(tmp_path, "thread-actual", [USER_ITEM, EXEC_ITEM, AGENT_ITEM],
                        cwd="/tmp", at=opened)
        with db.tx() as c:
            c.execute("UPDATE work_runs SET started_at = ?, "
                      "agent_session_id = 'thread-phantom' WHERE session_id = ?",
                      (started, sid))
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))

        detail = work_store.item_detail(item_id)

        assert [e["kind"] for e in detail["timeline"]] == ["prompt", "tool", "text"]
        run = db.query_one(
            "SELECT transcript_path, agent_session_id FROM work_runs WHERE session_id = ?",
            (sid,))
        assert run["transcript_path"] == path
        assert run["agent_session_id"] == "thread-actual"

    def test_a_claude_run_is_never_searched(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun("claude run without a transcript", provider="claude")
        _rollout(tmp_path, "thread-not-mine", [USER_ITEM], cwd="/tmp",
                 at=self._times()[2])
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))
        assert work_store.item_detail(item_id)["timeline"] == []
        run = db.query_one("SELECT transcript_path FROM work_runs WHERE session_id = ?", (sid,))
        assert run["transcript_path"] == ""

    def test_a_rollout_claimed_by_another_run_is_not_taken_twice(self, tmp_path, monkeypatch):
        _, started, opened = self._times()
        first_id, first_sid = _mkrun("codex first run")
        second_id, second_sid = _mkrun("codex second run")
        path = _rollout(tmp_path, "thread-shared", [USER_ITEM, AGENT_ITEM], cwd="/tmp", at=opened)
        with db.tx() as c:
            c.execute("UPDATE work_runs SET started_at = ?, transcript_path = ? "
                      "WHERE session_id = ?", (started, path, first_sid))
            c.execute("UPDATE work_runs SET started_at = ? WHERE session_id = ?",
                      (started, second_sid))
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))
        work_store.item_detail(second_id)
        run = db.query_one("SELECT transcript_path FROM work_runs WHERE session_id = ?",
                           (second_sid,))
        assert run["transcript_path"] == ""

    def test_the_debrief_reads_a_discovered_rollout(self, tmp_path, monkeypatch):
        from services import work_debrief
        _, started, opened = self._times()
        item_id, sid = _mkrun("codex debrief without a recorded transcript")
        _rollout(tmp_path, "thread-debrief-find", [USER_ITEM, EXEC_ITEM, AGENT_ITEM],
                 cwd="/tmp", at=opened)
        with db.tx() as c:
            c.execute("UPDATE work_runs SET started_at = ? WHERE session_id = ?",
                      (started, sid))
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))
        work_store.apply_action(item_id, "done")
        seen = {}

        def fake_llm(prompt, **kw):
            seen["prompt"] = prompt
            return '{"summary": "counted the files", "followups": []}'

        monkeypatch.setattr(work_debrief.llm, "run_balanced", fake_llm)
        assert work_debrief.run_debrief(item_id)["summary"] == "counted the files"
        assert "AGENT: Top-level files: 32" in seen["prompt"]

    def test_the_stale_sweep_reads_a_discovered_rollout(self, tmp_path, monkeypatch):
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        started = (now - timedelta(hours=3)).isoformat().replace("+00:00", "Z")
        opened = (now - timedelta(hours=2, minutes=55)).isoformat().replace("+00:00", "Z")
        item_id, sid = _mkrun("codex sweep discovery")
        done = {"type": "AgentMessage",
                "content": [{"type": "Text", "text": "All set.\nWORK_DONE"}]}
        path = _rollout(tmp_path, "thread-sweep", [USER_ITEM, done], cwd="/tmp", at=opened)
        quiet = (now - timedelta(hours=2, minutes=50)).timestamp()
        os.utime(path, (quiet, quiet))
        stale = (now - timedelta(hours=3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_runs SET started_at = ? WHERE session_id = ?",
                      (started, sid))
            c.execute("UPDATE work_items SET updated_at = ? WHERE id = ?", (stale, item_id))
        monkeypatch.setattr(codex_session, "HOME_DIR", str(tmp_path))
        monkeypatch.setattr(work_store, "pane_activity", lambda k: "")
        monkeypatch.setattr(work_store, "agent_running", lambda k, a="claude": True)
        work_store.sweep_stale_items()
        item = db.query_one("SELECT state FROM work_items WHERE id = ?", (item_id,))
        assert item["state"] == "done"


class TestCodexFollowupAgent:
    def test_followup_send_carries_the_agent(self, monkeypatch):
        from services import work_debrief
        item_id, _ = _mkrun("codex followup source")
        work_store.apply_action(item_id, "done")
        now = work_store._now()
        with db.tx() as c:
            fid = c.execute(
                "INSERT INTO work_followups(work_item_id, kind, workspace, recipient, "
                "draft, created_at, updated_at) VALUES (?, 'work_item', '', '', ?, ?, ?)",
                (item_id, "do the next thing", now, now)).lastrowid
        seen = {}

        def fake_launch(source_item_id, objective, cwd="", contexts=None, slack=False,
                        agent="claude"):
            seen["agent"] = agent
            return {"item_id": 1}

        monkeypatch.setattr(work_debrief.work_launch, "launch_followup", fake_launch)
        out = work_debrief.send_followup(fid, agent="codex")
        assert out["status"] == "sent"
        assert seen["agent"] == "codex"


TRUST_PANE = ("> You are in /home/tipu/Documents/dev/frshty/frshty\n"
              "  Do you trust the contents of this directory? Working with untrusted contents\n"
              "\u203a 1. Yes, continue\n  2. No, quit\n")
READY_PANE = "> _ OpenAI Codex (v0.149.1)\n\u203a Ask Codex to do anything\n"


class TestCodexTrustPrompt:
    def test_enter_is_sent_only_while_the_question_shows(self, monkeypatch):
        calls = []
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda n: True)
        monkeypatch.setattr(terminal.subprocess, "run",
                            lambda argv, **kw: calls.append(argv) or MagicMock(returncode=0))
        monkeypatch.setattr(terminal, "pane_text", lambda k: TRUST_PANE)
        assert terminal.answer_codex_trust("work-1") is True
        assert calls[-1][-3:] == ["-t", "term-work-1", "Enter"]
        calls.clear()
        monkeypatch.setattr(terminal, "pane_text", lambda k: READY_PANE)
        assert terminal.answer_codex_trust("work-1") is False
        assert calls == []

    def test_pane_text_is_empty_without_a_session(self, monkeypatch):
        monkeypatch.setattr(terminal, "_tmux_session_exists", lambda n: False)
        assert terminal.pane_text("work-gone") == ""

    def test_kickoff_answers_the_trust_question_before_the_health_check(self, monkeypatch):
        order = []
        monkeypatch.setattr(work_launch.time, "sleep", lambda s: None)
        panes = [TRUST_PANE, TRUST_PANE, READY_PANE]

        def fake_answer(key):
            order.append("trust")
            return panes.pop(0) == TRUST_PANE if panes else False

        def fake_health(key, agent="claude"):
            order.append("health")
            return {"alive": True, "agent_running": True}

        monkeypatch.setattr(work_launch.terminal, "answer_codex_trust", fake_answer)
        monkeypatch.setattr(work_launch.terminal, "session_healthy", fake_health)
        monkeypatch.setattr(work_store, "tmux_send", MagicMock(return_value=True))
        work_launch._kickoff("work-1", 1, "codex")
        assert order[0] == "trust"
        assert "health" not in order[:1]

    def test_kickoff_skips_the_trust_question_for_claude(self, monkeypatch):
        answered = MagicMock()
        monkeypatch.setattr(work_launch.time, "sleep", lambda s: None)
        monkeypatch.setattr(work_launch.terminal, "answer_codex_trust", answered)
        monkeypatch.setattr(work_launch.terminal, "session_healthy",
                            lambda k, agent="claude": {"alive": True, "agent_running": True})
        monkeypatch.setattr(work_store, "tmux_send", MagicMock(return_value=True))
        work_launch._kickoff("work-1", 1, "claude")
        answered.assert_not_called()
