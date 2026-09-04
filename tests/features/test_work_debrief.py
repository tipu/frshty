import json
from unittest.mock import MagicMock

import core.db as db
from services import work_debrief, work_store


def _done_item(objective="debrief target"):
    item_id = work_store.create_item(objective)
    work_store.add_run(item_id, f"sid-db-{item_id}", f"work-{item_id}", "/tmp")
    work_store.apply_action(item_id, "done")
    return item_id


CLAUDE_OUT = json.dumps({
    "summary": "Bumped normalize_ace to 4.0.8.\nPR: https://github.com/x/y/pull/9\nAwaiting review from Sam.",
    "followups": [{"kind": "slack_message", "workspace": "aimyable",
                   "recipient": "Sam", "draft": "PR ready: https://github.com/x/y/pull/9"}],
})


class TestParse:
    def test_parse_valid(self):
        out = work_debrief._parse_debrief("noise before " + CLAUDE_OUT + " after")
        assert "4.0.8" in out["summary"]
        assert out["followups"][0]["recipient"] == "Sam"

    def test_parse_no_summary_rejected(self):
        import pytest
        with pytest.raises(ValueError):
            work_debrief._parse_debrief(json.dumps({"followups": []}))

    def test_parse_caps_followups(self):
        data = {"summary": "s", "followups": [
            {"workspace": "w", "recipient": f"p{i}", "draft": "d"} for i in range(5)]}
        assert len(work_debrief._parse_debrief(json.dumps(data))["followups"]) == 3

    def test_parse_drops_empty_drafts(self):
        data = {"summary": "s", "followups": [{"workspace": "w", "recipient": "p", "draft": " "}]}
        assert work_debrief._parse_debrief(json.dumps(data))["followups"] == []


class TestRunDebrief:
    def test_stores_summary_and_followups(self, monkeypatch, tmp_path):
        t = tmp_path / "t.jsonl"
        t.write_text(json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "did the work"}]}}) + "\n")
        item_id = _done_item()
        db.query_one("SELECT 1")
        with db.tx() as c:
            c.execute("UPDATE work_runs SET transcript_path = ? WHERE work_item_id = ?",
                      (str(t), item_id))
        monkeypatch.setattr(work_debrief, "_run_claude", lambda p: CLAUDE_OUT)
        out = work_debrief.run_debrief(item_id)
        assert out["followups"] == 1
        item = db.query_one("SELECT summary FROM work_items WHERE id = ?", (item_id,))
        assert "4.0.8" in item["summary"]
        fus = work_debrief.followups_for(item_id)
        assert len(fus) == 1 and fus[0]["status"] == "draft"
        kinds = [e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))]
        assert "debrief_done" in kinds

    def test_a_task_waiting_for_acknowledgement_is_debriefed(self):
        """The summary is what the operator reads before acknowledging, so it
        has to be written while the task still waits in needs_ack."""
        item_id = work_store.create_item("waiting for acknowledgement")
        work_store.add_run(item_id, f"sid-db-{item_id}", f"work-{item_id}", "/tmp")
        db.execute("UPDATE work_items SET state = 'needs_ack' WHERE id = ?", (item_id,))
        assert item_id in work_debrief._pending_done_items()

    def test_no_transcript_fails_once(self):
        item_id = _done_item("no transcript")
        out = work_debrief.run_debrief(item_id)
        assert out["error"] == "no transcript"
        assert item_id in work_debrief._pending_done_items()
        for _ in range(work_debrief.MAX_FAILED_ATTEMPTS - 1):
            work_debrief._record_debrief_event(item_id, "debrief_failed", {})
        assert item_id not in work_debrief._pending_done_items()

    def test_claude_error_recorded(self, monkeypatch, tmp_path):
        t = tmp_path / "t.jsonl"
        t.write_text(json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "x"}]}}) + "\n")
        item_id = _done_item("claude fails")
        with db.tx() as c:
            c.execute("UPDATE work_runs SET transcript_path = ? WHERE work_item_id = ?",
                      (str(t), item_id))
        def boom(p):
            raise RuntimeError("claude exited 1")
        monkeypatch.setattr(work_debrief, "_run_claude", boom)
        out = work_debrief.run_debrief(item_id)
        assert "claude exited" in out["error"]
        kinds = [e["kind"] for e in db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))]
        assert "debrief_failed" in kinds


class TestSend:
    def _draft(self, kind="slack_message"):
        item_id = _done_item("send target")
        now = work_store._now()
        with db.tx() as c:
            cur = c.execute(
                "INSERT INTO work_followups(work_item_id, kind, workspace, recipient, draft, "
                "created_at, updated_at) VALUES (?, ?, 'aimyable', 'Sam', 'hi', ?, ?)",
                (item_id, kind, now, now))
            return cur.lastrowid

    def test_send_success(self, monkeypatch):
        fid = self._draft()
        monkeypatch.setattr(work_debrief, "_deliver_slack", lambda row: "sent to Sam (U123) ts=1.2")
        out = work_debrief.send_followup(fid, text="edited text")
        assert out["status"] == "sent"
        row = db.query_one("SELECT status, draft FROM work_followups WHERE id = ?", (fid,))
        assert row["status"] == "sent"
        assert row["draft"] == "edited text"

    def test_send_claims_once(self, monkeypatch):
        fid = self._draft()
        monkeypatch.setattr(work_debrief, "_deliver_slack", lambda row: "ok")
        assert work_debrief.send_followup(fid)["status"] == "sent"
        assert "not a draft" in work_debrief.send_followup(fid)["error"]

    def test_delivery_error_marks_failed(self, monkeypatch):
        fid = self._draft()
        def boom(row):
            raise RuntimeError("recipient 'Sam' resolves to 2 known people: a, b")
        monkeypatch.setattr(work_debrief, "_deliver_slack", boom)
        out = work_debrief.send_followup(fid)
        assert "2 known people" in out["error"]
        row = db.query_one("SELECT status FROM work_followups WHERE id = ?", (fid,))
        assert row["status"] == "failed"

    def test_work_item_kind_launches(self, monkeypatch):
        fid = self._draft(kind="work_item")
        launched = MagicMock(return_value={"item_id": 42})
        monkeypatch.setattr(work_debrief.work_launch, "launch_followup", launched)
        out = work_debrief.send_followup(fid)
        assert out["status"] == "sent"
        assert "#42" in out["detail"]
        item_id = db.query_one("SELECT work_item_id FROM work_followups WHERE id = ?",
                               (fid,))["work_item_id"]
        launched.assert_called_once_with(item_id, "hi", contexts=[], slack=False,
                                        agent="claude")

    def test_work_item_kind_passes_contexts(self, monkeypatch):
        fid = self._draft(kind="work_item")
        launched = MagicMock(return_value={"item_id": 43})
        monkeypatch.setattr(work_debrief.work_launch, "launch_followup", launched)
        out = work_debrief.send_followup(fid, contexts=["aimyable", 7], slack=True)
        assert out["status"] == "sent"
        item_id = db.query_one("SELECT work_item_id FROM work_followups WHERE id = ?",
                               (fid,))["work_item_id"]
        launched.assert_called_once_with(item_id, "hi", contexts=["aimyable"], slack=True,
                                        agent="claude")

    def test_dismiss(self):
        fid = self._draft()
        assert work_debrief.dismiss_followup(fid)["status"] == "dismissed"
        assert "not dismissable" in work_debrief.dismiss_followup(fid)["error"]


class TestDeliverSlack:
    def _row(self, **kw):
        base = {"kind": "slack_message", "workspace": "aimyable",
                "recipient": "Sam", "draft": "PR ready: https://x/pr/1"}
        base.update(kw)
        return base

    def _patch_env(self, monkeypatch):
        monkeypatch.setattr(work_debrief.os.path, "isdir", lambda p: True)
        monkeypatch.setattr(work_debrief, "_known_workspaces", lambda: ["aimyable"])
        monkeypatch.setattr(work_debrief, "_resolve_recipient",
                            lambda w, r: {"id": "U123", "name": "Sam", "email": "s@x.com"})
        monkeypatch.setattr(work_debrief, "_slack_send",
                            lambda w, ch, t: {"ok": True, "ts": "1.2"})

    def test_happy_path(self, monkeypatch):
        self._patch_env(monkeypatch)
        out = work_debrief._deliver_slack(self._row())
        assert "U123" in out and "ts=1.2" in out

    def test_unknown_workspace_rejected(self, monkeypatch):
        self._patch_env(monkeypatch)
        import pytest
        with pytest.raises(RuntimeError, match="unknown workspace"):
            work_debrief._deliver_slack(self._row(workspace="evil"))

    def test_broadcast_mention_rejected(self, monkeypatch):
        self._patch_env(monkeypatch)
        import pytest
        with pytest.raises(RuntimeError, match="broadcast"):
            work_debrief._deliver_slack(self._row(draft="hey <!channel> look"))

    def test_channel_target_rejected(self, monkeypatch):
        self._patch_env(monkeypatch)
        import pytest
        with pytest.raises(RuntimeError, match="channel targets"):
            work_debrief._deliver_slack(self._row(recipient="C0123ABC"))


class TestScanner:
    def test_pending_excludes_debriefed_and_skipped(self):
        a = _done_item("pending a")
        b = _done_item("skipped b")
        work_debrief._record_debrief_event(b, "debrief_skipped", {})
        pending = work_debrief._pending_done_items()
        assert a in pending and b not in pending


class TestRetryPolicy:
    def test_failed_retries_until_cap(self):
        item_id = _done_item("retry me")
        for _ in range(work_debrief.MAX_FAILED_ATTEMPTS - 1):
            work_debrief._record_debrief_event(item_id, "debrief_failed", {})
        assert item_id in work_debrief._pending_done_items()
        work_debrief._record_debrief_event(item_id, "debrief_failed", {})
        assert item_id not in work_debrief._pending_done_items()

    def test_debrief_lock_rejects_concurrent(self):
        item_id = _done_item("locked")
        lock = work_debrief._item_lock(item_id)
        lock.acquire()
        try:
            assert "already running" in work_debrief.run_debrief(item_id)["error"]
        finally:
            lock.release()


class TestSupersede:
    def test_regenerate_dismisses_old_drafts(self, monkeypatch, tmp_path):
        t = tmp_path / "t.jsonl"
        t.write_text(json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "x"}]}}) + "\n")
        item_id = _done_item("supersede")
        with db.tx() as c:
            c.execute("UPDATE work_runs SET transcript_path = ? WHERE work_item_id = ?",
                      (str(t), item_id))
        monkeypatch.setattr(work_debrief, "_run_claude", lambda p: CLAUDE_OUT)
        work_debrief.run_debrief(item_id)
        work_debrief.run_debrief(item_id)
        fus = work_debrief.followups_for(item_id)
        drafts = [f for f in fus if f["status"] == "draft"]
        dismissed = [f for f in fus if f["status"] == "dismissed"]
        assert len(drafts) == 1 and len(dismissed) == 1


class TestLaunchContexts:
    def _instances(self, tmp_path):
        from unittest.mock import MagicMock
        root = tmp_path / "aim-root"
        root.mkdir()
        entry = MagicMock()
        entry.config = {"workspace": {"root": root, "repos": [{"name": "apipe"}, {"name": "portal"}]}}
        return {"aimyable": entry}, str(root)

    def test_context_block_lists_project_and_slack(self, monkeypatch, tmp_path):
        from services import work_launch
        instances, root = self._instances(tmp_path)
        monkeypatch.setattr(work_launch.runtime, "instances", lambda: instances)
        block = work_launch._context_block(["aimyable"], slack=True)
        assert "Project aimyable" in block and root in block
        assert "apipe, portal" in block
        assert "messages.jsonl" in block

    def test_context_block_lists_project_rules(self, monkeypatch, tmp_path):
        from services import work_launch
        instances, root = self._instances(tmp_path)
        monkeypatch.setattr(work_launch.runtime, "instances", lambda: instances)
        (tmp_path / "aim-root" / "CLAUDE.md").write_text("root rules")
        (tmp_path / "aim-root" / "apipe").mkdir()
        (tmp_path / "aim-root" / "apipe" / "CLAUDE.md").write_text("repo rules")
        block = work_launch._context_block(["aimyable"], slack=False)
        assert f"rules: {root}/CLAUDE.md, {root}/apipe/CLAUDE.md" in block
        assert "Read every file listed as rules above before you do anything else" in block

    def test_context_block_omits_rules_when_no_claude_md(self, monkeypatch, tmp_path):
        from services import work_launch
        instances, _ = self._instances(tmp_path)
        monkeypatch.setattr(work_launch.runtime, "instances", lambda: instances)
        block = work_launch._context_block(["aimyable"], slack=False)
        assert "rules:" not in block
        assert "Read every file listed as rules" not in block

    def test_context_block_empty_when_nothing_selected(self, monkeypatch, tmp_path):
        from services import work_launch
        instances, _ = self._instances(tmp_path)
        monkeypatch.setattr(work_launch.runtime, "instances", lambda: instances)
        assert work_launch._context_block([], slack=False) == ""

    def test_single_context_defaults_cwd(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock, patch
        from services import work_launch
        instances, root = self._instances(tmp_path)
        personal_root = tmp_path / "personal"
        personal_root.mkdir()
        personal = MagicMock()
        personal.config = {"workspace": {"root": personal_root}}
        instances["personal"] = personal
        captured = {}
        def fake_launch_claude(key, cwd, sid, ctx, first, config=None):
            captured["cwd"] = cwd
            captured["ctx"] = ctx
        with patch.object(work_launch.runtime, "instances", lambda: instances), \
             patch.object(work_launch.terminal, "launch_claude", fake_launch_claude), \
             patch.object(work_launch.terminal, "session_healthy",
                          return_value={"alive": True, "agent_running": True}), \
             patch.object(work_launch.threading, "Thread", MagicMock()):
            out = work_launch.launch("do a thing", contexts=["aimyable"], slack=True)
        assert "error" not in out
        assert captured["cwd"] == root
        assert "Project aimyable" in captured["ctx"]
        assert "Context sources" in captured["ctx"]
        item = db.query_one("SELECT contexts FROM work_items WHERE id = ?", (out["item_id"],))
        assert item["contexts"] == "aimyable,slack_int"

    def test_frshty_repo_always_listed(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock
        from services import work_launch
        instances, _ = self._instances(tmp_path)
        personal = MagicMock()
        personal.config = {"workspace": {"root": str(tmp_path)}}
        instances["personal"] = personal
        monkeypatch.setattr(work_launch.runtime, "instances", lambda: instances)
        keys = [e["key"] for e in work_launch.project_entries()]
        assert "frshty" in keys and "aimyable" in keys and "personal" in keys
        assert keys == sorted(keys)
