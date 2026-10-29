"""The board waits on pull request checks for an agent and resumes it."""
import json
import subprocess
from datetime import datetime, timedelta, timezone

import pytest

import core.db as db
from services import work_ci_wait, work_store


def _parked(tail="Pushed the fix.\nwaiting on PR #42 checks"):
    item_id = work_store.create_item("ci wait item")
    sid = f"sid-ci-wait-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    with db.tx() as c:
        c.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
    outcome = work_store.maybe_autocontinue(sid, "", tail=tail)
    return item_id, outcome


def _item(item_id):
    return db.query_one("SELECT state, stop_reason, continues_used FROM work_items "
                        "WHERE id = ?", (item_id,))


def _events(item_id, kind):
    return [json.loads(r["payload"]) for r in db.query_all(
        "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = ? ORDER BY id",
        (item_id, kind))]


class _Checks:
    def __init__(self, checks):
        self.checks = checks
        self.read = []

    def get_pr_checks(self, repo, pr_id):
        self.read.append((repo, pr_id))
        return self.checks


@pytest.fixture
def board(monkeypatch):
    state = {"checks": _Checks([]), "sent": [], "send_ok": True}
    monkeypatch.setattr(work_ci_wait, "_repo_slug", lambda cwd: "owner/repo")
    monkeypatch.setattr(work_ci_wait, "_platform", lambda env_key, slug: state["checks"])
    monkeypatch.setattr(work_store, "tmux_send",
                        lambda key, text: state["sent"].append((key, text)) or state["send_ok"])
    return state


def _sent(board, item_id):
    return [text for key, text in board["sent"] if key == f"work-{item_id}"]


def _sweep(item_id, now=None):
    return [a for a in work_ci_wait.sweep_ci_waits(now) if a["id"] == item_id]


class TestTurnEnd:
    def test_a_wait_line_parks_the_item_without_a_continuation(self):
        item_id, outcome = _parked()
        assert outcome == "ci_wait"
        item = _item(item_id)
        assert item["state"] == "waiting_external"
        assert item["stop_reason"] == "Waiting on PR #42 checks"
        assert item["continues_used"] == 0
        assert _events(item_id, "ci_wait") == [{"pr": 42}]

    def test_the_idle_notification_after_the_turn_keeps_the_wait(self, tmp_path):
        item_id, _ = _parked()
        sid = f"sid-ci-wait-{item_id}"
        transcript = tmp_path / "t.jsonl"
        transcript.write_text(json.dumps({"type": "assistant", "message": {"content": [
            {"type": "text", "text": "Pushed.\nwaiting on PR #42 checks"}]}}) + "\n")
        work_store.record_event(sid, "Notification", {
            "message": "Claude is waiting for your input",
            "transcript_path": str(transcript)})
        assert _item(item_id)["state"] == "waiting_external"
        assert work_store.maybe_autocontinue(sid, str(transcript)) == "not_applicable"
        assert len(_events(item_id, "ci_wait")) == 1

    def test_a_turn_without_the_line_still_continues(self, monkeypatch):
        monkeypatch.setattr(work_store, "tmux_send", lambda key, text: True)
        item_id, outcome = _parked(tail="Pushed the fix to PR #42.")
        assert outcome == "continued"
        assert _events(item_id, "ci_wait") == []


class TestSweep:
    def test_pending_checks_keep_the_wait(self, board):
        item_id, _ = _parked()
        board["checks"] = _Checks([{"name": "test", "state": "IN_PROGRESS"}])
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []
        assert _item(item_id)["state"] == "waiting_external"

    def test_passing_checks_resume_the_agent_once(self, board):
        item_id, _ = _parked()
        board["checks"] = _Checks([{"name": "test", "state": "SUCCESS"}])
        assert [a["result"] for a in _sweep(item_id)] == ["every check passed"]
        assert ("owner/repo", 42) in board["checks"].read
        [text] = _sent(board, item_id)
        assert "PR #42" in text and "every check passed" in text
        assert _item(item_id)["state"] == "agent_working"
        assert _events(item_id, "ci_wait_done") == [{"pr": 42, "result": "every check passed"}]
        assert _sweep(item_id) == []
        assert len(_sent(board, item_id)) == 1

    def test_a_failed_check_resumes_the_agent_with_its_name(self, board):
        item_id, _ = _parked()
        board["checks"] = _Checks([{"name": "lint", "state": "FAILURE"},
                                   {"name": "test", "state": "SUCCESS"}])
        [action] = _sweep(item_id)
        assert action["result"] == "these checks failed: lint"
        assert "these checks failed: lint" in _sent(board, item_id)[0]

    def test_a_failed_send_is_tried_again(self, board):
        item_id, _ = _parked()
        board["checks"] = _Checks([{"name": "test", "state": "SUCCESS"}])
        board["send_ok"] = False
        assert _sweep(item_id) == []
        assert _item(item_id)["state"] == "waiting_external"
        board["send_ok"] = True
        assert len(_sweep(item_id)) == 1

    def test_no_checks_resume_the_agent_after_the_grace(self, board):
        item_id, _ = _parked()
        board["checks"] = _Checks([])
        assert _sweep(item_id) == []
        later = datetime.now(timezone.utc) + timedelta(
            minutes=work_ci_wait.CI_WAIT_NO_CHECKS_MINUTES + 1)
        [action] = _sweep(item_id, later)
        assert "no checks were reported" in action["result"]

    def test_an_unreadable_check_list_gives_up_after_the_cap(self, board):
        item_id, _ = _parked()
        board["checks"] = _Checks(None)
        assert _sweep(item_id) == []
        later = datetime.now(timezone.utc) + timedelta(
            minutes=work_ci_wait.CI_WAIT_MAX_MINUTES + 1)
        [action] = _sweep(item_id, later)
        assert "still unknown" in action["result"]

    def test_an_item_the_operator_took_back_is_left_alone(self, board):
        item_id, _ = _parked()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (item_id,))
        board["checks"] = _Checks([{"name": "test", "state": "SUCCESS"}])
        assert _sweep(item_id) == []
        assert _sent(board, item_id) == []


class TestRepoSlug:
    @pytest.mark.parametrize("url", [
        "git@github.com:owner/repo.git",
        "https://github.com/owner/repo",
        "https://github.com/owner/repo.git",
        "ssh://git@github-alias/owner/repo.git",
    ])
    def test_reads_owner_and_name_from_the_origin(self, tmp_path, url):
        subprocess.run(["git", "init", "-q", str(tmp_path)], check=True)
        subprocess.run(["git", "-C", str(tmp_path), "remote", "add", "origin", url], check=True)
        assert work_ci_wait._repo_slug(str(tmp_path)) == "owner/repo"

    def test_a_directory_without_an_origin_has_no_slug(self, tmp_path):
        assert work_ci_wait._repo_slug(str(tmp_path)) == ""

