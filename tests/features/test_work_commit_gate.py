import json
import os
import subprocess
import sys

import core.db as db
from services import work_launch, work_store


SESSION_LINK = "https://claude.ai/code/session_01Rc4rHada8sYLmk4zYTW8Gu"


def _mkrun(objective="commit gate item"):
    item_id = work_store.create_item(objective)
    sid = f"sid-commit-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    return item_id, sid


def _gate_events(item_id):
    return db.query_all(
        "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = 'commit_gate' "
        "ORDER BY id", (item_id,))


def _heredoc_commit(message: str) -> str:
    return ('git commit -m "$(cat <<\'MSG\'\n' + message + '\nMSG\n)"')


class TestParseCommit:
    def test_detects_commit_forms(self):
        assert work_launch.parse_commit("git commit -m x") == {"chdir": ""}
        assert work_launch.parse_commit("git commit --amend --no-edit") == {"chdir": ""}
        assert work_launch.parse_commit("git -C /x commit -m x") == {"chdir": "/x"}
        assert work_launch.parse_commit("cd /x && git commit -m x") == {"chdir": "/x"}
        assert work_launch.parse_commit("git add -A && git commit -m x && git push") == {"chdir": ""}
        assert work_launch.parse_commit(_heredoc_commit("fix: thing")) == {"chdir": ""}

    def test_ignores_non_commit(self):
        assert work_launch.parse_commit("git push origin main") is None
        assert work_launch.parse_commit('echo "git commit"') is None
        assert work_launch.parse_commit("git log --oneline | grep commit") is None
        assert work_launch.parse_commit("ls -la") is None

    def test_ignores_a_commit_inside_a_heredoc_body(self):
        command = "cat > note.md <<'DOC'\nRun git commit -m x to save.\nDOC"
        assert work_launch.parse_commit(command) is None

    def test_push_parse_still_ignores_a_commit(self):
        assert work_launch.parse_push("git commit -m push") is None
        assert work_launch.parse_push("git add -A && git commit -m x && git push") == {"chdir": ""}


class TestAttributionMatch:
    def test_flags_agent_attribution(self):
        for text in (
            f"fix: thing\n\nClaude-Session: {SESSION_LINK}",
            "fix: thing\n\nCo-Authored-By: Claude <noreply@anthropic.com>",
            "fix: thing\n\n\U0001F916 Generated with Claude Code",
            "fix: thing\n\nGenerated with an LLM",
            "fix: thing (AI-generated)",
            "fix: thing\n\nCodex-Session: local",
        ):
            assert work_launch.attribution_match(text), text

    def test_leaves_subject_matter_alone(self):
        for text in (
            "Run a work item with codex instead of claude",
            "fix: claude_runner timeout handling",
            "Read general GitHub review bodies as PR comments",
            "Serve artifact images next to an HTML artifact",
            "Say in the launch context that no agent generated line belongs in a message",
            "feat: add anthropic model ids to the config example",
        ):
            assert work_launch.attribution_match(text) is None, text


class TestGateCommit:
    def test_allows_a_clean_commit(self):
        item_id, sid = _mkrun("clean commit")
        out = work_launch.gate_commit(sid, 'git commit -m "fix: drop the stale index"')
        assert out["decision"] == "allow"
        assert _gate_events(item_id) == []

    def test_allows_a_command_that_does_not_commit(self):
        item_id, sid = _mkrun("no commit")
        out = work_launch.gate_commit(sid, f"echo {SESSION_LINK}")
        assert out["decision"] == "allow"
        assert _gate_events(item_id) == []

    def test_denies_a_session_link_trailer(self):
        item_id, sid = _mkrun("session link commit")
        out = work_launch.gate_commit(
            sid, _heredoc_commit(f"fix: drop the stale index\n\nClaude-Session: {SESSION_LINK}"))
        assert out["decision"] == "deny"
        assert "session link" in out["reason"]
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "fail"
        assert payload["label"] == "agent session link"

    def test_denies_a_co_author_trailer(self):
        item_id, sid = _mkrun("co-author commit")
        out = work_launch.gate_commit(
            sid, _heredoc_commit("fix: x\n\nCo-Authored-By: Claude <noreply@anthropic.com>"))
        assert out["decision"] == "deny"
        assert len(_gate_events(item_id)) == 1

    def test_denies_attribution_in_a_message_file(self, tmp_path):
        item_id, sid = _mkrun("message file commit")
        (tmp_path / "msg.txt").write_text(f"fix: thing\n\nClaude-Session: {SESSION_LINK}\n")
        out = work_launch.gate_commit(sid, "git commit -F msg.txt", str(tmp_path))
        assert out["decision"] == "deny"
        assert len(_gate_events(item_id)) == 1

    def test_allows_a_clean_message_file(self, tmp_path):
        item_id, sid = _mkrun("clean message file")
        (tmp_path / "msg.txt").write_text("fix: drop the stale index\n")
        out = work_launch.gate_commit(sid, "git commit --file=msg.txt", str(tmp_path))
        assert out["decision"] == "allow"
        assert _gate_events(item_id) == []


class TestHookCommitGate:
    def _run_hook(self, payload):
        return subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=json.dumps(payload), capture_output=True, text=True, timeout=120,
            env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)},
        )

    def test_hook_denies_an_attributed_commit(self, tmp_path):
        item_id, sid = _mkrun("hook commit deny")
        command = _heredoc_commit(f"fix: x\n\nClaude-Session: {SESSION_LINK}")
        r = self._run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": str(tmp_path),
                            "tool_input": {"command": command}})
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert "commit gate" in out["hookSpecificOutput"]["permissionDecisionReason"]
        assert len(_gate_events(item_id)) == 1

    def test_hook_denies_before_the_push_gate_runs(self, tmp_path):
        item_id, sid = _mkrun("hook commit then push deny")
        command = _heredoc_commit(f"fix: x\n\nClaude-Session: {SESSION_LINK}") + " && git push"
        r = self._run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": str(tmp_path),
                            "tool_input": {"command": command}})
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert "commit gate" in out["hookSpecificOutput"]["permissionDecisionReason"]
        assert db.query_all(
            "SELECT id FROM work_events WHERE work_item_id = ? AND kind = 'push_gate'",
            (item_id,)) == []

    def test_hook_allows_a_clean_commit(self, tmp_path):
        item_id, sid = _mkrun("hook commit allow")
        r = self._run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": str(tmp_path),
                            "tool_input": {"command": 'git commit -m "fix: drop the stale index"'}})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == ""
        assert _gate_events(item_id) == []

    def test_hook_leaves_a_foreign_session_alone(self, tmp_path):
        command = _heredoc_commit(f"fix: x\n\nClaude-Session: {SESSION_LINK}")
        r = self._run_hook({"session_id": "sid-not-a-work-run",
                            "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": str(tmp_path),
                            "tool_input": {"command": command}})
        assert r.returncode == 0
        assert r.stdout.strip() == ""
