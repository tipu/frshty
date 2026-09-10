import importlib.util
import io
import json
import os
import re

import pytest

import core.correspondence as correspondence
from features.platforms import BitbucketPlatform, GitHubPlatform
from services import work_debrief, work_launch, work_store


OPEN = {"features": {"correspondence": True}}
CLOSED = {"features": {"correspondence": False}}


class TestGate:
    def test_an_instance_that_says_nothing_may_still_correspond(self):
        assert correspondence.allowed({}) is True
        assert correspondence.allowed(None) is True

    def test_the_flag_closes_the_gate(self):
        assert correspondence.allowed(CLOSED) is False


class TestBashCommands:
    @pytest.mark.parametrize("command", [
        "gh pr comment 12 --repo org/repo --body hi",
        "gh issue comment 3 --body hi",
        "gh pr review 12 --approve",
        "gh api repos/org/repo/pulls/1/comments -f body=hi",
        "gh api -X PATCH repos/org/repo/pulls/comments/9 -f body=hi",
        "glab mr note 4 -m hi",
        "curl -X POST https://hooks.slack.com/services/T/B/X -d '{}'",
        "curl https://slack.com/api/chat.postMessage -d text=hi",
        "python3 ~/Documents/dev/slack_int/send.py myws U123 hi",
        "curl -X POST https://api.bitbucket.org/2.0/repositories/o/r/pullrequests/1/comments",
        "echo body | mail -s subject someone@example.com",
        "sendmail someone@example.com < body.txt",
    ])
    def test_a_sending_command_is_refused(self, command):
        assert correspondence.bash_reason(command) == correspondence.DENY_REASON

    @pytest.mark.parametrize("command", [
        "gh pr view 12 --json title",
        "gh pr create --fill",
        "gh pr merge 12 --squash",
        "gh api repos/org/repo/pulls/1 --jq .title",
        "git push origin HEAD",
        "pytest tests/",
        "grep -rn comment services/",
    ])
    def test_reading_and_shipping_stay_open(self, command):
        assert correspondence.bash_reason(command) == ""


class TestToolNames:
    @pytest.mark.parametrize("tool", [
        "mcp__slack__send_message",
        "mcp__claude_ai_Gmail__create_draft",
        "mcp__linear__create_comment",
        "mcp__github__add_issue_comment",
        "mcp__github__create_pull_request_review",
        "mcp__atlassian__add_comment",
    ])
    def test_a_sending_tool_is_refused(self, tool):
        assert correspondence.tool_reason(tool) == correspondence.DENY_REASON

    @pytest.mark.parametrize("tool", [
        "Bash", "Read", "Edit", "Grep",
        "mcp__slack__search_messages",
        "mcp__slack__list_channel_messages",
        "mcp__github__get_pull_request",
        "mcp__github__get_pull_request_comments",
        "mcp__github__list_review_comments",
        "mcp__linear__list_issues",
        "mcp__github__create_pull_request",
    ])
    def test_reading_tools_stay_open(self, tool):
        assert correspondence.tool_reason(tool) == ""


class TestOnDisk:
    def test_a_config_it_cannot_read_closes_the_gate(self):
        assert correspondence.allowed_on_disk("no-such-instance") is False

    def test_the_personal_instance_is_closed(self):
        assert correspondence.allowed_on_disk("personal") is False


class TestSeams:
    def _platform(self, cls, config):
        p = cls.__new__(cls)
        p.config = config
        return p

    @pytest.mark.parametrize("cls", [GitHubPlatform, BitbucketPlatform])
    def test_a_closed_instance_posts_no_pr_comment(self, cls):
        p = self._platform(cls, CLOSED)
        assert p.post_pr_comment("repo", 1, "hi")["detail"] == correspondence.DENY_REASON
        assert p.edit_pr_comment("repo", 1, 2, "hi")["detail"] == correspondence.DENY_REASON

    def test_a_closed_instance_sends_no_slack_message(self, monkeypatch):
        monkeypatch.setattr(work_launch, "personal_config", lambda: CLOSED)
        sent = []
        monkeypatch.setattr(work_debrief, "_slack_send",
                            lambda *a: sent.append(a) or {"ok": True})
        row = {"workspace": "ws", "recipient": "Sam", "draft": "hi"}
        with pytest.raises(RuntimeError, match="correspondence gate"):
            work_debrief._deliver_slack(row)
        assert sent == []


class TestCodexFindings:
    """Every route a reviewer reproduced against the first version of the gate."""

    @pytest.mark.parametrize("command", [
        "gh -R org/repo pr comment 12 --body hi",
        "gh pr close 12 --comment hi",
        "gh issue close 12 --comment hi",
        "gh api graphql -f 'query=mutation { addComment(input:{subjectId:\"I\",body:\"hi\"}) { clientMutationId } }'",
        "cd ~/Documents/dev/slack_int && python3 send.py ws U123 hi",
        "python3 -c 'from slack_sdk import WebClient; WebClient().chat_postMessage(channel=\"U1\",text=\"hi\")'",
        "python3 -c 'from services.work_debrief import _slack_send; _slack_send(\"ws\",\"U1\",\"hi\")'",
        "echo hi | mail person@example.com",
        "curl --url smtp://localhost --mail-from me@x.com --mail-rcpt you@x.com --upload-file /tmp/m.eml",
        "curl -X POST https://gmail.googleapis.com/gmail/v1/users/me/messages/send --data @/tmp/m.json",
        "curl -X POST https://discord.com/api/webhooks/1/abc -d '{\"content\":\"hi\"}'",
        "curl -X POST https://api.telegram.org/bot1:x/sendMessage -d chat_id=1 -d text=hi",
        "curl -X POST http://localhost:7100/api/tickets/ABC-1/pr-comments/9/reply -d '{\"body\":\"hi\"}'",
        "curl -X POST http://localhost:7100/api/wizard/slack_ping -d '{\"text\":\"hi\"}'",
        "gh \\\n  pr comment 12 --body hi",
        "gh api repos/org/repo/pulls/1/comments -f body=hi",
        "curl -X POST https://api.linear.app/graphql -d @/tmp/q.json",
    ])
    def test_every_reproduced_send_is_refused(self, command):
        assert correspondence.bash_reason(command) == correspondence.DENY_REASON

    @pytest.mark.parametrize("command", [
        "gh api repos/org/repo/pulls/12/comments",
        "gh api repos/org/repo/issues/12/comments",
        "gh api repos/org/repo/pulls/12/reviews",
        "curl https://slack.com/api/conversations.history?channel=C1",
        "curl https://slack.com/api/conversations.replies?channel=C1",
        "curl https://api.bitbucket.org/2.0/repositories/o/r/pullrequests/12/comments",
    ])
    def test_reading_the_same_surface_stays_open(self, command):
        assert correspondence.bash_reason(command) == ""

    @pytest.mark.parametrize("tool", [
        "mcp__slack__list_dms",
        "mcp__gmail__list_drafts",
        "mcp__gmail__get_draft",
        "mcp__gmail__search_messages_by_sender",
        "mcp__gmail__get_email_address",
        "mcp__postmark__get_message",
        "mcp__sendgrid__get_stats",
    ])
    def test_a_read_is_not_a_write_because_of_a_substring(self, tool):
        assert correspondence.tool_reason(tool) == ""

    def test_a_string_false_is_not_permission(self):
        assert correspondence.allowed({"features": {"correspondence": "false"}}) is False
        assert correspondence.allowed({"features": {"correspondence": 0}}) is False

    def test_a_malformed_features_block_does_not_raise(self):
        assert correspondence.allowed({"features": "invalid"}) is True

    def test_the_continue_prompt_states_the_rule_with_no_registry(self, monkeypatch):
        monkeypatch.setattr(work_launch, "personal_config", lambda: None)
        prompt = work_store.continue_prompt()
        assert "Never send a message to a person" in prompt
        assert "unless the operator explicitly asked" not in prompt

    def test_the_slack_seam_is_closed_with_no_registry(self, monkeypatch):
        monkeypatch.setattr(work_launch, "personal_config", lambda: None)
        monkeypatch.setattr(work_debrief, "_slack_send", lambda *a: pytest.fail("sent"))
        with pytest.raises(RuntimeError, match="correspondence gate"):
            work_debrief._deliver_slack({"workspace": "ws", "recipient": "Sam", "draft": "hi"})


class TestHookIsWired:
    """A matcher the installer never writes means the gate never runs.

    The gate was first written with no matcher of its own, so every MCP tool
    reached the agent untouched while the unit tests passed: they called the
    matcher directly and never asked whether Claude Code would invoke it."""

    def _installer(self):
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "scripts", "install_work_hooks.py")
        spec = importlib.util.spec_from_file_location("install_hooks_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _matchers(self):
        installer = self._installer()
        return [e["matcher"] for e in installer._wanted_entries("PreToolUse", "cmd")]

    @pytest.mark.parametrize("tool", [
        "mcp__slack__send_message",
        "mcp__claude_ai_Gmail__create_draft",
        "mcp__linear__create_comment",
        "mcp__github__add_issue_comment",
        "mcp__atlassian__add_comment",
    ])
    def test_every_refused_tool_reaches_a_registered_matcher(self, tool):
        assert correspondence.tool_reason(tool) == correspondence.DENY_REASON
        assert any(re.fullmatch(m, tool) for m in self._matchers() if m), tool

    def test_bash_still_has_its_own_matcher(self):
        assert "Bash" in self._matchers()


class TestHook:
    def _hook(self):
        path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))), "scripts", "work_hook.py")
        spec = importlib.util.spec_from_file_location("work_hook_under_test", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_an_unreadable_database_still_denies_a_send(self, tmp_path, monkeypatch):
        """A hook that cannot read the database must not fall open.

        The session lookup used to run first and swallow every database error,
        so a locked database turned `gh pr comment` into a silent allow."""
        hook = self._hook()
        monkeypatch.setattr(hook, "DB_PATH", str(tmp_path / "no-such.db"))
        payload = {"session_id": "s1", "hook_event_name": "PreToolUse",
                   "tool_name": "Bash",
                   "tool_input": {"command": "gh pr comment 1 --body hi"}}
        monkeypatch.setattr(hook.sys, "stdin", io.StringIO(json.dumps(payload)))
        out = io.StringIO()
        monkeypatch.setattr(hook.sys, "stdout", out)
        assert hook.main() == 0
        decision = json.loads(out.getvalue())["hookSpecificOutput"]
        assert decision["permissionDecision"] == "deny"
        assert decision["permissionDecisionReason"] == correspondence.DENY_REASON

    def test_an_unreadable_database_does_not_gate_an_ordinary_command(
            self, tmp_path, monkeypatch):
        hook = self._hook()
        monkeypatch.setattr(hook, "DB_PATH", str(tmp_path / "no-such.db"))
        payload = {"session_id": "s1", "hook_event_name": "PreToolUse",
                   "tool_name": "Bash", "tool_input": {"command": "git push origin HEAD"}}
        monkeypatch.setattr(hook.sys, "stdin", io.StringIO(json.dumps(payload)))
        out = io.StringIO()
        monkeypatch.setattr(hook.sys, "stdout", out)
        assert hook.main() == 0
        assert out.getvalue() == ""

    def test_the_hook_denies_a_sending_command(self):
        hook = self._hook()
        assert hook._correspondence_deny(
            "Bash", {"command": "gh pr comment 1 --body hi"}) == correspondence.DENY_REASON
        assert hook._correspondence_deny("Bash", {"command": "git push"}) == ""
        assert hook._correspondence_deny(
            "mcp__slack__send_message", {}) == correspondence.DENY_REASON
