"""A fix commit must say what it changed, and must never say nothing at all.

The failure this guards: every commit that answered a review carried the same
subject, `fix: address review comment on <path>`. The subject now comes from
the pending diff, so the guard has two halves — the model's sentence is used
when it is usable, and the old wording is still used when it is not.
"""
import subprocess
from unittest.mock import patch

import pytest

from core import commit_message
from core import git_util


def _repo(path):
    path.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=str(path), check=True)
    subprocess.run(["git", "config", "user.email", "t@t"], cwd=str(path), check=True)
    subprocess.run(["git", "config", "user.name", "t"], cwd=str(path), check=True)
    (path / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "add", "-A"], cwd=str(path), check=True)
    subprocess.run(["git", "commit", "-q", "-m", "base"], cwd=str(path), check=True)
    return path


class TestCleanSubject:
    @pytest.mark.parametrize("raw,expected", [
        ("Guard the empty reply list", "guard the empty reply list"),
        ("fix: guard the empty reply list", "guard the empty reply list"),
        ('"guard the empty reply list."', "guard the empty reply list"),
        ("Subject:\nguard the empty reply list", "guard the empty reply list"),
        ("`guard the empty reply list`", "guard the empty reply list"),
        ("- guard the empty reply list", "guard the empty reply list"),
        ("1. guard the empty reply list", "guard the empty reply list"),
        ("```\nguard the empty reply list\n```", "guard the empty reply list"),
    ])
    def test_reduces_a_reply_to_one_lower_case_sentence(self, raw, expected):
        assert commit_message.clean_subject(raw) == expected

    @pytest.mark.parametrize("raw", [
        None,
        "",
        "   \n\n  ",
        "[guard] skipped LLM invocation: recent usage-limit response (limit); cooldown 90s remaining",
        "ok",
    ])
    def test_rejects_output_that_is_not_a_subject(self, raw):
        assert commit_message.clean_subject(raw) == ""

    def test_truncates_on_a_word_boundary(self):
        raw = "guard the empty reply list so the poller stops raising on an empty page"
        got = commit_message.clean_subject(raw)
        assert commit_message.MIN_SUBJECT_CHARS <= len(got) <= commit_message.SUBJECT_LIMIT
        assert got.startswith("guard the empty reply list")
        assert raw.startswith(got)
        assert not got.endswith(" ")

    @pytest.mark.parametrize("raw", [
        "Here is the subject",
        "Here is the subject: restore the row count",
        "made with claude, restore the row count",
        "[skip ci] restore the row count",
        "restore the row count, fixes #123",
        "see https://example.com/report for the fix",
        "1. [guard] skipped LLM invocation: cooldown 90s remaining",
        "-- restore the row count",
    ])
    def test_rejects_output_that_must_never_reach_a_commit(self, raw):
        assert commit_message.clean_subject(raw) == ""

    @pytest.mark.parametrize("raw,expected", [
        ("Fix the crash. Remove the guard next", "fix the crash"),
        ("Handle errno. Preserve the fallback", "handle errno"),
        ("Restore the count! Remove the guard", "restore the count"),
        ("guard empty pages, e.g. the first poll after a reset",
         "guard empty pages, e.g. the first poll after a reset"),
    ])
    def test_keeps_one_sentence(self, raw, expected):
        assert commit_message.clean_subject(raw) == expected

    def test_strips_a_label_behind_a_type_prefix(self):
        assert commit_message.clean_subject("fix: Subject: restore the row count") == \
            "restore the row count"

    @pytest.mark.parametrize("raw", [
        "restore the row count, fixed #123",
        "restore the row count, closed owner/repo#7",
        "[ci skip] restore the row count",
        "restore the row count [skip-ci]",
    ])
    def test_rejects_every_directive_form(self, raw):
        assert commit_message.clean_subject(raw) == ""

    def test_strips_a_bidi_control_and_a_lone_surrogate(self):
        got = commit_message.clean_subject("restore the row\u202e count\ud800 now")
        assert got == "restore the row count now"
        got.encode("utf-8")

    def test_strips_a_control_character(self):
        got = commit_message.clean_subject("restore\x00 the row\tcount")
        assert got == "restore the row count"
        assert "\x00" not in got

    def test_keeps_only_the_first_sentence(self):
        assert commit_message.clean_subject(
            "Restore the row count. It was dropped in the last refactor.") == \
            "restore the row count"

    def test_an_abbreviation_does_not_cut_the_sentence(self):
        assert commit_message.clean_subject(
            "guard empty pages, e.g. the first poll after a reset") == \
            "guard empty pages, e.g. the first poll after a reset"

    def test_strips_a_subject_label(self):
        assert commit_message.clean_subject("Subject: restore the row count") == \
            "restore the row count"

    def test_a_language_tagged_fence_is_not_the_subject(self):
        raw = "```commit-message\nrestore the row count\n```"
        assert commit_message.clean_subject(raw) == "restore the row count"


class TestSafeMessage:
    def test_a_long_single_token_keeps_the_description(self):
        long_token = "x" * 70
        got = commit_message.safe_message(f"fix: DEV-635 {long_token}")
        assert len(got) <= commit_message.MESSAGE_LIMIT
        assert got != "fix: DEV-635", (
            "cutting at the only space leaves a message that describes "
            f"nothing; got {got!r}")
        assert got.startswith("fix: DEV-635 x")

    def test_a_directive_in_the_fallback_never_reaches_a_commit(self):
        got = commit_message.safe_message(
            "fix: address review comment on src/[skip ci]/report.py")
        assert got == commit_message.DEFAULT_SUBJECT

    def test_an_empty_message_becomes_the_default(self):
        assert commit_message.safe_message("  \n ") == commit_message.DEFAULT_SUBJECT


class TestSubjectPrefix:
    def test_keeps_the_conventional_commit_type(self):
        assert commit_message.subject_prefix("fix: address review comment on a.py") == "fix: "

    def test_keeps_the_ticket_key_it_is_given(self):
        assert commit_message.subject_prefix(
            "fix: address reported issue", "DEV-635") == "fix: DEV-635 "

    def test_no_type_gives_no_prefix(self):
        assert commit_message.subject_prefix("address review comment") == ""

    def test_a_path_is_never_read_as_a_ticket_key(self):
        assert commit_message.subject_prefix(
            "fix: address review comment on src/(API-2)/report.py") == "fix: "


class TestCommitSubject:
    def test_uses_the_model_sentence_for_a_pending_change(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        with patch("core.commit_message.run_haiku", return_value="Raise the counter start value") as haiku:
            got = commit_message.commit_subject(repo, "fix: address review comment on a.py", "bump it")
        assert got == "fix: raise the counter start value"
        assert "bump it" in haiku.call_args[0][0]

    def test_sees_a_change_that_is_already_staged(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        subprocess.run(["git", "add", "-A"], cwd=str(repo), check=True)
        with patch("core.commit_message.run_haiku", return_value="Raise the counter start value"):
            got = commit_message.commit_subject(repo, "fix: address review comment on a.py")
        assert got == "fix: raise the counter start value"

    def test_the_ticket_key_is_kept_exactly_once_in_upper_case(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        with patch("core.commit_message.run_haiku",
                   return_value="DEV-635 raise the counter start value"):
            got = commit_message.commit_subject(
                repo, "fix: address tri-review findings for DEV-635",
                ticket_key="DEV-635")
        assert got == "fix: DEV-635 raise the counter start value"

    def test_a_longer_key_does_not_suppress_the_real_one(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        with patch("core.commit_message.run_haiku",
                   return_value="port the DEV-6350 guard into the poller"):
            got = commit_message.commit_subject(
                repo, "fix: address tri-review findings for DEV-635",
                ticket_key="DEV-635")
        assert got.startswith("fix: DEV-635 port the dev-6350 guard")

    def test_a_long_subject_keeps_the_key_and_the_description(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        with patch("core.commit_message.run_haiku",
                   return_value="return none when the paginated items list comes back empty"):
            got = commit_message.commit_subject(
                repo, "fix: address tri-review findings for DEV-635",
                ticket_key="DEV-635")
        assert len(got) <= commit_message.MESSAGE_LIMIT
        assert got.startswith("fix: DEV-635 return none when")

    def test_clean_worktree_never_calls_the_model(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        with patch("core.commit_message.run_haiku") as haiku:
            got = commit_message.commit_subject(repo, "fix: address review comment on a.py")
        haiku.assert_not_called()
        assert got == "fix: address review comment on a.py"

    def test_unusable_model_output_keeps_the_fallback(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        with patch("core.commit_message.run_haiku", return_value=""):
            got = commit_message.commit_subject(repo, "fix: address review comment on a.py")
        assert got == "fix: address review comment on a.py"

    def test_model_failure_keeps_the_fallback(self, tmp_path):
        repo = _repo(tmp_path / "repo")
        (repo / "a.py").write_text("x = 2\n")
        with patch("core.commit_message.run_haiku", side_effect=RuntimeError("no provider")):
            got = commit_message.commit_subject(repo, "fix: address review comment on a.py")
        assert got == "fix: address review comment on a.py"

    def test_unreadable_repo_keeps_the_fallback(self, tmp_path):
        plain = tmp_path / "plain"
        plain.mkdir()
        with patch("core.commit_message.run_haiku") as haiku:
            got = commit_message.commit_subject(plain, "fix: address review comment on a.py")
        haiku.assert_not_called()
        assert got == "fix: address review comment on a.py"


class TestWorkspaceCommitWiring:
    """The ticket pipeline asks for a specific subject only where the commit
    answers a review or a bug report."""

    def _workspace_repo(self, ticket_dir):
        (ticket_dir / "workspace").mkdir(parents=True)
        return _repo(ticket_dir / "workspace" / "app")

    def _committed(self, repo):
        return (git_util.CommitOutcome(status="committed", phase="git_commit",
                                       repo=str(repo), exit_code=0, output="",
                                       before_head="a", after_head="b"), "committed")

    def test_describe_replaces_the_step_wording(self, tmp_path):
        from core.tasks import tickets as T
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = self._workspace_repo(ticket_dir)
        (repo / "a.py").write_text("x = 2\n")

        with patch("core.commit_message.run_haiku", return_value="Raise the counter start value"), \
             patch("core.tasks.tickets.commit_repo_changes") as mock_commit:
            mock_commit.return_value = self._committed(repo)
            T._commit_workspace_changes(ticket_dir, "PROJ-1", describe=True)

        assert mock_commit.call_args[0][2] == "fix: PROJ-1 raise the counter start value"

    def test_describe_stages_an_untracked_file_before_reading_the_diff(self, tmp_path):
        from core.tasks import tickets as T
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = self._workspace_repo(ticket_dir)
        (repo / "guard.py").write_text("def guard(items):\n    return items or None\n")

        with patch("core.commit_message.run_haiku", return_value="Add the guard helper") as haiku, \
             patch("core.tasks.tickets.commit_repo_changes") as mock_commit:
            mock_commit.return_value = self._committed(repo)
            T._commit_workspace_changes(ticket_dir, "PROJ-1", describe=True)

        prompt = haiku.call_args[0][0]
        assert "return items or None" in prompt, (
            "a new file must be staged before the diff is read, or the model "
            f"only sees its name; got prompt: {prompt!r}"
        )

    def test_without_describe_the_step_wording_stands(self, tmp_path):
        from core.tasks import tickets as T
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = self._workspace_repo(ticket_dir)
        (repo / "a.py").write_text("x = 2\n")

        with patch("core.commit_message.run_haiku") as haiku, \
             patch("core.tasks.tickets.commit_repo_changes") as mock_commit:
            mock_commit.return_value = self._committed(repo)
            T._commit_workspace_changes(ticket_dir, "PROJ-1",
                                        message="chore: finalize PROJ-1 worktree for PR")

        haiku.assert_not_called()
        assert mock_commit.call_args[0][2] == "chore: finalize PROJ-1 worktree for PR"


class TestPromptRule:
    """The fixing agent can commit on its own in the ticket paths, and those
    commits never reach `commit_subject`. The rule has to be in the prompt or
    the agent writes the subject it likes."""

    def test_the_rule_says_one_lower_case_sentence(self):
        rule = commit_message.COMMIT_SUBJECT_RULE.lower()
        assert "one sentence" in rule
        assert "lower case" in rule
        assert "not name any tool or model" in rule

    def test_the_bug_report_prompts_carry_the_rule(self):
        from core.tasks import tickets as T
        assert commit_message.COMMIT_SUBJECT_RULE in T.GENERIC_BUG_REPORT_PROMPT
        detailed = T._bug_report_prompt("PROJ-1", "a summary",
                                        [{"author": "r", "body": "it broke"}])
        assert commit_message.COMMIT_SUBJECT_RULE in detailed

    def test_the_review_fix_prompt_carries_the_rule(self, tmp_path):
        from core.tasks import tickets as T
        from core.tasks.registry import TaskContext
        ctx = TaskContext(
            instance_key="aimyable", ticket_key="PROJ-1", task="fix_review_findings",
            payload={}, job_id=0, triggering_event_id=None,
            config={"workspace": {"root": tmp_path, "tickets_dir": "tickets"},
                    "_base_url": "http://localhost:8000"},
            registry=None, now=None,
        )
        (tmp_path / "tickets" / "PROJ-1-x").mkdir(parents=True)
        with patch("core.state.load_ticket", return_value={"slug": "PROJ-1-x"}), \
             patch("core.tasks.tickets._claim_session", return_value=("s", False)), \
             patch("core.tasks.tickets._capture_repo_heads", return_value={}), \
             patch("core.tasks.tickets.run_claude_code", return_value=None) as fixer:
            T.fix_review_findings(ctx)
        assert commit_message.COMMIT_SUBJECT_RULE in fixer.call_args[0][0]
