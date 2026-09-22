"""The scope gate corrects the branch instead of describing it.

A FAIL verdict names the changes that do not serve the ticket. frshty used to
stop there: the PR was held and the operator had to take those changes off the
branch by hand before anything moved. DEV-738 sat that way. These cover the
correction pass that replaced the message, and the proof it forces afterwards,
because a branch that changed under a recorded proof is no longer proved.
"""
from types import SimpleNamespace
from unittest.mock import patch

import core.state as state
from core.consensus_scope import MAX_SCOPE_FIX_ATTEMPTS
from core.tasks.registry import TaskContext, TaskResult, run_task
from core.tasks.tickets import (
    _SCOPE_FIX_PROMPT, _branch_moved_off_the_verdict, _scope_fix_target,
    CommitBlocked, fix_scope_findings,
)
from features.tickets import _scope_fix_incomplete

KEY = "DEV-738"
SLUG = "DEV-738-paced-stream"

REPORT = "\n".join([
    "# Consensus scope review", "",
    f"Ticket: {KEY}",
    "Votes: agy=FAIL, codex=FAIL", "",
    "## agy", "", "Offending changes:", "",
    "- `saas-dashboard`: paced-stream catch-up fix at src/MessageLog.tsx:120",
    "- `django-drf-app`: root admin route at src/acme/urls.py:36", "",
    "SCOPE VERDICT: FAIL", ""])

FINDINGS = [
    "`saas-dashboard`: paced-stream catch-up fix at src/MessageLog.tsx:120",
    "`django-drf-app`: root admin route at src/acme/urls.py:36",
]


def _ticket_dir(tmp_path):
    docs = tmp_path / "tickets" / SLUG / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    (tmp_path / "tickets" / SLUG / "workspace").mkdir(parents=True, exist_ok=True)
    return tmp_path / "tickets" / SLUG


def _ctx(tmp_path):
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "features": {"scope_review": True},
        "_base_url": "http://localhost:8000",
    }
    return TaskContext(
        instance_key="test", ticket_key=KEY, task="fix_scope_findings",
        payload={}, job_id=0, triggering_event_id=None, config=config,
        registry=None, now=None,
    )


def _seed(tmp_path, tmp_state, **overrides):
    ticket_dir = _ticket_dir(tmp_path)
    (ticket_dir / "docs" / "scope-review.md").write_text(REPORT)
    ts = {"status": "pr_ready", "slug": SLUG, "branch": SLUG,
          "scope_review": {"fingerprint": "r:abc", "verdict": "fail",
                           "reason": "votes agy=FAIL, codex=FAIL"},
          "llm_sessions": {"prove": "s-1", "scope_review": "s-2"},
          "pr_descriptions_generated_at": "2026-09-11T00:00:00+00:00"}
    ts.update(overrides)
    state.save_ticket(KEY, ts)
    return ticket_dir


def _moved_heads():
    return iter([{"saas-dashboard": "aaa"}, {"saas-dashboard": "bbb"}])


def _fingerprints(*values):
    """Stand in for the branch-diff digest. Each call takes the next value and
    the last one repeats, so a test says what the branch looked like before the
    correction and what it looks like after."""
    seq = list(values)

    def _fp(config, ts):
        return seq.pop(0) if len(seq) > 1 else seq[0]
    return _fp


REVIEWED = "r:abc"
CORRECTED = "r:corrected"


class TestTheFixerIsToldWhatToRemove:
    def test_every_named_change_reaches_the_prompt(self):
        prompt = _SCOPE_FIX_PROMPT.format(
            findings="\n".join(f"- {f}" for f in FINDINGS))
        for finding in FINDINGS:
            assert finding in prompt
        assert "docs/scope-review.md" in prompt
        assert "docs/scope-fix.md" in prompt

    def test_the_prompt_carries_the_findings_of_this_report(self, tmp_path, tmp_state):
        ticket_dir = _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done") as rc:
            heads = _moved_heads()
            fix_scope_findings(_ctx(tmp_path))
        prompt = rc.call_args.args[0]
        for finding in FINDINGS:
            assert finding in prompt
        assert rc.call_args.kwargs["cwd"] == ticket_dir


class TestTheCorrectionForcesAFreshProof:
    def test_a_run_that_commits_sends_the_ticket_back_to_prove(self, tmp_path, tmp_state):
        ticket_dir = _seed(tmp_path, tmp_state)
        (ticket_dir / "docs" / "proof.md").write_text("PROOF: DEMOED\n")
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"):
            heads = _moved_heads()
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "ok"
        assert result.artifacts["repos"] == ["saas-dashboard"]
        assert not (ticket_dir / "docs" / "proof.md").exists()
        assert (ticket_dir / "docs" / "proof.prev.md").exists()
        ts = state.load_ticket(KEY)
        assert "prove" not in ts["llm_sessions"]
        assert ts["llm_sessions"]["scope_review"] == "s-2"
        assert "pr_descriptions_generated_at" not in ts
        for finding in FINDINGS:
            assert finding in ts["proof_feedback"]

    def test_a_run_that_commits_nothing_fails_and_keeps_the_proof(self, tmp_path, tmp_state):
        ticket_dir = _seed(tmp_path, tmp_state)
        (ticket_dir / "docs" / "proof.md").write_text("PROOF: DEMOED\n")
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   return_value={"saas-dashboard": "aaa"}), \
             patch("core.tasks.tickets._commit_workspace_changes", return_value=[]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"):
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "failed"
        assert "unchanged" in result.reason
        assert (ticket_dir / "docs" / "proof.md").exists()
        assert "proof_feedback" not in state.load_ticket(KEY)

    def test_a_dead_claude_run_leaves_the_proof_alone(self, tmp_path, tmp_state):
        ticket_dir = _seed(tmp_path, tmp_state)
        (ticket_dir / "docs" / "proof.md").write_text("PROOF: DEMOED\n")
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets._capture_repo_heads", return_value={}), \
             patch("core.tasks.tickets.run_claude_code", return_value=None):
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "failed"
        assert (ticket_dir / "docs" / "proof.md").exists()


class TestTheCorrectionIsBounded:
    def test_the_budget_stops_a_further_pass(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state,
              scope_fix={"attempts": MAX_SCOPE_FIX_ATTEMPTS})
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets.run_claude_code") as rc:
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "skipped"
        assert f"already ran {MAX_SCOPE_FIX_ATTEMPTS} times" in result.reason
        rc.assert_not_called()

    def test_a_pass_that_dies_still_spends_its_attempt(self, tmp_path, tmp_state):
        """An attempt counted only on success loops: the run dies, the verdict
        stands, and the dispatcher queues the same pass again."""
        _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets._capture_repo_heads", return_value={}), \
             patch("core.tasks.tickets.run_claude_code", return_value=None):
            fix_scope_findings(_ctx(tmp_path))
        assert state.load_ticket(KEY)["scope_fix"]["attempts"] == 1

    def test_a_report_that_names_nothing_is_not_guessed_at(self, tmp_path, tmp_state):
        ticket_dir = _seed(tmp_path, tmp_state)
        (ticket_dir / "docs" / "scope-review.md").write_text(
            "Votes: agy=FAIL\n\n## agy\n\nno list here\n\nSCOPE VERDICT: FAIL\n")
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets.run_claude_code") as rc:
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "failed"
        assert result.hard_block is True
        rc.assert_not_called()

    def test_a_verdict_that_no_longer_describes_the_branch_is_not_acted_on(
            self, tmp_path, tmp_state):
        """The findings name file and line. Work committed after the verdict
        moves those lines, so the fixer would strip code no reviewer read."""
        _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints("r:moved-on")), \
             patch("core.tasks.tickets.run_claude_code") as rc:
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "skipped"
        assert "fresh review" in result.reason
        rc.assert_not_called()
        assert "scope_fix" not in state.load_ticket(KEY)


class TestTheStatusMovesOnlyOnARecordedCorrection:
    """run_task, not the body: the move back to proving is the framework's,
    and it must come after the postcondition that says what was removed."""

    def _run(self, tmp_path, write_record):
        ticket_dir = _ticket_dir(tmp_path)

        def _fix(prompt, **kwargs):
            if write_record:
                (ticket_dir / "docs" / "scope-fix.md").write_text(
                    "- removed src/MessageLog.tsx:120\n- removed src/acme/urls.py:36\n")
            return "done"

        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", side_effect=_fix):
            heads = _moved_heads()
            return run_task(_ctx(tmp_path))

    def test_a_recorded_correction_moves_the_ticket_to_proving(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state)
        result = self._run(tmp_path, write_record=True)
        assert result.status == "ok", result.reason
        assert state.load_ticket(KEY)["status"] == "proving"

    def test_a_correction_before_the_proof_keeps_the_ticket_in_proving(
            self, tmp_path, tmp_state):
        """The gate runs before the proof. A ticket corrected there has no PR
        to push the removal to; it stays in proving, the gate re-runs against
        the corrected branch, and the proof follows the pass."""
        _seed(tmp_path, tmp_state, status="proving")
        result = self._run(tmp_path, write_record=True)
        assert result.status == "ok", result.reason
        assert state.load_ticket(KEY)["status"] == "proving"
        assert result.artifacts["reproved"] is True
        assert "push_failed" not in result.artifacts

    def test_a_correction_with_no_record_holds_the_ticket(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state)
        result = self._run(tmp_path, write_record=False)
        assert result.status == "failed"
        assert "scope-fix.md" in result.reason
        assert state.load_ticket(KEY)["status"] == "pr_ready"

    def test_a_stale_record_cannot_satisfy_the_gate(self, tmp_path, tmp_state):
        ticket_dir = _seed(tmp_path, tmp_state)
        (ticket_dir / "docs" / "scope-fix.md").write_text("- removed, last time\n")
        result = self._run(tmp_path, write_record=False)
        assert result.status == "failed"
        assert state.load_ticket(KEY)["status"] == "pr_ready"
        assert (ticket_dir / "docs" / "scope-fix.prev.md").exists()

    def test_an_override_mid_run_sends_the_removal_to_the_open_pr(self, tmp_path, tmp_state):
        """The modal that queues this correction also offers the override. An
        operator who takes it has an open PR by the time the correction lands,
        and pulling that ticket into proving would strand the review."""
        ticket_dir = _seed(tmp_path, tmp_state)
        (ticket_dir / "docs" / "proof.md").write_text("PROOF: DEMOED\n")

        def _fix(prompt, **kwargs):
            (ticket_dir / "docs" / "scope-fix.md").write_text("- removed src/a.ts\n")
            state.transition_ticket(KEY, "in_review",
                                    prs=[{"repo": "saas-dashboard", "id": 1}])
            return "done"

        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", side_effect=_fix), \
             patch("core.tasks.tickets._push_to_open_prs",
                   return_value=(["saas-dashboard"], [])) as push:
            heads = _moved_heads()
            result = run_task(_ctx(tmp_path))
        assert result.status == "ok", result.reason
        assert state.load_ticket(KEY)["status"] == "in_review"
        assert (ticket_dir / "docs" / "proof.md").exists()
        push.assert_called_once()

    def test_a_removal_that_cannot_reach_the_pr_is_not_reported_as_done(
            self, tmp_path, tmp_state):
        """The corrected code lives in the worktree, and the scope review reads
        the worktree. A push that failed would let a fresh PASS stand over a PR
        branch that still carries the changes the reviewers named."""
        ticket_dir = _ticket_dir(tmp_path)
        _seed(tmp_path, tmp_state)

        def _fix(prompt, **kwargs):
            (ticket_dir / "docs" / "scope-fix.md").write_text("- removed src/a.ts\n")
            state.transition_ticket(KEY, "in_review",
                                    prs=[{"repo": "saas-dashboard", "id": 1}])
            return "done"

        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", side_effect=_fix), \
             patch("core.tasks.tickets._push_to_open_prs",
                   return_value=([], ["saas-dashboard"])):
            heads = _moved_heads()
            result = run_task(_ctx(tmp_path))
        assert result.status == "failed"
        assert "did not reach the PR branch" in result.reason
        assert "did not reach the PR branch" in _scope_fix_incomplete(
            state.load_ticket(KEY))

    def test_a_ticket_in_review_is_not_corrected_by_this_task(self, tmp_path, tmp_state):
        """An open PR is a different correction: the fix has to reach the
        branch the reviewer is reading, not a worktree behind a fresh proof."""
        _seed(tmp_path, tmp_state, status="in_review")
        result = self._run(tmp_path, write_record=True)
        assert result.status == "skipped"
        assert state.load_ticket(KEY)["status"] == "in_review"


class TestARestartCannotCertifyACorrection:
    """A restart recovers an orphaned job by re-running its postconditions
    alone. The record the fixer writes is on disk before the commit, so the
    branch diff itself has to carry the evidence."""

    def test_the_postcondition_reads_the_branch_not_the_record(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state)
        ctx = _ctx(tmp_path)
        with patch("core.tasks.tickets.scope_fingerprint", return_value=REVIEWED):
            ok, reason = _branch_moved_off_the_verdict(ctx)
        assert ok is False
        assert "still carries" in reason
        with patch("core.tasks.tickets.scope_fingerprint", return_value=CORRECTED):
            ok, reason = _branch_moved_off_the_verdict(ctx)
        assert ok is True

    def test_a_branch_diff_that_cannot_be_derived_is_not_evidence(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint", return_value=""):
            ok, _ = _branch_moved_off_the_verdict(_ctx(tmp_path))
        assert ok is False


class TestAnUnfinishedPassLeavesAMark:
    """Every ship path asks features.tickets._scope_review_state whether the
    branch may go. A pass that started and did not finish has to answer that
    question, because the branch it left can pass a later review."""

    def test_a_pass_that_dies_leaves_the_mark_standing(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets._capture_repo_heads", return_value={}), \
             patch("core.tasks.tickets.run_claude_code", return_value=None):
            fix_scope_findings(_ctx(tmp_path))
        assert _scope_fix_incomplete(state.load_ticket(KEY))

    def test_a_blocked_commit_names_itself_in_the_mark(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets._capture_repo_heads", return_value={}), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   side_effect=CommitBlocked(
                       "django-drf-app", "hooks",
                       SimpleNamespace(phase="pre-commit", output="ruff E501"))):
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "failed"
        assert "blocked part way" in _scope_fix_incomplete(state.load_ticket(KEY))

    def test_only_a_pass_that_kept_its_record_takes_the_mark_off(self, tmp_path, tmp_state):
        """The body commits before it writes docs/scope-fix.md. Clearing the
        mark in the body would clear it for a pass whose own record never
        landed, and that branch changed with nothing to say what came off it."""
        ticket_dir = _ticket_dir(tmp_path)
        _seed(tmp_path, tmp_state)
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", return_value="done"):
            heads = _moved_heads()
            result = run_task(_ctx(tmp_path))
        assert result.status == "failed"
        assert "scope-fix.md" in result.reason
        assert _scope_fix_incomplete(state.load_ticket(KEY))

        def _fix(prompt, **kwargs):
            (ticket_dir / "docs" / "scope-fix.md").write_text("- removed src/a.ts\n")
            return "done"

        state.update_ticket(KEY, lambda t: {**t, "scope_fix": {"attempts": 0}})
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED, CORRECTED)), \
             patch("core.tasks.tickets._capture_repo_heads",
                   side_effect=lambda _d: next(heads)), \
             patch("core.tasks.tickets._commit_workspace_changes",
                   return_value=["saas-dashboard"]), \
             patch("core.tasks.tickets.run_claude_code", side_effect=_fix):
            heads = _moved_heads()
            result = run_task(_ctx(tmp_path))
        assert result.status == "ok", result.reason
        assert _scope_fix_incomplete(state.load_ticket(KEY)) == ""


class TestARecoveredJobDoesNotSpeakForThePush:
    """A restart recovers an orphaned job from its postconditions, which read
    local state only. The push to an open PR is the step such a job is most
    likely to have died before reaching, so recovery must not report the
    correction finished."""

    def test_a_result_with_no_body_behind_it_leaves_the_mark(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state, status="in_review",
              scope_fix={"attempts": 1, "incomplete": "queued and not finished"})
        _scope_fix_target(_ctx(tmp_path), TaskResult("ok"))
        assert _scope_fix_incomplete(state.load_ticket(KEY))

    def test_a_body_that_finished_takes_the_mark_off(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state, status="in_review",
              scope_fix={"attempts": 1, "incomplete": "queued and not finished"})
        _scope_fix_target(_ctx(tmp_path),
                          TaskResult("ok", artifacts={"completed": True}))
        assert _scope_fix_incomplete(state.load_ticket(KEY)) == ""


class TestAPassWithNothingToCorrectEnds:
    """The gate asks for a correction whenever a pass is open, including on a
    branch whose recorded verdict is a pass. Skipping there would be re-queued
    on every poll for the life of the ticket."""

    def test_a_recorded_pass_ends_the_run_instead_of_skipping_it(self, tmp_path, tmp_state):
        _seed(tmp_path, tmp_state,
              scope_review={"fingerprint": REVIEWED, "verdict": "pass"},
              scope_fix={"attempts": 1, "incomplete": "a commit was blocked part way"})
        with patch("core.tasks.tickets.scope_fingerprint",
                   side_effect=_fingerprints(REVIEWED)), \
             patch("core.tasks.tickets.run_claude_code") as rc:
            result = fix_scope_findings(_ctx(tmp_path))
        assert result.status == "failed"
        assert "nothing to correct" in result.reason
        rc.assert_not_called()
