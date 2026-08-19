"""A commit that cannot succeed must stop the loop, not feed it.

fix_review_findings sits in _RETRY_LOOP_TASKS, so a failure is treated as "not
yet converged" and the dispatcher re-enqueues it while tri-review.md still reads
FAIL. That is right for a fixer that has not converged. It is wrong for a commit
that can never succeed: each retry runs the full fixer again, with permissions
bypassed, before triage ever sees the cause. An unfixable dependency therefore
becomes an unbounded series of privileged edits, and if one of them happens to
satisfy the hook the model writes its own PASS and the ticket advances.
"""
from unittest.mock import patch

import core.tasks.registry as R
from core.tasks.registry import TaskContext, TaskResult


def _ctx(task="fix_review_findings"):
    return TaskContext(instance_key="i", ticket_key="DEV-1", task=task, payload={},
                       job_id=1, triggering_event_id=None,
                       config={"workspace": {}}, registry=None, now=None)


class TestHardBlockEscapesTheRetryExemption:
    def test_an_ordinary_failure_still_retries(self):
        """Convergence must keep working; this is why the exemption exists."""
        with patch.object(R, "_apply_status") as applied:
            out = R._release_gate_on_failure(_ctx(), TaskResult("failed", "not converged yet"))
        assert out.status == "failed"
        applied.assert_not_called()

    def test_a_hard_block_blocks_even_for_a_retry_loop_task(self):
        result = TaskResult("failed", "dependency missing")
        result.hard_block = True
        with patch("core.state.load_ticket", return_value={"status": "reviewing"}), \
             patch("core.state.transition_ticket") as moved, \
             patch("core.log.emit"):
            R._release_gate_on_failure(_ctx(), result)
        moved.assert_called_once()
        assert moved.call_args[0][1] == "blocked"

    def test_commit_blocked_is_marked_hard(self):
        import core.tasks.tickets as T
        from core import git_util as g
        outcome = g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1, "x", "a", "a")
        err = T.CommitBlocked("r", "block_dependency", outcome)
        assert getattr(err, "hard_block", False) is True

    def test_a_raised_hard_block_carries_the_flag_onto_the_result(self):
        """The exception has to mark the TaskResult, or the exemption swallows it."""
        import core.tasks.tickets as T
        from core import git_util as g
        err = T.CommitBlocked("r", "block_dependency",
                              g.CommitOutcome("hook_failed", "hook_pass_2", "r", 1,
                                              "unknown import symbol", "a", "a"))
        result = R._result_from_exception(err)
        assert result.status == "failed"
        assert getattr(result, "hard_block", False) is True
        assert "block_dependency" in result.reason
