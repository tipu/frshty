"""The fix-attempt cap must bound fix iterations, not tool outages.

`run_tests_and_fix` records the attempt before it calls claude, so a run that
never reached claude still spent one. DEV-678 burned all three attempts in
three minutes against a spend limit and recorded `cap_reached` with a FAIL
nobody had produced.
"""
from datetime import datetime, timezone
from unittest.mock import patch

import core.tasks.tickets as T
from core.tasks.registry import TaskContext


def _ctx(tmp_path):
	(tmp_path / "workspace" / "repo" / ".git").mkdir(parents=True, exist_ok=True)
	return TaskContext(instance_key="aimyable", ticket_key="DEV-1",
	                   task="run_tests_and_fix", payload={}, job_id=1,
	                   triggering_event_id=None, config={}, registry=None,
	                   now=datetime.now(timezone.utc))


def _run(tmp_path, attempts_before, fix_result):
	"""Drive the task to its fix step and report the persisted attempt count."""
	ticket = {"status": "testing", "test_fix_attempts": attempts_before}

	def _apply(key, fn):
		updated = fn(dict(ticket))
		ticket.clear()
		ticket.update(updated)
		return dict(ticket)

	with patch.object(T, "_ticket_dir", return_value=tmp_path), \
	     patch.object(T, "base_branch_for", return_value="main"), \
	     patch.object(T.state, "load_ticket", return_value=dict(ticket)), \
	     patch.object(T.state, "update_ticket", side_effect=_apply), \
	     patch.object(T, "_repo_has_changes_vs_base", return_value=True), \
	     patch.object(T, "_detect_runner", return_value=(["pytest"], {})), \
	     patch.object(T, "_run_repo_tests",
	                  return_value={"result": "fail", "exit_code": 1, "tail": "boom"}), \
	     patch.object(T, "_write_test_runs"), \
	     patch.object(T, "_build_fix_prompt", return_value="fix it"), \
	     patch.object(T, "_claim_session", return_value=("sid", False)), \
	     patch.object(T, "_commit_workspace_changes"), \
	     patch.object(T, "run_claude_code", return_value=fix_result), \
	     patch.object(T, "log"):
		result = T.run_tests_and_fix(_ctx(tmp_path))
	return result, ticket.get("test_fix_attempts")


class TestAttemptBudget:
	def test_an_attempt_that_never_reached_claude_is_given_back(self, tmp_path):
		result, attempts = _run(tmp_path, attempts_before=0, fix_result=None)
		assert result.status == "failed"
		assert attempts == 0, "a fix that never ran must not spend an attempt"

	def test_a_dispatched_fix_spends_its_attempt(self, tmp_path):
		result, attempts = _run(tmp_path, attempts_before=0, fix_result="fixed")
		assert result.status == "ok"
		assert result.artifacts["fix_dispatched"] is True
		assert attempts == 1

	def test_repeated_outages_do_not_reach_the_cap(self, tmp_path):
		from features.tickets import MAX_TEST_FIX_ATTEMPTS
		attempts = 0
		for _ in range(MAX_TEST_FIX_ATTEMPTS + 2):
			result, attempts = _run(tmp_path, attempts_before=attempts, fix_result=None)
			assert not result.artifacts.get("cap_reached")
		assert attempts == 0
