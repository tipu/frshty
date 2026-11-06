"""A ticket parked because the implement stage left uncommitted work goes back
through review on its own, and that work is committed before any review."""
import subprocess
from unittest.mock import patch

import core.state as state
from core.tasks.registry import TaskContext
from core.tasks.tickets import enter_proving, enter_testing, start_reviewing
from core.ticket_status import DIRTY_REVIEW
from features import tickets
from tests.conftest import make_ticket, make_ticket_state

SLUG = "PROJ-1-do-the-thing"
DIRTY_JOB = {"task": "enter_testing", "status": "failed",
             "response": '{"reason": "worktree has uncommitted changes in: repo"}'}
DISPATCH_JOB = {"task": "advance_ticket", "status": "ok", "response": "{}"}


def _git(repo, *args):
    return subprocess.run(["git", *args], cwd=repo, check=True,
                          capture_output=True, text=True).stdout


def _ticket_dir(config):
    root = config["workspace"]["root"] / config["workspace"].get("tickets_dir", "tickets")
    ticket_dir = root / SLUG
    (ticket_dir / "docs").mkdir(parents=True, exist_ok=True)
    return ticket_dir


def _repo(ticket_dir):
    repo = ticket_dir / "workspace" / "repo"
    repo.mkdir(parents=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    (repo / "kept.py").write_text("a = 1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    return repo


def _ctx(config, task):
    return TaskContext(instance_key="inst", ticket_key="PROJ-1", task=task, payload={},
                       job_id=0, triggering_event_id=None, config=config, registry=None, now=None)


def _poll(config, jobs, gate=None):
    with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
         patch("features.tickets._fetch_open_prs", return_value=[]), \
         patch("features.tickets.get_repos", return_value=[{"name": "repo", "path": "/r"}]), \
         patch("features.tickets._reconcile_prs", side_effect=lambda ts, *a, **k: ts), \
         patch("features.tickets._process_ticket_comments"), \
         patch("features.tickets._maybe_enqueue_ranker"), \
         patch("features.tickets.enqueue_prd_backfill"), \
         patch("features.tickets._repo_gate_blocked", return_value=gate), \
         patch("core.queue.jobs_for_ticket", return_value=jobs):
        tickets.check(config, instance_key="inst")
    return state.load_ticket("PROJ-1")


def _park(config, **fields):
    ticket_dir = _ticket_dir(config)
    review = ticket_dir / "docs" / "tri-review.md"
    review.write_text("VERDICT: PASS\n")
    state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=SLUG, branch=SLUG,
                                                       **fields)})
    state.transition_ticket("PROJ-1", "blocked", reason="task enter_testing failed; releasing repo gate")
    return review


class TestReviewSeesEveryChange:
    def test_leftover_work_is_committed_before_the_review_runs(self, fake_config, tmp_state, tmp_log):
        ticket_dir = _ticket_dir(fake_config)
        repo = _repo(ticket_dir)
        (repo / "left.spec.ts").write_text("test\n")
        (ticket_dir / "docs" / "change-manifest.md").write_text("m\n")
        state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=SLUG)})
        seen = {}

        def review(*_a, **_k):
            seen["status"] = _git(repo, "status", "--porcelain")
            seen["tracked"] = _git(repo, "ls-files")
            return "done"

        with patch("core.tasks.tickets._run_pipeline_agent", side_effect=review), \
             patch("core.tasks.tickets.commit_subject", return_value="PROJ-1 add the spec"), \
             patch("core.tasks.tickets.repos_with_branch_diff", return_value=[]):
            result = start_reviewing(_ctx(fake_config, "start_reviewing"))

        assert result.status == "ok"
        assert seen["status"] == ""
        assert "left.spec.ts" in seen["tracked"]

    def test_scratch_is_removed_and_not_committed(self, fake_config, tmp_state, tmp_log):
        ticket_dir = _ticket_dir(fake_config)
        repo = _repo(ticket_dir)
        (repo / "test-results").mkdir()
        (repo / "test-results" / "trace.zip").write_text("x")
        state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=SLUG)})

        with patch("core.tasks.tickets._run_pipeline_agent", return_value="done"), \
             patch("core.tasks.tickets.repos_with_branch_diff", return_value=[]):
            start_reviewing(_ctx(fake_config, "start_reviewing"))

        assert "test-results" not in _git(repo, "ls-files")
        assert not (repo / "test-results").exists()


class TestGatesReadTheRealWorktree:
    def test_scratch_alone_does_not_stop_enter_testing(self, fake_config, tmp_state, tmp_log):
        ticket_dir = _ticket_dir(fake_config)
        repo = _repo(ticket_dir)
        (repo / ".playwright-cli").mkdir()
        (repo / ".playwright-cli" / "page.yml").write_text("x")
        state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=SLUG)})

        assert enter_testing(_ctx(fake_config, "enter_testing")).status == "ok"

    def test_unreviewed_work_still_stops_enter_testing(self, fake_config, tmp_state, tmp_log):
        ticket_dir = _ticket_dir(fake_config)
        repo = _repo(ticket_dir)
        (repo / "left.py").write_text("b = 2\n")
        state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=SLUG)})

        result = enter_testing(_ctx(fake_config, "enter_testing"))

        assert result.status == "failed"
        assert "uncommitted changes" in result.reason
        assert "left.py" not in _git(repo, "ls-files")

    def test_enter_proving_commits_what_the_test_run_left(self, fake_config, tmp_state, tmp_log):
        ticket_dir = _ticket_dir(fake_config)
        repo = _repo(ticket_dir)
        (repo / "left_test.py").write_text("b = 2\n")
        state.save("tickets", {"PROJ-1": make_ticket_state(status="testing", slug=SLUG)})

        with patch("core.tasks.tickets._fire_ticket_dev_complete"):
            result = enter_proving(_ctx(fake_config, "enter_proving"))

        assert result.status == "ok"
        assert "left_test.py" in _git(repo, "ls-files")


class TestBlockedTicketResumes:
    def test_a_parked_ticket_returns_to_review_without_the_stale_verdict(
            self, fake_config, tmp_state, tmp_log):
        review = _park(fake_config)

        saved = _poll(fake_config, [DISPATCH_JOB, DIRTY_JOB])

        assert saved["status"] == "reviewing"
        assert saved["dirty_review_attempts"] == 1
        assert "blocked_from" not in saved
        assert not review.exists()

    def test_it_waits_while_another_ticket_holds_the_gate(self, fake_config, tmp_state, tmp_log):
        review = _park(fake_config)

        saved = _poll(fake_config, [DIRTY_JOB], gate="PROJ-2")

        assert saved["status"] == "blocked"
        assert review.exists()

    def test_a_spent_budget_stays_blocked(self, fake_config, tmp_state, tmp_log):
        _park(fake_config, dirty_review_attempts=DIRTY_REVIEW.budget)

        assert _poll(fake_config, [DIRTY_JOB])["status"] == "blocked"

    def test_a_block_with_another_cause_stays_blocked(self, fake_config, tmp_state, tmp_log):
        _park(fake_config)
        commit_failed = {"task": "start_reviewing", "status": "failed",
                         "response": '{"reason": "repo: commit blocked"}'}

        assert _poll(fake_config, [commit_failed, DIRTY_JOB])["status"] == "blocked"

    def test_another_failure_of_enter_testing_stays_blocked(self, fake_config, tmp_state, tmp_log):
        _park(fake_config)
        crashed = {"task": "enter_testing", "status": "failed",
                   "response": '{"reason": "OSError: disk full"}'}

        assert _poll(fake_config, [crashed])["status"] == "blocked"
