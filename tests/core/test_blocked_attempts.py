"""A blocked action is tried a bounded number of times, then waits for the
input that can unblock it.

Each case replays a loop from the live databases. DEV-624 ran sync_pr_base
and advance_ticket 74,762 times in 6 days: the merge aborted on a dirty
worktree, the task still reported ok, and the two tasks queued each other
with the ticket unchanged. A PR whose branch held commits by another author
had its push refused 235 times: each new commit on main reset the attempt
counter and bought two more refused pushes."""
import json
from unittest.mock import MagicMock, patch

import pytest

import core.blocked as blocked_attempts
import core.branch_sync as branch_sync
import core.db as db
import core.loops as loops
import core.queue as q
import core.state as state
import core.tasks  # noqa: F401
import core.worker as worker_mod
import features.tickets as tix
from core.tasks.registry import _REGISTRY, TaskContext, TaskResult, task

N = loops.LOOP_THRESHOLD
FAKE_TASKS = ("loop_advance", "loop_sync", "always_refused")


class FakeRegistry:
    def __init__(self, config=None):
        self.instance_key = "t"
        self.config = config or {}
        self.base_url = ""


@pytest.fixture()
def pool(fresh_db):
    state.init("t")
    state._instance_key_cv.set("t")
    yield worker_mod.WorkerPool(
        {"t": FakeRegistry({"pr": {"auto_update_branch": True}, "_base_url": "http://base"})},
        size=1)
    for name in FAKE_TASKS:
        _REGISTRY.pop(name, None)


def _run_queue(pool, limit=500) -> int:
    runs = 0
    while runs < limit:
        job = q.claim_next()
        if not job:
            break
        pool._run_one(job)
        runs += 1
    return runs


def _statuses(task_name: str) -> list[str]:
    return [r["status"] for r in db.query_all(
        "SELECT status FROM jobs WHERE task=? ORDER BY id", (task_name,))]


def _events(event: str) -> list[dict]:
    return db.query_all("SELECT summary, meta FROM log_events WHERE event=? ORDER BY ts", (event,))


class TestTheLoopDetectorStopsTheNextJob:
    def test_two_tasks_that_queue_each_other_with_the_ticket_unchanged_stop(self, pool):
        @task("loop_advance")
        def loop_advance(ctx: TaskContext) -> TaskResult:
            q.enqueue_job(ctx.instance_key, "loop_sync", ticket_key=ctx.ticket_key)
            return TaskResult("ok")

        @task("loop_sync")
        def loop_sync(ctx: TaskContext) -> TaskResult:
            q.enqueue_job(ctx.instance_key, "loop_advance", ticket_key=ctx.ticket_key)
            return TaskResult("ok")

        state.save_ticket("T-1", {"status": "in_review", "branch": "feat/t-1"})
        q.enqueue_job("t", "loop_advance", ticket_key="T-1")
        runs = _run_queue(pool)

        assert runs < 500, "the chain ends; before the block it ran until the test limit"
        assert _statuses("loop_advance").count("ok") == N
        assert _statuses("loop_sync").count("ok") == N
        assert _statuses("loop_advance")[-1] == "skipped"
        flagged = [json.loads(e["meta"]) for e in _events(loops.LOOP_EVENT)]
        assert {m["task"] for m in flagged} == {"loop_advance", "loop_sync"}
        assert all(m["blocked"] for m in flagged)
        assert tix._enqueue_stage("t", "T-1", "loop_sync") is None, (
            "the stage dispatcher queues no job the loop detector stopped")

        state.save_ticket("T-1", {"status": "in_review", "branch": "feat/t-1", "rounds": 1})
        assert blocked_attempts.job_blocked("t", "T-1", "loop_sync") is None, (
            "a ticket change wakes the block")

    def test_a_failure_that_comes_back_after_the_wake_is_blocked_at_once(self, pool):
        @task("always_refused")
        def always_refused(ctx: TaskContext) -> TaskResult:
            return TaskResult("failed", "push refused: 2 commit(s) are not authored by me")

        state.save_ticket("T-1", {"status": "in_review"})
        for _ in range(N):
            q.enqueue_job("t", "always_refused", ticket_key="T-1")
            _run_queue(pool)
        assert len(_events(loops.LOOP_EVENT)) == 1

        q.enqueue_job("t", "always_refused", ticket_key="T-1")
        _run_queue(pool)
        assert _statuses("always_refused")[-1] == "skipped"

        state.save_ticket("T-1", {"status": "in_review", "note": "operator looked"})
        q.enqueue_job("t", "always_refused", ticket_key="T-1")
        _run_queue(pool)
        assert _statuses("always_refused")[-1] == "failed", "the wake buys one more run"
        assert len(_events("job_blocked_again")) == 1

        q.enqueue_job("t", "always_refused", ticket_key="T-1")
        _run_queue(pool)
        assert _statuses("always_refused")[-1] == "skipped", (
            "the same failure blocks again without another LOOP_THRESHOLD runs")

    def test_the_dispatcher_itself_is_never_blocked(self, pool):
        assert blocked_attempts.block_job("t", "T-1", "advance_ticket", "ok", "") is False
        assert blocked_attempts.job_blocked("t", "T-1", "advance_ticket") is None


class TestABaseSyncThatDidNotMergeFails:
    def test_a_dirty_worktree_sync_trips_the_stage_retry_cap(self, pool):
        state.save_ticket("DEV-624", {
            "status": "in_review", "branch": "dev-624-branch",
            "prs": [{"repo": "app", "id": 31, "url": "http://pr/31", "base": "main"}]})
        dirty = {"result": "dirty_worktree", "capped": True, "attempts": 2,
                 "error": "worktree has uncommitted changes (a.py); merge would overwrite them"}
        runs = 0
        with patch("features.tickets.make_platform", return_value=MagicMock()), \
             patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.branch_sync.sync_branch_with_base", return_value=dirty):
            for _ in range(30):
                if tix._enqueue_stage("t", "DEV-624", "sync_pr_base"):
                    runs += _run_queue(pool)
        assert _statuses("sync_pr_base") == ["failed"] * tix.MAX_STAGE_RETRIES
        assert runs == tix.MAX_STAGE_RETRIES, (
            "a sync that reported ok on an aborted merge ran on every tick")
        reason = json.loads(db.query_one(
            "SELECT response FROM jobs WHERE task='sync_pr_base' ORDER BY id DESC LIMIT 1",
            ())["response"])["reason"]
        assert reason.startswith("app#31 dirty_worktree: worktree has uncommitted changes")


class TestARefusedPushWaitsForTheBranchHead:
    REFUSED = "2 commit(s) on feature are not authored by me; refusing to push"

    def _tick(self, platform, st, base_sha, head_sha):
        refs = {"main": base_sha, "refs/heads/feature": head_sha}
        fetch = MagicMock(returncode=0, stdout="", stderr="")
        with patch("core.branch_sync.ls_remote_sha", side_effect=lambda repo, ref: refs[ref]), \
             patch("core.branch_sync.subprocess.run", return_value=fetch), \
             patch("core.branch_sync.git_util.run_git",
                   side_effect=lambda wt, args, timeout: MagicMock(
                       stdout="3\n" if args[0] == "rev-list" else "")):
            return branch_sync.sync_branch_with_base(
                platform, "/repo", "main", "feature", st, lambda: "/wt",
                ticket_key="feature", resource="app#4562")

    def test_a_moving_main_buys_no_new_push(self, pool):
        platform = MagicMock()
        platform.merge_base.return_value = {"ok": True}
        platform.push_branch.return_value = {"ok": False, "error": self.REFUSED}
        st = {}
        results = [self._tick(platform, st, f"main{n}", "head1")["result"] for n in range(10)]

        assert platform.push_branch.call_count == branch_sync.MAX_BASE_SYNC_ATTEMPTS, (
            "each new commit on main used to reset the counter and push again")
        assert results[:2] == ["push_failed", "push_failed"]
        assert set(results[2:]) == {"capped"}
        rows = blocked_attempts.find("app#4562", blocked_attempts.SYNC_BASE, instance_key="t")
        assert len(rows) == 1
        assert rows[0]["blocker"].startswith("push_failed: ")
        assert rows[0]["attempts"] == branch_sync.MAX_BASE_SYNC_ATTEMPTS
        assert rows[0]["wake"] == "head1"

        assert self._tick(platform, st, "main10", "head2")["result"] == "push_failed", (
            "a new branch head can clear the blocker, so it buys one more try")
        assert platform.push_branch.call_count == branch_sync.MAX_BASE_SYNC_ATTEMPTS + 1

        platform.push_branch.return_value = {"ok": True}
        assert self._tick(platform, st, "main11", "head3")["result"] == "synced"
        assert st["base_sync_attempts"] == 0
        assert blocked_attempts.find("app#4562", blocked_attempts.SYNC_BASE, instance_key="t") == []

    def test_a_merge_whose_push_failed_is_pushed_again_not_called_up_to_date(self, pool):
        platform = MagicMock()
        platform.push_branch.return_value = {"ok": False, "error": self.REFUSED}
        st = {"base_sync_sha": "main0", "base_sync_attempts": 1,
              "last_base_error": self.REFUSED, "base_sync_head": "head1"}
        refs = {"main": "main1", "refs/heads/feature": "head1"}

        def run_git(wt, args, timeout, allowed_codes=(0,)):
            return MagicMock(returncode=0, stdout={"rev-list": "0\n", "rev-parse": "merged\n"}.get(args[0], ""))

        with patch("core.branch_sync.ls_remote_sha", side_effect=lambda repo, ref: refs[ref]), \
             patch("core.branch_sync.subprocess.run", return_value=MagicMock(returncode=0, stderr="")), \
             patch("core.branch_sync.git_util.run_git", side_effect=run_git):
            out = branch_sync.sync_branch_with_base(platform, "/repo", "main", "feature", st,
                                                    lambda: "/wt", resource="app#4562")
        assert out["result"] == "push_failed", "a local merge that never reached the PR is not synced"
        assert st["base_sync_attempts"] == branch_sync.MAX_BASE_SYNC_ATTEMPTS
        platform.merge_base.assert_not_called()


class TestAJobBlockLapses:
    def test_a_block_older_than_the_window_lets_one_run_through(self, pool):
        state.save_ticket("T-1", {"status": "reviewing"})
        assert blocked_attempts.block_job("t", "T-1", "loop_sync", "failed", "provider down")
        assert blocked_attempts.job_blocked("t", "T-1", "loop_sync")
        db.execute("UPDATE blocked_attempts SET last_at='2000-01-01T00:00:00+00:00'")
        assert blocked_attempts.job_blocked("t", "T-1", "loop_sync") is None

    def test_an_operator_retry_clears_the_block(self, pool):
        state.save_ticket("T-1", {"status": "reviewing"})
        blocked_attempts.block_job("t", "T-1", "loop_sync", "failed", "provider down")
        blocked_attempts.clear_job("t", "T-1", "loop_sync")
        assert blocked_attempts.job_blocked("t", "T-1", "loop_sync") is None
