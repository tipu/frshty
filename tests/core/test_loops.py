"""core.loops flags a job, a retry or a proposal that repeats with no state
change, once per loop. Each case mirrors a loop that ran for days unseen: a
ticket job chain that re-queued itself with the ticket unchanged, a poll that
failed on a deleted worktree, a comment push that failed inside an ok job,
and a proposal filed again after each decline."""
import json

import pytest

import core.db as db
import core.event_bus as bus
import core.log as log
import core.loops as loops
import core.queue as q
import core.state as state
import core.tasks  # noqa: F401
import core.worker as worker_mod
from core.tasks.registry import _REGISTRY, TaskContext, TaskResult, task
from services import work_store

N = loops.LOOP_THRESHOLD


class FakeRegistry:
    def __init__(self):
        self.instance_key = "t"
        self.config = {}
        self.base_url = ""


@pytest.fixture()
def pool(fresh_db):
    state.init("t")
    state._instance_key_cv.set("t")
    yield worker_mod.WorkerPool({"t": FakeRegistry()}, size=1)
    for name in ("requeue_self", "noop_ticket", "touch_ticket", "poll_gone"):
        _REGISTRY.pop(name, None)


def _loop_events() -> list[dict]:
    rows = db.query_all("SELECT summary, meta FROM log_events WHERE event=? ORDER BY ts",
                        (loops.LOOP_EVENT,))
    return [{"summary": r["summary"], "meta": json.loads(r["meta"])} for r in rows]


def _run_next(pool) -> dict:
    job = q.claim_next()
    assert job, "a job was expected in the queue"
    pool._run_one(job)
    return job


def test_a_job_chain_that_requeues_itself_with_the_ticket_unchanged_is_one_loop(pool):
    @task("requeue_self")
    def requeue_self(ctx: TaskContext) -> TaskResult:
        q.enqueue_job(ctx.instance_key, "requeue_self", ticket_key=ctx.ticket_key)
        return TaskResult("ok")

    state.save_ticket("T-1", {"status": "in_review", "branch": "feat/t-1"})
    first = q.enqueue_job("t", "requeue_self", ticket_key="T-1")
    for _ in range(N - 1):
        _run_next(pool)
    assert _loop_events() == []

    _run_next(pool)
    found = _loop_events()
    assert len(found) == 1
    assert found[0]["meta"]["loop"] == "job"
    assert found[0]["meta"]["task"] == "requeue_self"
    assert found[0]["meta"]["ticket"] == "T-1"
    assert found[0]["meta"]["first_job_id"] == first

    for _ in range(N):
        _run_next(pool)
    assert len(_loop_events()) == 1, "a loop is flagged once, not once per run"
    roots = {r["root_job_id"] for r in db.query_all(
        "SELECT root_job_id FROM jobs WHERE id > ?", (first,))}
    assert roots == {first}, "every job the chain queued carries the root that started it"
    assert db.query_all("SELECT id FROM jobs WHERE status='ok' AND ticket_digest IS NULL", ()) == [], (
        "a job is finished and digested in one write, so a successor never reads it half done")


def test_the_same_no_op_job_queued_by_separate_cron_ticks_is_not_a_loop(pool):
    @task("noop_ticket")
    def noop_ticket(ctx: TaskContext) -> TaskResult:
        return TaskResult("ok")

    state.save_ticket("T-1", {"status": "in_review"})
    for _ in range(N + 5):
        q.enqueue_job("t", "noop_ticket", ticket_key="T-1")
        _run_next(pool)
    assert _loop_events() == []


def test_a_chain_that_changes_the_ticket_each_run_is_not_a_loop(pool):
    @task("touch_ticket")
    def touch_ticket(ctx: TaskContext) -> TaskResult:
        ts = state.load_ticket(ctx.ticket_key)
        ts["rounds"] = ts.get("rounds", 0) + 1
        state.save_ticket(ctx.ticket_key, ts)
        q.enqueue_job(ctx.instance_key, "touch_ticket", ticket_key=ctx.ticket_key)
        return TaskResult("ok")

    state.save_ticket("T-1", {"status": "in_review"})
    q.enqueue_job("t", "touch_ticket", ticket_key="T-1")
    for _ in range(N + 5):
        _run_next(pool)
    assert _loop_events() == []


def test_a_timestamp_alone_is_not_a_state_change():
    a = {"status": "in_review", "base_sync": {"r/1": {"attempts": 0, "checked_at": "2026-10-01"}}}
    b = {"status": "in_review", "base_sync": {"r/1": {"attempts": 0, "checked_at": "2026-10-02"}}}
    c = {"status": "in_review", "base_sync": {"r/1": {"attempts": 1, "checked_at": "2026-10-02"}}}
    assert loops.ticket_digest(a) == loops.ticket_digest(b)
    assert loops.ticket_digest(b) != loops.ticket_digest(c)


def test_a_poll_that_fails_on_a_deleted_worktree_every_tick_is_one_loop(pool):
    runs = iter(range(1000))

    @task("poll_gone")
    def poll_gone(ctx: TaskContext) -> TaskResult:
        n = next(runs)
        return TaskResult("failed", "FileNotFoundError: [Errno 2] No such file or directory: "
                                    f"'/state/worktrees/t/work-{n}/workspace/repo'")

    for _ in range(N - 1):
        q.enqueue_job("t", "poll_gone")
        _run_next(pool)
    assert _loop_events() == []
    q.enqueue_job("t", "poll_gone")
    _run_next(pool)
    found = _loop_events()
    assert len(found) == 1
    assert found[0]["meta"]["loop"] == "job" and found[0]["meta"]["ticket"] is None
    assert "No such file or directory" in found[0]["summary"]


def test_a_different_failure_in_between_breaks_the_streak(pool):
    reasons = iter(["boom"] * (N - 1) + ["other"] + ["boom"] * (N - 1))

    @task("poll_gone")
    def poll_gone(ctx: TaskContext) -> TaskResult:
        return TaskResult("failed", next(reasons))

    for _ in range(2 * N - 1):
        q.enqueue_job("t", "poll_gone")
        _run_next(pool)
    assert _loop_events() == []


def test_a_root_carried_by_an_event_reaches_the_job_the_dispatcher_queues(pool):
    token = q.use_chain(41)
    try:
        with db.tx() as c:
            q.insert_event(c, "task", "ticket_advance", {"ticket_key": "T-1"}, "t")
    finally:
        q.reset_chain(token)
    bus.Dispatcher({"t": FakeRegistry()})._drain()
    row = db.query_one("SELECT task, root_job_id FROM jobs WHERE ticket_key='T-1'", ())
    assert row["task"] == "advance_ticket" and row["root_job_id"] == 41


def _push_failed(ticket: str, sha: str) -> None:
    log.emit("ticket_pr_comment_push_failed",
             f"{ticket} · repo: Comment fixes committed but push failed; leaving comments "
             f"unresolved: ! [rejected] {sha} -> feat/{ticket} (fetch first)",
             meta={"ticket": ticket, "repo": "repo"})


def test_a_push_that_fails_the_same_way_every_scan_is_one_retry_loop(pool):
    for i in range(N - 1):
        _push_failed("T-1", f"{i:07x}a1")
        _push_failed("T-2", f"{i:07x}b2")
    assert _loop_events() == []
    _push_failed("T-1", "fffffffa1")
    found = _loop_events()
    assert len(found) == 1
    assert found[0]["meta"] == {"loop": "retry", "event": "ticket_pr_comment_push_failed",
                                "ticket": "T-1", "count": N}
    for i in range(N):
        _push_failed("T-1", f"{i:07x}c3")
    assert len(_loop_events()) == 1


def test_an_event_that_is_not_a_failure_is_never_a_retry_loop(pool):
    for _ in range(N + 1):
        log.emit("ticket_base_synced", "Merged main into T-1 PR #4", meta={"ticket": "T-1"})
    assert _loop_events() == []


def _file(objective: str) -> int:
    return work_store.create_proposal(objective, instance_key="board")


def test_a_proposal_filed_again_after_two_declines_is_one_loop(pool):
    first = _file("Unblock T-9 — it has sat in stuck since 2026-09-17")
    work_store.apply_action(first, "decline")
    other = _file("Unblock T-10 — it has sat in stuck since 2026-09-17")
    work_store.apply_action(other, "decline")
    second = _file("Unblock T-9 — it has sat in stuck since 2026-09-18")
    work_store.apply_action(second, "decline")
    assert _loop_events() == []

    third = _file("Unblock T-9 — it has sat in stuck since 2026-09-19")
    found = _loop_events()
    assert len(found) == 1
    assert found[0]["meta"] == {"loop": "proposal", "work_item_id": third,
                                "declined": [first, second]}

    work_store.apply_action(third, "decline")
    _file("Unblock T-9 — it has sat in stuck since 2026-09-20")
    assert len(_loop_events()) == 1


def test_an_approved_proposal_does_not_count_as_declined(pool):
    for _ in range(3):
        item = _file("Answer task #142 — pick a base branch")
        assert work_store.claim_proposal(item)
    assert _loop_events() == []


def test_the_digest_is_the_ticket_state_the_transition_left(pool):
    state.save_ticket("T-1", {"status": "in_review", "rounds": 1})
    job_id = q.enqueue_job("t", "noop_ticket", ticket_key="T-1")
    worker_mod.transition_ticket_and_emit(
        "T-1", "t", mutate=lambda t: {**t, "rounds": 2}, job_id=job_id, advance=False)
    row = db.query_one("SELECT ticket_digest FROM jobs WHERE id=?", (job_id,))
    assert row["ticket_digest"] == loops.ticket_digest(state.load_ticket("T-1"))
    assert row["ticket_digest"] != loops.ticket_digest({"status": "in_review", "rounds": 1})
