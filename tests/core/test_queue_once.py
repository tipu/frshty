import threading

import core.db as db
import core.queue as q


def _live(task: str, ticket_key: str) -> int:
    return db.query_one(
        "SELECT COUNT(*) AS n FROM jobs WHERE instance_key = 'once' AND ticket_key = ?"
        " AND task = ? AND status IN ('queued', 'running')", (ticket_key, task))["n"]


def test_once_refuses_a_second_queued_job():
    first = q.enqueue_job("once", "start_planning", ticket_key="ONCE-1", once=True)
    second = q.enqueue_job("once", "start_planning", ticket_key="ONCE-1", once=True)
    assert first and second is None
    assert _live("start_planning", "ONCE-1") == 1


def test_once_refuses_while_the_job_runs_and_allows_after_it_ends():
    job_id = q.enqueue_job("once", "start_reviewing", ticket_key="ONCE-2", once=True)
    db.execute("UPDATE jobs SET status = 'running' WHERE id = ?", (job_id,))
    assert q.enqueue_job("once", "start_reviewing", ticket_key="ONCE-2", once=True) is None
    q.mark_done(job_id, "ok", {})
    assert q.enqueue_job("once", "start_reviewing", ticket_key="ONCE-2", once=True)


def test_once_keys_on_ticket_and_task():
    assert q.enqueue_job("once", "start_planning", ticket_key="ONCE-3", once=True)
    assert q.enqueue_job("once", "start_planning", ticket_key="ONCE-4", once=True)
    assert q.enqueue_job("once", "address_pm_findings", ticket_key="ONCE-3", once=True)


def test_without_once_duplicates_still_queue():
    q.enqueue_job("once", "fix_pr_comments", ticket_key="ONCE-5")
    q.enqueue_job("once", "fix_pr_comments", ticket_key="ONCE-5")
    assert _live("fix_pr_comments", "ONCE-5") == 2


def test_racing_callers_queue_one_job():
    barrier = threading.Barrier(8)
    results = []

    def enqueue():
        barrier.wait()
        results.append(q.enqueue_job("once", "start_planning", ticket_key="ONCE-6", once=True))

    threads = [threading.Thread(target=enqueue) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len([r for r in results if r]) == 1
    assert _live("start_planning", "ONCE-6") == 1


def test_once_refuses_a_second_job_without_a_ticket():
    first = q.enqueue_job("once", "rank_new_tickets", once=True)
    assert first and q.enqueue_job("once", "rank_new_tickets", once=True) is None
    q.mark_done(first, "ok", {})
