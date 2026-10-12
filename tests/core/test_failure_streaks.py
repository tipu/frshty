import core.worker as worker_mod
from core.tasks.registry import TaskResult
from core.worker import FAILURE_REPORT_INTERVAL, FailureStreaks, WorkerPool, normalize_reason


class Clock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def test_normalize_reason_drops_shas_and_counts():
    a = normalize_reason("push refused: 2 commits by another author (a1b2c3d, 9f8e7d6c5b)")
    b = normalize_reason("push refused:  3 commits by another author (0011aab, ffee009988)")
    assert a == b
    assert normalize_reason("defaced branch") == "defaced branch"
    assert normalize_reason(None) == ""


def test_first_failure_reported_repeats_quiet_until_interval():
    clock = Clock()
    s = FailureStreaks(clock)
    assert s.failed("i", "sync", "T-1", "push refused abc1234") == ("first", 1)
    assert s.failed("i", "sync", "T-1", "push refused def5678") == ("repeat", 2)
    clock.t += FAILURE_REPORT_INTERVAL - 1
    assert s.failed("i", "sync", "T-1", "push refused 1234567") == ("repeat", 3)
    clock.t += 1
    assert s.failed("i", "sync", "T-1", "push refused") == ("first", 1)
    assert s.failed("i", "sync", "T-1", "push refused 7654321") == ("still", 4)
    assert s.failed("i", "sync", "T-1", "push refused 7654321") == ("repeat", 5)


def test_streaks_are_separate_per_instance_task_ticket_and_reason():
    s = FailureStreaks(Clock())
    assert s.failed("i", "sync", "T-1", "boom")[0] == "first"
    assert s.failed("j", "sync", "T-1", "boom")[0] == "first"
    assert s.failed("i", "other", "T-1", "boom")[0] == "first"
    assert s.failed("i", "sync", "T-2", "boom")[0] == "first"
    assert s.failed("i", "sync", "T-1", "different")[0] == "first"
    assert s.failed("i", "sync", "T-1", "boom")[0] == "repeat"


def test_ok_ends_only_its_own_streaks():
    s = FailureStreaks(Clock())
    s.failed("i", "sync", "T-1", "boom")
    s.failed("i", "sync", "T-1", "boom")
    s.failed("i", "sync", "T-2", "boom")
    assert s.succeeded("i", "sync", "T-1") == [("boom", 2)]
    assert s.succeeded("i", "sync", "T-1") == []
    assert s.failed("i", "sync", "T-1", "boom")[0] == "first"
    assert s.failed("i", "sync", "T-2", "boom")[0] == "repeat"


def test_emit_job_finished_events(monkeypatch):
    events = []
    monkeypatch.setattr(worker_mod.log, "emit",
                        lambda event, summary, meta=None, **kw: events.append((event, summary, meta)))
    clock = Clock()
    pool = WorkerPool({}, size=1)
    pool._failures = FailureStreaks(clock)
    job = {"id": 1, "instance_key": "i", "task": "sync", "ticket_key": "T-1"}
    fail = TaskResult("failed", "push refused for abc1234")

    for _ in range(201):
        pool._emit_job_finished(job, fail)
    loud = [e for e in events if (e[2] or {}).get("category") != "noise"]
    assert len(loud) == 1 and "status=failed" in loud[0][1]
    assert events[-1][2] == {"category": "noise", "repeat": 201}

    clock.t += FAILURE_REPORT_INTERVAL
    pool._emit_job_finished(job, fail)
    assert events[-1][2] is None and events[-1][1].endswith("still failing x202")

    pool._emit_job_finished(job, TaskResult("ok"))
    recovered = [e for e in events if e[0] == "job_recovered"]
    assert len(recovered) == 1 and "recovered after 202 failures" in recovered[0][1]
    assert events[-1][0] == "job_finished" and events[-1][2] == {"category": "noise"}

    pool._emit_job_finished(job, TaskResult("ok"))
    assert len([e for e in events if e[0] == "job_recovered"]) == 1
    pool._emit_job_finished(job, fail)
    assert events[-1][2] is None
