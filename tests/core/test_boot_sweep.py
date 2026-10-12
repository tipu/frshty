import core.db as db
import core.runtime as runtime


def _insert(inv_id, function_name, status):
    db.execute(
        "INSERT INTO claude_invocations(id, instance_key, job_key, function_name, model, "
        "prompt, prompt_length, cwd, started_at, status) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (inv_id, "k", "j", function_name, "m", "p", 1, "/", "2026-10-12T00:00:00+00:00", status))


def _row(inv_id):
    return db.query_one("SELECT status, output FROM claude_invocations WHERE id = ?", (inv_id,))


def test_boot_sweep_marks_in_process_calls_and_skips_task_runs():
    ids = ("boot-sweep-call-running", "boot-sweep-call-queued", "work-run-boot-sweep")
    db.execute("DELETE FROM claude_invocations WHERE id IN (?,?,?)", ids)
    try:
        _insert("boot-sweep-call-running", "direct_inbox_gmail", "running")
        _insert("boot-sweep-call-queued", "global_watch_agent", "queued")
        _insert("work-run-boot-sweep", "work_task", "running")

        runtime.mark_killed_invocations()

        for inv_id in ("boot-sweep-call-running", "boot-sweep-call-queued"):
            row = _row(inv_id)
            assert row["status"] == "error"
            assert row["output"] == "[killed by restart]"
        row = _row("work-run-boot-sweep")
        assert row["status"] == "running"
        assert row["output"] is None
    finally:
        db.execute("DELETE FROM claude_invocations WHERE id IN (?,?,?)", ids)
