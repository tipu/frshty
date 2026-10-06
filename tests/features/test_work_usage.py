import json
import os

import core.config as core_config
import core.db as db
from services import work_store


def _assistant(msg_id, model, usage):
    return json.dumps({"type": "assistant", "message": {
        "id": msg_id, "model": model, "role": "assistant",
        "content": [{"type": "text", "text": "x"}], "usage": usage}}) + "\n"


def _usage(inp, cc, cr, out):
    return {"input_tokens": inp, "cache_creation_input_tokens": cc,
            "cache_read_input_tokens": cr, "output_tokens": out}


def _claude_transcript(tmp_path):
    path = tmp_path / "sess.jsonl"
    path.write_text(
        _assistant("m1", "claude-opus-5-5", _usage(2, 100, 1000, 10))
        + _assistant("m1", "claude-opus-5-5", _usage(2, 100, 1000, 10))
        + json.dumps({"type": "user", "message": {"role": "user", "content": "hi"}}) + "\n"
        + _assistant("m2", "claude-opus-5-5", _usage(3, 50, 2000, 20))
        + _assistant("s1", "<synthetic>", _usage(0, 0, 0, 0)))
    sub = tmp_path / "sess" / "subagents"
    sub.mkdir(parents=True)
    (sub / "agent-a.jsonl").write_text(
        _assistant("m3", "claude-haiku-4-5-20251001", _usage(1, 7, 70, 5)))
    return path


def _codex_rollout(tmp_path):
    path = tmp_path / "rollout-2026-10-06T21-12-40-abc.jsonl"
    lines = [
        {"type": "turn_context", "payload": {"model": "gpt-6-astra"}},
        {"type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {
            "input_tokens": 100, "cached_input_tokens": 60, "cache_write_input_tokens": 0,
            "output_tokens": 5}}}},
        {"type": "event_msg", "payload": {"type": "token_count", "info": None}},
        {"type": "event_msg", "payload": {"type": "token_count", "info": {"total_token_usage": {
            "input_tokens": 400, "cached_input_tokens": 300, "cache_write_input_tokens": 0,
            "output_tokens": 15}}}},
    ]
    path.write_text("".join(json.dumps(x) + "\n" for x in lines))
    return path


def _row(run_id):
    return db.query_one("SELECT * FROM claude_invocations WHERE id = ?", (f"work-run-{run_id}",))


class TestTranscriptUsage:
    def test_claude_counts_each_message_once_and_includes_subagents(self, tmp_path):
        got = work_store.transcript_usage(str(_claude_transcript(tmp_path)))
        assert got == {"model": "claude-opus-5-5", "input_tokens": 6,
                       "cache_creation_input_tokens": 157, "cache_read_input_tokens": 3070,
                       "output_tokens": 35}

    def test_codex_reads_the_last_cumulative_total(self, tmp_path):
        got = work_store.transcript_usage(str(_codex_rollout(tmp_path)))
        assert got == {"model": "codex:gpt-6-astra", "input_tokens": 100,
                       "cache_creation_input_tokens": 0, "cache_read_input_tokens": 300,
                       "output_tokens": 15}

    def test_transcript_without_usage_is_none(self, tmp_path):
        path = tmp_path / "empty.jsonl"
        path.write_text(json.dumps({"type": "user"}) + "\n")
        assert work_store.transcript_usage(str(path)) is None


class TestSweepUsage:
    def test_task_run_appears_on_the_invocation_log(self, tmp_path):
        item_id = work_store.create_item("make usage visible")
        run_id = work_store.add_run(item_id, f"sid-usage-{item_id}", f"work-{item_id}", str(tmp_path))
        path = _claude_transcript(tmp_path)
        db.execute("UPDATE work_runs SET transcript_path = ?, status = 'running' WHERE id = ?",
                   (str(path), run_id))

        assert work_store.sweep_usage() >= 1
        row = _row(run_id)
        assert row["function_name"] == "work_task"
        assert row["instance_key"] == core_config.BOARD_INSTANCE_KEY
        assert row["job_key"] == f"work-{item_id}"
        assert row["prompt"] == "make usage visible"
        assert row["model"] == "claude-opus-5-5"
        assert row["status"] == "running"
        assert (row["input_tokens"], row["output_tokens"]) == (6, 35)
        assert row["cache_read_input_tokens"] == 3070

    def test_sweep_skips_an_unchanged_run_and_rereads_a_grown_one(self, tmp_path):
        item_id = work_store.create_item("grow")
        run_id = work_store.add_run(item_id, f"sid-grow-{item_id}", f"work-{item_id}", str(tmp_path))
        path = _claude_transcript(tmp_path)
        db.execute("UPDATE work_runs SET transcript_path = ?, status = 'running' WHERE id = ?",
                   (str(path), run_id))
        work_store.sweep_usage()
        db.execute("UPDATE claude_invocations SET output_tokens = -1 WHERE id = ?",
                   (f"work-run-{run_id}",))
        work_store.sweep_usage()
        assert _row(run_id)["output_tokens"] == -1

        with open(path, "a") as f:
            f.write(_assistant("m9", "claude-opus-5-5", _usage(0, 0, 0, 100)))
        later = os.path.getmtime(path) + 5
        os.utime(path, (later, later))
        work_store.sweep_usage()
        assert _row(run_id)["output_tokens"] == 135

    def test_status_change_alone_updates_the_row(self, tmp_path):
        item_id = work_store.create_item("finish")
        run_id = work_store.add_run(item_id, f"sid-fin-{item_id}", f"work-{item_id}", str(tmp_path))
        path = _claude_transcript(tmp_path)
        db.execute("UPDATE work_runs SET transcript_path = ?, status = 'running' WHERE id = ?",
                   (str(path), run_id))
        work_store.sweep_usage()
        db.execute("UPDATE work_runs SET status = 'finished' WHERE id = ?", (run_id,))
        work_store.sweep_usage()
        assert _row(run_id)["status"] == "success"
