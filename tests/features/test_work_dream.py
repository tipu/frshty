"""The nightly memory review: repeated corrections become memory proposals.

These tests pin that the review files a proposal only for a pattern that
verified quotes from two or more sessions back, that it never writes the
memory folder, and that a change the operator declined is not filed again."""
import json
from datetime import datetime, timezone

import pytest

import core.db as db
from core.tasks.registry import TaskContext, run_task
from services import work_dream, work_store

STALE = "the soheastco instance is retired; test on bekeanlabs"
COLUMN = "work_items has state, not status"


@pytest.fixture(autouse=True)
def _db(fresh_db):
    yield


@pytest.fixture
def memory(tmp_path):
    folder = tmp_path / "memory"
    folder.mkdir()
    (folder / "MEMORY.md").write_text("- [Soheastco](feedback_soheastco.md) - test on soheastco\n")
    (folder / "feedback_soheastco.md").write_text("---\nname: soheastco\n---\ntest on soheastco\n")
    return folder


@pytest.fixture
def config(tmp_path, memory):
    (tmp_path / "frshty").mkdir()
    return {"job": {"key": "frshty"},
            "workspace": {"root": tmp_path, "repos": ["frshty"]},
            "dream": {"memory_dir": str(memory)}}


def _snapshot(folder) -> dict:
    return {p.name: p.read_bytes() for p in folder.iterdir()}


def _session(tmp_path, prompts: list[str], summary: str = "fixed it") -> int:
    item_id = work_store.create_item("fix the board")
    transcript = tmp_path / f"t-{item_id}.jsonl"
    with open(transcript, "w") as f:
        for text in prompts:
            f.write(json.dumps({"type": "user", "message": {"content": text},
                                "timestamp": "2026-11-01T00:00:00Z"}) + "\n")
    work_store.add_run(item_id, f"sid-dream-{item_id}", f"work-{item_id}", "/tmp")
    db.execute("UPDATE work_runs SET transcript_path = ? WHERE work_item_id = ?",
               (str(transcript), item_id))
    db.execute("UPDATE work_items SET state = 'done', summary = ? WHERE id = ?",
               (summary, item_id))
    return item_id


def _llm(monkeypatch, proposals):
    calls = []

    def fake(prompt, **kwargs):
        calls.append(prompt)
        return proposals if proposals is None else json.dumps({"proposals": proposals})
    monkeypatch.setattr(work_dream.llm, "run_fast", fake)
    return calls


def _proposal(items, quotes, change="update", file="feedback_soheastco.md"):
    return {"change": change, "file": file, "type": "project",
            "description": "soheastco is retired", "memory": "Test on bekeanlabs.",
            "pattern": "agents test on the retired soheastco instance",
            "evidence": [{"item": i, "quote": q} for i, q in zip(items, quotes)]}


def _proposals() -> list[dict]:
    return db.query_all("SELECT * FROM work_items WHERE proposal_key LIKE 'memory:%' "
                        "ORDER BY id")


def test_a_pattern_seen_in_two_sessions_is_proposed_and_memory_is_untouched(
        config, memory, tmp_path, monkeypatch):
    a = _session(tmp_path, ["launch prompt", STALE])
    b = _session(tmp_path, ["launch prompt", "no. " + STALE.upper()])
    before = _snapshot(memory)
    calls = _llm(monkeypatch, [_proposal([a, b], [STALE, STALE])])
    result = work_dream.run(config)
    assert _snapshot(memory) == before
    assert [p["sessions"] for p in result["proposed"]] == [[a, b]]
    [row] = _proposals()
    assert row["state"] == work_store.PROPOSED_STATE
    assert row["proposal_key"] == "memory:update:feedback_soheastco.md"
    assert "2 sessions" in row["current_checkpoint"]
    assert f"#{a}" in row["current_checkpoint"] and f"#{b}" in row["current_checkpoint"]
    assert str(tmp_path / f"t-{a}.jsonl") in row["launch_brief"]
    assert "feedback_soheastco.md" in calls[0]


def test_a_pattern_from_one_session_is_dropped(config, tmp_path, monkeypatch):
    a = _session(tmp_path, ["launch prompt", STALE])
    _session(tmp_path, ["launch prompt", COLUMN])
    _llm(monkeypatch, [_proposal([a, a], [STALE, STALE])])
    result = work_dream.run(config)
    assert result["proposed"] == []
    assert "1 verified sessions" in result["dropped"][0]
    assert _proposals() == []


def test_a_quote_not_in_the_session_does_not_count(config, tmp_path, monkeypatch):
    a = _session(tmp_path, ["launch prompt", STALE])
    b = _session(tmp_path, ["launch prompt", COLUMN])
    _llm(monkeypatch, [_proposal([a, b], [STALE, STALE])])
    assert work_dream.run(config)["proposed"] == []
    assert _proposals() == []


def test_board_prompts_and_the_launch_prompt_are_not_corrections(tmp_path):
    item_id = _session(tmp_path, [STALE, f"{work_store.BOARD_PROMPT_MARKER} {COLUMN}",
                                  "<task-notification>done</task-notification>"])
    item = db.query_one("SELECT * FROM work_items WHERE id = ?", (item_id,))
    assert work_dream.item_digest(item)["text"] == ""


def test_an_operator_reply_event_is_a_correction(tmp_path):
    item_id = _session(tmp_path, ["launch prompt"])
    db.execute("INSERT INTO work_events(work_item_id, kind, payload, created_at) "
               "VALUES (?, 'operator_reply', ?, ?)",
               (item_id, json.dumps({"text": COLUMN}), work_store._now()))
    item = db.query_one("SELECT * FROM work_items WHERE id = ?", (item_id,))
    assert COLUMN in work_dream.item_digest(item)["text"]


def test_a_declined_change_is_not_proposed_again(config, tmp_path, monkeypatch):
    a = _session(tmp_path, ["launch prompt", STALE])
    b = _session(tmp_path, ["launch prompt", STALE])
    _llm(monkeypatch, [_proposal([a, b], [STALE, STALE])])
    work_dream.run(config)
    [row] = _proposals()
    assert not work_store.apply_action(row["id"], "decline").get("error")
    result = work_dream.run(config)
    assert result["proposed"] == []
    assert len(_proposals()) == 1


@pytest.mark.parametrize("change, file", [("add", "feedback_soheastco.md"),
                                          ("update", "feedback_missing.md"),
                                          ("remove", "feedback_missing.md"),
                                          ("add", "../escape.md"),
                                          ("add", "MEMORY.md"),
                                          ("rename", "feedback_soheastco.md")])
def test_a_change_that_does_not_fit_the_memory_folder_is_dropped(
        config, tmp_path, monkeypatch, change, file):
    a = _session(tmp_path, ["launch prompt", STALE])
    b = _session(tmp_path, ["launch prompt", STALE])
    _llm(monkeypatch, [_proposal([a, b], [STALE, STALE], change=change, file=file)])
    assert work_dream.run(config)["proposed"] == []
    assert _proposals() == []


def test_a_failed_model_call_files_nothing(config, tmp_path, monkeypatch):
    _session(tmp_path, ["launch prompt", STALE])
    _session(tmp_path, ["launch prompt", STALE])
    _llm(monkeypatch, None)
    assert work_dream.run(config)["error"] == "llm invocation failed"
    assert _proposals() == []


def test_fewer_than_two_corrected_sessions_skips_the_model(config, tmp_path, monkeypatch):
    _session(tmp_path, ["launch prompt", STALE])
    calls = _llm(monkeypatch, [])
    work_dream.run(config)
    assert calls == []


def test_the_memory_folder_defaults_to_the_checkout_s_claude_project(config, tmp_path,
                                                                     monkeypatch):
    del config["dream"]
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "cfg"))
    slug = str(tmp_path / "frshty").replace("/", "-").replace(".", "-")
    assert work_dream.memory_dir(config) == str(tmp_path / "cfg" / "projects" / slug / "memory")


@pytest.mark.parametrize("cfg, expected", [
    ({"job": {"key": "frshty"}}, True),
    ({"job": {"key": "personal"}}, False),
    ({"job": {"key": "personal"}, "dream": {"enabled": True}}, True),
    ({"job": {"key": "frshty"}, "dream": {"enabled": False}}, False),
])
def test_only_the_frshty_board_dreams_by_default(cfg, expected):
    assert work_dream.enabled(cfg) is expected


def test_the_task_skips_an_instance_where_it_is_off(config, monkeypatch):
    calls = _llm(monkeypatch, [])
    config["dream"]["enabled"] = False
    ctx = TaskContext(instance_key="frshty", ticket_key=None, task="work_dream",
                      payload={}, job_id=0, triggering_event_id=None, config=config,
                      registry=None, now=datetime.now(timezone.utc))
    assert run_task(ctx).status == "ok"
    assert calls == []
