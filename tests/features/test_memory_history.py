"""Memory folders keep a git history, and a check flags stale index entries.

A wrong memory edit could not be traced to the session that made it, and the
index once told agents to test on an instance another entry said was retired.
These tests pin that every memory write becomes a commit naming its session,
and that the check reports the contradicting entries."""
import json
import os
import subprocess
import sys
from pathlib import Path

import core.db as db
from core.git_util import run_git
from scripts import install_work_hooks
from core import memory_git
from services import memory_history

INDEX = """# Memory

- [feedback_frontend.md](feedback_frontend.md) — Frontend changes must be tested via Playwright on alpha/beta before marking done
- [feedback_style.md](feedback_style.md) — reply style
- [project_instances.md](project_instances.md) — live instances are alpha and gamma; beta retired 2026-09-21, delta 2026-09-08
- [project_old.md](project_old.md) — delta config moved
"""


def _memory(tmp_path) -> Path:
    directory = tmp_path / ".claude" / "projects" / "-home-x" / "memory"
    directory.mkdir(parents=True)
    return directory


def _log(directory: Path) -> list[str]:
    return run_git(directory, ["log", "--format=%B%x00"]).stdout.strip("\x00\n").split("\x00")


def test_memory_dir_finds_the_folder_and_ignores_other_paths(tmp_path):
    directory = _memory(tmp_path)
    assert memory_git.memory_dir(str(directory / "a.md")) == directory
    assert memory_git.memory_dir(str(directory / "sub" / "a.md")) == directory
    assert memory_git.memory_dir(str(tmp_path / "memory" / "a.md")) is None
    assert memory_git.memory_dir("") is None


def test_each_write_is_a_commit_that_names_its_session(tmp_path):
    directory = _memory(tmp_path)
    (directory / "old.md").write_text("predates the history\n")
    (directory / "new.md").write_text("first\n")
    commits = memory_git.record_write(str(directory / "new.md"), "sid-one", "Write", "/t/one.jsonl")
    assert len(commits) == 2
    first, baseline = _log(directory)
    assert "Session: sid-one" not in baseline and "Record memory changes" in baseline
    assert "Session: sid-one" in first and "Transcript: /t/one.jsonl" in first
    shown = run_git(directory, ["show", "--name-only", "--format=", "HEAD"]).stdout.split()
    assert shown == ["new.md"]

    (directory / "new.md").write_text("second\n")
    assert len(memory_git.record_write(str(directory / "new.md"), "sid-two", "Edit")) == 1
    assert "Memory write new.md by session sid-two" in _log(directory)[0]
    assert memory_git.record_write(str(directory / "new.md"), "sid-three", "Edit") == []


def test_a_change_outside_a_tracked_write_is_not_credited_to_the_session(tmp_path):
    directory = _memory(tmp_path)
    (directory / "a.md").write_text("a\n")
    (directory / "b.md").write_text("b\n")
    memory_git.record_write(str(directory / "a.md"), "sid-one")
    (directory / "b.md").unlink()
    (directory / "a.md").write_text("a2\n")
    assert len(memory_git.record_write(str(directory / "a.md"), "sid-two")) == 2
    head = run_git(directory, ["show", "--name-only", "--format=", "HEAD"]).stdout.split()
    before = run_git(directory, ["show", "--name-only", "--format=%s", "HEAD~1"]).stdout
    assert head == ["a.md"]
    assert "b.md" in before and "Record memory changes found before" in before


def test_a_write_outside_a_memory_folder_commits_nothing(tmp_path):
    target = tmp_path / "notes.md"
    target.write_text("x\n")
    assert memory_git.record_write(str(target), "sid") == []
    assert not (tmp_path / ".git").exists()


def test_the_check_flags_an_entry_that_names_a_retired_instance(tmp_path):
    directory = _memory(tmp_path)
    (directory / "MEMORY.md").write_text(INDEX)
    for name in ("feedback_frontend.md", "feedback_style.md", "project_instances.md", "project_old.md"):
        (directory / name).write_text("x\n")
    findings = memory_history.stale_entries(directory)
    assert [(f["kind"], f["line"], f["name"]) for f in findings] == [
        ("retired", 3, "beta"), ("retired", 6, "delta")]
    assert findings[0]["detail"] == "MEMORY.md:3 names beta, which MEMORY.md:5 declares retired"


def test_the_check_passes_a_consistent_index(tmp_path):
    directory = _memory(tmp_path)
    (directory / "MEMORY.md").write_text(
        "- [a.md](a.md) — test on alpha\n- [b.md](b.md) — beta retired 2026-09-21\n")
    (directory / "a.md").write_text("x\n")
    (directory / "b.md").write_text("x\n")
    assert memory_history.stale_entries(directory) == []


def test_the_check_flags_missing_and_duplicate_links(tmp_path):
    directory = _memory(tmp_path)
    (directory / "MEMORY.md").write_text(
        "- [a.md](a.md) — one\n- [gone.md](gone.md) — two\n- [again](a.md) — three\n")
    (directory / "a.md").write_text("x\n")
    kinds = [(f["kind"], f["line"]) for f in memory_history.stale_entries(directory)]
    assert kinds == [("missing", 2), ("duplicate", 3)]


def test_run_reports_on_the_feed_once_per_change(tmp_path, tmp_state):
    directory = _memory(tmp_path)
    (directory / "MEMORY.md").write_text(INDEX)
    config = {"job": {"key": "frshty"},
              "llm": {"claude": {"config_dir": str(tmp_path / ".claude")}}}
    before = db.query_one("SELECT COUNT(*) AS n FROM log_events WHERE event = 'memory_stale_entries'")["n"]
    assert memory_history.run(config)[0]["findings"]
    assert memory_history.run(config) == []
    (directory / "MEMORY.md").write_text(INDEX.replace("alpha/beta", "alpha"))
    assert memory_history.run(config)
    rows = db.query_all("SELECT summary FROM log_events WHERE event = 'memory_stale_entries' "
                        "ORDER BY ts")
    assert len(rows) - before == 2
    assert "MEMORY.md:3 names beta" in rows[before]["summary"]


def test_the_hook_commits_a_memory_write(tmp_path):
    directory = _memory(tmp_path)
    (directory / "x.md").write_text("x\n")
    payload = json.dumps({"session_id": "sid-hook-mem", "hook_event_name": "PostToolUse",
                          "tool_name": "Write", "tool_input": {"file_path": str(directory / "x.md")}})
    r = subprocess.run([sys.executable, "scripts/work_hook.py"], input=payload,
                       capture_output=True, text=True, timeout=30,
                       env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)})
    assert r.returncode == 0, r.stderr
    assert r.stdout == ""
    assert "by session sid-hook-mem" in _log(directory)[0]


def test_the_installer_adds_the_post_tool_use_hook(tmp_path):
    settings = tmp_path / "settings.json"
    added = install_work_hooks.install_into(str(settings))
    assert f"PostToolUse[{install_work_hooks.WRITE_GATE_MATCHER}]" in added
    entry = json.loads(settings.read_text())["hooks"]["PostToolUse"][0]
    assert "async" not in entry["hooks"][0]
    assert install_work_hooks.install_into(str(settings)) == []


def test_an_edit_made_outside_a_write_is_committed_before_the_write(tmp_path):
    directory = _memory(tmp_path)
    target = directory / "a.md"
    target.write_text("one\ntwo\n")
    memory_git.record_write(str(target), "sid-one")
    target.write_text("ONE\ntwo\n")
    assert len(memory_git.record_pending(str(target), "sid-two")) == 1
    target.write_text("ONE\nTWO\n")
    assert len(memory_git.record_write(str(target), "sid-two")) == 1
    diff = run_git(directory, ["show", "--format=", "HEAD"]).stdout
    assert "+TWO" in diff and "+ONE" not in diff


def test_a_file_name_with_glob_characters_is_matched_literally(tmp_path):
    directory = _memory(tmp_path)
    (directory / "a1.md").write_text("x\n")
    (directory / "a[1].md").write_text("y\n")
    commits = memory_git.record_write(str(directory / "a[1].md"), "sid-glob")
    assert len(commits) == 2
    assert run_git(directory, ["show", "--name-only", "--format=", "HEAD"]).stdout.split() == ["a[1].md"]


def test_a_commit_says_so_when_another_writer_replaced_the_text(tmp_path):
    directory = _memory(tmp_path)
    target = directory / "a.md"
    target.write_text("from session b\n")
    memory_git.record_write(str(target), "sid-a", "Write", expected="from session a\n")
    assert "Another writer changed it" in _log(directory)[0]
    target.write_text("from session a\n")
    memory_git.record_write(str(target), "sid-a", "Write", expected="from session a\n")
    assert "Another writer" not in _log(directory)[0]


def test_a_retirement_in_the_passive_voice_names_its_subjects(tmp_path):
    directory = _memory(tmp_path)
    (directory / "MEMORY.md").write_text(
        "- [a.md](a.md) — alpha and beta are retired\n"
        "- [b.md](b.md) — alpha is the live instance\n"
        "- [c.md](c.md) — these are the notes\n"
        "- [d.md](d.md) — gamma was retired on 2026-09-21\n")
    for name in "abcd":
        (directory / f"{name}.md").write_text("x\n")
    assert [(f["line"], f["name"]) for f in memory_history.stale_entries(directory)] == [(2, "alpha")]


def test_a_bare_bullet_and_a_pronoun_are_not_names(tmp_path):
    directory = _memory(tmp_path)
    (directory / "MEMORY.md").write_text(
        "- test frontend changes on zicioxly\n"
        "- zicioxly was retired\n"
        "- we replaced alpha; it is retired\n"
        "- run it before shipping\n")
    assert [(f["line"], f["name"]) for f in memory_history.stale_entries(directory)] == [(1, "zicioxly")]
