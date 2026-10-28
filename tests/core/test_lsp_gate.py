import json
import os
import subprocess
import sys

import pytest

import core.db as db
import core.lsp_gate as gate
from services import work_store

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _tool_use(name, tool_input):
    return json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "tool_use", "id": "toolu_1", "name": name, "input": tool_input}]}})


def _transcript(tmp_path, *lines):
    path = tmp_path / "session.jsonl"
    path.write_text("".join(line + "\n" for line in lines))
    return str(path)


@pytest.mark.parametrize("command,name", [
    ('grep -n "class OpenCodeProvider" -A 200 core/llm.py', "OpenCodeProvider"),
    ('grep -rn "def parse_structured" -A40 frshty/core/', "parse_structured"),
    ("rg 'async def launch' services/", "launch"),
    ('cd web && grep -rn "function renderBoard" static/', "renderBoard"),
    ('git grep -n "interface TaskRow"', "TaskRow"),
])
def test_denies_a_symbol_grep_before_lsp_is_loaded(tmp_path, command, name):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "ls"}))
    assert gate.bash_reason(command, path) == gate.DENY_REASON.format(name=name)


@pytest.mark.parametrize("command", [
    'grep -rn "llm_schema_invalid" core/',
    "grep -n LSP_RULE core/llm.py",
    'git log --grep "class Foo"',
    'echo "def foo"',
    "ls -la",
    "",
])
def test_allows_a_grep_that_is_not_a_definition_lookup(tmp_path, command):
    path = _transcript(tmp_path)
    assert gate.bash_reason(command, path) == ""


@pytest.mark.parametrize("line", [
    _tool_use("ToolSearch", {"query": "select:LSP", "max_results": 1}),
    _tool_use("LSP", {"operation": "workspaceSymbol", "filePath": "a.py",
                      "line": 1, "character": 1, "query": "x"}),
])
def test_one_lsp_load_opens_the_gate(tmp_path, line):
    path = _transcript(tmp_path, line)
    assert gate.bash_reason('grep -n "def foo" a.py', path) == ""


def test_a_tool_search_for_another_tool_keeps_the_gate_shut(tmp_path):
    path = _transcript(tmp_path, _tool_use("ToolSearch", {"query": "select:WebFetch"}))
    assert gate.bash_reason('grep -n "def foo" a.py', path) != ""


def test_an_unreadable_transcript_opens_the_gate(tmp_path):
    assert gate.bash_reason('grep -n "def foo" a.py', str(tmp_path / "missing.jsonl")) == ""
    assert gate.bash_reason('grep -n "def foo" a.py', "") == ""


def _run_hook(session_id, command, transcript_path):
    return subprocess.run(
        [sys.executable, "scripts/work_hook.py"], cwd=ROOT,
        input=json.dumps({"session_id": session_id, "hook_event_name": "PreToolUse",
                          "tool_name": "Bash", "cwd": ROOT,
                          "transcript_path": transcript_path,
                          "tool_input": {"command": command}}),
        capture_output=True, text=True, timeout=120,
        env={**os.environ, "FRSHTY_DB": str(db._DB_PATH)},
    )


def test_hook_denies_a_symbol_grep_in_a_task(tmp_path):
    item_id = work_store.create_item("lsp gate")
    sid = f"sid-lsp-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    path = _transcript(tmp_path)
    r = _run_hook(sid, 'grep -n "def parse_structured" core/llm.py', path)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert out["permissionDecisionReason"] == gate.DENY_REASON.format(name="parse_structured")


def test_hook_leaves_a_session_that_is_not_a_task_alone(tmp_path):
    r = _run_hook("sid-not-a-work-run", 'grep -n "def parse_structured" core/llm.py',
                  _transcript(tmp_path))
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""
