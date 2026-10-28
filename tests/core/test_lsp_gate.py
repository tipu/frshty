import importlib.util
import json
import os
import subprocess
import sys
import time

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
    (r'grep -rnE "def\s+parse_structured" core/', "parse_structured"),
    (r'rg "^\s*class\s+OpenCodeProvider\b" core/', "OpenCodeProvider"),
    (r'grep -n "def\\s\+parse_structured" core/llm.py', "parse_structured"),
    (r'grep -rnE "def[[:space:]]+parse_structured" core/', "parse_structured"),
    (r'grep -rnP "\bdef\s*launch\(" services/', "launch"),
    (r'grep -rnE "(def|class)\s+OpenCodeProvider" core/', "OpenCodeProvider"),
    (r'grep -rn "def.*parse_structured" core/', "parse_structured"),
    (r'grep -rn "def\ parse_structured" core/', "parse_structured"),
])
def test_denies_a_symbol_grep_before_lsp_is_loaded(tmp_path, command, name):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "ls"}))
    assert gate.bash_reason(command, path) == gate.DENY_REASON.format(name=name)


@pytest.mark.parametrize("command", [
    'grep -rn "llm_schema_invalid" core/',
    "grep -n LSP_RULE core/llm.py",
    'git log --grep "class Foo"',
    'echo "def foo"',
    r'grep -rnE "def\s+\w+" core/',
    r'grep -rn "class\s*$" core/',
    'grep -rn "Function.prototype" web/',
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


@pytest.mark.parametrize("pattern,name", [
    ("def parse_structured", "parse_structured"),
    (r"class\s+OpenCodeProvider", "OpenCodeProvider"),
    (r"^\s*(?:async\s+)?def\s+launch\b", "launch"),
    (r"function\s*renderBoard\(", "renderBoard"),
    (r"(class|interface)\s+TaskRow", "TaskRow"),
    (r"def\s+\bparse_structured\b", "parse_structured"),
    (r"def\s+(parse_structured)", "parse_structured"),
    (r"def\s+\<parse_structured", "parse_structured"),
    (r"def\s{1,}parse_structured", "parse_structured"),
    (r"def\s+?parse_structured", "parse_structured"),
    (r"def[ ]+parse_structured", "parse_structured"),
    (r"def[ \t]{1,4}parse_structured", "parse_structured"),
    (r"def\t+parse_structured", "parse_structured"),
    (r"def\W+parse_structured", "parse_structured"),
    (r"\ndef\s+parse_structured", "parse_structured"),
    (r"\sclass\s+OpenCodeProvider", "OpenCodeProvider"),
    (r"\bdef\b\s+parse_structured\b", "parse_structured"),
    (r"def(?:\s+)parse_structured", "parse_structured"),
    (r"def\>[[:space:]]+parse_structured", "parse_structured"),
])
def test_grep_tool_denies_a_definition_pattern_before_lsp_is_loaded(tmp_path, pattern, name):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "ls"}))
    assert gate.grep_reason(pattern, path) == gate.DENY_REASON.format(name=name)


@pytest.mark.parametrize("pattern", [
    "llm_schema_invalid",
    "LSP_RULE",
    r"def\s+\w+",
    "",
])
def test_grep_tool_allows_a_pattern_that_is_not_a_definition_lookup(tmp_path, pattern):
    assert gate.grep_reason(pattern, _transcript(tmp_path)) == ""


def test_grep_tool_passes_once_lsp_is_loaded(tmp_path):
    path = _transcript(tmp_path, _tool_use("ToolSearch", {"query": "select:LSP"}))
    assert gate.grep_reason(r"def\s+foo", path) == ""


def test_a_long_alternation_pattern_is_read_in_linear_time(tmp_path):
    start = time.monotonic()
    assert gate.grep_reason("def" + "|def" * 20000, _transcript(tmp_path)) == ""
    assert time.monotonic() - start < 2


def test_an_unreadable_transcript_opens_the_gate(tmp_path):
    assert gate.bash_reason('grep -n "def foo" a.py', str(tmp_path / "missing.jsonl")) == ""
    assert gate.bash_reason('grep -n "def foo" a.py', "") == ""


def _run_hook(session_id, command, transcript_path, tool="Bash"):
    tool_input = {"command": command} if tool == "Bash" else {"pattern": command}
    return subprocess.run(
        [sys.executable, "scripts/work_hook.py"], cwd=ROOT,
        input=json.dumps({"session_id": session_id, "hook_event_name": "PreToolUse",
                          "tool_name": tool, "cwd": ROOT,
                          "transcript_path": transcript_path,
                          "tool_input": tool_input}),
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


def test_hook_denies_a_definition_grep_through_the_grep_tool(tmp_path):
    item_id = work_store.create_item("lsp gate grep tool")
    sid = f"sid-lsp-grep-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    r = _run_hook(sid, r"def\s+parse_structured", _transcript(tmp_path), tool="Grep")
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert out["permissionDecisionReason"] == gate.DENY_REASON.format(name="parse_structured")


def test_hook_allows_a_text_search_through_the_grep_tool(tmp_path):
    item_id = work_store.create_item("lsp gate grep tool text")
    sid = f"sid-lsp-grep-text-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    r = _run_hook(sid, "llm_schema_invalid", _transcript(tmp_path), tool="Grep")
    assert r.returncode == 0, r.stderr
    assert r.stdout.strip() == ""


def test_installer_registers_the_grep_tool():
    installer = importlib.util.module_from_spec(
        importlib.util.spec_from_file_location(
            "install_work_hooks_lsp", os.path.join(ROOT, "scripts", "install_work_hooks.py")))
    installer.__spec__.loader.exec_module(installer)
    matchers = [e["matcher"] for e in installer._wanted_entries("PreToolUse", "cmd")]
    assert "Grep" in matchers
