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


def _tool_use(name, tool_input, tool_id="toolu_1"):
    return json.dumps({"type": "assistant", "message": {"role": "assistant", "content": [
        {"type": "tool_use", "id": tool_id, "name": name, "input": tool_input}]}})


def _tool_result(content, tool_id="toolu_1"):
    return json.dumps({"type": "user", "message": {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": tool_id, "content": content}]}})


def _lsp(operation, query=None, tool_id="toolu_lsp"):
    tool_input = {"operation": operation, "filePath": "a.py", "line": 1, "character": 1}
    if query is not None:
        tool_input["query"] = query
    return _tool_use("LSP", tool_input, tool_id)


_LOADED = (_tool_use("ToolSearch", {"query": "select:LSP", "max_results": 1}, "toolu_ts"),
           _tool_result([{"type": "tool_reference", "tool_name": "LSP"}], "toolu_ts"))


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
    ('cat a.py | grep -n "def foo"', "foo"),
    ('cd core && grep -n "def foo" a.py', "foo"),
    ('ls; rg "class Foo"', "Foo"),
    ('find . -name "*.py" | xargs grep -n "def foo"', "foo"),
    ('find . -name "*.py" -exec grep -n "def foo" {} +', "foo"),
    ('bash -c \'grep -rn "def foo" core/\'', "foo"),
    ("python3 - <<'EOF'\nprint(1)\nEOF\ngrep -n 'def foo' a.py", "foo"),
    ('LC_ALL=C /usr/bin/grep -n "def foo" a.py', "foo"),
    ('find . -name "*.py" | xargs -I {} grep -n "def foo" {}', "foo"),
    ('sudo -u root grep -n "def foo" a.py', "foo"),
    ('bash -e -c \'grep -rn "def foo" core/\'', "foo"),
    ('diff <(grep -n "def foo" a.py) b.txt', "foo"),
    ('"grep" -n "def foo" a.py', "foo"),
    ('grep -rn \\\n  "def foo" core/', "foo"),
    ("cat <<A <<B\none\nA\ntwo\nB\ngrep -n 'def foo' a.py", "foo"),
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
    'echo "grep -n def foo" > notes.txt',
    'python3 -c "import subprocess; subprocess.run([\'grep\', \'def foo\'])"',
    "python3 - <<'EOF'\nimport re\ntext = open('a.py').read()\nprint('grep' in text)\n# def X\nEOF",
    "cat > a.sh <<EOF\ngrep -n 'def foo' a.py\nEOF\nls",
    '# grep -n "def foo" a.py',
    '# note; grep -n "def foo" a.py',
    "grep -n ordinary a.py # def foo",
    'echo ";" "grep" "def foo"',
    'env echo grep "def foo"',
    'xargs echo grep "def foo"',
    'echo a\\; grep "def foo" a.py',
    'echo prefix"; grep def foo"',
    'grep -n TODO a.py \\\n# def foo',
    r'grep -rnE "def\s+\w+" core/',
    r'grep -rn "class\s*$" core/',
    'grep -rn "Function.prototype" web/',
    "ls -la",
    "",
])
def test_allows_a_grep_that_is_not_a_definition_lookup(tmp_path, command):
    path = _transcript(tmp_path)
    assert gate.bash_reason(command, path) == ""


def test_a_tool_search_alone_keeps_the_gate_shut(tmp_path):
    path = _transcript(tmp_path, *_LOADED)
    assert gate.bash_reason('grep -n "def foo" a.py', path) == gate.DENY_REASON.format(name="foo")


def test_a_workspace_symbol_for_the_name_opens_the_gate_for_that_name(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "foo"),
                       _tool_result("No symbols found in workspace.", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def foo" a.py', path) == ""
    assert gate.grep_reason(r"def\s+foo", path) == ""


def test_a_workspace_symbol_for_another_name_keeps_the_gate_shut(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "foobar"),
                       _tool_result("Found 1 symbols in workspace:\n\na.py:\n  foobar (Function) - Line 3",
                                    "toolu_lsp"))
    assert gate.bash_reason('grep -n "def foo" a.py', path) == gate.DENY_REASON.format(name="foo")
    assert gate.grep_reason(r"def\s+foo", path) == gate.DENY_REASON.format(name="foo")


def test_a_lookup_result_that_names_the_symbol_opens_the_gate(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "parse"),
                       _tool_result("Found 2 symbols in workspace:\n\ncore/llm.py:\n"
                                    "  parse_structured (Function) - Line 40", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def parse_structured" core/llm.py', path) == ""


def test_a_document_symbol_call_does_not_open_the_gate(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("documentSymbol"),
                       _tool_result("foo (Function) - Line 3", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def foo" a.py', path) == gate.DENY_REASON.format(name="foo")


def test_no_lsp_server_opens_the_gate_for_the_session(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "bar"),
                       _tool_result("No LSP server available for file type: .sh", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def foo" a.py', path) == ""
    assert gate.grep_reason(r"function\s+baz", path) == ""


def test_a_tool_search_that_finds_no_lsp_tool_opens_the_gate(tmp_path):
    path = _transcript(tmp_path, _tool_use("ToolSearch", {"query": "select:LSP"}, "toolu_ts"),
                       _tool_result("No matching deferred tools found", "toolu_ts"))
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


def test_grep_tool_passes_once_the_symbol_is_looked_up(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("findReferences", "foo"))
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


def test_a_heredoc_body_is_not_a_grep():
    command = "python3 - <<'EOF'\nimport re\nsrc = re.sub('grep', 'rg', src)\n# `def X`\nEOF"
    assert gate._symbol(command) == ""


def test_every_definition_in_one_command_needs_a_lookup(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "foo"))
    assert gate.bash_reason('grep -n "def foo" a.py; grep -n "def bar" a.py', path) == \
        gate.DENY_REASON.format(name="bar")
    assert gate.grep_reason("def foo|def bar", path) == gate.DENY_REASON.format(name="bar")


@pytest.mark.parametrize("line", [
    '["tool_use"]',
    '"tool_use"',
    json.dumps({"message": {"content": [{"type": "tool_use", "id": "u", "name": "LSP",
                                         "input": {"operation": []}}]}}),
    json.dumps({"message": {"content": [{"type": "tool_use", "id": "u", "name": "LSP",
                                         "input": "workspaceSymbol"}]}}),
])
def test_a_malformed_transcript_line_does_not_break_the_gate(tmp_path, line):
    path = _transcript(tmp_path, line)
    assert gate.bash_reason('grep -n "def foo" a.py', path) == gate.DENY_REASON.format(name="foo")
    assert gate.grep_reason("def foo", path) == gate.DENY_REASON.format(name="foo")
