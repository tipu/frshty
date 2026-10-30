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


def _position_lookup(operation, file_path, line, character, cwd, tool_id="toolu_pos"):
    entry = json.loads(_tool_use("LSP", {"operation": operation, "filePath": file_path,
                                         "line": line, "character": character}, tool_id))
    entry["cwd"] = cwd
    return json.dumps(entry)


@pytest.mark.parametrize("operation", ["findReferences", "goToDefinition"])
def test_a_position_lookup_opens_the_gate_for_the_identifier_there(tmp_path, operation):
    (tmp_path / "a.py").write_text("import os\n\ndef split_pin(x):\n    return x\n")
    path = _transcript(tmp_path, *_LOADED, _position_lookup(operation, "a.py", 3, 5, str(tmp_path)),
                       _tool_result("Found 1 references across 1 files:\n\na.py:\n  Line 3:5",
                                    "toolu_pos"))
    assert gate.bash_reason('grep -n "def split_pin" a.py', path) == ""
    assert gate.bash_reason('grep -n "def other" a.py', path) == gate.DENY_REASON.format(name="other")


def test_grep_tool_passes_once_the_symbol_is_looked_up(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "foo"))
    assert gate.grep_reason(r"def\s+foo", path) == ""


def test_a_long_alternation_pattern_is_read_in_linear_time(tmp_path):
    start = time.monotonic()
    assert gate.grep_reason("def" + "|def" * 20000, _transcript(tmp_path)) == ""
    assert time.monotonic() - start < 2


def test_an_unreadable_transcript_opens_the_gate(tmp_path):
    assert gate.bash_reason('grep -n "def foo" a.py', str(tmp_path / "missing.jsonl")) == ""
    assert gate.bash_reason('grep -n "def foo" a.py', "") == ""


def _run_hook(session_id, command, transcript_path, tool="Bash", pipeline=False):
    env = {k: v for k, v in os.environ.items() if k != gate.GATE_ENV}
    if pipeline:
        env[gate.GATE_ENV] = "1"
    tool_input = {"command": command} if tool == "Bash" else {"pattern": command}
    return subprocess.run(
        [sys.executable, "scripts/work_hook.py"], cwd=ROOT,
        input=json.dumps({"session_id": session_id, "hook_event_name": "PreToolUse",
                          "tool_name": tool, "cwd": ROOT,
                          "transcript_path": transcript_path,
                          "tool_input": tool_input}),
        capture_output=True, text=True, timeout=120,
        env={**env, "FRSHTY_DB": str(db._DB_PATH)},
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


def test_hook_denies_a_symbol_grep_in_a_pipeline_session(tmp_path):
    for tool, pattern in (("Bash", 'grep -n "def parse_structured" core/llm.py'),
                          ("Grep", r"def\s+parse_structured")):
        r = _run_hook("sid-pipeline-run", pattern, _transcript(tmp_path), tool=tool, pipeline=True)
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)["hookSpecificOutput"]
        assert out["permissionDecision"] == "deny"
        assert out["permissionDecisionReason"] == gate.DENY_REASON.format(name="parse_structured")


def test_hook_allows_a_text_grep_in_a_pipeline_session(tmp_path):
    r = _run_hook("sid-pipeline-run", 'grep -rn "llm_schema_invalid" core/',
                  _transcript(tmp_path), pipeline=True)
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
    assert gate.grep_reason("def (foo|bar)", path) == gate.DENY_REASON.format(name="bar")
    assert gate.bash_reason(r'grep -rnE "(def|class)\s+(foo|bar)" core/', path) == \
        gate.DENY_REASON.format(name="bar")


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


@pytest.mark.parametrize("command", [
    'grep -n "^def test" tests/test_a.py',
    'grep -n "def test_" tests/test_a.py',
    r'grep -n "def test_.*redact\|def test_.*raw" tests/test_a.py',
    'grep -rhn "def test_[a-z]*_[a-z]*[A-Z]" src',
    'grep -n "def get_.*" a.py',
    r'grep -n "def get_\w*" a.py',
    'grep -n "class \\|def test" tests/test_a.py',
])
def test_a_prefix_listing_is_not_a_definition_lookup(tmp_path, command):
    assert gate.bash_reason(command, _transcript(tmp_path)) == ""


def _refusal(name, tool_id, tool="Bash"):
    return json.dumps({"type": "user", "message": {"role": "user", "content": [
        {"type": "tool_result", "tool_use_id": tool_id, "is_error": True,
         "content": f"PreToolUse:{tool} hook error: " + gate.DENY_REASON.format(name=name)}]}})


def test_a_refused_symbol_cannot_be_grepped_in_another_form(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": 'grep -n "def foo" a.py'}, "toolu_d"),
                       _refusal("foo", "toolu_d"))
    assert gate.bash_reason('grep -n "foo" a.py', path) == gate.REPEAT_REASON.format(name="foo")
    assert gate.bash_reason('sed -n 1,5p a.py; git grep -n "foo(" -- src', path) == \
        gate.REPEAT_REASON.format(name="foo")
    assert gate.grep_reason("foo", path) == gate.REPEAT_REASON.format(name="foo")
    assert gate.bash_reason('grep -n "foobar" a.py', path) == ""
    assert gate.bash_reason("sed -n 1,5p foo.py; awk '/foo/' a.py", path) == ""


def test_an_lsp_lookup_releases_a_refused_symbol(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": 'grep -n "def foo" a.py'}, "toolu_d"),
                       _refusal("foo", "toolu_d"), *_LOADED, _lsp("workspaceSymbol", "foo"))
    assert gate.bash_reason('grep -n "foo" a.py', path) == ""
    assert gate.bash_reason('grep -n "def foo" a.py', path) == ""


def test_the_old_deny_message_still_counts_as_a_refusal(tmp_path):
    old = ("PreToolUse:Grep hook error: Blocked by the LSP gate: this grep looks up the definition "
           "of `foo`, and this session has not looked up `foo` with LSP. If LSP is not loaded, load "
           "it once with ToolSearch, query \"select:LSP\". Then call LSP with operation "
           "workspaceSymbol, query \"foo\", and any file of the project as filePath.")
    path = _transcript(tmp_path, _tool_use("Grep", {"pattern": "def foo"}, "toolu_d"),
                       json.dumps({"type": "user", "message": {"role": "user", "content": [
                           {"type": "tool_result", "tool_use_id": "toolu_d", "is_error": True,
                            "content": old}]}}))
    assert gate.grep_reason("foo", path) == gate.REPEAT_REASON.format(name="foo")


def test_quoted_deny_text_in_a_successful_result_refuses_nothing(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "cat log.txt"}, "toolu_c"),
                       _tool_result("Blocked by the LSP gate: x. LSP with operation "
                                    'workspaceSymbol, query "foo"', "toolu_c"))
    assert gate.bash_reason('grep -n "foo" a.py', path) == ""


def test_the_deny_message_asks_for_the_lsp_call_first():
    reason = gate.DENY_REASON.format(name="foo")
    assert "Nothing in this command ran" in reason
    assert "next tool call" in reason
    assert gate._REFUSED.match("PreToolUse:Bash hook error: " + reason).group(1) == "foo"
    assert gate._REFUSED.match(gate.REPEAT_REASON.format(name="foo")).group(1) == "foo"


def _notice(name):
    return "echo '" + gate.REMOVED_NOTICE.format(name=name) + "'"


@pytest.mark.parametrize("command,expected", [
    ('sed -n 1,5p a.py; grep -n "def foo" -A3 a.py | head; sed -n 9p b.py',
     'sed -n 1,5p a.py; ' + _notice("foo") + '; sed -n 9p b.py'),
    ('cd d && sed -n 1p a.py && grep -rnP "\\bdef\\s*foo\\(" a.py 2>&1',
     'cd d && sed -n 1p a.py && ' + _notice("foo")),
    ('grep -n "def foo" a.py\nls',
     _notice("foo") + "\nls"),
    ('cat a.py | grep -n "class Foo"; wc -l a.py',
     _notice("Foo") + "; wc -l a.py"),
    ('ls; LC_ALL=C grep -rn "def foo" src 2>/dev/null | sort | uniq -c | sed -n 1,5p',
     'ls; ' + _notice("foo")),
    ('ls; find . -name "*.py" | xargs grep -n "def foo" | head -3',
     'ls; ' + _notice("foo")),
    ("ls; grep -rn 'def foo' src | sort -u | uniq -c | sed -n '$p'",
     'ls; ' + _notice("foo")),
])
def test_rewrite_removes_only_the_refused_grep(tmp_path, command, expected):
    assert gate.bash_rewrite(command, _transcript(tmp_path)) == expected


@pytest.mark.parametrize("command", [
    'grep -n "def foo" a.py',
    'cd d && grep -n "def foo" a.py',
    'grep -n "def foo" a.py && sed -n 1p a.py',
    'grep -q "def foo" a.py || echo missing; ls',
    'ls $(grep -l "def foo" .); ls',
    'ls `grep -l "def foo" .`; ls',
    '(grep -n "def foo" a.py); ls',
    'if grep -q "def foo" a.py; then ls; fi',
    'for f in *.py; do grep -n "def foo" $f; done',
    "cat <<EOF > x\nhi\nEOF\ngrep -n 'def foo' a.py; ls",
    'grep -n "def foo" a.py & ls',
    'ls; grep -n "def foo" a.py # note',
    'ls; grep -n "def foo',
    'grep -n "def foo" a.py > out.txt; wc -l out.txt',
    'grep -n "def foo" a.py | tee out.txt >> log; ls',
    "set -e; grep -q 'def ready' input.py; rm valuable.txt",
    "set -euo pipefail\ngrep -n 'def ready' input.py\nls",
    "grep -q 'def ready' input.py; echo $?",
    "bash -c 'cp valuable.txt backup.txt; grep \"def ready\" input.py'; rm valuable.txt",
    "grep -v 'def obsolete' input.py | tee new.py; mv new.py valuable.txt",
    "grep -n 'def foo' a.py | sed -i s/x/y/ b.py; ls",
    "grep -n 'def foo' a.py | xargs rm; ls",
    "find . -name '*.pyc' -delete | grep -n 'def foo'; ls",
    "find . -delete -exec grep -n 'def foo' {} +; ls",
    "set -u -e; grep -q 'def ready' input.py; rm valuable.txt",
    "grep -q 'def ready' input.py; test \"${?}\" -eq 0 && rm x",
    "grep -v 'def obsolete' input.py | sort -o new.py; mv new.py valuable.txt",
    "grep -v 'def obsolete' input.py | uniq - new.py; mv new.py valuable.txt",
    "grep -n 'def foo' a.py | sed 'w new.py'; ls",
    "grep 'def ready' \"$(cp valuable.txt backup.txt; echo input.py)\"; rm valuable.txt",
    "find input.py -exec cp valuable.txt backup.txt \\; -exec grep 'def ready' '{}' \\;; rm valuable.txt",
    "git grep -O'rm -rf x' -n 'def foo'; ls",
    "rg --pre ./run.sh 'def foo'; ls",
    "exec grep 'def ready' input.py; rm valuable.txt",
    "grep -v 'def obsolete' input.py | sort -uonew.py; mv new.py valuable.txt",
    "grep -v 'def obsolete' input.py | sort --compress-program=sh; ls",
    "env find input.py -exec cp valuable.txt backup.txt \\; -exec grep 'def ready' '{}' \\;; rm valuable.txt",
    "ls; env grep -n 'def foo' a.py",
    "exec 3>new.py; grep -v 'def obsolete' input.py >&3; mv new.py valuable.txt",
    "ls; grep -n 'def foo' a.py >&3",
    "git -c core.fsmonitor=./x grep -n 'def foo'; ls",
    "eval grep -n 'def foo' a.py; ls",
    "ls; . ./env.sh; grep -n 'def foo' a.py",
    "grep -q 'def ready' input.py; test \"$PIPESTATUS\" -eq 0 && rm valuable.txt",
    "grep 'def ready' valuable.txt; cp \"$_\" backup.txt; rm valuable.txt",
    "git grep -nO./save.sh 'def ready'; rm valuable.txt",
    "ls $HOME; grep -n 'def foo' a.py",
    "grep -n 'def foo' a.py | python3 -c 'import sys'; ls",
])
def test_rewrite_refuses_a_command_it_cannot_split_safely(tmp_path, command):
    assert gate.bash_rewrite(command, _transcript(tmp_path)) == ""


def test_rewrite_keeps_a_grep_the_session_looked_up(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "foo"))
    assert gate.bash_rewrite('ls; grep -n "def foo" a.py; grep -n "def bar" a.py', path) == \
        'ls; grep -n "def foo" a.py; ' + _notice("bar")


def test_rewrite_follows_redirects_of_stderr(tmp_path):
    assert gate.bash_rewrite('ls 2>&1; grep -n "def foo" a.py &>/dev/null; ls', _transcript(tmp_path)) == \
        'ls 2>&1; ' + _notice("foo") + "; ls"


def test_the_removed_notice_counts_as_a_refusal(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "ls; " + _notice("foo")}, "toolu_r"),
                       _tool_result("a.py\n" + gate.REMOVED_NOTICE.format(name="foo"), "toolu_r"))
    assert gate.bash_reason('grep -n "foo" a.py', path) == gate.REPEAT_REASON.format(name="foo")


def test_the_notice_runs_in_a_shell(tmp_path):
    out = subprocess.run(["bash", "-c", gate.bash_rewrite("true; grep -n 'def x$y' a.py",
                                                          _transcript(tmp_path))],
                         capture_output=True, text=True, timeout=30)
    assert out.stdout.strip() == gate.REMOVED_NOTICE.format(name="x$y")


def test_hook_runs_the_rest_of_a_compound_command(tmp_path):
    item_id = work_store.create_item("lsp gate rewrite")
    sid = f"sid-lsp-rw-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    r = _run_hook(sid, 'sed -n 1p a.py; grep -n "def parse_structured" core/llm.py', _transcript(tmp_path))
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "allow"
    assert out["updatedInput"]["command"] == "sed -n 1p a.py; " + _notice("parse_structured")


def test_hook_denies_a_compound_command_that_commits(tmp_path):
    item_id = work_store.create_item("lsp gate commit")
    sid = f"sid-lsp-commit-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    r = _run_hook(sid, 'git commit -m x; grep -n "def parse_structured" core/llm.py', _transcript(tmp_path))
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["hookSpecificOutput"]["permissionDecision"] == "deny"
