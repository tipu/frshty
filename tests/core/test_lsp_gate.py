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
    ("rg 'async def launch_task' services/", "launch_task"),
    ('cd web && grep -rn "function renderBoard" static/', "renderBoard"),
    ('git grep -n "interface TaskRowView"', "TaskRowView"),
    ('cat a.py | grep -n "def fetch_rows"', "fetch_rows"),
    ('cd core && grep -n "def fetch_rows" a.py', "fetch_rows"),
    ('ls; rg "class FetchRows"', "FetchRows"),
    ('find . -name "*.py" | xargs grep -n "def fetch_rows"', "fetch_rows"),
    ('find . -name "*.py" -exec grep -n "def fetch_rows" {} +', "fetch_rows"),
    ('bash -c \'grep -rn "def fetch_rows" core/\'', "fetch_rows"),
    ("python3 - <<'EOF'\nprint(1)\nEOF\ngrep -n 'def fetch_rows' a.py", "fetch_rows"),
    ('LC_ALL=C /usr/bin/grep -n "def fetch_rows" a.py', "fetch_rows"),
    ('find . -name "*.py" | xargs -I {} grep -n "def fetch_rows" {}', "fetch_rows"),
    ('sudo -u root grep -n "def fetch_rows" a.py', "fetch_rows"),
    ('bash -e -c \'grep -rn "def fetch_rows" core/\'', "fetch_rows"),
    ('diff <(grep -n "def fetch_rows" a.py) b.txt', "fetch_rows"),
    ('"grep" -n "def fetch_rows" a.py', "fetch_rows"),
    ('grep -rn \\\n  "def fetch_rows" core/', "fetch_rows"),
    ("cat <<A <<B\none\nA\ntwo\nB\ngrep -n 'def fetch_rows' a.py", "fetch_rows"),
    (r'grep -rnE "def\s+parse_structured" core/', "parse_structured"),
    (r'rg "^\s*class\s+OpenCodeProvider\b" core/', "OpenCodeProvider"),
    (r'grep -n "def\\s\+parse_structured" core/llm.py', "parse_structured"),
    (r'grep -rnE "def[[:space:]]+parse_structured" core/', "parse_structured"),
    (r'grep -rnP "\bdef\s*launch_task\(" services/', "launch_task"),
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
    'git log --grep "class FetchRows"',
    'echo "def fetch_rows"',
    'echo "grep -n def fetch_rows" > notes.txt',
    'python3 -c "import subprocess; subprocess.run([\'grep\', \'def fetch_rows\'])"',
    "python3 - <<'EOF'\nimport re\ntext = open('a.py').read()\nprint('grep' in text)\n# def X\nEOF",
    "cat > a.sh <<EOF\ngrep -n 'def fetch_rows' a.py\nEOF\nls",
    '# grep -n "def fetch_rows" a.py',
    '# note; grep -n "def fetch_rows" a.py',
    "grep -n ordinary a.py # def fetch_rows",
    'echo ";" "grep" "def fetch_rows"',
    'env echo grep "def fetch_rows"',
    'xargs echo grep "def fetch_rows"',
    'echo a\\; grep "def fetch_rows" a.py',
    'echo prefix"; grep def fetch_rows"',
    'grep -n TODO a.py \\\n# def fetch_rows',
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
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == gate.DENY_REASON.format(name="fetch_rows")


def test_a_workspace_symbol_for_the_name_opens_the_gate_for_that_name(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "fetch_rows"),
                       _tool_result("No symbols found in workspace.", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == ""
    assert gate.grep_reason(r"def\s+fetch_rows", path) == ""


def test_a_workspace_symbol_for_another_name_keeps_the_gate_shut(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "fetch_rowsbar"),
                       _tool_result("Found 1 symbols in workspace:\n\na.py:\n  fetch_rowsbar (Function) - Line 3",
                                    "toolu_lsp"))
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == gate.DENY_REASON.format(name="fetch_rows")
    assert gate.grep_reason(r"def\s+fetch_rows", path) == gate.DENY_REASON.format(name="fetch_rows")


def test_a_lookup_result_that_names_the_symbol_opens_the_gate(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "parse"),
                       _tool_result("Found 2 symbols in workspace:\n\ncore/llm.py:\n"
                                    "  parse_structured (Function) - Line 40", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def parse_structured" core/llm.py', path) == ""


def test_a_document_symbol_result_that_names_the_symbol_opens_the_gate(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("documentSymbol"),
                       _tool_result("Document symbols:\nloop (Function) - Line 3", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def loop" a.py', path) == ""
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == \
        gate.DENY_REASON.format(name="fetch_rows")


def _short(template, name="loop", file="core/a.py"):
    return template.format(name=name, file=file)


@pytest.mark.parametrize("command,file", [
    ('grep -n "def loop" core/a.py', "core/a.py"),
    ('grep -rn -A 3 "def loop" core/ core/a.py', "core/a.py"),
    ('grep -n -e "def loop" core/a.py', "core/a.py"),
    ('rg -g "*.py" "def loop" -- core/a.py', "core/a.py"),
    ('git grep -n "def loop" -- ./web/a.js', "web/a.js"),
    ('sed -n 1p b.py; grep -n "def loop" core/a.py', "core/a.py"),
    ('cd core && grep -n "def loop" a.py', "core/a.py"),
    ('cd core; bash -c \'grep -n "def loop" ../web/a.js\'', "web/a.js"),
    ('git -C core grep -n "def loop" a.py', "core/a.py"),
    ('grep -n -e"def loop" a.py', "a.py"),
    ('grep -rnA3 "def loop" a.py', "a.py"),
    ('grep -n "def loop" < a.py', "a.py"),
    ('git grep -C 3 -n "def loop" core/a.py', "core/a.py"),
    ('grep -T "def loop" a.py', "a.py"),
    ('rg -T js "def loop" a.py', "a.py"),
    ('grep -n "def loop" a.py>out.txt', "a.py"),
    ('grep -n "def loop" "my file.py"', "my file.py"),
    ('grep -n "def loop" "my file.py">out.txt', "my file.py"),
    ('grep -n "def loop" <"my file.py"', "my file.py"),
    ('cd core && grep -n "def loop(" a.py', "core/a.py"),
    ("cd core && rg 'def loop\\(' a.py", "core/a.py"),
])
def test_a_short_name_in_a_file_grep_asks_for_document_symbol(tmp_path, command, file):
    reason = gate.bash_reason(command, _transcript(tmp_path))
    assert reason == _short(gate.SHORT_DENY_REASON, file=file)
    assert f'operation documentSymbol, filePath "{file}"' in reason
    assert "workspaceSymbol, query" not in reason
    assert gate._REFUSED.match(reason).group(1) == "loop"


@pytest.mark.parametrize("command", [
    'grep -rn "def loop" core/',
    'grep -rn "def loop" .',
    'grep -n "def loop" core/*.py',
    'cat core/a.py | grep -n "def loop"',
    'find . -name "*.py" | xargs grep -n "def loop"',
    'grep -rn "def loop" --include "*.py" core',
    'cat core/a.py | grep -n "def loop" > out.txt',
    'cat core/a.py | grep -n "def loop" 2>/dev/null >> out.txt',
    'cd "$DIR" && grep -n "def loop" a.py',
    'grep -rn "def loop" pkg.d',
    'grep -rn "def fetch" Makefile',
    'cd - && grep -n "def loop" a.py',
    '(cd core; true); grep -n "def loop" a.py',
])
def test_a_short_name_in_a_grep_that_names_no_file_runs(tmp_path, command, monkeypatch):
    (tmp_path / "pkg.d").mkdir()
    monkeypatch.chdir(tmp_path)
    assert gate.bash_reason(command, _transcript(tmp_path)) == ""


def test_a_file_on_disk_without_an_extension_counts(tmp_path, monkeypatch):
    (tmp_path / "script").write_text("def loop():\n    pass\n")
    monkeypatch.chdir(tmp_path)
    assert gate.bash_reason('grep -n "def loop" script', _transcript(tmp_path)) == \
        _short(gate.SHORT_DENY_REASON, file="script")


def test_a_long_name_keeps_the_workspace_symbol_notice(tmp_path):
    assert gate.bash_reason('grep -n "def fetch_rows" core/a.py', _transcript(tmp_path)) == \
        gate.DENY_REASON.format(name="fetch_rows")
    assert gate.bash_reason('grep -n "def run_task" core/a.py', _transcript(tmp_path)) == \
        _short(gate.SHORT_DENY_REASON, name="run_task")


def test_the_grep_tool_reads_its_path_for_a_short_name(tmp_path):
    path = _transcript(tmp_path)
    assert gate.grep_reason(r"def\s+loop", path, "core/a.py") == _short(gate.SHORT_DENY_REASON)
    assert gate.grep_reason(r"def\s+loop", path, "core") == ""
    assert gate.grep_reason(r"def\s+loop", path) == ""
    assert gate.grep_reason(r"def\s+fetch_rows", path, "core") == \
        gate.DENY_REASON.format(name="fetch_rows")


def test_a_refused_short_name_is_refused_only_in_a_file_grep(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": 'grep -n "def loop" core/a.py'}, "toolu_d"),
                       json.dumps({"type": "user", "message": {"role": "user", "content": [
                           {"type": "tool_result", "tool_use_id": "toolu_d", "is_error": True,
                            "content": "PreToolUse:Bash hook error: "
                                       + _short(gate.SHORT_DENY_REASON)}]}}))
    assert gate.bash_reason('grep -n "loop" core/b.py', path) == \
        _short(gate.SHORT_REPEAT_REASON, file="core/b.py")
    assert gate.bash_reason('grep -rn "loop" core/', path) == ""


def test_no_lsp_server_opens_the_gate_for_the_session(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "build_index"),
                       _tool_result("No LSP server available for file type: .sh", "toolu_lsp"))
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == ""
    assert gate.grep_reason(r"function\s+bake_cache", path) == ""


def test_a_tool_search_that_finds_no_lsp_tool_opens_the_gate(tmp_path):
    path = _transcript(tmp_path, _tool_use("ToolSearch", {"query": "select:LSP"}, "toolu_ts"),
                       _tool_result("No matching deferred tools found", "toolu_ts"))
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == ""


def test_a_tool_search_for_another_tool_keeps_the_gate_shut(tmp_path):
    path = _transcript(tmp_path, _tool_use("ToolSearch", {"query": "select:WebFetch"}))
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) != ""


@pytest.mark.parametrize("pattern,name", [
    ("def parse_structured", "parse_structured"),
    (r"class\s+OpenCodeProvider", "OpenCodeProvider"),
    (r"^\s*(?:async\s+)?def\s+launch_task\b", "launch_task"),
    (r"function\s*renderBoard\(", "renderBoard"),
    (r"(class|interface)\s+TaskRowView", "TaskRowView"),
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
    assert gate.bash_reason('grep -n "def other_symbol" a.py', path) == gate.DENY_REASON.format(name="other_symbol")


def test_grep_tool_passes_once_the_symbol_is_looked_up(tmp_path):
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "fetch_rows"))
    assert gate.grep_reason(r"def\s+fetch_rows", path) == ""


def test_a_long_alternation_pattern_is_read_in_linear_time(tmp_path):
    start = time.monotonic()
    assert gate.grep_reason("def" + "|def" * 20000, _transcript(tmp_path)) == ""
    assert time.monotonic() - start < 2


def test_an_unreadable_transcript_opens_the_gate(tmp_path):
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', str(tmp_path / "missing.jsonl")) == ""
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', "") == ""


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
    path = _transcript(tmp_path, *_LOADED, _lsp("workspaceSymbol", "fetch_rows"))
    assert gate.bash_reason('grep -n "def fetch_rows" a.py; grep -n "def build_index" a.py', path) == \
        gate.DENY_REASON.format(name="build_index")
    assert gate.grep_reason("def fetch_rows|def build_index", path) == gate.DENY_REASON.format(name="build_index")
    assert gate.grep_reason("def (fetch_rows|build_index)", path) == gate.DENY_REASON.format(name="build_index")
    assert gate.bash_reason(r'grep -rnE "(def|class)\s+(fetch_rows|build_index)" core/', path) == \
        gate.DENY_REASON.format(name="build_index")


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
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == gate.DENY_REASON.format(name="fetch_rows")
    assert gate.grep_reason("def fetch_rows", path) == gate.DENY_REASON.format(name="fetch_rows")


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
    path = _transcript(tmp_path, _tool_use("Bash", {"command": 'grep -n "def fetch_rows" a.py'}, "toolu_d"),
                       _refusal("fetch_rows", "toolu_d"))
    assert gate.bash_reason('grep -n "fetch_rows" a.py', path) == gate.REPEAT_REASON.format(name="fetch_rows")
    assert gate.bash_reason('sed -n 1,5p a.py; git grep -n "fetch_rows(" -- src', path) == \
        gate.REPEAT_REASON.format(name="fetch_rows")
    assert gate.grep_reason("fetch_rows", path) == gate.REPEAT_REASON.format(name="fetch_rows")
    assert gate.bash_reason('grep -n "fetch_rowsbar" a.py', path) == ""
    assert gate.bash_reason("sed -n 1,5p fetch_rows.py; awk '/fetch_rows/' a.py", path) == ""


def test_an_lsp_lookup_releases_a_refused_symbol(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": 'grep -n "def fetch_rows" a.py'}, "toolu_d"),
                       _refusal("fetch_rows", "toolu_d"), *_LOADED, _lsp("workspaceSymbol", "fetch_rows"))
    assert gate.bash_reason('grep -n "fetch_rows" a.py', path) == ""
    assert gate.bash_reason('grep -n "def fetch_rows" a.py', path) == ""


def test_the_old_deny_message_still_counts_as_a_refusal(tmp_path):
    old = ("PreToolUse:Grep hook error: Blocked by the LSP gate: this grep looks up the definition "
           "of `fetch_rows`, and this session has not looked up `fetch_rows` with LSP. If LSP is not loaded, load "
           "it once with ToolSearch, query \"select:LSP\". Then call LSP with operation "
           "workspaceSymbol, query \"fetch_rows\", and any file of the project as filePath.")
    path = _transcript(tmp_path, _tool_use("Grep", {"pattern": "def fetch_rows"}, "toolu_d"),
                       json.dumps({"type": "user", "message": {"role": "user", "content": [
                           {"type": "tool_result", "tool_use_id": "toolu_d", "is_error": True,
                            "content": old}]}}))
    assert gate.grep_reason("fetch_rows", path) == gate.REPEAT_REASON.format(name="fetch_rows")


def test_quoted_deny_text_in_a_successful_result_refuses_nothing(tmp_path):
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "cat log.txt"}, "toolu_c"),
                       _tool_result("Blocked by the LSP gate: x. LSP with operation "
                                    'workspaceSymbol, query "fetch_rows"', "toolu_c"))
    assert gate.bash_reason('grep -n "fetch_rows" a.py', path) == ""


def test_the_deny_message_asks_for_the_lsp_call_first():
    reason = gate.DENY_REASON.format(name="fetch_rows")
    assert "Nothing in this command ran" in reason
    assert "next tool call" in reason
    assert gate._REFUSED.match("PreToolUse:Bash hook error: " + reason).group(1) == "fetch_rows"
    assert gate._REFUSED.match(gate.REPEAT_REASON.format(name="fetch_rows")).group(1) == "fetch_rows"


def test_a_notice_from_the_removed_rewrite_still_counts_as_a_refusal(tmp_path):
    notice = "[LSP gate] The grep for `fetch_rows` in this command did not run; the rest of the command ran. Make your next tool call LSP."
    path = _transcript(tmp_path, _tool_use("Bash", {"command": "ls; echo '" + notice + "'"}, "toolu_r"),
                       _tool_result("a.py\n" + notice, "toolu_r"))
    assert gate.bash_reason('grep -n "fetch_rows" a.py', path) == gate.REPEAT_REASON.format(name="fetch_rows")


def test_hook_denies_a_compound_command_that_commits(tmp_path):
    item_id = work_store.create_item("lsp gate commit")
    sid = f"sid-lsp-commit-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    r = _run_hook(sid, 'git commit -m x; grep -n "def parse_structured" core/llm.py', _transcript(tmp_path))
    assert r.returncode == 0, r.stderr
    assert json.loads(r.stdout)["hookSpecificOutput"]["permissionDecision"] == "deny"

def test_hook_denies_a_compound_command_with_a_refused_grep(tmp_path):
    item_id = work_store.create_item("lsp gate compound")
    sid = f"sid-lsp-compound-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    r = _run_hook(sid, 'sed -n 1p a.py; grep -n "def parse_structured" core/llm.py', _transcript(tmp_path))
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert "updatedInput" not in out
    assert out["permissionDecisionReason"] == gate.DENY_REASON.format(name="parse_structured")


def test_hook_denies_a_compound_command_in_a_pipeline_run(tmp_path):
    r = _run_hook("sid-pipeline-compound", 'sed -n 1p a.py; grep -n "def parse_structured" core/llm.py',
                  _transcript(tmp_path), pipeline=True)
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)["hookSpecificOutput"]
    assert out["permissionDecision"] == "deny"
    assert "updatedInput" not in out
