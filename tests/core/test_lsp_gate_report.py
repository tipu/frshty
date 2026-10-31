import importlib.util
import json
import os

import core.lsp_gate as gate

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_spec = importlib.util.spec_from_file_location(
    "lsp_gate_report", os.path.join(ROOT, "scripts", "lsp_gate_report.py"))
report = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(report)


def _use(tool_id, name, tool_input, stamp="2026-10-30T10:00:00Z"):
    return json.dumps({"timestamp": stamp, "message": {"content": [
        {"type": "tool_use", "id": tool_id, "name": name, "input": tool_input}]}})


def _result(tool_id, text, is_error=False):
    return json.dumps({"message": {"content": [
        {"type": "tool_result", "tool_use_id": tool_id, "is_error": is_error, "content": text}]}})


_OLD_NOTICE = ("[LSP gate] The grep for `baz` in this command did not run; the rest of the "
               "command ran. Make your next tool call LSP.")


def _denied(tool_id, name="foo"):
    return _result(tool_id, "PreToolUse:Bash hook error: " + gate.DENY_REASON.format(name=name), True)


def test_each_refusal_is_classed_by_the_next_call_past_a_tool_search(tmp_path):
    path = tmp_path / "s.jsonl"
    path.write_text("\n".join([
        _use("a", "Bash", {"command": 'grep -n "def foo" a.py'}), _denied("a"),
        _use("b", "ToolSearch", {"query": "select:LSP"}), _result("b", "ok"),
        _use("c", "LSP", {"operation": "workspaceSymbol", "query": "foo"}), _result("c", "x"),
        _use("d", "Bash", {"command": 'grep -n "def bar" a.py'}), _denied("d", "bar"),
        _use("e", "Bash", {"command": 'grep -n "bar" a.py'}), _result("e", "1:bar"),
        _use("f", "Bash", {"command": "ls; echo '" + _OLD_NOTICE + "'"}),
        _result("f", "a.py\n" + _OLD_NOTICE),
        _use("g", "Read", {"file_path": "a.py"}), _result("g", "x"),
        _use("h", "Grep", {"pattern": "def qux"}), _denied("h", "qux"),
    ]) + "\n")
    found = report.transcript_denials(str(path))
    assert [(d["id"], d["next"]) for d in found] == [("a", "lsp"), ("d", "grep"), ("f", "other"),
                                                     ("h", "end")]


def test_quoted_deny_text_in_a_successful_result_is_not_a_refusal(tmp_path):
    path = tmp_path / "s.jsonl"
    path.write_text("\n".join([
        _use("a", "Bash", {"command": "cat core/lsp_gate.py"}),
        _result("a", gate.DENY_REASON.format(name="foo")),
    ]) + "\n")
    assert report.transcript_denials(str(path)) == []


def test_the_summary_reports_the_lsp_share():
    found = [{"next": "lsp", "path": "a"}, {"next": "grep", "path": "a"},
             {"next": "lsp", "path": "b"}, {"next": "other", "path": "b"}]
    assert report.gate_summary(found) == {"denials": 4, "sessions": 2, "lsp": 2, "grep": 1,
                                          "other": 1, "end": 0, "lsp_share": 0.5, "flooded": 0}


def test_a_workspace_symbol_flood_after_a_refusal_is_counted(tmp_path):
    short = gate.SHORT_DENY_REASON.format(name="loop", file="a.py")
    path = tmp_path / "s.jsonl"
    path.write_text("\n".join([
        _use("a", "Bash", {"command": 'grep -n "def loop" a.py'}), _denied("a", "fetch_rows"),
        _use("b", "LSP", {"operation": "workspaceSymbol", "query": "loop"}),
        _result("b", "Found 531 symbols in workspace:"),
        _use("c", "Bash", {"command": 'grep -n "def loop" b.py'}),
        _result("c", "PreToolUse:Bash hook error: " + short, True),
        _use("d", "LSP", {"operation": "documentSymbol", "filePath": "b.py"}),
        _result("d", "Document symbols:\nloop (Function) - Line 3"),
    ]) + "\n")
    found = report.transcript_denials(str(path))
    assert [(d["id"], d["next"], d["flooded"]) for d in found] == [("a", "lsp", True),
                                                                   ("c", "lsp", False)]
    assert report.gate_summary(found)["flooded"] == 1
