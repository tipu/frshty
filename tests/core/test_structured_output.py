"""Decision prompts ask the CLI for schema-checked JSON.

claude -p takes --json-schema and puts the checked answer in the result
envelope as structured_output. codex exec takes --output-schema with a file.
opencode run has no schema flag, so a schema call goes to a private
`opencode serve` process with format json_schema and reads info.structured.
"""
import json
import os
import stat
import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.llm as llm  # noqa: E402


class _FakeProc:
    def __init__(self, stdout: bytes):
        self.returncode = 0
        self._stdout = stdout

    def communicate(self, input=None, timeout=None):
        return self._stdout, b""


VERDICT = llm.strict_object({
    "verdict": {"type": "string", "enum": ["PASS", "FAIL"]},
    "reason": {"type": "string"},
})


def _fake_claude(tmp_path: Path, monkeypatch, envelope: dict) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    args_file = tmp_path / "args.json"
    envelope_file = tmp_path / "envelope.json"
    envelope_file.write_text(json.dumps(envelope))
    fake = bin_dir / "claude"
    fake.write_text(
        "#!/bin/sh\ncat >/dev/null\n"
        f"python3 -c 'import json,sys; json.dump(sys.argv[1:], open(\"{args_file}\", \"w\"))' \"$@\"\n"
        f"cat \"{envelope_file}\"\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    db.execute("DELETE FROM kv WHERE key='llm_guard'")
    return args_file


def _envelope(**extra) -> dict:
    return {"type": "result", "subtype": "success", "is_error": False,
            "result": "free text the caller must not parse", "num_turns": 2, **extra}


def test_every_print_tier_passes_the_schema_and_returns_the_checked_answer(tmp_path, monkeypatch):
    args_file = _fake_claude(tmp_path, monkeypatch, _envelope(
        structured_output={"verdict": "PASS", "reason": "7 is prime"}))
    provider = llm.ClaudeProvider({"llm": {"claude": {"bin": "claude", "args": []}}})

    for run in (lambda: provider.fast("q", schema=VERDICT),
                lambda: provider.balanced("q", schema=VERDICT),
                lambda: provider.agentic("q", cwd=tmp_path, schema=VERDICT)):
        out = run()
        assert out is not None
        args = json.loads(args_file.read_text())
        assert json.loads(args[args.index("--json-schema") + 1]) == VERDICT
        assert json.loads(out) == {"verdict": "PASS", "reason": "7 is prime"}


def test_a_print_tier_without_a_schema_passes_no_schema_flag(tmp_path, monkeypatch):
    args_file = _fake_claude(tmp_path, monkeypatch, _envelope())
    out = llm.ClaudeProvider().fast("q")
    assert "--json-schema" not in json.loads(args_file.read_text())
    assert out == "free text the caller must not parse"


def test_a_run_with_no_structured_output_answers_none_and_says_so(tmp_path, monkeypatch):
    _fake_claude(tmp_path, monkeypatch, _envelope())
    llm.reset_last_error()
    with patch.object(llm.log, "emit") as emit:
        out = llm.ClaudeProvider().balanced("q", schema=VERDICT)
    assert out is None
    assert "no structured output" in llm.consume_last_error()
    assert [c.args[0] for c in emit.call_args_list].count("llm_schema_invalid") == 1


def test_the_checked_answer_parses_to_the_dict(tmp_path, monkeypatch):
    _fake_claude(tmp_path, monkeypatch, _envelope(
        structured_output={"verdict": "FAIL", "reason": "9 = 3 x 3"}))
    with patch.object(llm, "_get_provider", return_value=llm.ClaudeProvider()):
        text = llm.run_fast("q", schema=VERDICT)
    assert llm.parse_structured(text, VERDICT, fn_name="t") == {
        "verdict": "FAIL", "reason": "9 = 3 x 3"}


def test_an_agentic_schema_run_counts_the_answer_turn(tmp_path, monkeypatch):
    """The schema answer is one tool call, so a run that used no other tool
    reports two turns."""
    _fake_claude(tmp_path, monkeypatch, _envelope(
        structured_output={"verdict": "PASS", "reason": "r"}))
    with patch.object(llm.log, "emit") as emit:
        llm.ClaudeProvider().agentic("q", cwd=tmp_path, schema=VERDICT)
    assert "llm_agentic_single_turn" in [c.args[0] for c in emit.call_args_list]


def test_parse_structured_rejects_an_answer_outside_the_schema():
    with patch.object(llm.log, "emit") as emit:
        assert llm.parse_structured('{"verdict": "MAYBE", "reason": "r"}', VERDICT,
                                    fn_name="t") is None
        assert llm.parse_structured('{"verdict": "PASS"}', VERDICT, fn_name="t") is None
        assert llm.parse_structured("no json at all", VERDICT, fn_name="t") is None
    assert [c.args[0] for c in emit.call_args_list] == ["llm_schema_invalid"] * 3


def test_parse_structured_reads_an_answer_inside_the_schema():
    text = 'thinking {"verdict": "FAIL"} then\n```json\n{"verdict": "PASS", "reason": "r"}\n```'
    assert llm.parse_structured(text, VERDICT, fn_name="t") == {"verdict": "PASS", "reason": "r"}
    assert llm.parse_structured(None, VERDICT, fn_name="t") is None


FAKE_OPENCODE_SERVE = r"""
import base64, json, os, sys
from http.server import BaseHTTPRequestHandler, HTTPServer

RECORD = os.environ["FAKE_OPENCODE_RECORD"]
REPLY = json.load(open(os.environ["FAKE_OPENCODE_REPLY"]))
EXPECTED = "Basic " + base64.b64encode(
    ("opencode:" + os.environ["OPENCODE_SERVER_PASSWORD"]).encode()).decode()


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["content-length"])))
        if self.headers.get("authorization") != EXPECTED:
            self.send_response(401)
            self.end_headers()
            return
        with open(RECORD, "a") as f:
            f.write(json.dumps({"argv": sys.argv[1:], "path": self.path, "body": body,
                                "config": os.environ.get("OPENCODE_CONFIG_CONTENT"),
                                "cwd": os.getcwd()}) + "\n")
        out = {"id": "ses_1"} if self.path == "/session" else REPLY
        data = json.dumps(out).encode()
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


server = HTTPServer(("127.0.0.1", 0), Handler)
print(f"opencode server listening on http://127.0.0.1:{server.server_port}", flush=True)
server.serve_forever()
"""


def _fake_opencode(tmp_path: Path, monkeypatch, reply: dict) -> Path:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    record = tmp_path / "record.jsonl"
    reply_file = tmp_path / "reply.json"
    reply_file.write_text(json.dumps(reply))
    server = tmp_path / "fake_serve.py"
    server.write_text(FAKE_OPENCODE_SERVE)
    fake = bin_dir / "opencode"
    fake.write_text(f"#!/bin/sh\nexec {sys.executable} {server} \"$@\"\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("FAKE_OPENCODE_RECORD", str(record))
    monkeypatch.setenv("FAKE_OPENCODE_REPLY", str(reply_file))
    db.execute("DELETE FROM kv WHERE key='llm_guard'")
    return record


def _requests(record: Path) -> list[dict]:
    return [json.loads(line) for line in record.read_text().splitlines()]


def test_the_opencode_tiers_send_the_schema_to_the_serve_api(tmp_path, monkeypatch):
    record = _fake_opencode(tmp_path, monkeypatch, {
        "info": {"structured": {"verdict": "PASS", "reason": "7 is prime"}}, "parts": []})
    provider = llm.OpenCodeProvider({"llm": {"opencode": {}}})

    outs = [provider.fast("q", schema=VERDICT),
            provider.balanced("q", worktree=tmp_path, schema=VERDICT),
            provider.agentic("q", cwd=tmp_path, system_prompt="sys", schema=VERDICT)]

    for out in outs:
        assert json.loads(out) == {"verdict": "PASS", "reason": "7 is prime"}
    messages = [r for r in _requests(record) if r["path"] == "/session/ses_1/message"]
    assert len(messages) == 3
    for m in messages:
        assert m["argv"][:3] == ["serve", "--port", "0"]
        assert m["body"]["format"] == {"type": "json_schema", "schema": VERDICT}
    fast, balanced, agentic = messages
    assert fast["body"]["tools"] == {"*": False, "StructuredOutput": True}
    assert fast["body"]["model"] == {"providerID": "openrouter",
                                     "modelID": "deepseek/deepseek-v4-flash"}
    assert fast["body"]["parts"] == [{"type": "text", "text": "q"}]
    assert fast["config"] == llm.NO_TOOLS_OPENCODE_ENV["OPENCODE_CONFIG_CONTENT"]
    assert balanced["body"]["tools"] == {"*": False, "StructuredOutput": True}
    assert balanced["cwd"] == str(tmp_path)
    assert "tools" not in agentic["body"]
    assert agentic["body"]["parts"] == [{"type": "text", "text": "sys\n\nq"}]
    assert agentic["config"] == llm.ALL_TOOLS_OPENCODE_ENV["OPENCODE_CONFIG_CONTENT"]


def test_an_opencode_reply_without_structured_output_answers_none_and_says_so(
        tmp_path, monkeypatch):
    _fake_opencode(tmp_path, monkeypatch, {"info": {"error": {
        "name": "StructuredOutputError",
        "data": {"message": "Model did not produce structured output"}}}, "parts": []})
    llm.reset_last_error()
    with patch.object(llm.log, "emit") as emit:
        out = llm.OpenCodeProvider({"llm": {"opencode": {}}}).fast("q", schema=VERDICT)
    assert out is None
    assert "no structured output" in llm.consume_last_error()
    invalid = [c for c in emit.call_args_list if c.args[0] == "llm_schema_invalid"]
    assert len(invalid) == 1
    assert "StructuredOutputError" in invalid[0].args[1]


def test_an_opencode_server_that_dies_before_listening_answers_none(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "opencode"
    fake.write_text("#!/bin/sh\necho boom\nexit 3\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    db.execute("DELETE FROM kv WHERE key='llm_guard'")
    out = llm.OpenCodeProvider({"llm": {"opencode": {}}}).fast("q", schema=VERDICT, timeout=10)
    assert out is None


def test_an_opencode_call_without_a_schema_still_uses_opencode_run():
    provider = llm.OpenCodeProvider({"llm": {"opencode": {}}})
    with patch.object(provider, "_run", return_value="plain answer") as run, \
         patch.object(provider, "_run_structured") as structured:
        assert provider.fast("plain") == "plain answer"
    assert run.call_args.args[0][:2] == ["opencode", "run"]
    structured.assert_not_called()


def test_codex_gets_the_schema_as_a_file_after_exec(tmp_path):
    seen = {}

    def fake_popen(cmd, **kwargs):
        at = cmd.index("--output-schema")
        seen["cmd"] = cmd
        seen["schema"] = json.loads(Path(cmd[at + 1]).read_text())
        seen["path"] = Path(cmd[at + 1])
        return _FakeProc(b'{"verdict":"PASS","reason":"r"}')

    with patch.object(llm.subprocess, "Popen", side_effect=fake_popen):
        text, code = llm.run_external_model(
            [*llm.CODEX_EXEC, "-"], fn_name="t", model="codex", prompt="q",
            stdin_text="q", output_schema=VERDICT)

    assert seen["cmd"][:3] == ["codex", "exec", "--output-schema"]
    assert seen["cmd"][-1] == "-"
    assert seen["schema"] == VERDICT
    assert not seen["path"].exists()
    assert code == 0
    assert llm.parse_structured(text, VERDICT, fn_name="t") == {"verdict": "PASS", "reason": "r"}


def test_a_codex_run_without_a_schema_gets_no_schema_flag():
    with patch.object(llm.subprocess, "Popen", return_value=_FakeProc(b"x")) as popen:
        llm.run_external_model([*llm.CODEX_EXEC, "-"], fn_name="t", model="codex", prompt="q")
    assert "--output-schema" not in popen.call_args.args[0]


def test_a_timed_out_external_run_kills_the_child_the_wrapper_started(tmp_path):
    pid_file = tmp_path / "child.pid"
    wrapper = tmp_path / "wrapper.sh"
    wrapper.write_text(f"#!/bin/sh\nsleep 60 &\necho $! > {pid_file}\nwait\n")
    wrapper.chmod(wrapper.stat().st_mode | stat.S_IEXEC)

    text, code = llm.run_external_model([str(wrapper)], fn_name="t", model="codex",
                                        prompt="q", timeout=1)

    assert (text, code) == (None, None)
    child = int(pid_file.read_text())
    with pytest.raises(ProcessLookupError):
        for _ in range(50):
            os.kill(child, 0)
            time.sleep(0.1)
