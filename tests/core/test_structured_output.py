"""Decision prompts ask the CLI for schema-checked JSON.

claude -p takes --json-schema and puts the checked answer in the result
envelope as structured_output. codex exec takes --output-schema with a file.
opencode run has no schema flag, so the schema goes into the prompt and
parse_structured checks the answer afterwards.
"""
import json
import os
import stat
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.llm as llm  # noqa: E402

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


def test_the_opencode_tiers_put_the_schema_in_the_prompt():
    provider = llm.OpenCodeProvider({"llm": {"opencode": {}}})
    prompts = []

    def capture(cmd, fn_name, model, prompt, cwd, timeout, env_overrides=None):
        prompts.append(prompt)
        return '{"verdict": "PASS", "reason": "r"}'

    with patch.object(provider, "_run", side_effect=capture):
        provider.fast("q", schema=VERDICT)
        provider.balanced("q", schema=VERDICT)
        provider.agentic("q", cwd=Path("."), schema=VERDICT)
        provider.fast("plain")

    for prompt in prompts[:3]:
        assert prompt.startswith("q\n\n")
        assert json.dumps(VERDICT) in prompt
    assert prompts[3] == "plain"
    with patch.object(provider, "_run", return_value='{"verdict": "NO", "reason": "r"}'), \
         patch.object(llm.log, "emit"):
        assert llm.parse_structured(provider.fast("q", schema=VERDICT), VERDICT,
                                    fn_name="t") is None


def test_codex_gets_the_schema_as_a_file_after_exec(tmp_path):
    seen = {}

    def fake_run(cmd, **kwargs):
        at = cmd.index("--output-schema")
        seen["cmd"] = cmd
        seen["schema"] = json.loads(Path(cmd[at + 1]).read_text())
        seen["path"] = Path(cmd[at + 1])
        return type("R", (), {"returncode": 0, "stdout": b'{"verdict":"PASS","reason":"r"}',
                              "stderr": b""})()

    with patch.object(llm.subprocess, "run", side_effect=fake_run):
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
    with patch.object(llm.subprocess, "run") as run:
        run.return_value = type("R", (), {"returncode": 0, "stdout": b"x", "stderr": b""})()
        llm.run_external_model([*llm.CODEX_EXEC, "-"], fn_name="t", model="codex", prompt="q")
    assert "--output-schema" not in run.call_args.args[0]
