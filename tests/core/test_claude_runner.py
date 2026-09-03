"""Unit tests for run_claude_code streaming.

Uses a shell subprocess instead of real claude, injected via PATH so the
binary name `claude` resolves to a test script. Each test creates a stub
`claude` in tmp_path that prints stream-json NDJSON lines, then verifies
the tee'd log file and the return value.
"""
import json
import os
import stat
import sys
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.job_logs as job_logs  # noqa: E402
import core.llm as llm  # noqa: E402
import core.state as state  # noqa: E402
from core.claude_runner import run_claude_code  # noqa: E402


def _install_fake_claude(bin_dir: Path, script_body: str) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    fake = bin_dir / "claude"
    fake.write_text(f"#!/bin/sh\n{script_body}\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _text_delta(text: str) -> str:
    return json.dumps({
        "type": "stream_event",
        "event": {
            "type": "content_block_delta",
            "delta": {"type": "text_delta", "text": text},
        },
    })


def _write_events(tmp_path: Path, events: list[dict]) -> Path:
    f = tmp_path / "events.ndjson"
    f.write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return f


def test_run_claude_code_tees_text_deltas_to_log(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(
        _text_delta("hello ") + "\n"
        + _text_delta("world") + "\n"
        + _text_delta("!") + "\n"
    )
    _install_fake_claude(bin_dir, f'cat "{events_file}"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    finally:
        job_logs._active_live_log.reset(token)

    assert out == "hello world!"
    assert log_path.exists()
    assert log_path.read_bytes() == b"hello world!"


def test_run_claude_code_skips_non_text_events(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events = [
        {"type": "system", "subtype": "init"},
        json.loads(_text_delta("hi")),
        {"type": "stream_event", "event": {"type": "content_block_delta",
            "delta": {"type": "input_json_delta", "partial_json": "{\"x\":1}"}}},
        {"type": "assistant", "message": {"content": [{"type": "text", "text": "hi"}]}},
        json.loads(_text_delta(" there")),
        {"type": "result", "subtype": "success", "result": "hi there"},
    ]
    events_file = _write_events(tmp_path, events)
    _install_fake_claude(bin_dir, f'cat "{events_file}"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    finally:
        job_logs._active_live_log.reset(token)

    assert out == "hi there"
    assert log_path.read_bytes() == b"hi there"


def test_run_claude_code_ignores_malformed_lines(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(
        "not json\n"
        + _text_delta("ok") + "\n"
        + "{partial broken\n"
    )
    _install_fake_claude(bin_dir, f'cat "{events_file}"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    finally:
        job_logs._active_live_log.reset(token)

    assert out == "ok"
    assert log_path.read_bytes() == b"ok"


def test_run_claude_code_without_contextvar_does_not_write_log(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(_text_delta("x") + "\n")
    _install_fake_claude(bin_dir, f'cat "{events_file}"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    sentinel = tmp_path / "should_not_exist.log"
    out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    assert out == "x"
    assert not sentinel.exists()


def test_run_claude_code_non_zero_exit_writes_exit_marker_and_returns_none(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(_text_delta("partial") + "\n")
    _install_fake_claude(bin_dir, f'cat "{events_file}"; exit 7')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    finally:
        job_logs._active_live_log.reset(token)

    assert out is None
    data = log_path.read_bytes()
    assert b"partial" in data
    assert b"[EXIT code=7]" in data


def test_run_claude_code_timeout_writes_timeout_marker_and_returns_none(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(_text_delta("started") + "\n")
    _install_fake_claude(bin_dir, f'cat "{events_file}"; sleep 5')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=1)
    finally:
        job_logs._active_live_log.reset(token)

    assert out is None
    data = log_path.read_bytes()
    assert b"started" in data
    assert b"[TIMEOUT after 1s]" in data


def test_run_claude_code_preserves_unicode_in_text_deltas(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(_text_delta("héllo — 世界") + "\n")
    _install_fake_claude(bin_dir, f'cat "{events_file}"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=5)
    finally:
        job_logs._active_live_log.reset(token)

    assert out == "héllo — 世界"
    assert log_path.read_bytes() == "héllo — 世界".encode("utf-8")


def test_run_claude_code_honors_instance_claude_config_dir(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    env_capture = tmp_path / "env.txt"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(_text_delta("ok") + "\n")
    _install_fake_claude(
        bin_dir,
        f'printf "%s" "$CLAUDE_CONFIG_DIR" > "{env_capture}"\ncat "{events_file}"',
    )
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    prev_default = state._default_instance_key
    llm._providers.pop("aimyable", None)
    state._default_instance_key = "aimyable"
    llm.configure({
        "job": {"key": "aimyable"},
        "llm": {"provider": "claude", "claude": {"config_dir": "~/.aimyable-claude"}},
    })
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=5)
    finally:
        state._default_instance_key = prev_default
        llm._providers.pop("aimyable", None)

    assert out == "ok"
    assert env_capture.read_text() == str(Path("~/.aimyable-claude").expanduser())


def test_run_claude_code_blocks_followup_after_usage_limit(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    attempts_file = tmp_path / "attempts.txt"
    _install_fake_claude(
        bin_dir,
        f'echo x >> "{attempts_file}"\n'
        'printf "%s\\n" "You\'re out of extra usage · resets 2:40pm (America/Los_Angeles)"\n'
        "exit 1",
    )
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    import core.db as db
    db.execute("DELETE FROM kv WHERE key='llm_guard'")
    try:
        first = run_claude_code("hi", cwd=tmp_path, timeout=5)
        second = run_claude_code("hi again", cwd=tmp_path, timeout=5)
    finally:
        db.execute("DELETE FROM kv WHERE key='llm_guard'")

    assert first is None
    assert second is None
    assert attempts_file.read_text().splitlines() == ["x"]


def test_llm_guard_persists_to_kv_table_after_trip(tmp_path, monkeypatch):
    from datetime import datetime, timezone
    bin_dir = tmp_path / "bin"
    _install_fake_claude(
        bin_dir,
        'printf "%s\\n" "Key limit exceeded - please add credits"\n'
        "exit 1",
    )
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    import core.db as db
    db.execute("DELETE FROM kv WHERE key='llm_guard'")
    try:
        run_claude_code("hi", cwd=tmp_path, timeout=5)
        row = db.query_one("SELECT data FROM kv WHERE key='llm_guard'")
        assert row is not None
        payload = json.loads(row["data"])
        blocked_until = datetime.fromisoformat(payload["blocked_until"])
        assert blocked_until > datetime.now(timezone.utc)
        assert payload["reason"] == "key limit exceeded"
    finally:
        db.execute("DELETE FROM kv WHERE key='llm_guard'")


def test_llm_guard_clears_after_cooldown_expires(tmp_path, monkeypatch):
    from datetime import datetime, timedelta, timezone
    bin_dir = tmp_path / "bin"
    events_file = tmp_path / "events.ndjson"
    events_file.write_text(_text_delta("ok") + "\n")
    _install_fake_claude(bin_dir, f'cat "{events_file}"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    import core.db as db
    guard_key = llm._guard_key()
    expired = (datetime.now(timezone.utc) - timedelta(seconds=10)).isoformat()
    payload = json.dumps({"blocked_until": expired, "reason": "stale"})
    db.execute(
        "INSERT INTO kv(instance_key, key, data, updated_at) "
        "VALUES (?, 'llm_guard', ?, ?) "
        "ON CONFLICT(instance_key, key) DO UPDATE SET data=excluded.data",
        (guard_key, payload, datetime.now(timezone.utc).isoformat()),
    )
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=5)
    finally:
        db.execute("DELETE FROM kv WHERE key='llm_guard'")

    assert out == "ok"
    row = db.query_one(
        "SELECT data FROM kv WHERE instance_key=? AND key='llm_guard'",
        (guard_key,),
    )
    assert row is None


def test_llm_guard_scoped_per_instance(tmp_path):
    import core.db as db
    import core.state as _state
    prev_default = _state._default_instance_key
    db.execute("DELETE FROM kv WHERE key='llm_guard'")
    try:
        _state._default_instance_key = "alpha"
        llm._trip_llm_guard("You're out of extra usage today")
        blocked_a, _, _ = llm._guard_status()
        assert blocked_a is True

        _state._default_instance_key = "beta"
        blocked_b, _, _ = llm._guard_status()
        assert blocked_b is False
    finally:
        _state._default_instance_key = prev_default
        db.execute("DELETE FROM kv WHERE key='llm_guard'")


def test_the_fast_tier_never_gets_tools():
    """run_haiku classifies text written by other people: Slack messages, PR
    review comments, ticket descriptions. An instance configured with
    --dangerously-skip-permissions would otherwise let that text drive Bash."""
    provider = llm.ClaudeProvider({"llm": {"claude": {
        "bin": "claude", "args": ["--dangerously-skip-permissions"]}}})
    seen = {}

    def capture(cmd, **kwargs):
        seen["cmd"] = cmd
        return ("ok", {})

    with patch.object(provider, "_run_print", side_effect=capture):
        provider.fast("classify this: <untrusted slack text>")

    cmd = seen["cmd"]
    assert "--dangerously-skip-permissions" not in cmd
    # --tools "" empties the built-in set and overrides the settings files, so
    # no deny list has to stay in step with the tools a release adds. Proven
    # against the real CLI: with this flag the model answers in one turn
    # without running Bash; without it, it runs Bash and returns its output.
    assert cmd[cmd.index("--tools") + 1] == ""
    assert "--strict-mcp-config" in cmd


def test_the_opencode_fast_tier_denies_every_tool():
    """opencode.json in this repository allows every tool to every agent, and
    `opencode run` has no flag that empties the set."""
    provider = llm.OpenCodeProvider({"llm": {"opencode": {}}})
    seen = {}

    def capture(cmd, fn_name, model, prompt, cwd, timeout, env_overrides=None):
        seen["cmd"], seen["env"] = cmd, env_overrides or {}
        return "ok"

    with patch.object(provider, "_run", side_effect=capture):
        provider.fast("classify this: <untrusted slack text>")

    assert "--dangerously-skip-permissions" not in seen["cmd"]
    assert json.loads(seen["env"]["OPENCODE_CONFIG_CONTENT"]) == {
        "permission": {"*": {"*": "deny"}}}
