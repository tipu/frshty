"""A model CLI that cannot be started must degrade, not crash the caller.

Every runner in core.llm is documented to answer None when a model run fails.
Only subprocess.TimeoutExpired was caught, so a missing, unreadable or
non-executable binary raised FileNotFoundError straight through the runner and
out of the caller. That is what broke review_pr on a host without the claude
CLI, and it left the claude_invocations row stuck at 'running' with no end.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.db as db  # noqa: E402
import core.llm as llm  # noqa: E402


@pytest.fixture
def no_model_cli(tmp_path, monkeypatch):
    """Point PATH at an empty directory so no model binary resolves."""
    empty = tmp_path / "empty-bin"
    empty.mkdir()
    monkeypatch.setenv("PATH", str(empty))
    llm._providers.clear()
    yield
    llm._providers.clear()


def _latest_status(function_name: str) -> str | None:
    row = db.query_one(
        "SELECT status FROM claude_invocations WHERE function_name=? "
        "ORDER BY started_at DESC, rowid DESC LIMIT 1",
        (function_name,),
    )
    return row["status"] if row else None


def _spawn_failures() -> int:
    row = db.query_one(
        "SELECT COUNT(*) AS n FROM log_events WHERE event='llm_spawn_failed'")
    return row["n"]


def test_thinking_returns_none_when_the_cli_is_missing(no_model_cli):
    assert llm.run_thinking("hi", timeout=5) is None


def test_balanced_returns_none_when_the_cli_is_missing(no_model_cli):
    assert llm.run_balanced("hi", timeout=5) is None


def test_agentic_returns_none_when_the_cli_is_missing(no_model_cli, tmp_path):
    assert llm.run_agentic("hi", cwd=tmp_path, timeout=5) is None


def test_fast_returns_none_when_the_cli_is_missing(no_model_cli):
    assert llm.run_fast("hi", timeout=5) is None


def test_external_model_returns_no_text_and_no_exit_code(no_model_cli):
    text, exit_code = llm.run_external_model(
        ["codex", "exec", "-"], fn_name="probe", model="codex", prompt="hi", timeout=5)
    assert text is None
    assert exit_code is None


def test_opencode_returns_none_when_the_cli_is_missing(no_model_cli):
    provider = llm.OpenCodeProvider({"llm": {"opencode": {}}})
    assert provider.balanced("hi", timeout=5) is None


def test_a_failed_spawn_closes_the_invocation_row(no_model_cli):
    llm.run_balanced("hi", timeout=5)
    assert _latest_status("run_balanced") == "error"


def test_a_failed_streaming_spawn_closes_the_invocation_row(no_model_cli):
    llm.run_thinking("hi", timeout=5)
    assert _latest_status("run_claude_code") == "error"


def test_the_spawn_failure_reaches_the_event_feed(no_model_cli):
    before = _spawn_failures()
    llm.run_balanced("hi", timeout=5)
    assert _spawn_failures() == before + 1
