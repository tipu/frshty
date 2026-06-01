"""Unit tests for the consensus planning sanity-gate, quorum, and CLI flags.

The gate is structural (exit code + size) so real plans that merely mention
error strings are never rejected; the quorum requires >=2 valid plans before
any judgment step runs; the fan-out must build codex/gemini commands with the
flags that keep them from silently failing.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.consensus_plan as cp  # noqa: E402
from core.llm import validate_model_output  # noqa: E402


REAL_PLAN = "# Technical Plan\n\n" + ("Detailed proposal with file:line evidence. " * 20)


def test_validate_accepts_real_plan():
    ok, reason = validate_model_output(REAL_PLAN, exit_code=0)
    assert ok, reason


def test_validate_rejects_nonzero_exit():
    ok, reason = validate_model_output(REAL_PLAN, exit_code=127)
    assert not ok
    assert "127" in reason


def test_validate_rejects_empty():
    assert not validate_model_output("", exit_code=0)[0]
    assert not validate_model_output(None, exit_code=0)[0]


def test_validate_rejects_short_capture():
    ok, reason = validate_model_output("You've hit your limit", exit_code=0)
    assert not ok
    assert "short" in reason


def test_validate_allows_plan_mentioning_error_strings():
    plan = "# Plan\n\n" + "On failure return permission denied or no such file. " * 12
    ok, reason = validate_model_output(plan, exit_code=0)
    assert ok, reason


def test_fanout_builds_expected_commands(tmp_path, monkeypatch):
    recorded = {}

    def fake_external(cmd, *, fn_name, model, prompt, **kw):
        recorded[model] = {"cmd": cmd, "kw": kw}
        return REAL_PLAN, 0

    monkeypatch.setattr(cp, "run_external_model", fake_external)
    monkeypatch.setattr(cp, "run_thinking", lambda prompt, **kw: REAL_PLAN)

    run_dir = tmp_path / "run"
    run_dir.mkdir()
    results = cp._fan_out("PROMPT", tmp_path, run_dir, [str(tmp_path)], 30)

    assert results["claude"]["valid"]
    assert results["codex"]["valid"]
    assert results["gemini"]["valid"]

    gem = recorded["gemini"]
    assert "--yolo" in gem["cmd"]
    assert "--include-directories" in gem["cmd"]
    assert gem["kw"]["env_extra"]["GEMINI_CLI_TRUST_WORKSPACE"] == "true"

    cdx = recorded["codex"]
    assert "--skip-git-repo-check" in cdx["cmd"] and "-o" in cdx["cmd"]


def test_quorum_degrades_to_claude_only_when_others_unavailable(tmp_path, monkeypatch):
    # When only claude's plan is valid (others dropped for availability:
    # non-zero exit, empty), proceed claude-only rather than deadlocking the
    # pipeline. The 2-model cross-check resumes once a vendor is reachable.
    monkeypatch.setattr(cp.log, "emit", lambda *a, **k: None)
    monkeypatch.setattr(cp, "_capture_baselines",
                        lambda config, slug: {"repo": (tmp_path, "abc123")})
    monkeypatch.setattr(cp, "_fan_out", lambda *a, **k: {
        "claude": {"text": REAL_PLAN, "valid": True, "reason": "ok"},
        "codex": {"text": None, "valid": False, "reason": "exit_code=127"},
        "gemini": {"text": "", "valid": False, "reason": "empty output"},
    })
    synth_called = {"hit": False}
    monkeypatch.setattr(cp, "_synthesize_and_implement",
                        lambda *a, **k: synth_called.__setitem__("hit", True) or True)
    monkeypatch.setattr(cp, "_assemble_diff", lambda *a, **k: (tmp_path / "p.patch", ["repo"]))
    monkeypatch.setattr(cp, "_write_manifest", lambda *a, **k: True)
    (tmp_path / "p.patch").write_text("diff")

    ok, reason = cp.run_consensus_plan({}, tmp_path, "slug", ticket_key="T-1")
    assert ok
    assert synth_called["hit"] is True


def test_quorum_fails_when_claude_plan_invalid(tmp_path, monkeypatch):
    # If claude itself is invalid, there's nothing to fall back to -> fail.
    monkeypatch.setattr(cp.log, "emit", lambda *a, **k: None)
    monkeypatch.setattr(cp, "_capture_baselines",
                        lambda config, slug: {"repo": (tmp_path, "abc123")})
    monkeypatch.setattr(cp, "_fan_out", lambda *a, **k: {
        "claude": {"text": None, "valid": False, "reason": "exit_code=1"},
        "codex": {"text": None, "valid": False, "reason": "exit_code=127"},
        "gemini": {"text": "", "valid": False, "reason": "empty output"},
    })
    synth_called = {"hit": False}
    monkeypatch.setattr(cp, "_synthesize_and_implement",
                        lambda *a, **k: synth_called.__setitem__("hit", True) or True)

    ok, reason = cp.run_consensus_plan({}, tmp_path, "slug", ticket_key="T-1")
    assert not ok
    assert "quorum" in reason
    assert synth_called["hit"] is False


def test_quorum_proceeds_with_two_valid(tmp_path, monkeypatch):
    monkeypatch.setattr(cp.log, "emit", lambda *a, **k: None)
    monkeypatch.setattr(cp, "_capture_baselines",
                        lambda config, slug: {"repo": (tmp_path, "abc123")})
    monkeypatch.setattr(cp, "_fan_out", lambda *a, **k: {
        "claude": {"text": REAL_PLAN, "valid": True, "reason": "ok"},
        "codex": {"text": REAL_PLAN, "valid": True, "reason": "ok"},
        "gemini": {"text": "", "valid": False, "reason": "empty output"},
    })
    seen = {}

    def fake_synth(ticket_dir, plan_files, timeout):
        seen["plan_files"] = plan_files
        return False

    monkeypatch.setattr(cp, "_synthesize_and_implement", fake_synth)

    ok, reason = cp.run_consensus_plan({}, tmp_path, "slug", ticket_key="T-1")
    assert not ok
    assert "synthesis" in reason
    assert len(seen["plan_files"]) == 2
