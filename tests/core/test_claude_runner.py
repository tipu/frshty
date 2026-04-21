"""Unit tests for run_claude_code streaming.

Uses a shell subprocess instead of real claude, injected via PATH so the
binary name `claude` resolves to a test script. Each test creates a stub
`claude` in tmp_path that prints lines on a schedule, then verifies the
tee'd log file and the return value.
"""
import os
import stat
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.job_logs as job_logs  # noqa: E402
from core.claude_runner import run_claude_code  # noqa: E402


def _install_fake_claude(bin_dir: Path, script_body: str) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    fake = bin_dir / "claude"
    fake.write_text(f"#!/bin/sh\n{script_body}\n")
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def test_run_claude_code_tees_stdout_to_log_when_contextvar_set(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    _install_fake_claude(bin_dir, 'echo "line1"; echo "line2"; echo "line3"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    finally:
        job_logs._active_live_log.reset(token)

    assert out == "line1\nline2\nline3\n"
    assert log_path.exists()
    assert log_path.read_bytes() == b"line1\nline2\nline3\n"


def test_run_claude_code_without_contextvar_does_not_write_log(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    _install_fake_claude(bin_dir, 'echo "x"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    sentinel = tmp_path / "should_not_exist.log"
    # Contextvar defaults to None
    out = run_claude_code("hi", cwd=tmp_path, timeout=10)
    assert out == "x\n"
    assert not sentinel.exists()


def test_run_claude_code_non_zero_exit_writes_exit_marker_and_returns_none(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    _install_fake_claude(bin_dir, 'echo "partial"; exit 7')
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
    # Print something, then sleep longer than the timeout
    _install_fake_claude(bin_dir, 'echo "started"; sleep 5')
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


def test_run_claude_code_preserves_ansi_bytes_in_log(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    # ANSI red "red" reset
    _install_fake_claude(bin_dir, 'printf "\\033[31mred\\033[0m\\n"')
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")

    log_path = tmp_path / "job.log"
    token = job_logs._active_live_log.set(log_path)
    try:
        out = run_claude_code("hi", cwd=tmp_path, timeout=5)
    finally:
        job_logs._active_live_log.reset(token)

    assert out == "\x1b[31mred\x1b[0m\n"
    assert log_path.read_bytes() == b"\x1b[31mred\x1b[0m\n"
