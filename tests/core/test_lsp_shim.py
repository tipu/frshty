"""scripts/lsp/typescript-language-server: the shim that ClaudeProvider puts
first on PATH so the TypeScript server answers from the semantic tsserver.

Uses a stub typescript-language-server later on PATH that records the bytes
it reads on stdin and exits with a chosen code.
"""
import json
import os
import shutil
import stat
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.llm as llm  # noqa: E402
import core.terminal as terminal  # noqa: E402

SHIM = llm.LSP_SHIM_DIR / "typescript-language-server"

needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")


def _frame(message: dict) -> bytes:
    body = json.dumps(message).encode()
    return b"Content-Length: %d\r\n\r\n" % len(body) + body


def _frames(data: bytes) -> list[bytes]:
    bodies = []
    while data:
        header, _, rest = data.partition(b"\r\n\r\n")
        length = int(header.split(b":")[1])
        bodies.append(rest[:length])
        data = rest[length:]
    return bodies


def _run_shim(tmp_path, stdin: bytes, exit_code: int = 0):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    record = tmp_path / "record.bin"
    args = tmp_path / "args.txt"
    stub = bin_dir / "typescript-language-server"
    stub.write_text(f'#!/bin/sh\necho "$@" > "{args}"\ncat > "{record}"\nexit {exit_code}\n')
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    env = {**os.environ, "PATH": os.pathsep.join([str(llm.LSP_SHIM_DIR), str(bin_dir), os.environ["PATH"]])}
    proc = subprocess.run([str(SHIM), "--stdio"], input=stdin, env=env, capture_output=True, timeout=30)
    return proc, record.read_bytes(), args.read_text().strip()


def test_provider_env_puts_shim_first_on_path(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    path = llm.ClaudeProvider()._env()["PATH"].split(os.pathsep)
    assert path == [str(llm.LSP_SHIM_DIR), "/usr/bin"]
    assert os.access(SHIM, os.X_OK)


def test_provider_env_keeps_shim_first_over_a_path_override(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    provider = llm.ClaudeProvider({"llm": {"claude": {"env": {"PATH": "/opt/bin"}}}})
    assert provider._env()["PATH"].split(os.pathsep) == [str(llm.LSP_SHIM_DIR), "/opt/bin"]


@needs_node
def test_shim_sets_semantic_only_and_passes_later_messages_through(tmp_path):
    initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                  "params": {"rootUri": "file:///r", "initializationOptions": {}}}
    later = _frame({"jsonrpc": "2.0", "id": 2, "method": "textDocument/references",
                    "params": {"text": "é"}})
    proc, seen, args = _run_shim(tmp_path, _frame(initialize) + later)
    assert proc.returncode == 0, proc.stderr
    assert args == "--stdio"
    first, second = _frames(seen)
    sent = json.loads(first)
    assert sent["params"]["initializationOptions"] == {"tsserver": {"useSyntaxServer": "never"}}
    assert sent["params"]["rootUri"] == "file:///r"
    assert seen.endswith(later)
    assert second == _frames(later)[0]


@needs_node
def test_shim_adds_options_when_the_client_sends_none(tmp_path):
    initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}}
    proc, seen, _ = _run_shim(tmp_path, _frame(initialize))
    assert proc.returncode == 0, proc.stderr
    sent = json.loads(_frames(seen)[0])
    assert sent["params"]["initializationOptions"]["tsserver"]["useSyntaxServer"] == "never"


@needs_node
def test_shim_keeps_a_client_choice(tmp_path):
    initialize = {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                  "params": {"initializationOptions": {"tsserver": {"useSyntaxServer": "auto"}}}}
    proc, seen, _ = _run_shim(tmp_path, _frame(initialize))
    assert proc.returncode == 0, proc.stderr
    sent = json.loads(_frames(seen)[0])
    assert sent["params"]["initializationOptions"]["tsserver"]["useSyntaxServer"] == "auto"


@needs_node
def test_shim_returns_the_server_exit_code(tmp_path):
    proc, _, _ = _run_shim(tmp_path, b"", exit_code=3)
    assert proc.returncode == 3


@needs_node
def test_shim_fails_when_no_real_server_is_on_path(tmp_path):
    node_dir = tmp_path / "node"
    node_dir.mkdir()
    (node_dir / "node").symlink_to(shutil.which("node") or "node")
    env = {**os.environ, "PATH": os.pathsep.join([str(llm.LSP_SHIM_DIR), str(node_dir)])}
    proc = subprocess.run([str(SHIM), "--stdio"], input=b"", env=env, capture_output=True, timeout=30)
    assert proc.returncode == 127
    assert b"no other typescript-language-server on PATH" in proc.stderr


@needs_node
def test_shim_reraises_the_server_signal(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "typescript-language-server"
    stub.write_text("#!/bin/sh\nkill -TERM $$\n")
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    env = {**os.environ, "PATH": os.pathsep.join([str(llm.LSP_SHIM_DIR), str(bin_dir), os.environ["PATH"]])}
    proc = subprocess.run([str(SHIM), "--stdio"], input=b"", env=env, capture_output=True, timeout=30)
    assert proc.returncode == -15


@needs_node
def test_shim_runs_a_server_found_through_a_relative_path_entry(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "typescript-language-server"
    stub.write_text("#!/bin/sh\nexit 4\n")
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    node_dir = tmp_path / "node"
    node_dir.mkdir()
    (node_dir / "node").symlink_to(shutil.which("node") or "node")
    env = {**os.environ, "PATH": os.pathsep.join([str(llm.LSP_SHIM_DIR), ".", str(node_dir)])}
    proc = subprocess.run([str(SHIM), "--stdio"], input=b"", env=env, cwd=bin_dir,
                          capture_output=True, timeout=30)
    assert proc.returncode == 4


@needs_node
def test_shim_treats_an_empty_path_entry_as_the_working_directory(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "typescript-language-server"
    stub.write_text("#!/bin/sh\nexit 5\n")
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    node_dir = tmp_path / "node"
    node_dir.mkdir()
    (node_dir / "node").symlink_to(shutil.which("node") or "node")
    env = {**os.environ, "PATH": f"{llm.LSP_SHIM_DIR}::{node_dir}"}
    proc = subprocess.run([str(SHIM), "--stdio"], input=b"", env=env, cwd=bin_dir,
                          capture_output=True, timeout=30)
    assert proc.returncode == 5


def test_pane_env_puts_shim_first_on_path(monkeypatch):
    monkeypatch.setenv("PATH", "/usr/bin")
    expected = os.pathsep.join([str(llm.LSP_SHIM_DIR), "/usr/bin"])
    assert terminal._child_env()["PATH"] == expected
    args = terminal._pane_env_args()
    assert args[args.index(f"PATH={expected}") - 1] == "-e"


def test_pane_claude_command_keeps_shim_first_over_a_path_override():
    cmd = terminal.claude_cmd({"llm": {"claude": {"env": {"PATH": "/usr/bin:/bin"}}}})
    assert f"PATH={llm.LSP_SHIM_DIR}{os.pathsep}/usr/bin:/bin " in cmd


def test_pane_claude_command_expands_a_home_path_override():
    cmd = terminal.claude_cmd({"llm": {"claude": {"env": {"PATH": "~/.local/bin"}}}})
    assert f"PATH={llm.LSP_SHIM_DIR}{os.pathsep}{os.path.expanduser('~/.local/bin')} " in cmd


def test_the_review_plugin_keeps_every_request_on_the_semantic_server():
    manifest = json.loads(
        (llm.REVIEW_TS_PLUGIN_DIR / ".claude-plugin" / "plugin.json").read_text())
    server = manifest["lspServers"]["typescript"]
    assert server["command"] == SHIM.name
    assert server["initializationOptions"]["tsserver"]["useSyntaxServer"] == "never"
