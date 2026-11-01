"""scripts/lsp/pyright-langserver: the shim that ClaudeProvider puts first on
PATH so pyright reads the project file of the repository an opened file sits
in, and answers workspace/symbol only after it has listed the files.

Uses a stub pyright-langserver later on PATH that records each message it
reads on stdin, with the time it read it, and after `initialized` writes
chosen log messages at chosen delays.
"""
import json
import os
import shutil
import stat
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.llm as llm  # noqa: E402

SHIM = llm.LSP_SHIM_DIR / "pyright-langserver"

pytestmark = pytest.mark.skipif(shutil.which("node") is None, reason="node is not installed")

STUB = r'''#!/usr/bin/env python3
import json, os, sys, threading, time
record = open(os.environ["STUB_RECORD"], "a")
def write(entry):
    record.write(json.dumps(entry) + "\n")
    record.flush()
def logs():
    for delay, text in json.loads(os.environ["STUB_LOGS"]):
        time.sleep(delay)
        body = json.dumps({"jsonrpc": "2.0", "method": "window/logMessage",
                           "params": {"type": 3, "message": text}}).encode()
        write({"logged": time.time(), "text": text})
        sys.stdout.buffer.write(b"Content-Length: %d\r\n\r\n" % len(body) + body)
        sys.stdout.buffer.flush()
stdin = sys.stdin.buffer
while True:
    header = b""
    while not header.endswith(b"\r\n\r\n"):
        c = stdin.read(1)
        if not c:
            sys.exit(0)
        header += c
    message = json.loads(stdin.read(int(header.split(b":")[1])))
    write({"at": time.time(), "message": message})
    if message.get("method") == "initialized" and os.environ.get("STUB_LOGS"):
        threading.Thread(target=logs, daemon=True).start()
'''


def _frame(message: dict) -> bytes:
    body = json.dumps(message).encode()
    return b"Content-Length: %d\r\n\r\n" % len(body) + body


def _initialize(root: Path) -> dict:
    uri = root.as_uri()
    return {"jsonrpc": "2.0", "id": 0, "method": "initialize",
            "params": {"rootUri": uri, "rootPath": str(root),
                       "workspaceFolders": [{"uri": uri, "name": root.name}],
                       "capabilities": {"workspace": {"workspaceFolders": False}}}}


def _open(path: Path) -> dict:
    return {"jsonrpc": "2.0", "method": "textDocument/didOpen",
            "params": {"textDocument": {"uri": path.as_uri(), "languageId": "python",
                                        "version": 1, "text": ""}}}


SYMBOL = {"jsonrpc": "2.0", "id": 1, "method": "workspace/symbol", "params": {"query": "ReceiptLine"}}
INITIALIZED = {"jsonrpc": "2.0", "method": "initialized", "params": {}}


def _run(tmp_path, messages: list[dict], wait_for: int, logs: list | None = None,
         hold_ms: int = 60000) -> list[dict]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "pyright-langserver"
    stub.write_text(STUB)
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    record = tmp_path / "record.jsonl"
    record.touch()
    env = {**os.environ, "STUB_RECORD": str(record), "FRSHTY_PYRIGHT_HOLD_MS": str(hold_ms),
           "PATH": os.pathsep.join([str(llm.LSP_SHIM_DIR), str(bin_dir), os.environ["PATH"]])}
    if logs is not None:
        env["STUB_LOGS"] = json.dumps(logs)
    proc = subprocess.Popen([str(SHIM), "--stdio"], stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)
    proc.stdin.write(b"".join(_frame(m) for m in messages))
    proc.stdin.flush()
    deadline = time.time() + 30
    entries: list[dict] = []
    while time.time() < deadline:
        entries = [json.loads(line) for line in record.read_text().splitlines()]
        if sum("message" in e for e in entries) >= wait_for:
            break
        time.sleep(0.05)
    proc.stdin.close()
    proc.wait(timeout=30)
    return entries


def _messages(entries: list[dict]) -> list[dict]:
    return [e["message"] for e in entries if "message" in e]


def test_a_root_without_a_project_file_is_dropped_and_the_repository_is_added(tmp_path):
    workspace = tmp_path / "workspace"
    repo = workspace / "tickets" / "work-1" / "app"
    (repo / "src" / "purchasing").mkdir(parents=True)
    (repo / "pyrightconfig.json").write_text("{}")
    models = repo / "src" / "purchasing" / "models.py"
    models.write_text("")
    entries = _run(tmp_path, [_initialize(workspace), INITIALIZED, _open(models)], wait_for=4)
    initialize, initialized, added, opened = _messages(entries)
    assert initialize["params"]["workspaceFolders"] == []
    assert initialize["params"]["rootUri"] is None
    assert initialize["params"]["capabilities"]["workspace"]["workspaceFolders"] is True
    assert initialized["method"] == "initialized"
    assert added["method"] == "workspace/didChangeWorkspaceFolders"
    assert added["params"]["event"]["added"] == [{"uri": repo.as_uri(), "name": "app"}]
    assert opened["method"] == "textDocument/didOpen"


def test_a_repository_is_added_once(tmp_path):
    workspace = tmp_path / "workspace"
    repo = workspace / "app"
    (repo / "src").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("")
    first, second = repo / "src" / "a.py", repo / "src" / "b.py"
    entries = _run(tmp_path, [_initialize(workspace), INITIALIZED, _open(first), _open(second)],
                   wait_for=5)
    methods = [m.get("method") for m in _messages(entries)]
    assert methods.count("workspace/didChangeWorkspaceFolders") == 1


def test_a_root_with_a_project_file_is_kept_and_nothing_is_added(tmp_path):
    repo = tmp_path / "repo"
    (repo / "pkg").mkdir(parents=True)
    (repo / "pyproject.toml").write_text("")
    (repo / "pkg" / "pyproject.toml").write_text("")
    entries = _run(tmp_path, [_initialize(repo), INITIALIZED, _open(repo / "pkg" / "m.py")],
                   wait_for=3)
    initialize, _, opened = _messages(entries)
    assert initialize["params"]["workspaceFolders"] == [{"uri": repo.as_uri(), "name": "repo"}]
    assert initialize["params"]["rootUri"] == repo.as_uri()
    assert opened["method"] == "textDocument/didOpen"


def test_workspace_symbol_waits_for_the_file_count(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("")
    entries = _run(tmp_path, [_initialize(repo), INITIALIZED, SYMBOL], wait_for=3,
                   logs=[[0, 'Starting service instance "repo"'], [1.0, "Found 3 source files"]])
    listed = next(e["logged"] for e in entries if e.get("text") == "Found 3 source files")
    symbol = next(e for e in entries if e.get("message", {}).get("method") == "workspace/symbol")
    assert symbol["at"] >= listed


def test_a_file_count_without_a_started_workspace_releases_nothing(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("")
    entries = _run(tmp_path, [_initialize(repo), INITIALIZED, SYMBOL], wait_for=3,
                   logs=[[0, "Found 9 source files"], [0, 'Starting service instance "repo"'],
                         [1.0, "Found 3 source files"]])
    listed = next(e["logged"] for e in entries if e.get("text") == "Found 3 source files")
    symbol = next(e for e in entries if e.get("message", {}).get("method") == "workspace/symbol")
    assert symbol["at"] >= listed


def test_messages_after_a_held_request_keep_their_order(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("")
    edit = {"jsonrpc": "2.0", "method": "textDocument/didChange",
            "params": {"textDocument": {"uri": (repo / "m.py").as_uri(), "version": 2},
                       "contentChanges": [{"text": "# x\n"}]}}
    entries = _run(tmp_path, [_initialize(repo), INITIALIZED, SYMBOL, edit], wait_for=4,
                   logs=[[0, 'Starting service instance "repo"'], [0.5, "Found 3 source files"]])
    methods = [m.get("method") for m in _messages(entries)]
    assert methods[2:] == ["workspace/symbol", "textDocument/didChange"]


def test_workspace_symbol_is_sent_after_the_hold_without_a_file_count(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "pyproject.toml").write_text("")
    started = time.time()
    entries = _run(tmp_path, [_initialize(repo), INITIALIZED, SYMBOL], wait_for=3, hold_ms=500)
    symbol = next(e for e in entries if e.get("message", {}).get("method") == "workspace/symbol")
    assert symbol["at"] - started >= 0.5


def test_workspace_symbol_is_not_held_without_a_workspace_folder(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    entries = _run(tmp_path, [_initialize(workspace), INITIALIZED, SYMBOL], wait_for=3)
    assert _messages(entries)[2]["method"] == "workspace/symbol"


def test_shim_fails_when_no_real_server_is_on_path(tmp_path):
    node_dir = tmp_path / "node"
    node_dir.mkdir()
    (node_dir / "node").symlink_to(shutil.which("node") or "node")
    env = {**os.environ, "PATH": os.pathsep.join([str(llm.LSP_SHIM_DIR), str(node_dir)])}
    proc = subprocess.run([str(SHIM), "--stdio"], input=b"", env=env, capture_output=True, timeout=30)
    assert proc.returncode == 127
    assert b"no other pyright-langserver on PATH" in proc.stderr
