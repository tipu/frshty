"""A pull request checkout must not run its own codex project config.

codex loads .codex/config.toml of its project root, and an [mcp_servers]
entry there starts a command when the session starts. The probe server in
these tests touches a marker file, so a marker means the pull request ran a
command.
"""
import json
import os
import shlex
import stat
import subprocess
import sys
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.llm as llm  # noqa: E402
import core.terminal as terminal  # noqa: E402
from core.paths import frshty_root  # noqa: E402

STUB = r'''#!/usr/bin/env python3
import json, os, sys
with open(os.environ["STUB_ARGV"], "a") as f:
    f.write(json.dumps(sys.argv[1:]) + "\n")
print("ok")
'''


def _review_worktree(root: Path) -> Path:
    return root / "titurco" / "reviews" / "app" / "feature-x" / "worktree"


def _own_worktree(root: Path) -> Path:
    return root / "titurco" / "worktrees" / "app"


def _checkout_with_mcp_server(path: Path, marker: Path) -> Path:
    path.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(path)], check=True)
    (path / ".codex").mkdir()
    (path / ".codex" / "config.toml").write_text(
        "[mcp_servers.probe]\n"
        'command = "sh"\n'
        f"args = {json.dumps(['-c', f'touch {shlex.quote(str(marker))}; sleep 5'])}\n")
    return path


def _projects(args: list[str]) -> dict:
    assert args[0] == "-c"
    return tomllib.loads(args[1])["projects"]


@pytest.fixture
def stub(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "codex"
    fake.write_text(STUB)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("STUB_ARGV", str(tmp_path / "argv.jsonl"))
    monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
    return tmp_path


def _argv(tmp_path: Path) -> list[list[str]]:
    return [json.loads(line) for line in (tmp_path / "argv.jsonl").read_text().splitlines()]


def test_no_codex_flags_outside_a_review_checkout(stub):
    assert llm.codex_review_checkout_args(_own_worktree(stub / "state")) == []
    assert llm.codex_review_checkout_args(stub / "elsewhere") == []
    assert llm.codex_review_checkout_args(None) == []


def test_a_review_checkout_marks_every_ancestor_untrusted(stub):
    worktree = _review_worktree(stub / "state") / "src"
    projects = _projects(llm.codex_review_checkout_args(worktree))

    for d in (worktree, *worktree.parents):
        assert projects[str(d)] == {"trust_level": "untrusted"}


def test_the_mark_covers_the_resolved_path_of_a_symlinked_checkout(stub):
    real = _review_worktree(stub / "state")
    real.mkdir(parents=True)
    link = stub / "pr-7"
    link.symlink_to(real)
    projects = _projects(llm.codex_review_checkout_args(link))

    assert str(link) in projects
    assert str(real) in projects


def test_a_path_with_quotes_parses_as_toml(stub):
    worktree = _review_worktree(stub / "state") / 'we"ird\\name-\U0001f680'
    projects = _projects(llm.codex_review_checkout_args(worktree))

    assert projects[str(worktree)] == {"trust_level": "untrusted"}


def test_codex_exec_in_a_review_checkout_gets_the_flags(stub):
    worktree = _review_worktree(stub / "state")
    worktree.mkdir(parents=True)

    _, code = llm.run_external_model([*llm.CODEX_EXEC, "-"], fn_name="t", model="codex",
                                     prompt="p", cwd=worktree, stdin_text="p")

    assert code == 0
    argv = _argv(stub)[0]
    assert argv[:1] == ["exec"]
    assert argv[1:3] == llm.codex_review_checkout_args(worktree)


def test_an_output_schema_run_gets_the_flags_once(stub):
    worktree = _review_worktree(stub / "state")
    worktree.mkdir(parents=True)

    llm.run_external_model([*llm.CODEX_EXEC, "-"], fn_name="t", model="codex", prompt="p",
                           cwd=worktree, stdin_text="p", output_schema={"type": "object"})

    assert _argv(stub)[0].count("-c") == 1


def test_codex_exec_outside_a_review_checkout_gets_no_flags(stub):
    own = _own_worktree(stub / "state")
    own.mkdir(parents=True)

    llm.run_external_model([*llm.CODEX_EXEC, "-"], fn_name="t", model="codex",
                           prompt="p", cwd=own, stdin_text="p")

    assert _argv(stub)[0] == [*llm.CODEX_EXEC[1:], "-"]


def test_another_cli_gets_no_codex_flags(stub):
    worktree = _review_worktree(stub / "state")
    assert llm._with_codex_review_args(["agy", "exec"], worktree) == ["agy", "exec"]


def test_the_codex_pane_in_a_review_checkout_gets_the_flags(stub):
    worktree = _review_worktree(stub / "state")
    for sub in ("", "resume"):
        argv = shlex.split(terminal.codex_cmd({}, sub, cwd=str(worktree)))
        assert argv[-2:] == llm.codex_review_checkout_args(worktree)
    assert terminal.codex_cmd({}, cwd=str(_own_worktree(stub / "state"))) == \
        "codex --dangerously-bypass-approvals-and-sandbox"


def _real_codex(stub_dir: Path) -> str | None:
    for d in os.environ.get("PATH", "").split(os.pathsep):
        if d and Path(d) != stub_dir and (Path(d) / "codex").is_file():
            return str(Path(d) / "codex")
    return None


@pytest.fixture
def real_codex(_no_real_model_binaries, tmp_path, monkeypatch):
    codex = _real_codex(_no_real_model_binaries)
    if codex is None:
        pytest.skip("no codex binary on PATH")
    monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
    return codex


def _run_real(codex: str, cwd: Path) -> int | None:
    cmd = [codex, *llm.CODEX_EXEC[1:], "--ephemeral", "-"]
    _, code = llm.run_external_model(cmd, fn_name="t", model="codex",
                                     prompt="Reply with the word ok.", cwd=cwd,
                                     timeout=300, stdin_text="Reply with the word ok.")
    return code


@pytest.mark.slow
def test_real_codex_starts_a_project_mcp_server_outside_a_review_checkout(real_codex, tmp_path):
    marker = tmp_path / "mcp-ran"
    own = _checkout_with_mcp_server(_own_worktree(frshty_root()), marker)

    assert _run_real(real_codex, own) == 0
    assert marker.exists()


@pytest.mark.slow
def test_real_codex_starts_no_project_mcp_server_in_a_review_checkout(real_codex, tmp_path):
    marker = tmp_path / "mcp-ran"
    worktree = _checkout_with_mcp_server(_review_worktree(frshty_root()), marker)

    assert _run_real(real_codex, worktree) == 0
    assert not marker.exists()
