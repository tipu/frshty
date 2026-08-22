"""The agentic run mode: a real working directory and a read-only tool set.

Uses a stub `claude` on PATH that records its argv and its working directory,
then prints one stream-json result envelope.
"""
import json
import os
import stat
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.llm as llm  # noqa: E402


def _install_recording_claude(bin_dir: Path, record: Path, num_turns: int = 4) -> None:
    bin_dir.mkdir(parents=True, exist_ok=True)
    fake = bin_dir / "claude"
    envelope = json.dumps({"type": "result", "result": "ok", "num_turns": num_turns})
    fake.write_text(
        "#!/bin/sh\n"
        f'pwd > "{record}"\n'
        f'for a in "$@"; do echo "$a" >> "{record}"; done\n'
        f"cat <<'JSON'\n{envelope}\nJSON\n"
    )
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)


def _run(tmp_path, monkeypatch, cwd: Path, **kwargs):
    record = tmp_path / "record.txt"
    _install_recording_claude(tmp_path / "bin", record)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}:{os.environ['PATH']}")
    llm._providers.clear()
    out = llm.run_agentic("review this", cwd=cwd, timeout=30, **kwargs)
    lines = record.read_text().splitlines()
    return out, lines[0], lines[1:]


def test_agentic_runs_inside_the_given_directory(tmp_path, monkeypatch):
    checkout = tmp_path / "worktree"
    checkout.mkdir()
    out, pwd, argv = _run(tmp_path, monkeypatch, checkout)
    assert out == "ok"
    assert Path(pwd).resolve() == checkout.resolve()


def test_agentic_passes_tools_add_dirs_and_system_prompt(tmp_path, monkeypatch):
    checkout = tmp_path / "worktree"
    checkout.mkdir()
    extra = tmp_path / "reviews"
    extra.mkdir()
    _, _, argv = _run(tmp_path, monkeypatch, checkout,
                      system_prompt="be a reviewer",
                      tools=["Read", "Bash(git:*)"],
                      denied_tools=["Write", "Edit"],
                      add_dirs=[extra])
    assert argv[argv.index("--permission-mode") + 1] == "dontAsk"
    assert argv[argv.index("--allowedTools") + 1] == "Read,Bash(git:*)"
    assert argv[argv.index("--disallowedTools") + 1] == "Write,Edit"
    assert argv[argv.index("--add-dir") + 1] == str(extra)
    assert argv[argv.index("--append-system-prompt") + 1] == "be a reviewer"
    assert "--dangerously-skip-permissions" not in argv


def test_agentic_logs_a_single_turn_answer(tmp_path, monkeypatch):
    checkout = tmp_path / "worktree"
    checkout.mkdir()
    record = tmp_path / "record.txt"
    _install_recording_claude(tmp_path / "bin", record, num_turns=1)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}:{os.environ['PATH']}")
    llm._providers.clear()
    emitted = []
    monkeypatch.setattr(llm.log, "emit",
                        lambda event, msg, **kw: emitted.append(event))
    assert llm.run_agentic("review this", cwd=checkout, timeout=30) == "ok"
    assert "llm_agentic_single_turn" in emitted


def test_agentic_does_not_log_when_the_model_used_tools(tmp_path, monkeypatch):
    checkout = tmp_path / "worktree"
    checkout.mkdir()
    record = tmp_path / "record.txt"
    _install_recording_claude(tmp_path / "bin", record, num_turns=9)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}:{os.environ['PATH']}")
    llm._providers.clear()
    emitted = []
    monkeypatch.setattr(llm.log, "emit",
                        lambda event, msg, **kw: emitted.append(event))
    llm.run_agentic("review this", cwd=checkout, timeout=30)
    assert "llm_agentic_single_turn" not in emitted


def test_balanced_runs_inside_the_worktree(tmp_path, monkeypatch):
    checkout = tmp_path / "worktree"
    checkout.mkdir()
    record = tmp_path / "record.txt"
    _install_recording_claude(tmp_path / "bin", record)
    monkeypatch.setenv("PATH", f"{tmp_path / 'bin'}:{os.environ['PATH']}")
    llm._providers.clear()
    assert llm.run_balanced("hi", worktree=checkout, tools=["Read", "Grep"], timeout=30) == "ok"
    lines = record.read_text().splitlines()
    assert Path(lines[0]).resolve() == checkout.resolve()
    argv = lines[1:]
    assert argv[argv.index("--allowedTools") + 1] == "Read,Grep"
