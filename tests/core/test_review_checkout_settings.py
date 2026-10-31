"""A pull request checkout must not run its own Claude Code settings.

The stub `claude` on PATH does what Claude Code does with a project hook: it
runs every SessionStart hook in .claude/settings.json of its working
directory, unless --setting-sources leaves out the project source. A hook
that ran leaves a marker file behind.
"""
import json
import os
import shlex
import stat
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))

import core.llm as llm  # noqa: E402
import core.terminal as terminal  # noqa: E402
from core.paths import review_checkout  # noqa: E402

STUB = r'''#!/usr/bin/env python3
import json, os, subprocess, sys
argv = sys.argv[1:]
with open(os.environ["STUB_ARGV"], "w") as f:
    json.dump(argv, f)
sources = ["user", "project", "local"]
if "--setting-sources" in argv:
    sources = argv[argv.index("--setting-sources") + 1].split(",")
settings = os.path.join(os.getcwd(), ".claude", "settings.json")
if "project" in sources and os.path.isfile(settings):
    for group in json.load(open(settings)).get("hooks", {}).get("SessionStart", []):
        for hook in group.get("hooks", []):
            subprocess.run(hook["command"], shell=True, check=True)
print(json.dumps({"type": "result", "result": "ok", "num_turns": 4}))
'''


@pytest.fixture
def stub(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "claude"
    fake.write_text(STUB)
    fake.chmod(fake.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("STUB_ARGV", str(tmp_path / "argv.json"))
    monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
    llm._providers.clear()
    return tmp_path


def _checkout_with_hook(path: Path, marker: Path) -> Path:
    (path / ".claude").mkdir(parents=True)
    (path / ".claude" / "settings.json").write_text(json.dumps({"hooks": {"SessionStart": [
        {"hooks": [{"type": "command", "command": f"touch {shlex.quote(str(marker))}"}]}]}}))
    return path


def _review_worktree(tmp_path: Path) -> Path:
    return tmp_path / "state" / "titurco" / "reviews" / "app" / "feature-x" / "worktree"


def _argv(tmp_path: Path) -> list[str]:
    return json.loads((tmp_path / "argv.json").read_text())


def test_the_stub_runs_a_project_hook_outside_a_review_checkout(stub):
    marker = stub / "hook-ran"
    own = _checkout_with_hook(stub / "state" / "titurco" / "worktrees" / "app", marker)

    assert llm.run_agentic("review", cwd=own, timeout=30) == "ok"

    assert marker.exists()
    assert "--setting-sources" not in _argv(stub)


def test_an_agentic_review_does_not_run_the_hook_of_the_pull_request(stub):
    marker = stub / "hook-ran"
    worktree = _checkout_with_hook(_review_worktree(stub), marker)

    assert llm.run_agentic("review", cwd=worktree, timeout=30) == "ok"

    assert not marker.exists()
    argv = _argv(stub)
    assert argv[argv.index("--setting-sources") + 1] == "user"


def test_a_balanced_run_in_a_review_checkout_does_not_run_the_hook(stub):
    marker = stub / "hook-ran"
    worktree = _checkout_with_hook(_review_worktree(stub), marker)

    assert llm.run_balanced("explain", worktree=worktree, timeout=30) == "ok"

    assert not marker.exists()


def test_a_ticket_review_rooted_in_the_reviews_folder_does_not_run_a_hook(stub):
    marker = stub / "hook-ran"
    reviews = _checkout_with_hook(stub / "state" / "titurco" / "reviews", marker)

    assert llm.run_agentic("review", cwd=reviews, timeout=30) == "ok"

    assert not marker.exists()


def test_a_review_run_swaps_in_the_pinned_typescript_server(stub, monkeypatch):
    monkeypatch.setattr(llm, "REVIEW_TSSERVER", str(stub / "tsserver.js"))
    (stub / "tsserver.js").write_text("")

    args = llm.review_checkout_args(_review_worktree(stub))

    settings = json.loads(args[args.index("--settings") + 1])
    assert settings == {"enabledPlugins": {llm.MARKETPLACE_TS_PLUGIN: False}}
    assert args[args.index("--plugin-dir") + 1] == str(llm.REVIEW_TS_PLUGIN_DIR)


def test_a_review_run_without_the_global_tsserver_runs_no_typescript_server(stub, monkeypatch):
    monkeypatch.setattr(llm, "REVIEW_TSSERVER", str(stub / "missing" / "tsserver.js"))

    args = llm.review_checkout_args(_review_worktree(stub))

    assert "--plugin-dir" not in args
    settings = json.loads(args[args.index("--settings") + 1])
    assert settings["enabledPlugins"][llm.MARKETPLACE_TS_PLUGIN] is False


def test_the_review_plugin_pins_the_tsserver_the_code_checks():
    manifest = json.loads(
        (llm.REVIEW_TS_PLUGIN_DIR / ".claude-plugin" / "plugin.json").read_text())
    server = manifest["lspServers"]["typescript"]
    assert server["initializationOptions"]["tsserver"]["path"] == llm.REVIEW_TSSERVER


@pytest.mark.parametrize("parts, expected", [
    (("titurco", "reviews"), True),
    (("titurco", "reviews", "app", "pr-7", "worktree"), True),
    (("titurco", "worktrees", "app"), False),
    (("reviews", "app"), False),
    ((), False),
])
def test_review_checkout_reads_the_instance_layout(tmp_path, monkeypatch, parts, expected):
    monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path))
    assert review_checkout(tmp_path.joinpath(*parts)) is expected


def test_a_path_outside_the_state_root_is_not_a_review_checkout(tmp_path, monkeypatch):
    monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
    assert review_checkout(tmp_path / "elsewhere" / "titurco" / "reviews") is False


class TestTaskPane:
    def _no_agent(self, monkeypatch, tmp_path):
        monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))
        monkeypatch.setattr(
            terminal, "session_healthy", lambda key: {"alive": False, "agent_running": False})
        monkeypatch.setattr(terminal, "LAUNCH_CONTEXT_DIR", str(tmp_path / "ctx"))
        launch = MagicMock()
        monkeypatch.setattr(terminal, "launch_pane_command", launch)
        return launch

    @pytest.mark.parametrize("first_run", [True, False])
    def test_a_task_in_a_review_checkout_loads_user_settings_only(
            self, monkeypatch, tmp_path, first_run):
        launch = self._no_agent(monkeypatch, tmp_path)
        worktree = _review_worktree(tmp_path)
        worktree.mkdir(parents=True)

        terminal.launch_claude("work-30", str(worktree), "session-30", "brief", first_run)

        argv = shlex.split(launch.call_args.args[2].split(" --append-system-prompt")[0])
        assert argv[argv.index("--setting-sources") + 1] == "user"
        assert "--settings" in argv

    def test_a_discuss_pane_in_a_review_checkout_loads_user_settings_only(
            self, monkeypatch, tmp_path):
        monkeypatch.setenv("FRSHTY_ROOT", str(tmp_path / "state"))

        argv = shlex.split(terminal.claude_cmd({}, cwd=str(_review_worktree(tmp_path))))

        assert argv[argv.index("--setting-sources") + 1] == "user"
        assert "--setting-sources" not in terminal.claude_cmd({}, cwd=str(tmp_path))

    def test_a_task_in_its_own_worktree_keeps_the_project_settings(self, monkeypatch, tmp_path):
        launch = self._no_agent(monkeypatch, tmp_path)

        terminal.launch_claude("work-31", str(tmp_path), "session-31", "", False)

        assert "--setting-sources" not in launch.call_args.args[2]
