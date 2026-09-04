"""The gates that keep a work session out of a shared checkout.

The launch puts a task in the right place. These cover the task that walks out
of it: an Edit into the shared tree, a commit into the shared tree, and the
resume and follow-up paths that would otherwise put a session back there.
"""
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


import core.config as core_config
import core.db as db
from services import work_launch, work_store, work_worktree
from tests.features.test_work_worktree import (_git, make_repo)


class FakeBoard:
    """A stand-in for the board's /worktree endpoint.

    The hook talks to the board over HTTP from its own process, so the
    transport is part of what these tests cover."""

    def __init__(self, answer=None, status=200):
        self.answer = answer
        self.status = status
        self.seen = []
        board = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(length) or b"{}")
                board.seen.append({"path": self.path, "body": body})
                payload = json.dumps(board.answer or {}).encode()
                self.send_response(board.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"

    def __enter__(self):
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        return self

    def __exit__(self, *exc):
        self.server.shutdown()
        self.server.server_close()


def _run(item_id, cwd="/tmp", board_url=""):
    sid = f"sid-gate-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", cwd, board_url=board_url)
    return sid


def run_hook(payload, board_file=None):
    env = {**os.environ, "FRSHTY_DB": str(db._DB_PATH)}
    env["FRSHTY_BOARD_FILE"] = str(board_file) if board_file else "/nonexistent/board.json"
    return subprocess.run([sys.executable, "scripts/work_hook.py"],
                          input=json.dumps(payload), capture_output=True,
                          text=True, timeout=180, env=env)


def deny_reason(result):
    if not result.stdout.strip():
        return ""
    return json.loads(result.stdout)["hookSpecificOutput"]["permissionDecisionReason"]


def board_file(tmp_path, url):
    path = tmp_path / "board.json"
    path.write_text(json.dumps({"base_url": url}))
    return path


class TestWriteGate:
    def test_denies_an_edit_in_a_shared_checkout_and_allows_it_in_the_worktree(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        item_id = work_store.create_item("write gate")
        sid = _run(item_id)
        with FakeBoard({"path": str(wt), "branch": "side"}) as board:
            denied = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, board.url))
            allowed = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                                "tool_name": "Edit",
                                "tool_input": {"file_path": str(wt / "README.md")}},
                               board_file(tmp_path, board.url))
        assert denied.returncode == 0, denied.stderr
        reason = deny_reason(denied)
        assert "shared checkout" in reason
        assert str(wt) in reason and "side" in reason
        assert allowed.stdout.strip() == ""
        assert len(board.seen) == 1
        assert board.seen[0]["body"] == {"repo_path": str(repo)}

    def test_denies_even_when_the_task_already_owns_a_worktree_of_that_repo(self, tmp_path,
                                                                           monkeypatch):
        """The repository id is the ownership key, never an authorisation. A
        shared checkout and its worktrees share one --git-common-dir."""
        repo = make_repo(tmp_path)
        monkeypatch.setattr(core_config, "TASK_WORKTREE_ROOT", tmp_path / "worktrees")
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = work_store.create_item("owns a worktree already")
        row = work_worktree.ensure(
            item_id, work_worktree._repo_entry(None, str(repo), "app", "proj"),
            "owns a worktree already")
        assert row
        sid = _run(item_id)
        with FakeBoard({"path": row["path"], "branch": row["branch"]}) as board:
            denied = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, board.url))
        assert deny_reason(denied).count("shared checkout") >= 1
        assert row["path"] in deny_reason(denied)

    def test_denies_when_no_board_address_resolves(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("no board")
        sid = _run(item_id)
        result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                           "tool_name": "Edit",
                           "tool_input": {"file_path": str(repo / "README.md")}})
        reason = deny_reason(result)
        assert "address of the work board could not be resolved" in reason
        assert "board.json" in reason

    def test_denies_when_the_board_is_unreachable(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("board down")
        sid = _run(item_id)
        result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                           "tool_name": "Edit",
                           "tool_input": {"file_path": str(repo / "README.md")}},
                          board_file(tmp_path, "http://127.0.0.1:1"))
        reason = deny_reason(result)
        assert "refused to create a worktree" in reason

    def test_a_board_that_answered_is_final(self, tmp_path):
        """A 409 means the board was reached and refused. Trying the next
        address would replace that with a connection error and hide why."""
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("answered final")
        with FakeBoard({"error": "no repository"}, status=409) as published:
            sid = _run(item_id, board_url="http://127.0.0.1:1")
            result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, published.url))
        reason = deny_reason(result)
        assert "the board answered 409" in reason
        assert "Connection refused" not in reason

    def test_denies_when_the_board_answers_an_error(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("board error")
        sid = _run(item_id)
        with FakeBoard({"error": "no repository"}, status=409) as board:
            result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, board.url))
        assert "the board answered 409" in deny_reason(result)

    def test_a_pre_migration_run_still_resolves_the_board_from_the_file(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        item_id = work_store.create_item("pre-migration run")
        sid = _run(item_id, board_url="")
        with FakeBoard({"path": str(wt), "branch": "side"}) as board:
            result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, board.url))
        assert str(wt) in deny_reason(result)

    def test_the_published_file_wins_over_the_recorded_url(self, tmp_path):
        """The file is written by the server that is up now. The recorded
        address belongs to the server that launched the run, which may have
        been restarted on another port since."""
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("published wins")
        with FakeBoard({"path": "/from/run", "branch": "b"}) as recorded:
            sid = _run(item_id, board_url=recorded.url)
            with FakeBoard({"path": "/from/file", "branch": "b"}) as published:
                result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                                   "tool_name": "Edit",
                                   "tool_input": {"file_path": str(repo / "README.md")}},
                                  board_file(tmp_path, published.url))
                assert len(published.seen) == 1
            assert recorded.seen == []
        assert "/from/file" in deny_reason(result)

    def test_a_stale_recorded_address_does_not_wedge_the_session(self, tmp_path):
        """The server can be restarted on another port. The address the run
        recorded is then dead, and the file the running server wrote is the
        one that works."""
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        item_id = work_store.create_item("stale address")
        sid = _run(item_id, board_url="http://127.0.0.1:1")
        with FakeBoard({"path": str(wt), "branch": "side"}) as board:
            result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, board.url))
        assert len(board.seen) == 1
        assert str(wt) in deny_reason(result)

    def test_a_dead_file_address_falls_back_to_the_recorded_one(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        item_id = work_store.create_item("dead file address")
        with FakeBoard({"path": str(wt), "branch": "side"}) as board:
            sid = _run(item_id, board_url=board.url)
            result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                               "tool_name": "Edit",
                               "tool_input": {"file_path": str(repo / "README.md")}},
                              board_file(tmp_path, "http://127.0.0.1:1"))
            assert len(board.seen) == 1
        assert str(wt) in deny_reason(result)

    def test_allows_a_write_outside_any_repository(self, tmp_path):
        item_id = work_store.create_item("outside")
        sid = _run(item_id)
        result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                           "tool_name": "Write",
                           "tool_input": {"file_path": str(tmp_path / "notes.md")}})
        assert result.stdout.strip() == ""

    def test_allows_a_write_when_the_task_opted_out(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("opted out", worktree_opt_out=True)
        sid = _run(item_id)
        result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                           "tool_name": "Write",
                           "tool_input": {"file_path": str(repo / "new.md")}})
        assert result.stdout.strip() == ""

    def test_a_new_file_in_a_directory_that_does_not_exist_yet_is_still_gated(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("new nested file")
        sid = _run(item_id)
        result = run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                           "tool_name": "Write",
                           "tool_input": {"file_path": str(repo / "a" / "b" / "c.py")}})
        assert "shared checkout" in deny_reason(result)

    def test_a_foreign_session_is_left_alone(self, tmp_path):
        repo = make_repo(tmp_path)
        result = run_hook({"session_id": "sid-not-a-work-run",
                           "hook_event_name": "PreToolUse", "tool_name": "Edit",
                           "tool_input": {"file_path": str(repo / "README.md")}})
        assert result.stdout.strip() == ""


class TestCommitGateRepository:
    def test_denies_a_shared_checkout_commit_for_a_task_that_owns_no_worktree(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("r6 commit")
        sid = _run(item_id)
        out = work_launch.gate_commit(sid, 'git commit -m "fix: thing"', str(repo))
        assert out["decision"] == "deny"
        assert out["need_worktree"] == str(repo)
        assert "shared checkout" in out["reason"]

    def test_allows_a_commit_inside_a_worktree(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        item_id = work_store.create_item("worktree commit")
        sid = _run(item_id)
        out = work_launch.gate_commit(sid, 'git commit -m "fix: thing"', str(wt))
        assert out["decision"] == "allow"

    def test_resolves_the_repository_the_command_targets_not_the_session_cwd(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("dash C commit")
        sid = _run(item_id)
        out = work_launch.gate_commit(
            sid, f'git -C {repo} commit -m "fix: thing"', str(tmp_path))
        assert out["decision"] == "deny"
        assert out["need_worktree"] == str(repo)

    def test_composes_a_cd_with_a_dash_c(self, tmp_path):
        root = tmp_path / "outer"
        root.mkdir()
        repo = make_repo(root, "inner")
        item_id = work_store.create_item("cd then dash C")
        sid = _run(item_id)
        out = work_launch.gate_commit(
            sid, f'cd {root} && git -C inner commit -m "fix"', str(tmp_path))
        assert out["decision"] == "deny"
        assert out["need_worktree"] == str(repo)

    def test_allows_when_the_task_opted_out(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("opted out commit", worktree_opt_out=True)
        sid = _run(item_id)
        out = work_launch.gate_commit(sid, 'git commit -m "fix"', str(repo))
        assert out["decision"] == "allow"

    def test_names_the_worktree_the_task_already_owns(self, tmp_path, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(core_config, "TASK_WORKTREE_ROOT", tmp_path / "worktrees")
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = work_store.create_item("owns one")
        row = work_worktree.ensure(
            item_id, work_worktree._repo_entry(None, str(repo), "app", "proj"), "owns one")
        sid = _run(item_id)
        out = work_launch.gate_commit(sid, 'git commit -m "fix"', str(repo))
        assert out["decision"] == "deny"
        assert row["path"] in out["reason"]


class TestCommitGateNamesTheRightWorktree:
    def test_it_names_the_worktree_of_the_repository_it_denied(self, tmp_path,
                                                               monkeypatch):
        """A task can hold a worktree of more than one repository. Naming the
        newest would send the agent to commit one repository's changes into
        another repository's tree."""
        monkeypatch.setattr(core_config, "TASK_WORKTREE_ROOT", tmp_path / "worktrees")
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        first = make_repo(tmp_path, "first")
        second = make_repo(tmp_path, "second")
        item_id = work_store.create_item("two repositories")
        row_one = work_worktree.ensure(
            item_id, work_worktree._repo_entry(None, str(first), "first", "proj"),
            "two repositories")
        row_two = work_worktree.ensure(
            item_id, work_worktree._repo_entry(None, str(second), "second", "proj"),
            "two repositories")
        assert row_one["path"] != row_two["path"]
        sid = _run(item_id)
        out = work_launch.gate_commit(sid, 'git commit -m "fix"', str(first))
        assert out["decision"] == "deny"
        assert row_one["path"] in out["reason"]
        assert row_two["path"] not in out["reason"]


class TestGitCommandParser:
    def test_composes_a_cd_with_a_dash_c(self):
        assert work_launch.parse_commit(
            "cd /shared/repo && git -C src commit -m fix") == {"chdir": "/shared/repo/src"}

    def test_an_absolute_dash_c_wins_over_the_cd(self):
        assert work_launch.parse_commit(
            "cd /shared && git -C /other commit -m fix") == {"chdir": "/other"}

    def test_reports_every_commit_in_one_command_line(self):
        found = work_launch._parse_git_all(
            "cd /a && git commit -m one && cd /b && git commit -m two", "commit")
        assert found == [{"chdir": "/a"}, {"chdir": "/b"}]

    def test_reports_every_push_in_one_command_line(self):
        found = work_launch._parse_git_all(
            "git -C /a push && git -C /b push", "push")
        assert found == [{"chdir": "/a"}, {"chdir": "/b"}]

    def test_a_second_commit_is_gated_too(self, tmp_path):
        repo = make_repo(tmp_path)
        item_id = work_store.create_item("second commit")
        sid = _run(item_id)
        out = work_launch.gate_commit(
            sid, f'cd {tmp_path} && git commit -m one && git -C {repo} commit -m two',
            str(tmp_path))
        assert out["decision"] == "deny"
        assert out["need_worktree"] == str(repo)


class TestInstaller:
    def _mod(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_work_hooks_dirs", "scripts/install_work_hooks.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_an_instance_with_no_config_dir_maps_to_the_default(self, tmp_path):
        mod = self._mod()
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        ws = tmp_path / "ws"
        ws.mkdir()
        (config_dir / "default.toml").write_text(
            f'[job]\nkey = "default"\nport = 1\n\n[workspace]\nroot = "{ws}"\n')
        (config_dir / "custom.toml").write_text(
            f'[job]\nkey = "custom"\nport = 2\n\n[workspace]\nroot = "{ws}"\n\n'
            f'[llm.claude]\nconfig_dir = "{tmp_path}/custom-claude"\n')
        mod.CONFIG_DIR = config_dir
        mod.DEFAULT_DIRS = ()
        dirs = mod.claude_config_dirs()
        assert os.path.expanduser("~/.claude") in dirs
        assert str(tmp_path / "custom-claude") in dirs

    def test_an_env_override_wins_over_config_dir(self, tmp_path):
        mod = self._mod()
        config_dir = tmp_path / "config"
        config_dir.mkdir()
        ws = tmp_path / "ws"
        ws.mkdir()
        (config_dir / "one.toml").write_text(
            f'[job]\nkey = "one"\nport = 1\n\n[workspace]\nroot = "{ws}"\n\n'
            f'[llm.claude]\nconfig_dir = "{tmp_path}/from-config"\n'
            f'[llm.claude.env]\nCLAUDE_CONFIG_DIR = "{tmp_path}/from-env"\n')
        mod.CONFIG_DIR = config_dir
        mod.DEFAULT_DIRS = ()
        dirs = mod.claude_config_dirs()
        assert str(tmp_path / "from-env") in dirs
        assert str(tmp_path / "from-config") not in dirs

    def test_the_write_matcher_is_registered(self, tmp_path):
        mod = self._mod()
        settings = tmp_path / "settings.json"
        added = mod.install_into(str(settings), events=("PreToolUse",))
        assert f"PreToolUse[{mod.WRITE_GATE_MATCHER}]" in added
        data = json.loads(settings.read_text())
        entry = [e for e in data["hooks"]["PreToolUse"]
                 if e["matcher"] == mod.WRITE_GATE_MATCHER]
        assert len(entry) == 1
        assert entry[0]["hooks"][0]["timeout"] == mod.WRITE_GATE_TIMEOUT
