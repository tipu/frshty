import json
import os
import subprocess
import sys

import core.db as db
import core.git_util as git_util
from services import work_launch, work_store


def _mkrun(objective="push gate item"):
    item_id = work_store.create_item(objective)
    sid = f"sid-gate-{item_id}"
    work_store.add_run(item_id, sid, f"work-{item_id}", "/tmp")
    return item_id, sid


def _gate_events(item_id):
    return db.query_all(
        "SELECT payload FROM work_events WHERE work_item_id = ? AND kind = 'push_gate' "
        "ORDER BY id", (item_id,))


class TestParsePush:
    def test_detects_push_forms(self):
        assert work_launch.parse_push("git push") == {"chdir": ""}
        assert work_launch.parse_push("git push origin main --force-with-lease") == {"chdir": ""}
        assert work_launch.parse_push("git -C /x push") == {"chdir": "/x"}
        assert work_launch.parse_push("cd /x && git push -u origin HEAD") == {"chdir": "/x"}
        assert work_launch.parse_push("git add -A && git commit -m x && git push") == {"chdir": ""}
        assert work_launch.parse_push("GIT_TRACE=1 git push") == {"chdir": ""}
        assert work_launch.parse_push("/usr/bin/git push") == {"chdir": ""}
        assert work_launch.parse_push("git status; git push") == {"chdir": ""}
        assert work_launch.parse_push("git status\ngit push") == {"chdir": ""}

    def test_ignores_non_push(self):
        assert work_launch.parse_push("git commit -m push") is None
        assert work_launch.parse_push('echo "git push"') is None
        assert work_launch.parse_push("grep push file.txt") is None
        assert work_launch.parse_push("git pull && git status") is None
        assert work_launch.parse_push("git log --oneline") is None
        assert work_launch.parse_push("ls -la") is None

    def test_unbalanced_quote_falls_back_to_regex(self):
        assert work_launch.parse_push("git push # don't") == {"chdir": ""}
        assert work_launch.parse_push("echo don't push") is None


class TestLintFiles:
    def _fake_pc(self, tmp_path, name, exit_code):
        pc = tmp_path / name
        pc.write_text(f"#!/bin/sh\necho checked \"$@\"\nexit {exit_code}\n")
        pc.chmod(0o755)
        return pc

    def test_no_config_passes(self, tmp_path):
        out = git_util.lint_files(tmp_path, ["a.py"])
        assert out["status"] == "no_config"

    def test_missing_binary_is_tooling_failed(self, tmp_path, monkeypatch):
        (tmp_path / ".pre-commit-config.yaml").write_text("repos: []\n")
        monkeypatch.setattr(git_util, "_find_pre_commit", lambda d: None)
        out = git_util.lint_files(tmp_path, ["a.py"])
        assert out["status"] == "tooling_failed"
        assert out["exit_code"] == 127

    def test_pass_and_fail(self, tmp_path, monkeypatch):
        (tmp_path / ".pre-commit-config.yaml").write_text("repos: []\n")
        (tmp_path / "a.py").write_text("x = 1\n")
        good = self._fake_pc(tmp_path, "pc-good", 0)
        monkeypatch.setattr(git_util, "_find_pre_commit", lambda d: good)
        out = git_util.lint_files(tmp_path, ["a.py", "missing.py"])
        assert out["status"] == "pass"
        assert "a.py" in out["output"]
        assert "missing.py" not in out["output"]
        bad = self._fake_pc(tmp_path, "pc-bad", 1)
        monkeypatch.setattr(git_util, "_find_pre_commit", lambda d: bad)
        out = git_util.lint_files(tmp_path, ["a.py"])
        assert out["status"] == "hook_failed"
        assert out["exit_code"] == 1

    def test_no_present_files_passes_without_running(self, tmp_path, monkeypatch):
        (tmp_path / ".pre-commit-config.yaml").write_text("repos: []\n")
        monkeypatch.setattr(git_util, "_find_pre_commit",
                            lambda d: tmp_path / "never-executed")
        out = git_util.lint_files(tmp_path, ["deleted.py"])
        assert out["status"] == "pass"
        assert "no files" in out["output"]


class TestOutgoingFiles:
    def _commit(self, repo, name):
        (repo / name).write_text("x\n")
        subprocess.run(["git", "add", name], cwd=repo, check=True)
        subprocess.run(["git", "-c", "user.email=t@t", "-c", "user.name=t",
                        "commit", "-q", "-m", name], cwd=repo, check=True)

    def test_no_remote_is_none(self, tmp_path):
        repo = tmp_path / "r"
        repo.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        self._commit(repo, "a.py")
        assert work_launch._outgoing_files(repo) is None

    def test_upstream_diff_lists_new_commits(self, tmp_path):
        origin = tmp_path / "origin.git"
        subprocess.run(["git", "init", "-q", "--bare", str(origin)], check=True)
        repo = tmp_path / "clone"
        repo.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        subprocess.run(["git", "remote", "add", "origin", str(origin)],
                       cwd=repo, check=True)
        self._commit(repo, "a.py")
        subprocess.run(["git", "push", "-q", "-u", "origin", "HEAD"],
                       cwd=repo, check=True)
        self._commit(repo, "b.py")
        assert work_launch._outgoing_files(repo) == ["b.py"]


class TestGatePush:
    def _resolve_to(self, monkeypatch, repo):
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: repo)
        monkeypatch.setattr(work_launch, "_outgoing_files", lambda r: ["a.py"])

    def test_not_a_push_allows_without_event(self):
        item_id, sid = _mkrun()
        out = work_launch.gate_push(sid, "ls -la", "/tmp")
        assert out["decision"] == "allow"
        assert _gate_events(item_id) == []

    def test_lint_failure_denies_before_tests(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun()
        self._resolve_to(monkeypatch, tmp_path)
        monkeypatch.setattr(work_launch.git_util, "lint_files",
                            lambda repo, files: {"status": "hook_failed",
                                                 "exit_code": 1,
                                                 "output": "E501 line too long"})
        tests_called = []
        monkeypatch.setattr(work_launch, "_gate_tests",
                            lambda repo: tests_called.append(1))
        out = work_launch.gate_push(sid, "git push", str(tmp_path))
        assert out["decision"] == "deny"
        assert "lint" in out["reason"]
        assert "E501" in out["reason"]
        assert tests_called == []
        events = _gate_events(item_id)
        assert len(events) == 1
        payload = json.loads(events[0]["payload"])
        assert payload["verdict"] == "fail"
        assert payload["lint"]["status"] == "hook_failed"

    def test_test_failure_denies(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun()
        self._resolve_to(monkeypatch, tmp_path)
        monkeypatch.setattr(work_launch.git_util, "lint_files",
                            lambda repo, files: {"status": "pass", "exit_code": 0,
                                                 "output": ""})
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda repo: (["bash", "-c", "echo 1 failed; exit 1"], {}))
        out = work_launch.gate_push(sid, "git push origin main", str(tmp_path))
        assert out["decision"] == "deny"
        assert "test suite" in out["reason"]
        assert "1 failed" in out["reason"]
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "fail"
        assert payload["tests"]["result"] == "fail"

    def test_pass_allows_and_records(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun()
        self._resolve_to(monkeypatch, tmp_path)
        monkeypatch.setattr(work_launch.git_util, "lint_files",
                            lambda repo, files: {"status": "pass", "exit_code": 0,
                                                 "output": "ok"})
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda repo: (["bash", "-c", "exit 0"], {}))
        out = work_launch.gate_push(sid, "git push", str(tmp_path))
        assert out["decision"] == "allow"
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "pass"
        assert payload["lint"]["status"] == "pass"
        assert payload["tests"]["result"] == "pass"

    def test_no_repo_allows_but_records(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun()
        monkeypatch.setattr(work_launch, "_repo_root", lambda d: None)
        out = work_launch.gate_push(sid, "git push", str(tmp_path))
        assert out["decision"] == "allow"
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "skipped"

    def test_no_local_venv_sentinel_denies(self, tmp_path, monkeypatch):
        item_id, sid = _mkrun()
        self._resolve_to(monkeypatch, tmp_path)
        monkeypatch.setattr(work_launch.git_util, "lint_files",
                            lambda repo, files: {"status": "no_config",
                                                 "exit_code": 0, "output": ""})
        monkeypatch.setattr(work_launch, "_detect_runner",
                            lambda repo: ([work_launch._NO_LOCAL_PY_VENV_SENTINEL], {}))
        out = work_launch.gate_push(sid, "git push", str(tmp_path))
        assert out["decision"] == "deny"
        assert "virtualenv" in out["reason"]
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "fail"


class TestHookGate:
    def _run_hook(self, payload):
        dbfile = str(db._DB_PATH)
        return subprocess.run(
            [sys.executable, "scripts/work_hook.py"],
            input=json.dumps(payload), capture_output=True, text=True, timeout=120,
            env={**os.environ, "FRSHTY_DB": dbfile},
        )

    def test_hook_denies_push_when_gate_fails(self, tmp_path):
        item_id, sid = _mkrun("hook gate deny")
        repo = tmp_path / "pyrepo"
        repo.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        (repo / "pyproject.toml").write_text("[project]\nname = 'x'\nversion = '0'\n")
        r = self._run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": str(repo),
                            "tool_input": {"command": "git push origin main"}})
        assert r.returncode == 0, r.stderr
        out = json.loads(r.stdout)
        assert out["hookSpecificOutput"]["permissionDecision"] == "deny"
        assert "code gate" in out["hookSpecificOutput"]["permissionDecisionReason"]
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "fail"

    def test_hook_allows_push_when_gate_passes(self, tmp_path):
        item_id, sid = _mkrun("hook gate allow")
        repo = tmp_path / "emptyrepo"
        repo.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        r = self._run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": str(repo),
                            "tool_input": {"command": "git push"}})
        assert r.returncode == 0, r.stderr
        assert r.stdout.strip() == ""
        payload = json.loads(_gate_events(item_id)[0]["payload"])
        assert payload["verdict"] == "pass"
        assert payload["tests"]["result"] == "no_runner"

    def test_hook_skips_non_push_bash(self):
        item_id, sid = _mkrun("hook gate non push")
        r = self._run_hook({"session_id": sid, "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": "/tmp",
                            "tool_input": {"command": "ls -la"}})
        assert r.returncode == 0
        assert r.stdout.strip() == ""
        assert _gate_events(item_id) == []

    def test_hook_foreign_session_push_untouched(self):
        r = self._run_hook({"session_id": "sid-not-a-work-run",
                            "hook_event_name": "PreToolUse",
                            "tool_name": "Bash", "cwd": "/tmp",
                            "tool_input": {"command": "git push"}})
        assert r.returncode == 0
        assert r.stdout.strip() == ""


class TestInstallerUpgrade:
    def _mod(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "install_work_hooks_upgrade", "scripts/install_work_hooks.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        return mod

    def test_adds_bash_matcher_to_existing_install(self, tmp_path):
        mod = self._mod()
        command = mod.hook_command()
        settings = tmp_path / "settings.json"
        settings.write_text(json.dumps({"hooks": {"PreToolUse": [
            {"matcher": "AskUserQuestion",
             "hooks": [{"type": "command", "command": command, "timeout": 10}]}]}}))
        added = mod.install_into(str(settings), events=("PreToolUse",))
        assert added == ["PreToolUse[Bash]"]
        data = json.loads(settings.read_text())
        bash = [e for e in data["hooks"]["PreToolUse"] if e["matcher"] == "Bash"]
        assert len(bash) == 1
        assert bash[0]["hooks"][0]["timeout"] == mod.PUSH_GATE_TIMEOUT
        assert mod.install_into(str(settings), events=("PreToolUse",)) == []
