"""commit_with_hooks must never bypass verification silently.

The docstring claimed it withholds --no-verify so real lint failures surface. It
did not: `args.append("--no-verify")` sat outside the config check, so any repo
where the pre-commit binary could not be located committed unverified, and a repo
with no pre-commit config had its native git hooks suppressed too. windows-rpa-client
committed that way during DEV-635 and was reported as clean.

It also could not find a binary it had itself just chosen: _find_pre_commit falls
back to ~/.local/bin and PATH, while _hook_env only put the repo's .venv/bin on
PATH, so the git-driven hook re-ran `command -v pre-commit` and failed. That is
the "pre-commit not found" seen on DEV-635, and it needs no classifier.
"""
import os
import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

from core import git_util


def _repo(tmp_path: Path, *, config: bool) -> Path:
    r = tmp_path / "r"
    r.mkdir()
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
    if config:
        (r / ".pre-commit-config.yaml").write_text("repos: []\n")
    (r / "a.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
    return r


class TestNoSilentBypass:
    def test_a_repo_with_config_but_no_binary_is_a_tooling_failure(self, tmp_path):
        """Committing unverified is worse than not committing: the ticket moves on
        as though the hooks had passed."""
        r = _repo(tmp_path, config=True)
        with patch.object(git_util, "_find_pre_commit", return_value=None):
            got = git_util.commit_with_hooks(r, message="m")
        assert got.returncode != 0
        assert "pre-commit" in ((got.stderr or "") + (got.stdout or "")).lower()
        log = subprocess.run(["git", "-C", str(r), "log", "--oneline"],
                             capture_output=True, text=True).stdout
        assert log.strip() == "", "nothing may be committed when the hooks cannot run"

    def test_no_config_still_commits_without_suppressing_native_hooks(self, tmp_path):
        r = _repo(tmp_path, config=False)
        with patch.object(git_util, "_find_pre_commit", return_value=None):
            got = git_util.commit_with_hooks(r, message="m")
        assert got.returncode == 0
        assert "--no-verify" not in " ".join(got.args)

    def test_no_verify_is_never_passed(self, tmp_path):
        """The one flag that turns this helper into a rubber stamp.

        Checks executable code only. The module docstring names the flag to
        explain why it is gone, and that prose must not fail the test."""
        import ast
        tree = ast.parse(Path(git_util.__file__).read_text())
        literals = [n.value for n in ast.walk(tree)
                    if isinstance(n, ast.Constant) and isinstance(n.value, str)]
        docstrings = set()
        for node in ast.walk(tree):
            if isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                d = ast.get_docstring(node, clean=False)
                if d:
                    docstrings.add(d)
        code_strings = [v for v in literals if v not in docstrings]
        assert not any("--no-verify" in v for v in code_strings), (
            "--no-verify appears in executable code; the helper must never bypass hooks")


class TestHookEnvFindsTheChosenBinary:
    def test_path_carries_the_binary_that_was_selected(self, tmp_path):
        r = _repo(tmp_path, config=True)
        elsewhere = tmp_path / "elsewhere" / "bin"
        elsewhere.mkdir(parents=True)
        pc = elsewhere / "pre-commit"
        pc.write_text("#!/bin/sh\nexit 0\n")
        pc.chmod(0o755)
        env = git_util._hook_env(r, pc)
        assert str(elsewhere) in env["PATH"].split(os.pathsep), (
            "the git-driven hook re-resolves pre-commit from PATH, so the binary "
            "we chose has to be on it")

    def test_repo_venv_still_wins_when_it_exists(self, tmp_path):
        r = _repo(tmp_path, config=True)
        venv_bin = r / ".venv" / "bin"
        venv_bin.mkdir(parents=True)
        env = git_util._hook_env(r, venv_bin / "pre-commit")
        assert env["PATH"].split(os.pathsep)[0] == str(venv_bin)
