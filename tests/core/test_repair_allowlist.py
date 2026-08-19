"""Repair is allowed only for diagnostics known to be mechanically fixable here.

A blacklist asks "does this edit look like cheating", which cannot be answered
by pattern matching: `FileExplorerAction: TypeAlias = Any` is ordinary code that
turns a missing dependency green. An allowlist asks the answerable question
instead — "is this diagnostic one we know how to fix by editing this repo" — and
everything else blocks.

TS2307 is the case that proves it. "Cannot find module '@acme/rpa-schema'" is a
dependency problem that a privileged agent can satisfy with a fake .d.ts.
"""
import subprocess
from pathlib import Path
from unittest.mock import patch

import core.tasks.tickets as T
from core import git_util as g


def _outcome(output, status="hook_failed", phase="hook_pass_2"):
    return g.CommitOutcome(status, phase, "r", 1, output, "aaa", "aaa")


def _repo(tmp_path: Path) -> Path:
    r = tmp_path / "wt"
    r.mkdir()
    subprocess.run(["git", "init", "-q", str(r)], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.email", "t@t"], check=True)
    subprocess.run(["git", "-C", str(r), "config", "user.name", "t"], check=True)
    (r / ".pre-commit-config.yaml").write_text("repos: []\n")
    (r / "src.py").write_text("x = 1\n")
    subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
    subprocess.run(["git", "-C", str(r), "commit", "-qm", "base", "--no-verify"], check=True)
    return r


class TestOnlyKnownDiagnosticsAreRepaired:
    def test_a_recognised_lint_code_is_repairable(self):
        assert T._is_repairable("tests/x.py:53:27: E741 Ambiguous variable name: `l`")

    def test_a_recognised_formatting_failure_is_repairable(self):
        assert T._is_repairable("would reformat src/a.py\n1 file would be reformatted")

    def test_a_missing_module_is_not_repairable(self):
        assert not T._is_repairable(
            "src/client.ts(4,33): error TS2307: Cannot find module '@acme/rpa-schema'")

    def test_an_unknown_import_symbol_is_not_repairable(self):
        assert not T._is_repairable('error: "FileExplorerAction" is unknown import symbol')

    def test_an_unrecognised_diagnostic_is_not_repairable(self):
        """The default has to be no. Anything else and the next unfamiliar tool
        output reaches a privileged agent."""
        assert not T._is_repairable("BUILD FAILED: something nobody has seen before")

    def test_empty_output_is_not_repairable(self):
        assert not T._is_repairable("")


    def test_the_diagnostic_that_started_this_is_repairable(self):
        """basedpyright's wording. The first version of this allowlist excluded
        it, which would have blocked the exact DEV-635 case it was built for."""
        assert T._is_repairable(
            'test_tool_http.py:62:32 - error: Argument of type "SimpleNamespace" '
            'cannot be assigned to parameter "request" of type "Request"')

    def test_common_formatters_and_linters_are_repairable(self):
        """Inverting the default risks blocking work frshty used to fix."""
        for name, text in (
            ("prettier", "[warn] Code style issues found in the above file."),
            ("eslint", "  4:1  error  'x' is unused  no-unused-vars\n\n1 problem (1 error, 0 warnings)"),
            ("isort", "ERROR: /src/a.py Imports are incorrectly sorted and/or formatted."),
            ("black", "would reformat app.py"),
        ):
            assert T._is_repairable(text), f"{name} output should be repairable"


class TestRoutingUsesTheAllowlist:
    def test_ts2307_blocks_instead_of_reaching_the_agent(self, tmp_path):
        r = _repo(tmp_path)
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(
                r, _outcome("src/a.ts(4,33): error TS2307: Cannot find module '@acme/x'"), "DEV-1")
        assert route == "block_dependency"
        agent.assert_not_called()

    def test_an_unknown_diagnostic_blocks_instead_of_reaching_the_agent(self, tmp_path):
        r = _repo(tmp_path)
        with patch.object(T, "run_claude_code") as agent, patch.object(T, "log"):
            route = T._route_hook_failure(r, _outcome("BUILD FAILED: mystery"), "DEV-1")
        assert route == "block_unknown"
        agent.assert_not_called()

    def test_a_recognised_lint_failure_still_reaches_the_agent(self, tmp_path):
        r = _repo(tmp_path)
        with patch.object(T, "run_claude_code",
                          side_effect=lambda *a, **k: (r / "src.py").write_text("entry = 1\n") or "ok"), \
             patch.object(T, "log"):
            route = T._route_hook_failure(r, _outcome("src.py:1:1: E741 Ambiguous name"), "DEV-1")
        assert route == "repair"


class TestRejectedRepairIsFullyUndone:
    def test_a_staged_cheat_is_removed_from_the_index_too(self, tmp_path):
        """`git checkout -- .` restores the worktree FROM the index, so a staged
        cheat survives in both. The earlier test passed only by never staging."""
        r = _repo(tmp_path)
        def cheat(*a, **k):
            (r / ".pre-commit-config.yaml").write_text("repos: []  # disabled\n")
            subprocess.run(["git", "-C", str(r), "add", ".pre-commit-config.yaml"], check=True)
            return "done"
        with patch.object(T, "run_claude_code", side_effect=cheat), patch.object(T, "log"):
            route = T._route_hook_failure(r, _outcome("src.py:1:1: E741 Ambiguous name"), "DEV-1")
        assert route == "block_unknown"
        assert "# disabled" not in (r / ".pre-commit-config.yaml").read_text()
        staged = subprocess.run(["git", "-C", str(r), "diff", "--cached", "--name-only"],
                                capture_output=True, text=True).stdout.strip()
        assert staged == "", f"the cheat survived in the index: {staged}"

    def test_an_untracked_shadow_file_is_removed(self, tmp_path):
        r = _repo(tmp_path)
        def cheat(*a, **k):
            (r / "rpa_schema.py").write_text("FileExplorerAction = object\n")
            return "done"
        with patch.object(T, "run_claude_code", side_effect=cheat), patch.object(T, "log"):
            T._route_hook_failure(r, _outcome("src.py:1:1: E741 Ambiguous name"), "DEV-1")
        assert not (r / "rpa_schema.py").exists(), "a shadow module must not survive rejection"

    def test_the_staged_fix_work_survives_a_rejected_repair(self, tmp_path):
        """Only the repair is undone. The fix that was already staged stays."""
        r = _repo(tmp_path)
        (r / "src.py").write_text("fixed = 1\n")
        subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
        def cheat(*a, **k):
            (r / ".pre-commit-config.yaml").write_text("repos: []  # disabled\n")
            subprocess.run(["git", "-C", str(r), "add", "-A"], check=True)
            return "done"
        with patch.object(T, "run_claude_code", side_effect=cheat), patch.object(T, "log"):
            T._route_hook_failure(r, _outcome("src.py:1:1: E741 Ambiguous name"), "DEV-1")
        assert (r / "src.py").read_text() == "fixed = 1\n"
