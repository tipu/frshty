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
            ("black check", "would reformat app.py"),
            ("black modifying", "reformatted app.py\nAll done!\n1 file reformatted."),
            # The per-file line and the summary line are separate patterns, so
            # each needs a case that only it matches.
            ("black per-file only", "reformatted app.py"),
            ("black summary only", "All done!\n3 files reformatted."),
            ("mypy arg-type", 'error: Argument 1 to "f" has incompatible type '
                              '"int"; expected "str"  [arg-type]'),
        ):
            assert T._is_repairable(text), f"{name} output should be repairable"

    def test_coloured_output_is_classified_the_same_as_plain(self):
        """Tools decide to colour from the environment, not from whether anyone
        is reading. ruff writes `\x1b[1m\x1b[91mE501 `, and the escape sits
        against the code, so `\\bE501\\b` finds no word boundary. Every case
        above used clean text, so only an end-to-end run surfaced this: a real
        ruff failure was classified unrecognised and blocked the ticket."""
        coloured = ("\x1b[1m\x1b[91mE501 \x1b[0m\x1b[1mLine too long (33 > 20)\x1b[0m\n"
                    " \x1b[1m\x1b[94m-->\x1b[0m app.py:2:21\n")
        assert T._is_repairable(coloured)
        assert not T._is_repairable(
            "\x1b[1m\x1b[91mF821 \x1b[0m\x1b[1mUndefined name `X`\x1b[0m")

    def test_coloured_output_is_triaged_the_same_as_plain(self):
        """The triage markers are phrases, so an escape between their words
        splits them. A hook that emphasises the tool name turns "pre-commit not
        found" into environment output the classifier cannot see."""
        assert g.triage_commit_failure(
            "hook_failed", "\x1b[91mpre-commit\x1b[0m not found") == "environment"
        assert g.triage_commit_failure(
            "hook_failed",
            "src/a.ts(4,33): \x1b[1mCannot find\x1b[0m module "
            "'@acme/rpa-schema'") == "dependency"

    def test_a_missing_name_is_not_repairable(self):
        """The only edit that satisfies "undefined name X" is inventing X, which
        is the cheat this allowlist exists to stop. The blanket ruff-code pattern
        admits F821 unless it is excluded by name."""
        for name, text in (
            ("ruff F821", "app.py:3:5: F821 Undefined name `FileExplorerAction`"),
            ("ruff F822", "app.py:1:1: F822 Undefined name `X` in `__all__`"),
            # F823 never says "undefined name", so only the code excludes it.
            ("ruff F823", "app.py:9:9: F823 Local variable `x` defined in enclosing "
                          "scope on line 4 referenced before assignment"),
            ("mypy name-defined", 'error: Name "FileExplorerAction" is not defined  [name-defined]'),
            ("mypy attr-defined", 'error: Module "pkg" has no attribute "X"  [attr-defined]'),
            ("eslint no-undef", "  1:1  error  'Foo' is not defined  no-undef"),
            ("tsc TS2304", "src/a.ts(1,1): error TS2304: Cannot find name 'Foo'."),
        ):
            assert not T._is_repairable(text), f"{name} output must not be repairable"

    def test_a_basedpyright_rule_name_is_repairable(self):
        """DEV-678 is the case that proves it. The allowlist carried two
        basedpyright wordings, and the message text differs per rule, so an
        ordinary type error in a test the agent had just written was classified
        unrecognised. That blocked with hard_block, so nothing retried it."""
        assert T._is_repairable(
            "  src/users/test_user_profile_view.py:19:19 - error: "
            '"__getitem__" method not defined on type "object" (reportIndexIssue)\n'
            "1 error, 0 warnings, 0 notes")

    def test_the_repairable_pyright_rules_mirror_the_mypy_codes(self):
        for name, rule in (
            ("arg-type", "reportArgumentType"),
            ("assignment", "reportAssignmentType"),
            ("return-value", "reportReturnType"),
            ("call-arg", "reportCallIssue"),
            ("index", "reportIndexIssue"),
            ("operator", "reportOperatorIssue"),
            ("override", "reportIncompatibleMethodOverride"),
            ("redundant-cast", "reportUnnecessaryCast"),
            ("unused-ignore", "reportUnnecessaryTypeIgnoreComment"),
            ("possibly-none", "reportOptionalMemberAccess"),
        ):
            text = f"a.py:1:1 - error: something is wrong ({rule})"
            assert T._is_repairable(text), f"{name} ({rule}) should be repairable"

    def test_a_basedpyright_name_or_import_rule_is_not_repairable(self):
        """The rule name carries the decision, not the wording. DEV-635 blocked
        only because no never pattern matched `Attribute "method" is unknown`
        and nothing else matched either, so it fell through to unrecognised."""
        for name, text in (
            ("attribute", '  src/main.py:369:14 - error: Cannot access attribute '
                          '"method" for class "Request"\n    Attribute "method" '
                          'is unknown (reportAttributeAccessIssue)'),
            ("import", 'a.py:1:8 - error: Import "acme" could not be '
                       "resolved (reportMissingImports)"),
            ("stub", 'a.py:1:8 - error: Stub file not found (reportMissingTypeStubs)'),
            ("unbound", "a.py:4:5 - error: X is possibly unbound (reportPossiblyUnbound)"),
            ("redeclaration", 'a.py:9:5 - error: Declaration "f" is obscured '
                              "(reportRedeclaration)"),
        ):
            assert not T._is_repairable(text), f"{name} output must not be repairable"

    def test_a_never_rule_wins_over_a_repairable_one_in_the_same_run(self):
        """basedpyright prints every diagnostic it found. A run that reports a
        missing import and an index error must block: repairing the half it can
        fix still leaves the agent facing the dependency it cannot."""
        assert not T._is_repairable(
            'a.py:1:8 - error: Import "acme" could not be resolved (reportMissingImports)\n'
            'b.py:2:2 - error: "__getitem__" method not defined (reportIndexIssue)\n'
            "2 errors, 0 warnings, 0 notes")


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
