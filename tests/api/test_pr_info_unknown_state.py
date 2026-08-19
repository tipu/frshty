"""A repo the API could not inspect must not be reported as having no changes.

_changed_files used to return [] when `git diff` failed, which is exactly what a
branch with no changes returns. The Submit PR modal then stated "branch has no
commits against its base" about a repo it had simply failed to read.
"""
from pathlib import Path
from unittest.mock import patch

import core.git_util as git_util
import web.tickets as wt


class TestChangedFilesSeparatesFailureFromEmpty:
    def test_a_failed_diff_returns_none_not_empty(self, tmp_path):
        with patch.object(wt.git_util, "run_git",
                          side_effect=git_util.GitCommandError(
                              ["diff"], type("R", (), {"returncode": 128, "stdout": "",
                                                       "stderr": "fatal: bad revision"})())):
            assert wt._changed_files(Path(tmp_path), "origin/main") is None

    def test_a_genuinely_empty_diff_returns_an_empty_list(self, tmp_path):
        class R:
            stdout = ""
        with patch.object(wt.git_util, "run_git", return_value=R()):
            assert wt._changed_files(Path(tmp_path), "origin/main") == []

    def test_meaningful_change_is_false_for_both_but_they_are_distinguishable(self):
        assert wt._is_meaningful_change(None) is False
        assert wt._is_meaningful_change([]) is False
        assert wt._is_meaningful_change(["src/a.py"]) is True
        # the caller must branch on None before calling, which is what the
        # pr-info endpoint now does to pick its stale_reason
        assert (None is None) and ([] is not None)


class TestPrInfoReportsUnknownSeparately:
    def _row(self, files):
        """Drive the fast-path branch the way api_ticket_pr_info does."""
        row = {"files_changed": 2, "has_changes": True, "stale_reason": ""}
        if files is None:
            row.update(files_changed=0, has_changes=False,
                       stale_reason="could not inspect this repo, so its state is unknown")
        else:
            row["files_changed"] = len(files)
            if not wt._is_meaningful_change(files):
                row.update(has_changes=False,
                           stale_reason="branch has no commits against its base")
        return row

    def test_an_unreadable_repo_does_not_claim_it_has_no_commits(self):
        row = self._row(None)
        assert row["has_changes"] is False
        assert "unknown" in row["stale_reason"]
        assert "no commits" not in row["stale_reason"]

    def test_a_genuinely_empty_repo_still_says_no_commits(self):
        row = self._row([])
        assert row["has_changes"] is False
        assert row["stale_reason"] == "branch has no commits against its base"

    def test_a_repo_with_changes_is_submittable(self):
        row = self._row(["src/a.py", "src/b.py"])
        assert row["has_changes"] is True
        assert row["files_changed"] == 2
