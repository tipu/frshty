import subprocess
from unittest.mock import patch

import pytest

import core.consensus_scope as consensus_scope


def _git(cwd, *args):
    subprocess.run(["git", "-C", str(cwd), *args], check=True,
                   capture_output=True)


def _make_worktree(path):
    """A repo whose origin/main ref points at the initial commit and whose
    HEAD carries one branch commit on top."""
    path.mkdir(parents=True)
    _git(path, "init", "-q", "-b", "main")
    _git(path, "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "--allow-empty", "-q", "-m", "base")
    base_sha = subprocess.run(
        ["git", "-C", str(path), "rev-parse", "HEAD"],
        capture_output=True, text=True, check=True).stdout.strip()
    _git(path, "update-ref", "refs/remotes/origin/main", base_sha)
    (path / "feature.txt").write_text("change\n")
    _git(path, "add", "feature.txt")
    _git(path, "-c", "user.email=t@t", "-c", "user.name=t",
         "commit", "-q", "-m", "feature")
    return path


@pytest.fixture()
def scope_config(tmp_path, fake_config):
    fake_config["workspace"]["root"] = tmp_path
    fake_config["features"]["scope_review"] = True
    return fake_config


class TestScopeFingerprint:
    def test_empty_without_slug(self, scope_config):
        assert consensus_scope.scope_fingerprint(scope_config, {}) == ""

    def test_empty_without_worktree(self, scope_config):
        ts = {"slug": "PROJ-1-x"}
        with patch("core.consensus_scope.get_repos",
                   return_value=[{"name": "r", "path": scope_config["workspace"]["root"] / "r"}]):
            assert consensus_scope.scope_fingerprint(scope_config, ts) == ""

    def test_stable_and_changes_with_new_commit(self, tmp_path, scope_config):
        wt = _make_worktree(tmp_path / "tickets" / "PROJ-1-x" / "r")
        ts = {"slug": "PROJ-1-x"}
        with patch("core.consensus_scope.get_repos", return_value=[{"name": "r", "path": wt}]), \
             patch("core.consensus_scope.ticket_worktree_path", return_value=wt):
            first = consensus_scope.scope_fingerprint(scope_config, ts)
            second = consensus_scope.scope_fingerprint(scope_config, ts)
            assert first and first == second
            (wt / "extra.txt").write_text("more\n")
            _git(wt, "add", "extra.txt")
            _git(wt, "-c", "user.email=t@t", "-c", "user.name=t",
                 "commit", "-q", "-m", "more")
            third = consensus_scope.scope_fingerprint(scope_config, ts)
            assert third != first


def _fanout_result(**texts):
    out = {}
    for name in ("claude", "codex", "agy"):
        text = texts.get(name)
        if text is None:
            out[name] = {"text": None, "valid": False, "reason": "unavailable"}
        else:
            out[name] = {"text": text, "valid": True, "reason": "ok"}
    return out


class TestRunScopeReview:
    def _run(self, tmp_path, scope_config, fanout):
        slug = "PROJ-1-x"
        wt = _make_worktree(tmp_path / "tickets" / slug / "r")
        ticket_dir = tmp_path / "tickets" / slug
        with patch("core.consensus_scope.get_repos", return_value=[{"name": "r", "path": wt}]), \
             patch("core.consensus_scope.ticket_worktree_path", return_value=wt), \
             patch("core.consensus_scope._fan_out", return_value=fanout), \
             patch("core.consensus_scope.log"):
            return consensus_scope.run_scope_review(
                scope_config, ticket_dir, slug, ticket_key="PROJ-1"), ticket_dir

    def test_all_pass(self, tmp_path, scope_config):
        (result, ticket_dir) = self._run(tmp_path, scope_config, _fanout_result(
            claude="review\nSCOPE VERDICT: PASS",
            codex="review\nSCOPE VERDICT: PASS",
            agy="review\nSCOPE VERDICT: PASS"))
        verdict, reason = result
        assert verdict == "pass"
        report = (ticket_dir / "docs" / "scope-review.md").read_text()
        assert report.rstrip().endswith("SCOPE VERDICT: PASS")

    def test_minority_fail_passes(self, tmp_path, scope_config):
        (result, _) = self._run(tmp_path, scope_config, _fanout_result(
            claude="SCOPE VERDICT: PASS",
            codex="SCOPE VERDICT: FAIL",
            agy="SCOPE VERDICT: PASS"))
        assert result[0] == "pass"

    def test_majority_fail_fails(self, tmp_path, scope_config):
        (result, ticket_dir) = self._run(tmp_path, scope_config, _fanout_result(
            claude="SCOPE VERDICT: FAIL",
            codex="SCOPE VERDICT: FAIL",
            agy="SCOPE VERDICT: PASS"))
        assert result[0] == "fail"
        report = (ticket_dir / "docs" / "scope-review.md").read_text()
        assert report.rstrip().endswith("SCOPE VERDICT: FAIL")

    def test_tie_fails(self, tmp_path, scope_config):
        (result, _) = self._run(tmp_path, scope_config, _fanout_result(
            claude="SCOPE VERDICT: FAIL",
            codex="SCOPE VERDICT: PASS"))
        assert result[0] == "fail"

    def test_voice_without_verdict_dropped(self, tmp_path, scope_config):
        (result, _) = self._run(tmp_path, scope_config, _fanout_result(
            claude="no verdict here",
            codex="SCOPE VERDICT: PASS",
            agy="SCOPE VERDICT: PASS"))
        verdict, reason = result
        assert verdict == "pass"
        assert "claude" in reason

    def test_no_verdicts_returns_none(self, tmp_path, scope_config):
        (result, _) = self._run(tmp_path, scope_config, _fanout_result())
        assert result[0] is None


class TestReposWithBranchDiff:
    """The shared repo enumeration that both the scope review and the
    tri-review prompt build their repo list from."""

    def test_empty_without_slug(self, scope_config):
        assert consensus_scope.repos_with_branch_diff(scope_config, "") == []

    def test_skips_a_missing_worktree(self, tmp_path, scope_config):
        missing = tmp_path / "tickets" / "PROJ-1-x" / "gone"
        with patch("core.consensus_scope.get_repos", return_value=[{"name": "gone"}]), \
             patch("core.consensus_scope.ticket_worktree_path", return_value=missing):
            assert consensus_scope.repos_with_branch_diff(scope_config, "PROJ-1-x") == []

    def test_skips_a_worktree_with_no_branch_diff(self, tmp_path, scope_config):
        wt = tmp_path / "tickets" / "PROJ-1-x" / "clean"
        wt.mkdir(parents=True)
        _git(wt, "init", "-q", "-b", "main")
        _git(wt, "-c", "user.email=t@t", "-c", "user.name=t",
             "commit", "--allow-empty", "-q", "-m", "base")
        sha = subprocess.run(["git", "-C", str(wt), "rev-parse", "HEAD"],
                             capture_output=True, text=True, check=True).stdout.strip()
        _git(wt, "update-ref", "refs/remotes/origin/main", sha)
        with patch("core.consensus_scope.get_repos", return_value=[{"name": "clean"}]), \
             patch("core.consensus_scope.ticket_worktree_path", return_value=wt):
            assert consensus_scope.repos_with_branch_diff(scope_config, "PROJ-1-x") == []

    def test_returns_every_repo_that_carries_a_branch_diff(self, tmp_path, scope_config):
        slug = "PROJ-1-x"
        a = _make_worktree(tmp_path / "tickets" / slug / "app")
        b = _make_worktree(tmp_path / "tickets" / slug / "schema")
        with patch("core.consensus_scope.get_repos",
                   return_value=[{"name": "app"}, {"name": "schema"}]), \
             patch("core.consensus_scope.ticket_worktree_path",
                   side_effect=lambda c, s, name: tmp_path / "tickets" / s / name):
            found = consensus_scope.repos_with_branch_diff(scope_config, slug)
        assert found == [("app", a, "main"), ("schema", b, "main")]


class TestScopeDirective:
    """The directive must be able to report a dead-code finding. Running the
    prior wording against windows-rpa-client-schema at e374679 returned
    SCOPE VERDICT: PASS over an enum with no caller, because the prompt told
    every voice that code quality must not affect the verdict."""

    def test_asks_for_a_caller_or_a_reader(self):
        d = consensus_scope.SCOPE_DIRECTIVE
        assert "Reachability" in d
        assert "name a caller or a reader" in d
        assert "Answer three questions" in d

    def test_does_not_exempt_reachability_from_the_verdict(self):
        d = consensus_scope.SCOPE_DIRECTIVE
        assert "Code quality, style, and correctness are reviewed elsewhere" not in d
        assert "an addition with no caller and no reader must fail this review" in d


class TestReportSummary:
    """report_summary feeds the Submit PR modal. The operator decides whether
    to strip the branch or override the gate from what it returns, so it must
    carry the offending changes the failing voices named."""

    def _report(self, tmp_path, text):
        report = tmp_path / "scope-review.md"
        report.write_text(text)
        return report

    def test_reads_votes_dropped_and_the_offending_changes(self, tmp_path):
        report = self._report(tmp_path, "\n".join([
            "# Consensus scope review", "",
            "Votes: agy=FAIL, codex=FAIL",
            "Dropped voices: claude (no SCOPE VERDICT line)", "",
            "## agy", "", "### Offending Changes", "",
            "- `dash`: pacing fix at [src/a.ts:28](file:///x/src/a.ts#L28) is unrelated",
            "- `api`: duplicate route at src/urls.py:36", "",
            "SCOPE VERDICT: FAIL", "",
            "## codex", "", "Offending changes:", "",
            "- `api`: duplicate route at src/urls.py:36", "",
            "SCOPE VERDICT: FAIL", "", "SCOPE VERDICT: FAIL", ""]))
        out = consensus_scope.report_summary(report)
        assert out["votes"] == "agy=FAIL, codex=FAIL"
        assert out["dropped"] == "claude (no SCOPE VERDICT line)"
        assert out["findings"] == [
            "`dash`: pacing fix at src/a.ts:28 is unrelated",
            "`api`: duplicate route at src/urls.py:36",
        ]

    def test_a_pass_report_has_no_findings(self, tmp_path):
        report = self._report(tmp_path, "\n".join([
            "Votes: agy=PASS, codex=PASS", "", "## agy", "",
            "- every change serves the ticket", "",
            "SCOPE VERDICT: PASS", ""]))
        out = consensus_scope.report_summary(report)
        assert out["findings"] == []
        assert out["votes"] == "agy=PASS, codex=PASS"

    def test_missing_report_is_empty(self, tmp_path):
        out = consensus_scope.report_summary(tmp_path / "nope.md")
        assert out == {"votes": "", "dropped": "", "findings": []}

    def test_only_the_first_bytes_are_read(self, tmp_path):
        """A web request reads this file, and it holds three reviewer
        transcripts. A report past the cap must not be pulled into memory
        whole."""
        report = self._report(tmp_path, "\n".join(
            ["Votes: agy=FAIL", "", "## agy", "", "x" * consensus_scope.MAX_REPORT_BYTES,
             "", "- `repo`: out of scope", "", "SCOPE VERDICT: FAIL", ""]))
        assert report.stat().st_size > consensus_scope.MAX_REPORT_BYTES
        out = consensus_scope.report_summary(report)
        assert out["votes"] == "agy=FAIL"
        assert out["findings"] == []

    def test_a_long_space_run_does_not_stall_the_parser(self, tmp_path):
        """A bullet marker followed by a long run of spaces made the old
        pattern backtrack quadratically. A web request runs this parser, so a
        report like that must not hold the worker."""
        import time
        report = self._report(tmp_path, "\n".join(
            ["## agy", "", "- " + " " * 200_000, "", "SCOPE VERDICT: FAIL", ""]))
        start = time.monotonic()
        out = consensus_scope.report_summary(report)
        assert time.monotonic() - start < 5
        assert out["findings"] == []

    def test_a_long_bracket_run_does_not_stall_the_parser(self, tmp_path):
        """The link pattern rescans to the end of its input for every unmatched
        "[". A bullet built out of them must not hold the worker."""
        import time
        report = self._report(tmp_path, "\n".join(
            ["## agy", "", "- " + "[" * 200_000, "", "SCOPE VERDICT: FAIL", ""]))
        start = time.monotonic()
        out = consensus_scope.report_summary(report)
        assert time.monotonic() - start < 5
        assert out["findings"] == ["[" * consensus_scope.MAX_FINDING_CHARS]

    def test_findings_are_capped(self, tmp_path):
        bullets = [f"- finding {i}" for i in range(30)]
        report = self._report(tmp_path, "\n".join(
            ["## agy", ""] + bullets + ["", "SCOPE VERDICT: FAIL", ""]))
        out = consensus_scope.report_summary(report)
        assert len(out["findings"]) == consensus_scope.MAX_REPORT_FINDINGS
        assert out["findings"][0] == "finding 0"
