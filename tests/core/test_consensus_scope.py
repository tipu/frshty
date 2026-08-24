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
