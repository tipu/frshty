"""The differential must reject a test that does not measure the claim.

These build a real git repo with a real branch so the reverse-patch path runs for
real. The rejection cases matter more than the acceptance case: the whole point of
this module is that it refuses to substantiate a claim its evidence does not carry.
"""
import subprocess
from pathlib import Path
from unittest.mock import patch

import features.defence as defence


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, check=True)


def _repo(tmp_path: Path, test_body: str, source_after: str = "def add(a, b):\n    return a + b\n"):
    """A repo whose branch changes src.py and adds a test for it."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "main")
    _git(repo, "config", "user.email", "t@t.com")
    _git(repo, "config", "user.name", "t")
    (repo / "src.py").write_text("def add(a, b):\n    return 0\n")
    (repo / "tests").mkdir()
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "base")
    _git(repo, "branch", "-f", "origin/main", "HEAD")

    (repo / "src.py").write_text(source_after)
    (repo / "tests" / "test_add.py").write_text(test_body)
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "feature")
    return repo


REAL_TEST = (
    "import sys, pathlib\n"
    "sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))\n"
    "from src import add\n\n"
    "def test_add():\n"
    "    assert add(2, 3) == 5\n"
)

TAUTOLOGY_TEST = "def test_add():\n    assert True\n"

REVISION_TEST = (
    "import subprocess\n\n"
    "def test_add():\n"
    "    head = subprocess.run(['git', 'rev-parse', 'HEAD'], capture_output=True, text=True).stdout\n"
    "    base = subprocess.run(['git', 'rev-parse', 'origin/main'], capture_output=True, text=True).stdout\n"
    "    assert head != base\n"
)


def _substantiate(repo: Path, proposal: dict, judge=("codex", defence.SUBSTANTIATED, "ok")):
    with patch.object(defence, "_propose_test", return_value=proposal), \
         patch.object(defence, "relink_shared_venv"), \
         patch.object(defence, "_judge_blind", return_value=judge), \
         patch.object(defence, "log"):
        return defence.substantiate({}, "add returns the sum", "repo", repo, "main")


class TestDifferential:
    def test_accepts_a_test_that_measures_the_change(self, tmp_path):
        repo = _repo(tmp_path, REAL_TEST)
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/test_add.py",
                                 "test_name": "test_add"})
        assert r.with_change_exit == 0, r.with_change_output
        assert r.without_change_exit != 0, "test must fail once the source change is reversed"
        assert r.verdict == defence.SUBSTANTIATED

    def test_rejects_a_tautology(self, tmp_path):
        """assert True passes with and without the change, so it proves nothing."""
        repo = _repo(tmp_path, TAUTOLOGY_TEST)
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/test_add.py",
                                 "test_name": "test_add"})
        assert r.with_change_exit == 0
        assert r.without_change_exit == 0
        assert r.verdict == defence.INCONCLUSIVE
        assert "does not measure" in r.reason

    def test_rejects_a_test_that_only_observes_the_revision(self, tmp_path):
        """The counterexample that killed the earlier head-versus-base rule.

        Reversing the source inside one checkout leaves HEAD untouched, so a test
        comparing revisions passes in both runs and is rejected.
        """
        repo = _repo(tmp_path, REVISION_TEST)
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/test_add.py",
                                 "test_name": "test_add"})
        assert r.with_change_exit == 0
        assert r.without_change_exit == 0, "HEAD must be identical across both runs"
        assert r.verdict == defence.INCONCLUSIVE

    def test_rejects_a_test_that_does_not_pass_on_the_branch(self, tmp_path):
        repo = _repo(tmp_path, REAL_TEST, source_after="def add(a, b):\n    return 999\n")
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/test_add.py",
                                 "test_name": "test_add"})
        assert r.with_change_exit != 0
        assert r.verdict == defence.INCONCLUSIVE
        assert "does not pass" in r.reason

    def test_judge_can_veto_a_clean_differential(self, tmp_path):
        repo = _repo(tmp_path, REAL_TEST)
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/test_add.py",
                                 "test_name": "test_add"},
                          judge=("codex", defence.INCONCLUSIVE, "test measures something else"))
        assert r.without_change_exit != 0
        assert r.verdict == defence.INCONCLUSIVE


class TestGuards:
    def test_missing_test_file_is_rejected(self, tmp_path):
        repo = _repo(tmp_path, REAL_TEST)
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/nope.py",
                                 "test_name": "test_add"})
        assert r.verdict == defence.INCONCLUSIVE
        assert "does not exist" in r.reason

    def test_model_declining_is_rejected(self, tmp_path):
        repo = _repo(tmp_path, REAL_TEST)
        r = _substantiate(repo, {"can_prove": False, "reason": "nothing covers this"})
        assert r.verdict == defence.INCONCLUSIVE
        assert r.test_id == ""

    def test_branch_with_no_source_change_is_rejected(self, tmp_path):
        repo = tmp_path / "empty"
        repo.mkdir()
        _git(repo, "init", "-q", "-b", "main")
        _git(repo, "config", "user.email", "t@t.com")
        _git(repo, "config", "user.name", "t")
        (repo / "a.txt").write_text("x\n")
        _git(repo, "add", "-A")
        _git(repo, "commit", "-qm", "base")
        _git(repo, "branch", "-f", "origin/main", "HEAD")
        r = _substantiate(repo, {"can_prove": True, "test_file": "tests/test_add.py",
                                 "test_name": "test_add"})
        assert r.verdict == defence.INCONCLUSIVE
        assert "no source changes" in r.reason


class TestSourceSelection:
    def test_test_files_are_excluded_from_the_reversed_patch(self, tmp_path):
        """Reversing the test alongside the source would delete the probe."""
        repo = _repo(tmp_path, REAL_TEST)
        diff = defence.source_diff(repo, "main")
        assert "src.py" in diff
        assert "test_add.py" not in diff

    def test_is_test_path_covers_both_ecosystems(self):
        for p in ("tests/test_x.py", "src/x_test.py", "src/__tests__/a.ts",
                  "src/a.test.tsx", "src/a.spec.ts", "test/x.py"):
            assert defence.is_test_path(p), p
        for p in ("src/x.py", "src/latest.py", "src/contest.ts"):
            assert not defence.is_test_path(p), p


class TestRunnerConstruction:
    def test_the_model_never_supplies_the_command(self, tmp_path):
        """A proposal names a test. frshty builds the command around it."""
        repo = tmp_path / "r"
        (repo / "x").mkdir(parents=True)
        cmd = defence.detect_runner(repo, "tests/test_a.py", "test_b")
        assert cmd[1:3] == ["-m", "pytest"]
        assert "tests/test_a.py::test_b" in cmd

    def test_pipenv_repo_uses_pipenv(self, tmp_path):
        repo = tmp_path / "r"
        repo.mkdir()
        (repo / "Pipfile").write_text("[packages]\n")
        assert defence.detect_runner(repo, "t/test_a.py", "test_b")[:2] == ["pipenv", "run"]

    def test_unknown_runner_returns_none(self, tmp_path):
        repo = tmp_path / "r"
        repo.mkdir()
        assert defence.detect_runner(repo, "t/test_a.rb", "test_b") is None


class TestRender:
    def test_header_is_machine_readable(self):
        r = defence.DefenceResult(claim="c", verdict=defence.SUBSTANTIATED,
                                  test_id="t.py::t", with_change_exit=0,
                                  without_change_exit=1, judge="codex",
                                  judge_verdict=defence.SUBSTANTIATED, head_sha="abc")
        out = defence.render(r)
        assert "VERDICT: SUBSTANTIATED" in out
        assert "PROOF: t.py::t with_change=0 without_change=1" in out
        assert "JUDGE: codex=SUBSTANTIATED" in out
