"""mark_ready self-heals the worktree before the clean-worktree gate:
it strips ephemeral tool scratch (playwright artifacts, test output) and
commits real leftover work, so only genuinely uncommitted source blocks.
"""
import subprocess
from unittest.mock import patch

from core.tasks.tickets import (
    mark_ready, _clean_workspace_scratch, _dirty_workspace_repos, _NOISE_ONLY,
)
from core.tasks.registry import TaskContext


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=cwd, capture_output=True, check=True)


def _make_repo(root, name):
    repo = root / "workspace" / name
    repo.mkdir(parents=True)
    _git(repo, "init", "-q")
    _git(repo, "config", "user.email", "t@t.com")
    _git(repo, "config", "user.name", "t")
    (repo / "src.py").write_text("x = 1\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-qm", "init")
    return repo


def _ctx(tmp_path, slug="PROJ-1-x"):
    return TaskContext(
        instance_key="aimyable", ticket_key="PROJ-1", task="mark_ready",
        payload={}, job_id=0, triggering_event_id=None,
        config={"workspace": {"root": tmp_path, "tickets_dir": "tickets"},
                "_base_url": "http://localhost:8000"},
        registry=None, now=None,
    )


class TestNoiseSet:
    def test_lockfiles_are_noise(self):
        for lf in ("Pipfile.lock", "pnpm-lock.yaml", "yarn.lock",
                   "package-lock.json", "uv.lock", "poetry.lock"):
            assert lf in _NOISE_ONLY


class TestCleanScratch:
    def test_removes_playwright_and_test_results(self, tmp_path):
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = _make_repo(ticket_dir, "saas-dashboard")
        (repo / ".playwright-cli").mkdir()
        (repo / ".playwright-cli" / "page.yml").write_text("snap\n")
        (repo / "test-results").mkdir()
        (repo / "test-results" / "out.txt").write_text("x\n")

        removed = _clean_workspace_scratch(ticket_dir)

        assert not (repo / ".playwright-cli").exists()
        assert not (repo / "test-results").exists()
        assert removed
        assert _dirty_workspace_repos(ticket_dir) == []

    def test_leaves_real_untracked_source(self, tmp_path):
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = _make_repo(ticket_dir, "saas-dashboard")
        (repo / "new_feature.py").write_text("y = 2\n")
        (repo / ".playwright-cli").mkdir()
        (repo / ".playwright-cli" / "p.yml").write_text("s\n")

        _clean_workspace_scratch(ticket_dir)

        assert (repo / "new_feature.py").exists()
        assert not (repo / ".playwright-cli").exists()


class TestMarkReadySelfHeal:
    def test_scratch_only_worktree_passes(self, tmp_path):
        """A worktree dirtied only by playwright scratch must NOT block —
        mark_ready cleans it and reaches the success path."""
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = _make_repo(ticket_dir, "saas-dashboard")
        (repo / ".playwright-cli").mkdir()
        (repo / ".playwright-cli" / "p.yml").write_text("s\n")
        ctx = _ctx(tmp_path)
        with patch("core.state.load_ticket", return_value={"slug": "PROJ-1-x"}), \
             patch("core.tasks.tickets._fire_ticket_dev_complete") as fire:
            result = mark_ready(ctx)
        assert result.status == "ok", result.reason
        fire.assert_called_once()

    def test_real_leftover_is_committed_not_blocked(self, tmp_path):
        """Real uncommitted source left by a prior stage is committed, not
        blocked."""
        ticket_dir = tmp_path / "tickets" / "PROJ-1-x"
        repo = _make_repo(ticket_dir, "saas-dashboard")
        (repo / "leftover.py").write_text("z = 3\n")
        ctx = _ctx(tmp_path)
        with patch("core.state.load_ticket", return_value={"slug": "PROJ-1-x"}), \
             patch("core.tasks.tickets._fire_ticket_dev_complete"), \
             patch("core.git_util.commit_with_hooks",
                   side_effect=lambda repo_dir, message, **kw: _git(repo_dir, "commit", "-qm", message)):
            result = mark_ready(ctx)
        assert result.status == "ok", result.reason
        assert _dirty_workspace_repos(ticket_dir) == []
        log = subprocess.run(["git", "log", "--oneline"], cwd=repo,
                             capture_output=True, text=True).stdout
        assert "finalize" in log
