import subprocess
from datetime import datetime
from unittest.mock import patch

import features.tickets as tickets


def _cfg(enabled=True):
    return {"pr": {"auto_update_branch": enabled},
            "workspace": {"base_branch": "main"}, "_base_url": "http://base"}


def _ts(**over):
    ts = {"status": "in_review", "slug": "t-1-slug", "branch": "t-1-branch",
          "summary": "Do the thing", "prs": [{"repo": "myrepo", "id": 7, "url": "http://pr/7"}]}
    ts.update(over)
    return ts


def _dev_cfg():
    return {"pr": {"auto_update_branch": True},
            "workspace": {"base_branch": "main", "base_branches": {"myrepo": "development"}},
            "_base_url": "http://base"}


def _main_pr_ts(**over):
    return _ts(prs=[{"repo": "myrepo", "id": 25, "url": "http://pr/25", "base": "main"}], **over)


class TestPrTargetBase:
    def test_missing_worktree_is_created_from_the_pr_base(self, tmp_path):
        ts = _main_pr_ts()
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=tmp_path / "wt"), \
             patch("features.tickets.git_util.add_or_reuse_worktree",
                   return_value=tmp_path / "wt") as mock_add:
            tickets._ensure_pr_worktree(_dev_cfg(), {"key": "T-1"}, ts, ts["prs"][0], "http://base")
        assert mock_add.call_args.args[3] == "main"


def _git(cwd, *args):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True)


class TestEnsurePrWorktree:
    def test_creates_a_worktree_on_the_pr_branch_when_the_ticket_has_none(self, tmp_path):
        origin = tmp_path / "origin.git"
        seed = tmp_path / "seed"
        clone = tmp_path / "clone"
        _git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
        _git(tmp_path, "clone", "-q", str(origin), str(seed))
        _git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "base")
        _git(seed, "push", "-q", "origin", "HEAD:main")
        _git(tmp_path, "clone", "-q", str(origin), str(clone))
        _git(seed, "checkout", "-q", "-b", "pr-branch")
        _git(seed, "-c", "user.email=a@b", "-c", "user.name=a", "commit", "-q", "--allow-empty", "-m", "pr")
        _git(seed, "push", "-q", "origin", "pr-branch")
        wt = tmp_path / "tickets" / "t-1-slug" / "myrepo"
        ts = _ts(prs=[{"repo": "myrepo", "id": 7, "url": "http://pr/7", "branch": "pr-branch"}])

        with patch("features.tickets._ticket_repo_path", return_value=str(clone)), \
             patch("features.tickets.ticket_worktree_path", return_value=wt):
            got = tickets._ensure_pr_worktree(_cfg(), {"key": "T-1"}, ts, ts["prs"][0], "http://base")

        assert got == wt
        head = subprocess.run(["git", "rev-parse", "--abbrev-ref", "HEAD"], cwd=str(wt),
                              capture_output=True, text=True).stdout.strip()
        assert head == "pr-branch"

    def test_a_failed_creation_is_reported(self, tmp_path):
        wt = tmp_path / "tickets" / "t-1-slug" / "myrepo"
        ts = _ts()
        with patch("features.tickets._ticket_repo_path", return_value="/repo"), \
             patch("features.tickets.ticket_worktree_path", return_value=wt), \
             patch("features.tickets.git_util.add_or_reuse_worktree", return_value=None), \
             patch("features.tickets.log.emit") as mock_emit:
            got = tickets._ensure_pr_worktree(_cfg(), {"key": "T-1"}, ts, ts["prs"][0], "http://base")
        assert got is None
        assert any(c.args[0] == "ticket_pr_worktree_failed" for c in mock_emit.call_args_list)


