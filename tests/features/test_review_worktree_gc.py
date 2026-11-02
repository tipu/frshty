import json
import os
import subprocess
import time
from pathlib import Path

import core.git_util as git_util
from features import review_worktree_gc

OLD = time.time() - review_worktree_gc.IDLE_S - 3600


def _later() -> float:
    return time.time() + review_worktree_gc.IDLE_S + 3600


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True,
                          check=True).stdout


class _Platform:
    def __init__(self, states: dict):
        self.states = states
        self.asked = []

    def get_pr_state(self, repo, pr_id):
        self.asked.append((repo, pr_id))
        state = self.states[pr_id]
        if isinstance(state, Exception):
            raise state
        return state


def _project(tmp_path, monkeypatch, states):
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    root = tmp_path / "ws"
    repo = root / "app"
    _git(tmp_path, "clone", str(origin), str(repo))
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / "a.txt").write_text("a\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "init")
    _git(repo, "push", "origin", "main")
    for b in ("merged", "closed", "open", "dirty", "nofile", "declined"):
        _git(repo, "push", "origin", f"main:{b}")
    _git(repo, "fetch", "origin")
    state = tmp_path / "state"
    config = {"job": {"key": "test", "platform": "github"},
              "workspace": {"root": root, "repos": ["app"]}, "_state_dir": state}
    platform = _Platform(states)
    monkeypatch.setattr(review_worktree_gc, "make_platform", lambda config: platform)
    return repo, state, config, platform


def _age(path, age=OLD):
    git_dir, _ = git_util.git_dirs(path)
    for name in ("index", "HEAD", os.path.join("logs", "HEAD")):
        p = os.path.join(git_dir, name)
        if os.path.exists(p):
            os.utime(p, (age, age))


def _review(repo, state, branch, pr_id, queued=False, age=OLD):
    review_dir = state / "reviews" / "app" / branch
    path = review_dir / "worktree"
    assert git_util.add_detached_worktree(repo, path, branch) == path
    _age(path, age)
    if pr_id is not None:
        if queued:
            (review_dir / "queued_comments.json").write_text(json.dumps([{"pr_id": pr_id}]))
        else:
            (review_dir / "review.json").write_text(json.dumps({"pr_id": pr_id}))
            (review_dir / "queued_comments.json").write_text("[]")
    (review_dir / "diff.txt").write_text("diff")
    return path


def _listed(repo, path):
    return any(lines[0] == f"worktree {path}" for lines in git_util.worktree_entries(repo) or [])


def test_removes_the_worktree_of_a_closed_pr_and_keeps_its_review_files(tmp_path, monkeypatch):
    repo, state, config, platform = _project(
        tmp_path, monkeypatch, {1: "MERGED", 2: "CLOSED", 3: "OPEN", 4: "MERGED", 6: "DECLINED"})
    merged = _review(repo, state, "merged", 1)
    closed = _review(repo, state, "closed", 2, queued=True)
    still_open = _review(repo, state, "open", 3)
    dirty = _review(repo, state, "dirty", 4)
    (dirty / "a.txt").write_text("changed\n")
    nofile = _review(repo, state, "nofile", None)
    declined = _review(repo, state, "declined", 6)

    result = review_worktree_gc.sweep(config, now=_later(), force=True)

    assert sorted(result["removed"]) == sorted([str(merged), str(closed), str(declined)])
    assert result["kept"] == {str(dirty): "local changes",
                              str(nofile): "no review file names the PR"}
    for path in (merged, closed, declined):
        assert not path.exists()
        assert not _listed(repo, str(path))
        assert (path.parent / "diff.txt").read_text() == "diff"
        assert (path.parent / "queued_comments.json").exists()
    assert (merged.parent / "review.json").exists()
    assert still_open.is_dir() and _listed(repo, str(still_open))
    assert (dirty / "a.txt").read_text() == "changed\n"
    assert nofile.is_dir()
    assert ("app", 3) in platform.asked


def test_keeps_the_worktree_while_any_pr_of_the_branch_is_open(tmp_path, monkeypatch):
    repo, state, config, _ = _project(tmp_path, monkeypatch, {1: "MERGED", 2: "OPEN"})
    merged = _review(repo, state, "merged", 1)
    (merged.parent / "review.codex.json").write_text(json.dumps({"pr_id": 2}))

    result = review_worktree_gc.sweep(config, now=_later(), force=True)

    assert result == {"removed": [], "kept": {}}
    assert merged.is_dir()


def test_skips_a_worktree_a_review_used_recently(tmp_path, monkeypatch):
    repo, state, config, platform = _project(tmp_path, monkeypatch, {1: "MERGED"})
    merged = _review(repo, state, "merged", 1, age=time.time())

    result = review_worktree_gc.sweep(config, force=True)

    assert result == {"removed": [], "kept": {}}
    assert merged.is_dir() and platform.asked == []


def test_skips_a_worktree_a_new_review_just_reset(tmp_path, monkeypatch):
    repo, state, config, platform = _project(tmp_path, monkeypatch, {1: "MERGED"})
    merged = _review(repo, state, "merged", 1)
    _git(merged, "reset", "--hard", "origin/merged")

    result = review_worktree_gc.sweep(config, force=True)

    assert result == {"removed": [], "kept": {}}
    assert merged.is_dir() and platform.asked == []


def test_keeps_the_worktree_when_a_review_file_cannot_be_read(tmp_path, monkeypatch):
    repo, state, config, _ = _project(tmp_path, monkeypatch, {1: "MERGED"})
    merged = _review(repo, state, "merged", 1)
    (merged.parent / "review.codex.json").write_text("{\"pr_id\": 2")

    result = review_worktree_gc.sweep(config, now=_later(), force=True)

    assert result == {"removed": [], "kept": {str(merged): "a review file cannot be read"}}
    assert merged.is_dir()


def test_keeps_the_worktree_when_the_state_lookup_fails(tmp_path, monkeypatch):
    repo, state, config, _ = _project(tmp_path, monkeypatch, {1: RuntimeError("gh down")})
    merged = _review(repo, state, "merged", 1)

    result = review_worktree_gc.sweep(config, now=_later(), force=True)

    assert result == {"removed": [], "kept": {str(merged): "PR state lookup failed: gh down"}}
    assert merged.is_dir()


def test_leaves_worktrees_outside_the_review_folders(tmp_path, monkeypatch):
    repo, state, config, platform = _project(tmp_path, monkeypatch, {1: "MERGED"})
    other = state / "pr_worktrees" / "merged"
    assert git_util.add_detached_worktree(repo, other, "merged") == other

    result = review_worktree_gc.sweep(config, now=_later(), force=True)

    assert result == {"removed": [], "kept": {}}
    assert other.is_dir() and platform.asked == []


def test_dry_run_removes_nothing(tmp_path, monkeypatch):
    repo, state, config, _ = _project(tmp_path, monkeypatch, {1: "MERGED"})
    merged = _review(repo, state, "merged", 1)

    result = review_worktree_gc.sweep(config, now=_later(), dry_run=True)

    assert result == {"removed": [str(merged)], "kept": {}}
    assert merged.is_dir() and _listed(repo, str(merged))


def test_only_limits_the_sweep(tmp_path, monkeypatch):
    repo, state, config, _ = _project(tmp_path, monkeypatch, {1: "MERGED", 2: "CLOSED"})
    merged = _review(repo, state, "merged", 1)
    closed = _review(repo, state, "closed", 2)

    result = review_worktree_gc.sweep(config, now=_later(), force=True, only={str(merged)})

    assert result == {"removed": [str(merged)], "kept": {}}
    assert closed.is_dir()


def test_throttles_between_sweeps(tmp_path, monkeypatch):
    repo, state, config, _ = _project(tmp_path, monkeypatch, {1: "MERGED"})
    review_worktree_gc.sweep(config, now=time.time())
    merged = _review(repo, state, "merged", 1)

    assert review_worktree_gc.sweep(config, now=time.time()) == {"throttled": True}
    assert merged.is_dir()
