import os
import subprocess
import time
from pathlib import Path

import core.git_util as git_util
from features import pr_worktree_gc

OLD = time.time() - (pr_worktree_gc.IDLE_DAYS + 1) * 86400


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True,
                          check=True).stdout


def _project(tmp_path):
    origin = tmp_path / "origin.git"
    _git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    root = tmp_path / "ws"
    repo = root / "app"
    _git(tmp_path, "clone", str(origin), str(repo))
    _git(repo, "config", "user.email", "t@t")
    _git(repo, "config", "user.name", "t")
    (repo / ".gitignore").write_text("node_modules/\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "init")
    _git(repo, "push", "origin", "main")
    for b in ("done", "fresh", "dirty", "local"):
        _git(repo, "push", "origin", f"main:{b}")
    _git(repo, "fetch", "origin")
    state = tmp_path / "state"
    config = {"job": {"key": "test"}, "workspace": {"root": root, "repos": ["app"]}, "_state_dir": state}
    return repo, state, config


def _age(path, age=OLD):
    git_dir, _ = git_util.git_dirs(path)
    for name in ("index", "HEAD", os.path.join("logs", "HEAD")):
        p = os.path.join(git_dir, name)
        if os.path.exists(p):
            os.utime(p, (age, age))


def _add(repo, state, sub, branch, age=OLD):
    path = state / sub / branch
    added = git_util.add_detached_worktree(repo, path, branch)
    assert added == path
    _age(path, age)
    return path


def _locked(repo, path):
    for lines in git_util.worktree_entries(repo) or []:
        if lines[0] == f"worktree {path}":
            return any(ln.startswith("locked") for ln in lines)
    return None


def test_removes_an_idle_pushed_clean_worktree_and_keeps_the_rest(tmp_path):
    repo, state, config = _project(tmp_path)
    done = _add(repo, state, "pr_worktrees", "done")
    (done / "node_modules").mkdir()
    (done / "node_modules" / "big").write_text("x")
    fresh = _add(repo, state, "autofix_worktrees", "fresh", age=time.time())
    dirty = _add(repo, state, "autofix_worktrees", "dirty")
    (dirty / "new.txt").write_text("work")
    local = _add(repo, state, "pr_worktrees", "local")
    _git(local, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "--allow-empty",
         "-m", "unpushed")
    _age(local)

    result = pr_worktree_gc.sweep(config, force=True)

    assert result["removed"] == [str(done)]
    assert result["kept"] == {str(dirty): "local changes",
                              str(local): "HEAD is on no remote branch"}
    assert not done.exists()
    assert _locked(repo, str(done)) is None
    assert fresh.is_dir() and _locked(repo, str(fresh))
    assert dirty.is_dir() and _locked(repo, str(dirty))
    assert local.is_dir() and _locked(repo, str(local))


def test_leaves_a_worktree_frshty_did_not_lock(tmp_path):
    repo, state, config = _project(tmp_path)
    path = state / "pr_worktrees" / "done"
    _git(repo, "worktree", "add", "--detach", str(path), "origin/done")
    other = _add(repo, state, "pr_worktrees", "fresh")
    _git(repo, "worktree", "unlock", str(other))
    _git(repo, "worktree", "lock", "--reason", "someone else", str(other))

    result = pr_worktree_gc.sweep(config, force=True)

    assert result == {"removed": [], "kept": {}}
    assert path.is_dir() and other.is_dir()


def test_dry_run_removes_nothing(tmp_path):
    repo, state, config = _project(tmp_path)
    done = _add(repo, state, "pr_worktrees", "done")
    dirty = _add(repo, state, "pr_worktrees", "dirty")
    (dirty / "new.txt").write_text("work")

    result = pr_worktree_gc.sweep(config, dry_run=True)

    assert result == {"removed": [str(done)], "kept": {str(dirty): "local changes"}}
    assert done.is_dir() and _locked(repo, str(done))


def test_only_limits_the_sweep(tmp_path):
    repo, state, config = _project(tmp_path)
    done = _add(repo, state, "pr_worktrees", "done")
    other = _add(repo, state, "autofix_worktrees", "fresh")

    result = pr_worktree_gc.sweep(config, force=True, only={str(done)})

    assert result == {"removed": [str(done)], "kept": {}}
    assert not done.exists() and other.is_dir()


def test_throttles_between_sweeps(tmp_path):
    repo, state, config = _project(tmp_path)
    _add(repo, state, "pr_worktrees", "done")
    pr_worktree_gc.sweep(config, now=time.time())

    done = _add(repo, state, "pr_worktrees", "local")

    assert pr_worktree_gc.sweep(config, now=time.time()) == {"throttled": True}
    assert done.is_dir()


def test_a_failed_remove_puts_the_lock_back(tmp_path, monkeypatch):
    repo, state, _ = _project(tmp_path)
    done = _add(repo, state, "pr_worktrees", "done")
    real = git_util.run_git_status

    def fail_remove(cwd, args, **kw):
        if args[:2] == ["worktree", "remove"]:
            return subprocess.CompletedProcess(args, 1, "", "boom")
        return real(cwd, args, **kw)

    monkeypatch.setattr(git_util, "run_git_status", fail_remove)

    assert git_util.release_worktree(repo, done) == "git worktree remove failed: boom"
    monkeypatch.undo()
    assert _locked(repo, str(done))


def test_keeps_an_untracked_file_that_status_config_hides(tmp_path):
    repo, state, config = _project(tmp_path)
    _git(repo, "config", "status.showUntrackedFiles", "no")
    done = _add(repo, state, "pr_worktrees", "done")
    (done / "unique").write_text("work")

    result = pr_worktree_gc.sweep(config, force=True)

    assert result == {"removed": [], "kept": {str(done): "local changes"}}
    assert (done / "unique").read_text() == "work"


def test_keeps_a_change_behind_skip_worktree(tmp_path):
    repo, state, config = _project(tmp_path)
    done = _add(repo, state, "pr_worktrees", "done")
    _git(done, "update-index", "--skip-worktree", ".gitignore")
    (done / ".gitignore").write_text("changed\n")
    _age(done)

    result = pr_worktree_gc.sweep(config, force=True)

    assert result == {"removed": [], "kept": {
        str(done): "a file is marked skip-worktree or assume-unchanged"}}
    assert (done / ".gitignore").read_text() == "changed\n"


def test_a_worktree_used_during_the_sweep_stays_locked(tmp_path):
    repo, state, _ = _project(tmp_path)
    done = _add(repo, state, "pr_worktrees", "done")

    assert git_util.release_worktree(repo, done, still_idle=lambda: False) == \
        "used during the sweep"
    assert done.is_dir() and _locked(repo, str(done))
