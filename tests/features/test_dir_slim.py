"""The slim sweep: which dependency trees and videos it deletes from finished
task worktrees and done ticket directories, and which it keeps."""
import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import core.db as db
from services import dir_slim, work_worktree
from tests.features.test_work_worktree import (_finish, _git, _item, _spec,
                                               make_repo, wt_root)

DAY = 86400
OLD = dir_slim.SLIM_AFTER_DAYS + 1


@pytest.fixture(autouse=True)
def _isolated(fresh_db, monkeypatch):
    dir_slim._last_sweep = 0.0
    monkeypatch.setattr(dir_slim.runtime, "instances", lambda: None)
    yield
    dir_slim._last_sweep = 0.0


def _bulk(tree: Path):
    (tree / "node_modules" / "pkg").mkdir(parents=True)
    (tree / "node_modules" / "pkg" / "index.js").write_text("x" * 4096)
    (tree / ".venv" / "lib").mkdir(parents=True)
    (tree / ".venv" / "lib" / "mod.py").write_text("x" * 4096)
    (tree / "test-results").mkdir()
    (tree / "test-results" / "run.webm").write_bytes(b"v" * 4096)
    (tree / "demo.MP4").write_bytes(b"v" * 4096)


def _task(tmp_path, monkeypatch, days_ago):
    repo = make_repo(tmp_path)
    monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
    item_id = _item("slim me")
    row = work_worktree.ensure(item_id, _spec(repo), "slim me")
    tree = Path(row["path"])
    (tree / "tracked.webm").write_bytes(b"keep")
    _git(tree, "add", "tracked.webm")
    _git(tree, "commit", "-m", "a tracked video")
    _bulk(tree)
    if days_ago is not None:
        _finish(item_id, archived=False, days_ago=days_ago)
    return item_id, tree


class TestTasks:
    def test_deletes_untracked_bulk_of_a_task_finished_long_ago(self, tmp_path, wt_root,
                                                                monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, OLD)
        results = dir_slim.sweep(force=True)
        assert [r["path"] for r in results] == [str(tree)]
        assert sorted(results[0]["removed"]) == sorted(
            str(tree / p) for p in (".venv", "node_modules", "demo.MP4",
                                    "test-results/run.webm"))
        assert results[0]["bytes"] > 0
        for gone in (".venv", "node_modules", "demo.MP4", "test-results/run.webm"):
            assert not os.path.lexists(tree / gone)
        assert (tree / "tracked.webm").read_bytes() == b"keep"
        assert (tree / "README.md").exists()

    def test_keeps_a_task_finished_recently(self, tmp_path, wt_root, monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, 1)
        assert dir_slim.sweep(force=True) == []
        assert (tree / "node_modules").is_dir()

    def test_keeps_a_task_that_is_not_finished(self, tmp_path, wt_root, monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, None)
        assert dir_slim.sweep(force=True) == []
        assert (tree / "demo.MP4").exists()

    def test_keeps_a_task_with_a_live_session(self, tmp_path, wt_root, monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, OLD)
        monkeypatch.setattr(dir_slim.terminal, "session_healthy",
                            lambda key, agent="claude": {"alive": True})
        assert dir_slim.sweep(force=True) == []
        assert (tree / ".venv").is_dir()

    def test_deletes_build_caches_and_keeps_generic_build_dirs(self, tmp_path, wt_root,
                                                                monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, OLD)
        caches = (".next", "cdk.out", "src/__pycache__", "apps/desktop/.webpack_cache_main")
        for cache in caches:
            (tree / cache).mkdir(parents=True)
            (tree / cache / "blob").write_bytes(b"c" * 4096)
        for kept in ("out", "dist", "build", ".webpack"):
            (tree / kept).mkdir()
            (tree / kept / "app.js").write_text("x")
        results = dir_slim.sweep(force=True)
        for cache in caches:
            assert str(tree / cache) in results[0]["removed"]
            assert not os.path.lexists(tree / cache)
        for kept in ("out", "dist", "build", ".webpack"):
            assert (tree / kept / "app.js").exists()

    def test_keeps_a_tracked_dependency_dir(self, tmp_path, wt_root, monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, OLD)
        _git(tree, "add", "-f", "node_modules/pkg/index.js")
        _git(tree, "commit", "-m", "vendored")
        dir_slim.sweep(force=True)
        assert (tree / "node_modules" / "pkg" / "index.js").exists()
        assert not (tree / ".venv").exists()

    def test_never_follows_a_symlinked_venv(self, tmp_path, wt_root, monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, OLD)
        store = tmp_path / "venv-store" / "app-1234"
        (store / "lib").mkdir(parents=True)
        (store / "lib" / "big.py").write_text("x")
        shutil.rmtree(tree / ".venv")
        (tree / ".venv").symlink_to(store)
        dir_slim.sweep(force=True)
        assert (tree / ".venv").is_symlink()
        assert (store / "lib" / "big.py").exists()
        assert not (tree / "node_modules").exists()

    def test_throttles_between_sweeps(self, tmp_path, wt_root, monkeypatch):
        _, tree = _task(tmp_path, monkeypatch, OLD)
        dir_slim._last_sweep = time.time()
        assert dir_slim.sweep() == []
        assert (tree / "node_modules").is_dir()


def _ticket_config(root: Path):
    return {"features": {"tickets": True},
            "workspace": {"root": root, "tickets_dir": "tickets"}}


def _ticket(root: Path, slug: str, status: str, age_days: float):
    db.execute(
        "INSERT INTO tickets(instance_key, ticket_key, status, slug, data, updated_at) "
        "VALUES (?, ?, ?, ?, '{}', ?)",
        ("inst", slug.upper(), status, slug, datetime.now(timezone.utc).isoformat()))
    ticket_dir = root / "tickets" / slug
    (ticket_dir / "docs").mkdir(parents=True)
    (ticket_dir / "docs" / "proof.md").write_text("proof")
    (ticket_dir / "docs" / "proof.webm").write_bytes(b"v" * 4096)
    (ticket_dir / "app" / "node_modules" / "pkg").mkdir(parents=True)
    (ticket_dir / "app" / "node_modules" / "pkg" / "index.js").write_text("x")
    stamp = time.time() - age_days * DAY
    for base, dirs, files in os.walk(ticket_dir):
        for name in dirs + files:
            os.utime(os.path.join(base, name), (stamp, stamp))
    return ticket_dir


class TestTickets:
    @pytest.fixture()
    def root(self, tmp_path, monkeypatch, wt_root):
        root = tmp_path / "ws"
        reg = SimpleNamespace(instance_key="inst", config=_ticket_config(root))
        monkeypatch.setattr(dir_slim.runtime, "instances",
                            lambda: SimpleNamespace(all=lambda: [reg]))
        return root

    def test_deletes_bulk_of_a_merged_ticket_idle_long_ago(self, root):
        ticket_dir = _ticket(root, "t-1-merged", "merged", OLD)
        results = dir_slim.sweep(force=True)
        assert [r["path"] for r in results] == [str(ticket_dir)]
        assert not (ticket_dir / "docs" / "proof.webm").exists()
        assert not (ticket_dir / "app" / "node_modules").exists()
        assert (ticket_dir / "docs" / "proof.md").exists()

    def test_keeps_a_ticket_still_in_review(self, root):
        ticket_dir = _ticket(root, "t-2-review", "in_review", OLD)
        assert dir_slim.sweep(force=True) == []
        assert (ticket_dir / "docs" / "proof.webm").exists()

    def test_keeps_a_merged_ticket_touched_recently(self, root):
        ticket_dir = _ticket(root, "t-3-recent", "merged", 1)
        assert dir_slim.sweep(force=True) == []
        assert (ticket_dir / "app" / "node_modules").is_dir()

    def test_keeps_tracked_files_in_a_ticket_worktree(self, root, tmp_path):
        ticket_dir = _ticket(root, "t-4-tracked", "done", 0)
        repo = make_repo(tmp_path, name="src")
        wt = ticket_dir / "src"
        _git(repo, "worktree", "add", "-b", "t-4", str(wt))
        (wt / "clip.mp4").write_bytes(b"tracked")
        _git(wt, "add", "clip.mp4")
        _git(wt, "-c", "user.name=t", "-c", "user.email=t@example.com",
             "commit", "-m", "clip")
        (wt / "scratch.mp4").write_bytes(b"untracked")
        stamp = time.time() - OLD * DAY
        for base, dirs, files in os.walk(ticket_dir):
            for name in dirs + files:
                os.utime(os.path.join(base, name), (stamp, stamp))
        dir_slim.sweep(force=True)
        assert (wt / "clip.mp4").read_bytes() == b"tracked"
        assert not (wt / "scratch.mp4").exists()
        assert not (ticket_dir / "docs" / "proof.webm").exists()

    def test_keeps_files_an_enclosing_repo_tracks(self, root, tmp_path):
        ticket_dir = _ticket(root, "t-5-outer", "merged", OLD)
        _git(root, "init", "-b", "main")
        _git(root, "add", "-f", "tickets/t-5-outer/docs/proof.webm")
        _git(root, "-c", "user.name=t", "-c", "user.email=t@example.com",
             "commit", "-m", "proof")
        dir_slim.sweep(force=True)
        assert (ticket_dir / "docs" / "proof.webm").exists()
        assert not (ticket_dir / "app" / "node_modules").exists()

    def test_keeps_a_dependency_dir_that_holds_a_repo(self, root):
        ticket_dir = _ticket(root, "t-6-nested", "merged", OLD)
        package = ticket_dir / "app" / "node_modules" / "local-package"
        package.mkdir()
        _git(package, "init", "-b", "main")
        dir_slim.sweep(force=True)
        assert (package / ".git").is_dir()
        assert not (ticket_dir / "docs" / "proof.webm").exists()


def test_never_slims_through_a_symlinked_task_root(tmp_path, wt_root, monkeypatch):
    _, tree = _task(tmp_path, monkeypatch, OLD)
    outside = tmp_path / "personal"
    outside.mkdir()
    (outside / "personal.mp4").write_bytes(b"mine")
    shutil.rmtree(tree)
    tree.symlink_to(outside)
    assert dir_slim.sweep(force=True) == []
    assert (outside / "personal.mp4").read_bytes() == b"mine"


def test_never_slims_through_a_symlinked_task_ancestor(tmp_path, wt_root, monkeypatch):
    _, tree = _task(tmp_path, monkeypatch, OLD)
    outside = tmp_path / "personal"
    (outside / tree.name).mkdir(parents=True)
    (outside / tree.name / "personal.mp4").write_bytes(b"mine")
    shutil.rmtree(tree.parent)
    tree.parent.symlink_to(outside)
    assert dir_slim.sweep(force=True) == []
    assert (outside / tree.name / "personal.mp4").read_bytes() == b"mine"
