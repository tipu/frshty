"""The ticket folder sweep: which ended and orphan folders it removes, what a
removal salvages, and which folders it keeps."""
import os
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import core.db as db
import core.git_util as git_util
from services import ticket_gc, work_store

DAY = 86400


def _git(cwd, *args):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True, text=True,
                          check=True, timeout=60).stdout.strip()


@pytest.fixture()
def ws(fresh_db, tmp_path, monkeypatch):
    origin = tmp_path / "app.git"
    _git(tmp_path, "init", "--bare", "-b", "main", str(origin))
    root = tmp_path / "ws"
    repo = root / "projects" / "app"
    _git(tmp_path, "clone", str(origin), str(repo))
    _git(repo, "config", "user.name", "test")
    _git(repo, "config", "user.email", "test@example.com")
    (repo / ".gitignore").write_text("node_modules/\n")
    (repo / "a.txt").write_text("a\n")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-m", "init")
    _git(repo, "push", "-u", "origin", "main")
    (root / "tickets").mkdir()
    config = {"features": {"tickets": True},
              "workspace": {"root": root, "tickets_dir": "tickets"}}
    reg = SimpleNamespace(instance_key="inst", config=config)
    monkeypatch.setattr(ticket_gc.runtime, "instances",
                        lambda: SimpleNamespace(all=lambda: [reg]))
    monkeypatch.setattr(ticket_gc.terminal, "session_healthy",
                        lambda key, agent="claude": {"alive": False})
    ticket_gc._last_sweep = ticket_gc._last_orphan_sweep = 0.0
    yield SimpleNamespace(root=root, repo=repo, tickets=root / "tickets", config=config)
    ticket_gc._last_sweep = ticket_gc._last_orphan_sweep = 0.0


def _ago(days: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()


def _folder(ws, name: str, branch: str, age_days: float = 0) -> Path:
    """A ticket folder with docs, run output and a worktree of the app."""
    folder = ws.tickets / name
    (folder / "docs").mkdir(parents=True)
    (folder / "docs" / "ticket.md").write_text("ticket")
    (folder / "pr_comments.json").write_text("[]")
    (folder / "run" / "build").mkdir(parents=True)
    (folder / "run" / "build" / "app.bin").write_bytes(b"b" * 4096)
    wt = folder / "workspace" / "app"
    _git(ws.repo, *git_util.WORKTREE_ADD, "-b", branch, str(wt), "origin/main")
    (wt / "node_modules").mkdir()
    (wt / "node_modules" / "pkg.js").write_text("x")
    _age(folder, age_days)
    return folder


def _age(folder: Path, days: float):
    stamp = time.time() - days * DAY
    for base, dirs, files in os.walk(folder):
        for n in dirs + files:
            os.utime(os.path.join(base, n), (stamp, stamp), follow_symlinks=False)
    os.utime(folder, (stamp, stamp))


def _ticket(name: str, status: str, days: float, instance_key: str = "inst"):
    db.execute(
        "INSERT INTO tickets(instance_key, ticket_key, status, slug, data, updated_at) "
        "VALUES (?, ?, ?, ?, '{}', ?)",
        (instance_key, name.upper(), status, name, _ago(days)))


def _item(state: str, days: float, archived: bool = False) -> int:
    item_id = work_store.create_item("ticket gc test")
    when = _ago(days)
    with db.tx() as c:
        c.execute("UPDATE work_items SET state = ?, updated_at = ?, archived_at = ? "
                  "WHERE id = ?", (state, when, when if archived else None, item_id))
    return item_id


def _salvaged(repo, label):
    refs = _git(repo, "for-each-ref", "--format=%(refname)",
                git_util.salvage_ref(label)).split()
    assert len(refs) == 1, refs
    return refs[0]


def _listed(ws, path: Path) -> bool:
    return any(lines[0] == f"worktree {path}"
               for lines in git_util.worktree_entries(ws.repo) or [])


class TestEnded:
    def test_a_merged_ticket_keeps_only_its_docs_and_top_level_files(self, ws):
        _ticket("t-1-merged", "merged", 2)
        folder = _folder(ws, "t-1-merged", "t-1-merged")

        results = ticket_gc.sweep(force=True)

        assert [(r["path"], r["kept"], r["why"]) for r in results] == [
            (str(folder), "", "ended")]
        assert results[0]["bytes"] > 0
        assert sorted(os.listdir(folder)) == ["docs", "pr_comments.json"]
        assert (folder / "docs" / "ticket.md").read_text() == "ticket"
        assert not _listed(ws, folder / "workspace" / "app")
        assert _git(ws.repo, "branch", "--list", "t-1-merged")

    def test_a_removed_ticket_worktree_can_be_added_again(self, ws):
        _ticket("t-2-done", "done", 2)
        folder = _folder(ws, "t-2-done", "t-2-done")
        ticket_gc.sweep(force=True)

        wt = folder / "workspace" / "app"
        _git(ws.repo, *git_util.WORKTREE_ADD, str(wt), "t-2-done")

        assert (wt / "a.txt").read_text() == "a\n"

    def test_uncommitted_and_unpushed_work_is_salvaged_under_a_ref(self, ws):
        _ticket("t-3-done", "done", 2)
        folder = _folder(ws, "t-3-done", "t-3-done")
        wt = folder / "workspace" / "app"
        (wt / "b.txt").write_text("committed\n")
        _git(wt, "add", "b.txt")
        _git(wt, "commit", "-m", "unpushed")
        (wt / "a.txt").write_text("edited\n")
        (wt / "c.txt").write_text("untracked\n")

        results = ticket_gc.sweep(force=True)

        assert results[0]["kept"] == ""
        assert not wt.exists()
        ref = _salvaged(ws.repo, "tickets/t-3-done/workspace/app")
        assert _git(ws.repo, "show", f"{ref}:a.txt") == "edited"
        assert _git(ws.repo, "show", f"{ref}:b.txt") == "committed"
        assert _git(ws.repo, "show", f"{ref}:c.txt") == "untracked"
        assert _git(ws.repo, "rev-parse", f"{ref}^") == _git(ws.repo, "rev-parse", "t-3-done")

    def test_a_second_salvage_of_the_same_folder_keeps_the_first(self, ws):
        _ticket("t-9-again", "done", 2)
        folder = _folder(ws, "t-9-again", "t-9-again")
        (folder / "workspace" / "app" / "first.txt").write_text("1")
        ticket_gc.sweep(force=True)
        wt = folder / "workspace" / "app"
        _git(ws.repo, *git_util.WORKTREE_ADD, str(wt), "t-9-again")
        (wt / "second.txt").write_text("2")

        ticket_gc.sweep(force=True)

        refs = _git(ws.repo, "for-each-ref", "--format=%(refname)",
                    git_util.salvage_ref("tickets/t-9-again/workspace/app")).split()
        assert len(refs) == 2
        assert sorted(_git(ws.repo, "ls-tree", "--name-only", r) for r in refs) == [
            ".gitignore\na.txt\nfirst.txt", ".gitignore\na.txt\nsecond.txt"]

    def test_a_ticket_with_an_open_terminal_stays(self, ws, monkeypatch):
        _ticket("t-10-term", "done", 2)
        folder = _folder(ws, "t-10-term", "t-10-term")
        monkeypatch.setattr(ticket_gc.terminal, "session_healthy",
                            lambda key, agent="claude": {"alive": key == "T-10-TERM"})

        assert ticket_gc.sweep(force=True) == []
        assert (folder / "workspace" / "app" / "a.txt").exists()

    def test_a_ticket_that_ended_today_stays(self, ws):
        _ticket("t-4-fresh", "merged", 0.1)
        folder = _folder(ws, "t-4-fresh", "t-4-fresh")

        assert ticket_gc.sweep(force=True) == []
        assert (folder / "workspace" / "app" / "a.txt").exists()

    def test_a_ticket_still_in_review_stays(self, ws):
        _ticket("t-5-review", "in_review", 30)
        folder = _folder(ws, "t-5-review", "t-5-review", age_days=30)

        assert ticket_gc.sweep(force=True) == []
        assert (folder / "run").is_dir()

    def test_a_slug_another_instance_still_works_on_stays(self, ws):
        _ticket("t-6-shared", "done", 2)
        _ticket("t-6-shared", "planning", 2, instance_key="other")
        folder = _folder(ws, "t-6-shared", "t-6-shared", age_days=30)

        assert ticket_gc.sweep(force=True) == []
        assert (folder / "run").is_dir()

    def test_a_finished_work_folder_is_deleted_whole(self, ws):
        item_id = _item("done", 2)
        folder = _folder(ws, f"work-{item_id}-task", f"work-{item_id}")

        results = ticket_gc.sweep(force=True)

        assert [(r["kept"], r["why"]) for r in results] == [("", "ended")]
        assert not folder.exists()

    def test_an_open_work_folder_stays_however_old(self, ws):
        item_id = _item("running", 30)
        folder = _folder(ws, f"work-{item_id}", f"work-{item_id}", age_days=30)

        assert ticket_gc.sweep(force=True) == []
        assert folder.is_dir()

    def test_a_finished_work_folder_with_a_live_session_stays(self, ws, monkeypatch):
        item_id = _item("done", 2)
        folder = _folder(ws, f"work-{item_id}", f"work-{item_id}")
        monkeypatch.setattr(ticket_gc.terminal, "session_healthy",
                            lambda key, agent="claude": {"alive": key == f"work-{item_id}"})

        assert ticket_gc.sweep(force=True) == []
        assert folder.is_dir()

    def test_a_folder_an_open_task_runs_in_stays(self, ws):
        _ticket("t-7-busy", "done", 2)
        folder = _folder(ws, "t-7-busy", "t-7-busy")
        open_item = _item("running", 0)
        work_store.add_run(open_item, f"sid-{open_item}", f"work-{open_item}",
                           str(folder / "workspace" / "app"))

        assert ticket_gc.sweep(force=True) == []
        assert (folder / "workspace" / "app" / "a.txt").exists()


class TestOrphans:
    def test_an_orphan_idle_a_week_is_deleted_whole(self, ws):
        folder = _folder(ws, "DEV-1-review", "dev-1-review",
                         age_days=ticket_gc.ORPHAN_AFTER_DAYS + 1)

        results = ticket_gc.sweep(force=True)

        assert [(r["kept"], r["why"]) for r in results] == [("", "orphan")]
        assert not folder.exists()
        assert not _listed(ws, folder / "workspace" / "app")

    def test_a_work_folder_whose_item_is_in_no_database_is_an_orphan(self, ws):
        folder = _folder(ws, "work-999999-x", "work-999999",
                         age_days=ticket_gc.ORPHAN_AFTER_DAYS + 1)

        assert [r["why"] for r in ticket_gc.sweep(force=True)] == ["orphan"]
        assert not folder.exists()

    def test_an_orphan_touched_this_week_stays(self, ws):
        folder = _folder(ws, "DEV-2-review", "dev-2-review",
                         age_days=ticket_gc.ORPHAN_AFTER_DAYS + 1)
        (folder / "docs" / "note.md").write_text("new")

        assert ticket_gc.sweep(force=True) == []
        assert folder.is_dir()

    def test_dot_folders_files_and_symlinks_stay(self, ws, tmp_path):
        old = time.time() - 30 * DAY
        (ws.tickets / ".playwright").mkdir()
        (ws.tickets / "README.md").write_text("x")
        outside = tmp_path / "outside"
        outside.mkdir()
        (outside / "keep.txt").write_text("keep")
        (ws.tickets / "link").symlink_to(outside)
        for p in (ws.tickets / ".playwright", ws.tickets / "README.md", outside):
            os.utime(p, (old, old))

        assert ticket_gc.sweep(force=True) == []
        assert (outside / "keep.txt").exists()
        assert (ws.tickets / ".playwright").is_dir()

    def test_the_orphan_pass_runs_once_a_day(self, ws):
        now = time.time()
        ticket_gc.sweep(now=now)
        folder = _folder(ws, "DEV-3-review", "dev-3-review",
                         age_days=ticket_gc.ORPHAN_AFTER_DAYS + 1)

        assert ticket_gc.sweep(now=now + ticket_gc.SWEEP_INTERVAL_S) == []
        assert folder.is_dir()
        results = ticket_gc.sweep(now=now + ticket_gc.ORPHAN_SWEEP_INTERVAL_S)
        assert [r["why"] for r in results] == ["orphan"]

    def test_an_instance_without_the_tickets_feature_is_not_swept(self, ws):
        ws.config["features"]["tickets"] = False
        folder = _folder(ws, "DEV-4-review", "dev-4-review",
                         age_days=ticket_gc.ORPHAN_AFTER_DAYS + 1)

        assert ticket_gc.sweep(force=True) == []
        assert folder.is_dir()


class TestKeeps:
    def test_a_clone_with_unpushed_commits_keeps_the_folder(self, ws, tmp_path):
        folder = ws.tickets / "DEV-5-clone"
        folder.mkdir()
        clone = folder / "app"
        _git(tmp_path, "clone", str(tmp_path / "app.git"), str(clone))
        _git(clone, "-c", "user.name=t", "-c", "user.email=t@t", "commit",
             "--allow-empty", "-m", "local only")
        _age(folder, 30)

        results = ticket_gc.sweep(force=True)

        assert len(results) == 1
        assert "HEAD is on no remote branch" in results[0]["kept"]
        assert (clone / ".git").is_dir()

    @pytest.mark.parametrize("extra", ["branch", "stash", "worktree"])
    def test_a_clone_with_work_off_its_head_keeps_the_folder(self, ws, tmp_path, extra):
        folder = ws.tickets / f"DEV-7-{extra}"
        folder.mkdir()
        clone = folder / "app"
        _git(tmp_path, "clone", str(tmp_path / "app.git"), str(clone))
        ident = ("-c", "user.name=t", "-c", "user.email=t@t")
        if extra == "branch":
            _git(clone, "switch", "-c", "feature")
            _git(clone, *ident, "commit", "--allow-empty", "-m", "local only")
            _git(clone, "switch", "main")
        elif extra == "stash":
            (clone / "a.txt").write_text("stashed\n")
            _git(clone, *ident, "stash")
        else:
            _git(clone, "worktree", "add", "-b", "side", str(tmp_path / "side"))
        _age(folder, 30)

        results = ticket_gc.sweep(force=True)

        assert len(results) == 1 and results[0]["kept"]
        assert (clone / ".git").is_dir()

    def test_a_pushed_clean_clone_is_deleted(self, ws, tmp_path):
        folder = ws.tickets / "DEV-6-clone"
        folder.mkdir()
        _git(tmp_path, "clone", str(tmp_path / "app.git"), str(folder / "app"))
        _age(folder, 30)

        assert [r["kept"] for r in ticket_gc.sweep(force=True)] == [""]
        assert not folder.exists()

    def test_a_closed_pr_style_salvage_refuses_a_quoted_nested_repository(self, ws, tmp_path):
        wt = tmp_path / "wt"
        _git(ws.repo, "worktree", "add", "-b", "quoted", str(wt))
        nested = wt / "my clone"
        nested.mkdir()
        _git(nested, "init", "-b", "main")
        (nested / "x.txt").write_text("x")
        _git(nested, "add", "x.txt")
        _git(nested, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-m", "x")

        reason = git_util.release_worktree(ws.repo, wt, salvage_label="pr/quoted")

        assert "nested repository" in reason
        assert (nested / ".git").is_dir()

    def test_an_untracked_nested_repository_keeps_the_worktree(self, ws):
        _ticket("t-8-nested", "done", 2)
        folder = _folder(ws, "t-8-nested", "t-8-nested")
        nested = folder / "workspace" / "app" / "vendor" / "lib"
        nested.mkdir(parents=True)
        _git(nested, "init", "-b", "main")
        (nested / "x.txt").write_text("x")
        _git(nested, "add", "x.txt")
        _git(nested, "-c", "user.name=t", "-c", "user.email=t@t", "commit", "-m", "x")

        results = ticket_gc.sweep(force=True)

        assert results[0]["kept"]
        assert (nested / "x.txt").exists()
