"""Task worktrees: where a run starts, how one is made, and when one is removed.

Every test builds a real git repository. Nothing the failure depends on is
mocked away: the branch holders, the ignored files, the detached HEAD and the
prunable registration are all real git states.
"""
import os
import subprocess
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import core.config as core_config
import core.db as db
import core.git_util as git_util
from services import work_store, work_worktree


def _git(cwd, *args, check=True):
    return subprocess.run(["git", *args], cwd=str(cwd), capture_output=True,
                          text=True, check=check, timeout=60)


def make_repo(tmp_path, name="app", base="main"):
    """A bare origin plus a canonical checkout of it, with one commit."""
    origin = tmp_path / f"{name}.git"
    _git(tmp_path, "init", "--bare", "-b", base, str(origin))
    work = tmp_path / name
    _git(tmp_path, "clone", str(origin), str(work))
    _git(work, "config", "user.name", "test")
    _git(work, "config", "user.email", "test@example.com")
    (work / "README.md").write_text("hello\n")
    _git(work, "add", "-A")
    _git(work, "commit", "-m", "init")
    _git(work, "push", "-u", "origin", base)
    return work


def instances(**configs):
    registry = {}
    for key, config in configs.items():
        entry = MagicMock()
        entry.config = config
        registry[key] = entry
    return registry


def project_config(root, repos, base="main", layout="flat", prefix=""):
    return {"workspace": {"root": Path(root), "repos": list(repos),
                          "tickets_dir": "tickets", "ticket_layout": layout,
                          "base_branch": base, "branch_prefix": prefix,
                          "exclude": [], "dep_commands": []}}


@pytest.fixture()
def wt_root(tmp_path, monkeypatch):
    root = tmp_path / "worktrees"
    monkeypatch.setattr(core_config, "TASK_WORKTREE_ROOT", root)
    # The session database is shared, so a row another test left behind would
    # be swept by a gc() call here and counted as this test's result.
    db.execute("DELETE FROM work_worktrees")
    monkeypatch.setattr(work_worktree.terminal, "session_healthy",
                        lambda key, agent="claude": {"alive": False, "agent_running": False})
    return root


def _item(objective="do the thing", **kw):
    return work_store.create_item(objective, **kw)


def _spec(repo, name="app", key="proj", base="main", config=None):
    return work_worktree._repo_entry(config, str(repo), name, key)


# ------------------------------------------------------------------ helpers


class TestGitHelpers:
    def test_is_worktree_says_no_for_a_subdirectory_of_a_canonical_checkout(self, tmp_path):
        repo = make_repo(tmp_path)
        (repo / "src").mkdir()
        assert git_util.is_worktree(repo) is False
        assert git_util.is_worktree(repo / "src") is False

    def test_is_worktree_says_yes_inside_a_linked_worktree(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        (wt / "sub").mkdir()
        assert git_util.is_worktree(wt) is True
        assert git_util.is_worktree(wt / "sub") is True

    def test_is_worktree_says_no_outside_a_repository(self, tmp_path):
        assert git_util.is_worktree(tmp_path) is False

    def test_repo_root_of_a_worktree_is_the_canonical_checkout(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        assert work_worktree.repo_root(str(wt)) == str(repo)
        assert work_worktree.repo_root(str(repo / "src")) if (repo / "src").is_dir() else True

    def test_shared_checkout_names_the_repo_only_for_the_canonical_checkout(self, tmp_path):
        repo = make_repo(tmp_path)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        assert work_worktree.shared_checkout(str(repo)) == str(repo)
        assert work_worktree.shared_checkout(str(wt)) == ""
        assert work_worktree.shared_checkout(str(tmp_path)) == ""

    def test_branch_name_carries_the_item_and_the_objective(self):
        name = work_worktree.branch_name(None, 42, "Fix the flaky import in the loader now please")
        assert name == "work-42-fix-the-flaky-import-in-the-loader"
        prefixed = work_worktree.branch_name(
            project_config("/x", [], prefix="feat"), 42, "Fix it")
        assert prefixed == "feat/work-42-fix-it"

    def test_task_worktree_path_follows_the_ticket_layout(self, tmp_path):
        flat = project_config(tmp_path, ["app"], layout="flat")
        nested = project_config(tmp_path, ["app"], layout="workspace")
        assert core_config.task_worktree_path(flat, "proj", 7, "app").parts[-3:] == \
            ("proj", "work-7", "app")
        assert core_config.task_worktree_path(nested, "proj", 7, "app").parts[-4:] == \
            ("proj", "work-7", "workspace", "app")


# ------------------------------------------------------------------ plan


class TestMigrations:
    def test_the_item_repo_index_is_partial_on_every_database(self, tmp_path):
        """One database already ran 037 with an unconditional index, before it
        was made partial. A migration that has run is never re-read, so the
        correction has to be its own migration. Without the WHERE clause an
        INSERT OR REPLACE deletes the record of a removed worktree."""
        import sqlite3
        import core.db as _db
        migrations = Path(_db._MIGRATIONS_DIR or "migrations")
        for name in ("fresh", "already-ran-037"):
            conn = sqlite3.connect(tmp_path / f"{name}.db")
            files = sorted(migrations.glob("*.sql"))
            if name == "already-ran-037":
                # Reproduce the shape the live database is in: 037 ran before
                # the index was corrected, and it is never re-read.
                conn.executescript(
                    "CREATE TABLE work_worktrees (id INTEGER PRIMARY KEY, "
                    "work_item_id INTEGER, repo_common_dir TEXT, removed_at TEXT);"
                    "CREATE UNIQUE INDEX idx_work_worktrees_item_repo "
                    "ON work_worktrees(work_item_id, repo_common_dir);")
                files = [f for f in files if f.name.startswith("039")]
            else:
                files = [f for f in files if f.name.startswith(("037", "039"))]
            for f in files:
                conn.executescript(f.read_text())
            sql = conn.execute(
                "SELECT sql FROM sqlite_master WHERE name = "
                "'idx_work_worktrees_item_repo'").fetchone()[0]
            assert "WHERE removed_at IS NULL" in sql, (name, sql)
            conn.close()


class TestPlan:
    def test_r1_returns_the_worktree_the_task_already_has(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("reuse me")
        row = work_worktree.ensure(item_id, _spec(repo), "reuse me")
        assert row and os.path.isdir(row["path"])
        got = work_worktree.plan("reuse me", "", str(repo), ["proj"], [], item_id=item_id)
        assert got["rule"] == "R1"
        assert got["cwd"] == row["path"]
        assert got["create"] is False

    def test_r1_ignores_a_row_whose_directory_is_gone(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("gone")
        row = work_worktree.ensure(item_id, _spec(repo), "gone")
        _git(repo, "worktree", "remove", row["path"])
        got = work_worktree.plan("gone", "", str(tmp_path), [], [], item_id=item_id)
        assert got["rule"] != "R1"

    def test_r2_accepts_a_worktree_and_rejects_a_shared_subdirectory(self, tmp_path, wt_root,
                                                                     monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        wt = tmp_path / "linked"
        _git(repo, "worktree", "add", "-b", "side", str(wt))
        (wt / "sub").mkdir()
        (repo / "sub").mkdir()
        assert work_worktree.plan("x", "", str(wt), [], [])["rule"] == "R2"
        assert work_worktree.plan("x", "", str(wt / "sub"), [], [])["rule"] == "R2"
        # A bare --git-dir/--git-common-dir comparison calls this one a worktree.
        assert work_worktree.plan("x", "", str(repo / "sub"), [], [])["rule"] != "R2"

    def test_r3_keeps_a_ticket_directory_that_holds_a_worktree(self, tmp_path, wt_root,
                                                              monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        repo = make_repo(root, "app")
        config = project_config(root, ["app"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        ticket_dir = root / "tickets" / "DEV-1-thing"
        ticket_dir.mkdir(parents=True)
        _git(repo, "worktree", "add", "-b", "dev-1", str(ticket_dir / "app"))
        got = work_worktree.plan("x", str(ticket_dir), str(ticket_dir), ["proj"], [])
        assert got["rule"] == "R3"
        assert got["cwd"] == str(ticket_dir)

    def test_r3_does_not_fire_on_a_ticket_directory_with_no_worktree(self, tmp_path, wt_root,
                                                                     monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        make_repo(root, "app")
        config = project_config(root, ["app"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        ticket_dir = root / "tickets" / "DEV-1-thing"
        (ticket_dir / "app").mkdir(parents=True)
        got = work_worktree.plan("x", str(ticket_dir), str(ticket_dir), ["proj"], [])
        assert got["rule"] != "R3"

    def test_r4_uses_a_ticket_key_in_the_objective(self, tmp_path, wt_root, monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        repo = make_repo(root, "app")
        config = project_config(root, ["app"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        ticket_dir = root / "tickets" / "DEV-42-thing"
        ticket_dir.mkdir(parents=True)
        _git(repo, "worktree", "add", "-b", "dev-42", str(ticket_dir / "app"))
        with db.tx() as c:
            c.execute("INSERT OR REPLACE INTO tickets(instance_key, ticket_key, status, "
                      "slug, updated_at) VALUES ('proj', 'DEV-42', 'planning', "
                      "'DEV-42-thing', '2026-01-01')")
        got = work_worktree.plan("please look at DEV-42 today", "", str(root),
                                 ["proj"], [])
        assert got["rule"] == "R4"
        assert got["cwd"] == str(ticket_dir)

    def test_r4_never_fires_when_the_caller_named_a_directory(self, tmp_path, wt_root,
                                                              monkeypatch):
        """The ticket_doctor regression: it launches with an explicit cwd in the
        frshty checkout and an objective that names another project's ticket."""
        root = tmp_path / "proj"
        root.mkdir()
        repo = make_repo(root, "app")
        other = make_repo(tmp_path, "frshty")
        config = project_config(root, ["app"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        ticket_dir = root / "tickets" / "DEV-42-thing"
        ticket_dir.mkdir(parents=True)
        _git(repo, "worktree", "add", "-b", "dev-42", str(ticket_dir / "app"))
        with db.tx() as c:
            c.execute("INSERT OR REPLACE INTO tickets(instance_key, ticket_key, status, "
                      "slug, updated_at) VALUES ('proj', 'DEV-42', 'planning', "
                      "'DEV-42-thing', '2026-01-01')")
        got = work_worktree.plan("Doctor ticket DEV-42 (proj, status planning)",
                                 str(other), str(other), ["proj"],
                                 [{"key": "frshty", "root": str(other), "repos": []}])
        assert got["rule"] == "R5"
        assert got["repo_path"] == str(other)

    def test_r4_ignores_a_ticket_that_was_never_materialized(self, tmp_path, wt_root,
                                                             monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        make_repo(root, "app")
        config = project_config(root, ["app"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        with db.tx() as c:
            c.execute("INSERT OR REPLACE INTO tickets(instance_key, ticket_key, status, "
                      "slug, updated_at) VALUES ('proj', 'DEV-77', 'new', "
                      "'DEV-77-never', '2026-01-01')")
        got = work_worktree.plan("look at DEV-77", "", str(root), ["proj"], [])
        assert got["rule"] != "R4"

    def test_r5_resolves_a_single_repository(self, tmp_path, wt_root, monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        repo = make_repo(root, "app")
        config = project_config(root, ["app"], base="main")
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        got = work_worktree.plan("build it", "", str(root), ["proj"], [])
        assert got["rule"] == "R5"
        assert got["repo_path"] == str(repo)
        assert got["base_branch"] == "main"
        assert got["project_key"] == "proj"

    def test_r6_when_several_repositories_resolve_and_none_was_picked(self, tmp_path, wt_root,
                                                                     monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        make_repo(root, "app")
        make_repo(root, "web")
        config = project_config(root, ["app", "web"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        got = work_worktree.plan("build it", "", str(root), ["proj"], [])
        assert got["rule"] == "R6"
        assert got["cwd"] == str(root)

    def test_the_operator_pick_resolves_the_ambiguity(self, tmp_path, wt_root, monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        make_repo(root, "app")
        web = make_repo(root, "web")
        config = project_config(root, ["app", "web"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        got = work_worktree.plan("build it", "", str(root), ["proj"], [],
                                 repo_pick="web")
        assert got["rule"] == "R5"
        assert got["repo_path"] == str(web)

    def test_no_worktree_opts_out_before_r5(self, tmp_path, wt_root, monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        make_repo(root, "app")
        config = project_config(root, ["app"])
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        got = work_worktree.plan("build it", "", str(root), ["proj"], [],
                                 no_worktree=True)
        assert got["rule"] == "R6"

    def test_a_project_with_no_config_is_one_repository_at_its_root(self, tmp_path, wt_root,
                                                                    monkeypatch):
        repo = make_repo(tmp_path, "frshty")
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        entries = [{"key": "frshty", "root": str(repo), "repos": []}]
        got = work_worktree.plan("build it", "", str(repo), ["frshty"], entries)
        assert got["rule"] == "R5"
        assert got["repo_path"] == str(repo)

    def test_no_repository_means_the_workspace_root(self, tmp_path, wt_root, monkeypatch):
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        got = work_worktree.plan("write a report", "", str(tmp_path), [], [])
        assert got["rule"] == "R6"
        assert got["cwd"] == str(tmp_path)


# ------------------------------------------------------------------ ensure


class TestEnsure:
    def test_creates_a_worktree_on_a_branch_from_the_base(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("make a worktree")
        row = work_worktree.ensure(item_id, _spec(repo), "make a worktree")
        assert row["origin"] == "created"
        assert row["branch"] == f"work-{item_id}-make-a-worktree"
        assert git_util.is_worktree(row["path"])
        head = _git(row["path"], "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
        assert head == row["branch"]

    def test_is_idempotent(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("twice")
        first = work_worktree.ensure(item_id, _spec(repo), "twice")
        second = work_worktree.ensure(item_id, _spec(repo), "twice")
        assert first["path"] == second["path"]
        assert db.query_one(
            "SELECT COUNT(*) AS n FROM work_worktrees WHERE work_item_id = ?",
            (item_id,))["n"] == 1

    def test_reuses_the_existing_holder_of_the_branch(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("holder")
        branch = work_worktree.branch_name(None, item_id, "holder")
        elsewhere = tmp_path / "elsewhere"
        _git(repo, "worktree", "add", "-b", branch, str(elsewhere))
        row = work_worktree.ensure(item_id, _spec(repo), "holder")
        assert row["origin"] == "reused_holder"
        assert row["path"] == str(elsewhere)

    def test_prunes_a_stale_registration_instead_of_returning_a_dead_path(self, tmp_path,
                                                                         wt_root, monkeypatch):
        import shutil
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("stale")
        branch = work_worktree.branch_name(None, item_id, "stale")
        dead = tmp_path / "dead"
        _git(repo, "worktree", "add", "-b", branch, str(dead))
        shutil.rmtree(dead)
        row = work_worktree.ensure(item_id, _spec(repo), "stale")
        assert row["origin"] == "created"
        assert os.path.isdir(row["path"])
        assert row["path"] != str(dead)

    def test_never_returns_the_canonical_checkout_as_a_holder(self, tmp_path, wt_root,
                                                             monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("canonical")
        branch = work_worktree.branch_name(None, item_id, "canonical")
        _git(repo, "checkout", "-b", branch)
        before = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
        row = work_worktree.ensure(item_id, _spec(repo), "canonical")
        assert row["path"] != str(repo)
        assert row["branch"] == f"{branch}-2"
        after = _git(repo, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
        assert after == before

    def test_reuses_the_second_candidate_when_the_row_was_lost(self, tmp_path, wt_root,
                                                               monkeypatch):
        """A create interrupted between `worktree add` and the row insert must
        become the reuse case, not another failed add."""
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("lost row")
        branch = work_worktree.branch_name(None, item_id, "lost row")
        _git(repo, "checkout", "-b", branch)
        second = tmp_path / "second"
        _git(repo, "worktree", "add", "-b", f"{branch}-2", str(second))
        row = work_worktree.ensure(item_id, _spec(repo), "lost row")
        assert row["origin"] == "reused_holder"
        assert row["path"] == str(second)

    def test_leaves_the_shared_checkout_byte_identical(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        (repo / "dirty.txt").write_text("another agent's work\n")
        head_before = _git(repo, "rev-parse", "HEAD").stdout.strip()
        status_before = _git(repo, "status", "--porcelain").stdout
        item_id = _item("no touching")
        work_worktree.ensure(item_id, _spec(repo), "no touching")
        assert _git(repo, "rev-parse", "HEAD").stdout.strip() == head_before
        assert _git(repo, "status", "--porcelain").stdout == status_before
        assert (repo / "dirty.txt").read_text() == "another agent's work\n"

    def test_falls_back_when_the_worktree_cannot_be_created(self, tmp_path, wt_root,
                                                            monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("blocked")
        target = core_config.task_worktree_path(None, "proj", item_id, "app")
        target.mkdir(parents=True)
        (target / "in the way").write_text("x")
        assert work_worktree.ensure(item_id, _spec(repo), "blocked") == {}
        assert work_worktree.for_item(item_id) is None

    def test_a_repository_that_is_gone_is_reported_not_raised(self, tmp_path, wt_root,
                                                              monkeypatch):
        """Every git call against a directory that does not exist raises rather
        than reporting a status, and that would come out of the launch."""
        import shutil
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        spec = _spec(repo)
        shutil.rmtree(repo)
        assert work_worktree.ensure(_item("gone repo"), spec, "gone repo") == {}
        assert git_util.worktree_holding_branch(repo, "main") is None

    def test_a_project_whose_tree_is_gone_resolves_no_repository(self, tmp_path, wt_root,
                                                                 monkeypatch):
        """A projects directory can be missing: an unmounted disk, a renamed
        folder. That must not raise out of every launch."""
        root = tmp_path / "proj"
        root.mkdir()
        config = {"workspace": {"root": root, "projects_dir": "repos",
                                "tickets_dir": "tickets", "ticket_layout": "flat",
                                "base_branch": "main", "branch_prefix": "",
                                "exclude": [], "dep_commands": []}}
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        assert work_worktree._repos_of(config) == []
        got = work_worktree.plan("build it", "", str(root), ["proj"], [])
        assert got["rule"] == "R6"

    def test_two_concurrent_calls_produce_one_worktree(self, tmp_path, wt_root, monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("race")
        results = []
        barrier = threading.Barrier(2)

        def run():
            barrier.wait()
            results.append(work_worktree.ensure(item_id, _spec(repo), "race"))

        threads = [threading.Thread(target=run) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len(results) == 2
        assert results[0]["path"] == results[1]["path"]
        assert db.query_one(
            "SELECT COUNT(*) AS n FROM work_worktrees WHERE work_item_id = ?",
            (item_id,))["n"] == 1
        listed = _git(repo, "worktree", "list", "--porcelain").stdout
        assert listed.count("worktree ") == 2

    def test_ensure_for_repo_resolves_the_project_from_the_file(self, tmp_path, wt_root,
                                                                monkeypatch):
        root = tmp_path / "proj"
        root.mkdir()
        repo = make_repo(root, "app")
        config = project_config(root, ["app"], base="main")
        monkeypatch.setattr(work_worktree.runtime, "instances",
                            lambda: instances(proj=config))
        item_id = _item("from a file", contexts="proj")
        row = work_worktree.ensure_for_repo(item_id, str(repo / "README.md"),
                                            entries=[])
        assert row["project_key"] == "proj"
        assert row["repo_path"] == str(repo)
        assert git_util.is_worktree(row["path"])


# ------------------------------------------------------------------ gc


def _finish(item_id, archived=True, days_ago=0):
    when = (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()
    with db.tx() as c:
        c.execute("UPDATE work_items SET state = 'done', updated_at = ?, archived_at = ? "
                  "WHERE id = ?", (when, when if archived else None, item_id))


class TestGc:
    def _made(self, tmp_path, monkeypatch, objective="sweep me"):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item(objective)
        row = work_worktree.ensure(item_id, _spec(repo), objective)
        return repo, item_id, row

    def test_removes_a_clean_worktree_of_an_archived_task(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        gone = work_worktree.gc()
        assert [g["path"] for g in gone] == [row["path"]]
        assert not os.path.isdir(row["path"])
        branches = _git(repo, "branch", "--list").stdout
        assert row["branch"] not in branches
        assert work_worktree.for_item(item_id) is None

    def test_keeps_a_task_that_is_not_finished(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_finished_task_until_the_window_passes(self, tmp_path, wt_root,
                                                           monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id, archived=False, days_ago=1)
        assert work_worktree.gc() == []
        _finish(item_id, archived=False, days_ago=work_worktree.KEEP_FINISHED_DAYS + 1)
        assert work_worktree.gc() != []

    def test_keeps_a_worktree_holding_only_ignored_files(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        (Path(row["path"]) / ".gitignore").write_text("/notes/\n")
        _git(row["path"], "add", "-A")
        _git(row["path"], "-c", "user.name=t", "-c", "user.email=t@e.com",
             "commit", "-m", "ignore notes")
        _git(row["path"], "push", "-u", "origin", "HEAD:refs/heads/tmp-base")
        _git(repo, "fetch", "origin")
        with db.tx() as c:
            c.execute("UPDATE work_worktrees SET base_branch = 'tmp-base' WHERE id = ?",
                      (row["id"],))
        (Path(row["path"]) / "notes").mkdir()
        (Path(row["path"]) / "notes" / "plan.md").write_text("the plan\n")
        assert _git(row["path"], "status", "--porcelain").stdout.strip() == ""
        _finish(item_id)
        assert work_worktree.gc() == []
        assert (Path(row["path"]) / "notes" / "plan.md").is_file()

    def test_keeps_a_worktree_when_show_untracked_files_is_off(self, tmp_path, wt_root,
                                                              monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _git(row["path"], "config", "status.showUntrackedFiles", "no")
        (Path(row["path"]) / "scratch.md").write_text("uncommitted work\n")
        assert _git(row["path"], "status", "--porcelain", "--ignored").stdout.strip() == ""
        _finish(item_id)
        assert work_worktree.gc() == []
        assert (Path(row["path"]) / "scratch.md").is_file()

    def test_keeps_a_worktree_whose_head_is_detached(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _git(row["path"], "checkout", "--detach")
        (Path(row["path"]) / "x.txt").write_text("work\n")
        _git(row["path"], "add", "-A")
        _git(row["path"], "-c", "user.name=t", "-c", "user.email=t@e.com",
             "commit", "-m", "detached work")
        sha = _git(row["path"], "rev-parse", "HEAD").stdout.strip()
        _finish(item_id)
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])
        assert _git(repo, "cat-file", "-e", sha, check=False).returncode == 0

    def test_keeps_a_worktree_detached_at_its_own_base(self, tmp_path, wt_root, monkeypatch):
        """The branch check has to stand on its own. A HEAD detached at the
        base is not ahead of anything, so nothing else keeps this worktree."""
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _git(row["path"], "checkout", "--detach")
        assert _git(row["path"], "rev-list", "--count",
                    "origin/main..HEAD").stdout.strip() == "0"
        assert _git(row["path"], "status", "--porcelain", "--ignored").stdout.strip() == ""
        _finish(item_id)
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_dirty_worktree(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        (Path(row["path"]) / "README.md").write_text("edited\n")
        _finish(item_id)
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_branch_that_is_ahead_of_its_base(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        (Path(row["path"]) / "feature.txt").write_text("new\n")
        _git(row["path"], "add", "-A")
        _git(row["path"], "-c", "user.name=t", "-c", "user.email=t@e.com",
             "commit", "-m", "the work")
        _finish(item_id)
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_worktree_whose_task_still_has_a_live_session(self, tmp_path, wt_root,
                                                                  monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        monkeypatch.setattr(work_worktree.terminal, "session_healthy",
                            lambda key, agent="claude": {"alive": True, "agent_running": True})
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_worktree_a_live_task_is_running_in(self, tmp_path, wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        child = _item("the follow-up")
        work_store.add_run(child, f"sid-child-{child}", f"work-{child}", row["path"])
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_worktree_a_follow_up_finished_in_a_minute_ago(self, tmp_path, wt_root,
                                                                    monkeypatch):
        """A follow-up runs in its parent's worktree under R2 and records no
        row of its own. Archiving the parent must not delete a tree whose
        follow-up is still waiting to be acknowledged."""
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        child = _item("the follow-up")
        work_store.add_run(child, f"sid-fu-{child}", f"work-{child}", row["path"])
        _finish(child, archived=False, days_ago=0)
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_keeps_a_worktree_a_finished_follow_up_still_has_a_session_in(self, tmp_path,
                                                                          wt_root,
                                                                          monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        child = _item("the follow-up")
        work_store.add_run(child, f"sid-fu2-{child}", f"work-{child}", row["path"])
        _finish(child, archived=True)
        live = {f"work-{child}"}
        monkeypatch.setattr(
            work_worktree.terminal, "session_healthy",
            lambda key, agent="claude": {"alive": key in live, "agent_running": key in live})
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_removes_a_worktree_once_every_follow_up_is_old_enough(self, tmp_path, wt_root,
                                                                   monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        child = _item("the follow-up")
        work_store.add_run(child, f"sid-fu3-{child}", f"work-{child}", row["path"])
        _finish(child, archived=True)
        assert [g["path"] for g in work_worktree.gc()] == [row["path"]]

    def test_removes_a_branch_whose_commits_reached_the_base(self, tmp_path, wt_root,
                                                             monkeypatch):
        """A merged branch still looks ahead of a base ref this repository last
        fetched when the worktree was made, so gc has to ask the remote before
        it decides. The merge happens in another clone, which is how a pull
        request lands: nothing tells this repository that main moved."""
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        (Path(row["path"]) / "feature.txt").write_text("new\n")
        _git(row["path"], "add", "-A")
        _git(row["path"], "-c", "user.name=t", "-c", "user.email=t@e.com",
             "commit", "-m", "the work")
        _git(row["path"], "push", "origin", f"HEAD:refs/heads/{row['branch']}")

        elsewhere = tmp_path / "reviewer"
        _git(tmp_path, "clone", str(tmp_path / "app.git"), str(elsewhere))
        _git(elsewhere, "config", "user.name", "reviewer")
        _git(elsewhere, "config", "user.email", "reviewer@example.com")
        _git(elsewhere, "merge", f"origin/{row['branch']}", "--no-edit")
        _git(elsewhere, "push", "origin", "main")

        assert _git(row["path"], "rev-list", "--count",
                    "origin/main..HEAD").stdout.strip() == "1"
        _finish(item_id)
        assert [g["path"] for g in work_worktree.gc()] == [row["path"]]
        assert not os.path.isdir(row["path"])

    def test_keeps_a_worktree_a_task_is_running_in_a_subdirectory_of(self, tmp_path,
                                                                       wt_root,
                                                                       monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        child = _item("works in a subdirectory")
        sub = Path(row["path"]) / "src"
        sub.mkdir()
        work_store.add_run(child, f"sid-sub-{child}", f"work-{child}", str(sub))
        assert work_worktree.gc() == []
        assert os.path.isdir(row["path"])

    def test_a_reused_worktree_row_never_makes_the_sweep_remove_it(self, tmp_path,
                                                                   wt_root, monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        child = _item("the reuser")
        adopted = work_worktree.adopt_path(child, row["path"])
        assert adopted["origin"] == "reused_worktree"
        assert adopted["branch"] == row["branch"]
        _finish(item_id)
        assert work_worktree.gc() == []
        _finish(child)
        assert [g["path"] for g in work_worktree.gc()] == [row["path"]]

    def test_a_rebuilt_worktree_keeps_the_record_of_the_one_it_replaced(self, tmp_path,
                                                                          wt_root,
                                                                          monkeypatch):
        """The row that records a removal is history. A later worktree of the
        same task and repository must not overwrite it."""
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        assert work_worktree.gc() != []
        removed = db.query_one("SELECT removed_at FROM work_worktrees WHERE id = ?",
                               (row["id"],))
        assert removed["removed_at"]
        rebuilt = work_worktree.ensure(item_id, _spec(repo), "sweep me")
        assert rebuilt["id"] != row["id"]
        kept = db.query_one("SELECT removed_at FROM work_worktrees WHERE id = ?",
                            (row["id"],))
        assert kept is not None and kept["removed_at"] == removed["removed_at"]

    def test_rebuild_answers_for_the_repository_the_path_names(self, tmp_path, wt_root,
                                                               monkeypatch):
        """A task can hold a worktree of more than one repository. The path
        that went missing names which one has to come back."""
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        first = make_repo(tmp_path, "first")
        second = make_repo(tmp_path, "second")
        item_id = _item("two repositories")
        row_one = work_worktree.ensure(
            item_id, _spec(first, "first"), "two repositories")
        row_two = work_worktree.ensure(
            item_id, _spec(second, "second"), "two repositories")
        assert work_worktree.last_row(item_id)["path"] == row_two["path"]
        _git(first, "worktree", "remove", row_one["path"])
        back = work_worktree.rebuild(item_id, "two repositories", row_one["path"])
        assert back["repo_path"] == str(first)
        assert back["path"] == row_one["path"]
        assert os.path.isdir(row_two["path"])

    def test_rebuild_falls_back_to_the_task_row_when_no_path_matches(self, tmp_path,
                                                                     wt_root,
                                                                     monkeypatch):
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _git(repo, "worktree", "remove", row["path"])
        back = work_worktree.rebuild(item_id, "sweep me", str(tmp_path / "unknown"))
        assert back["repo_path"] == str(repo)
        assert os.path.isdir(back["path"])

    def test_rebuild_records_nothing_for_a_task_that_never_had_one(self, tmp_path,
                                                                   wt_root, monkeypatch):
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("never had one")
        assert work_worktree.rebuild(item_id, "never had one", str(tmp_path)) == {}

    def test_adopting_an_unknown_path_records_nothing(self, tmp_path, wt_root,
                                                      monkeypatch):
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        child = _item("nothing to adopt")
        assert work_worktree.adopt_path(child, str(tmp_path)) == {}
        assert work_worktree.for_item(child) is None

    def test_never_removes_a_worktree_frshty_did_not_create(self, tmp_path, wt_root,
                                                            monkeypatch):
        repo = make_repo(tmp_path)
        monkeypatch.setattr(work_worktree.runtime, "instances", lambda: None)
        item_id = _item("reused only")
        branch = work_worktree.branch_name(None, item_id, "reused only")
        elsewhere = tmp_path / "elsewhere"
        _git(repo, "worktree", "add", "-b", branch, str(elsewhere))
        row = work_worktree.ensure(item_id, _spec(repo), "reused only")
        assert row["origin"] == "reused_holder"
        _finish(item_id)
        assert work_worktree.gc() == []
        assert os.path.isdir(elsewhere)

    def test_reclaims_a_row_whose_directory_vanished(self, tmp_path, wt_root, monkeypatch):
        import shutil
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        shutil.rmtree(row["path"])
        _finish(item_id)
        gone = work_worktree.gc()
        assert [g["result"] for g in gone] == ["already gone"]
        assert work_worktree.for_item(item_id) is None

    def test_gc_and_a_resume_cannot_interleave(self, tmp_path, wt_root, monkeypatch):
        """gc must not remove the directory between a resume resolving it and
        tmux opening it. Both hold work_store.launch_lock, so the sweep blocks
        while the resume runs."""
        repo, item_id, row = self._made(tmp_path, monkeypatch)
        _finish(item_id)
        seen = {}
        released = threading.Event()

        def slow_resume():
            with work_store.launch_lock:
                seen["at_entry"] = os.path.isdir(row["path"])
                time.sleep(0.4)
                seen["at_exit"] = os.path.isdir(row["path"])
                released.set()

        holder = threading.Thread(target=slow_resume)
        holder.start()
        time.sleep(0.1)
        sweep = threading.Thread(target=work_worktree.gc)
        sweep.start()
        holder.join()
        sweep.join()
        assert seen == {"at_entry": True, "at_exit": True}
        assert released.is_set()
        assert not os.path.isdir(row["path"])
