import os
import time

import core.db as db
from services import work_artifacts, work_store

DAY = 86400


def _age(path, days):
    old = time.time() - days * DAY
    for dirpath, dirnames, filenames in os.walk(path, topdown=False):
        for name in filenames + dirnames:
            os.utime(os.path.join(dirpath, name), (old, old))
    os.utime(path, (old, old))


def _row(path: str) -> int:
    item_id = work_store.create_item("artifact gc test")
    with db.tx() as c:
        cur = c.execute(
            "INSERT INTO work_artifacts(work_item_id, work_run_id, path, note, created_at) "
            "VALUES (?, NULL, ?, '', '2026-08-01T00:00:00Z')", (item_id, path))
        return int(cur.lastrowid)


def _root(tmp_path, monkeypatch):
    root = tmp_path / "artifacts"
    root.mkdir()
    monkeypatch.setattr(work_artifacts, "root", lambda: root)
    monkeypatch.setattr(work_artifacts, "SCRATCH_PREFIX",
                        str(tmp_path / "scratch") + os.sep)
    return root


class TestItemDir:
    def test_creates_folder_per_item(self, tmp_path, monkeypatch):
        root = _root(tmp_path, monkeypatch)
        folder = work_artifacts.item_dir(4242)
        assert folder == root / "work-4242"
        assert folder.is_dir()
        assert work_artifacts.item_dir(4242) == folder


class TestGc:
    def test_deletes_old_folder_and_keeps_fresh_one(self, tmp_path, monkeypatch):
        root = _root(tmp_path, monkeypatch)
        old = work_artifacts.item_dir(1)
        (old / "report.html").write_text("old")
        _age(old, 40)
        fresh = work_artifacts.item_dir(2)
        (fresh / "report.html").write_text("fresh")
        result = work_artifacts.gc_artifacts()
        assert result["removed"] == ["work-1"]
        assert not old.exists()
        assert fresh.is_dir()
        assert (root / ".gc-stamp").exists()

    def test_recent_nested_file_keeps_the_folder(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        folder = work_artifacts.item_dir(3)
        (folder / "sub").mkdir()
        (folder / "sub" / "page.html").write_text("x")
        _age(folder, 40)
        recent = folder / "sub" / "page.html"
        os.utime(recent, None)
        assert work_artifacts.gc_artifacts()["removed"] == []
        assert folder.is_dir()

    def test_forgets_rows_under_the_deleted_folder(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        folder = work_artifacts.item_dir(5)
        (folder / "report.html").write_text("old")
        _age(folder, 40)
        gone = _row(str(folder / "report.html"))
        kept = _row(str(tmp_path / "elsewhere" / "report.html"))
        result = work_artifacts.gc_artifacts()
        assert result["removed"] == ["work-5"]
        assert result["forgotten"] == 1
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (gone,)) is None
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (kept,))

    def test_throttled_within_the_interval(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        assert work_artifacts.gc_artifacts()["removed"] == []
        folder = work_artifacts.item_dir(6)
        (folder / "report.html").write_text("old")
        _age(folder, 40)
        assert work_artifacts.gc_artifacts()["throttled"] is True
        assert folder.is_dir()
        later = time.time() + work_artifacts.GC_INTERVAL_S + 1
        assert work_artifacts.gc_artifacts(now=later)["removed"] == ["work-6"]

    def test_forgets_an_old_row_whose_scratch_file_is_gone(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        scratch = tmp_path / "scratch"
        scratch.mkdir()
        here = scratch / "still-here.html"
        here.write_text("here")
        kept = _row(str(here))
        lost = _row(str(scratch / "gone.html"))
        fresh = _row(str(scratch / "also-gone.html"))
        with db.tx() as c:
            c.execute("UPDATE work_artifacts SET created_at = ? WHERE id = ?",
                      ("2099-01-01T00:00:00+00:00", fresh))
        assert work_artifacts.gc_artifacts()["forgotten"] == 1
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (lost,)) is None
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (kept,))
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (fresh,))

    def test_creates_the_store_when_absent(self, tmp_path, monkeypatch):
        store = tmp_path / "absent"
        monkeypatch.setattr(work_artifacts, "root", lambda: store)
        monkeypatch.setattr(work_artifacts, "SCRATCH_PREFIX",
                            str(tmp_path / "scratch") + os.sep)
        assert work_artifacts.gc_artifacts() == {"removed": [], "forgotten": 0}
        assert store.is_dir()
