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
    monkeypatch.setattr(work_artifacts, "legacy_root",
                        lambda: tmp_path / "legacy")
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


MB = 1024 * 1024


def _write(path, size):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"x" * size)
    return path


class TestGc:
    def test_old_folder_keeps_its_report_and_loses_its_video(self, tmp_path, monkeypatch):
        root = _root(tmp_path, monkeypatch)
        old = work_artifacts.item_dir(1)
        report = _write(old / "report.html", 10)
        video = _write(old / "demo.mp4", 10)
        report_row = _row(str(report))
        video_row = _row(str(video))
        _age(old, 40)
        fresh = work_artifacts.item_dir(2)
        fresh_video = _write(fresh / "demo.mp4", 10)
        result = work_artifacts.gc_artifacts()
        assert result["removed"] == [str(video)]
        assert result["forgotten"] == 1
        assert old.is_dir()
        assert report.exists()
        assert not video.exists()
        assert fresh_video.exists()
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (report_row,))
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (video_row,)) is None
        assert (root / ".gc-stamp").exists()

    def test_size_and_type_rules(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        old = work_artifacts.item_dir(3)
        kept = [_write(old / "big.html", 2 * MB),
                _write(old / "big.md", 2 * MB),
                _write(old / "small.png", 500 * 1024),
                _write(old / "small.json", 500 * 1024),
                _write(old / "sub" / "frame.jpg", 500 * 1024)]
        gone = [_write(old / "big.png", 2 * MB),
                _write(old / "big.json", 2 * MB),
                _write(old / "repo.bundle", 10),
                _write(old / "sub" / "pack-1.pack", 10),
                _write(old / "sub" / "pack-1.idx", 10),
                _write(old / "sub" / "pack-1.rev", 10),
                _write(old / "sub" / "clip.WEBM", 10)]
        _age(old, 40)
        result = work_artifacts.gc_artifacts()
        assert sorted(result["removed"]) == sorted(str(p) for p in gone)
        assert all(p.exists() for p in kept)
        assert not any(p.exists() for p in gone)
        assert (old / "sub").is_dir()

    def test_old_folder_that_ends_up_empty_stays(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        old = work_artifacts.item_dir(4)
        _write(old / "demo.mp4", 10)
        (old / "empty").mkdir()
        _age(old, 40)
        work_artifacts.gc_artifacts()
        assert old.is_dir()
        assert list(old.iterdir()) == [old / "empty"]

    def test_never_follows_a_symlink(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        outside = tmp_path / "outside"
        target = _write(outside / "demo.mp4", 10)
        old = work_artifacts.item_dir(10)
        (old / "link.mp4").symlink_to(target)
        (old / "linkdir").symlink_to(outside, target_is_directory=True)
        _write(old / "report.html", 10)
        _age(old, 40)
        stamp = time.time() - 40 * DAY
        for link in ("link.mp4", "linkdir"):
            os.utime(old / link, (stamp, stamp), follow_symlinks=False)
        assert work_artifacts.gc_artifacts()["removed"] == []
        assert target.exists()
        assert (old / "link.mp4").is_symlink()

    def test_recent_nested_file_keeps_the_folder(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        folder = work_artifacts.item_dir(3)
        _write(folder / "sub" / "demo.mp4", 10)
        _age(folder, 40)
        recent = folder / "sub" / "demo.mp4"
        os.utime(recent, None)
        assert work_artifacts.gc_artifacts()["removed"] == []
        assert recent.exists()

    def test_sweeps_the_legacy_store(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        legacy = tmp_path / "legacy"
        old = legacy / "work-7"
        report = _write(old / "report.html", 10)
        video = _write(old / "demo.mp4", 10)
        _age(old, 40)
        fresh = legacy / "work-8"
        fresh_video = _write(fresh / "demo.mp4", 10)
        _write(legacy / "stray.mp4", 10)
        _age(legacy / "stray.mp4", 40)
        gone = _row(str(video))
        kept = _row(str(report))
        result = work_artifacts.gc_artifacts()
        assert result["removed"] == [str(video)]
        assert result["forgotten"] == 1
        assert report.exists()
        assert fresh_video.exists()
        assert (legacy / "stray.mp4").exists()
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (gone,)) is None
        assert db.query_one("SELECT id FROM work_artifacts WHERE id = ?", (kept,))

    def test_store_that_is_the_legacy_store_is_swept_once(self, tmp_path, monkeypatch):
        root = _root(tmp_path, monkeypatch)
        monkeypatch.setattr(work_artifacts, "legacy_root", lambda: root)
        old = work_artifacts.item_dir(9)
        video = _write(old / "demo.mp4", 10)
        _age(old, 40)
        assert work_artifacts.gc_artifacts()["removed"] == [str(video)]

    def test_throttled_within_the_interval(self, tmp_path, monkeypatch):
        _root(tmp_path, monkeypatch)
        assert work_artifacts.gc_artifacts()["removed"] == []
        folder = work_artifacts.item_dir(6)
        video = _write(folder / "demo.mp4", 10)
        _age(folder, 40)
        assert work_artifacts.gc_artifacts()["throttled"] is True
        assert video.exists()
        later = time.time() + work_artifacts.GC_INTERVAL_S + 1
        assert work_artifacts.gc_artifacts(now=later)["removed"] == [str(video)]

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
        monkeypatch.setattr(work_artifacts, "legacy_root",
                            lambda: tmp_path / "legacy")
        monkeypatch.setattr(work_artifacts, "SCRATCH_PREFIX",
                            str(tmp_path / "scratch") + os.sep)
        assert work_artifacts.gc_artifacts() == {"removed": [], "forgotten": 0}
        assert store.is_dir()
