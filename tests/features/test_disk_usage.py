"""The disk report: which idle folders it suggests clearing first, and when it
calls the usage high."""
import os
import time
from collections import namedtuple
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

import core.db as db
import core.state as state
from services import disk_usage, work_store
from tests.features.test_work_worktree import make_repo, _git

DAY = 86400
NOW = time.time()
Usage = namedtuple("Usage", "total used free")
MIN_SUGGEST_BYTES = disk_usage.MIN_SUGGEST_BYTES


@pytest.fixture(autouse=True)
def _isolated(tmp_path, fresh_db):
    state.init(tmp_path / "state")
    disk_usage._report = None
    with patch.object(disk_usage.shutil, "disk_usage", return_value=Usage(100, 50, 50)), \
            patch.object(disk_usage, "MIN_SUGGEST_BYTES", 0):
        yield
    disk_usage._report = None


def _folder(path, kb, age_days):
    path.mkdir(parents=True, exist_ok=True)
    (path / "blob.bin").write_bytes(b"x" * kb * 1024)
    stamp = NOW - age_days * DAY
    for root, dirs, files in os.walk(path):
        for name in dirs + files:
            os.utime(os.path.join(root, name), (stamp, stamp))
    os.utime(path, (stamp, stamp))
    return path


def _item(state_name, age_days):
    item_id = work_store.create_item("a task")
    when = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    with db.tx() as c:
        c.execute("UPDATE work_items SET state = ?, updated_at = ? WHERE id = ?",
                  (state_name, when, item_id))
    return item_id


def test_suggests_idle_unused_folders_largest_first(tmp_path):
    root = tmp_path / "root"
    done = _item("done", 10)
    working = _item("agent_working", 10)
    recent = _item("done", 0)
    _folder(root / "worktrees" / "app" / f"work-{done}", 64, 10)
    _folder(root / "worktrees" / "app" / f"work-{working}", 512, 10)
    _folder(root / "worktrees" / "app" / f"work-{recent}", 512, 10)
    _folder(root / "worktrees" / "app" / "work-999", 32, 10)
    _folder(root / "artifacts" / f"work-{done}", 128, 10)
    _folder(root / "artifacts" / "work-998", 16, 1)
    _folder(root / "launch", 1024, 30)

    report = disk_usage.scan(NOW, base=root)

    names = [(u["kind"], u["name"]) for u in report["suggestions"]]
    assert names == [("artifact", f"work-{done}"), ("worktree", f"work-{done}"),
                     ("worktree", "work-999")]
    assert report["suggestions"][1]["items"] == [{"id": done, "state": "done"}]
    assert report["suggestions"][2]["items"] == []
    assert report["reclaimable_count"] == 3
    assert report["by_kind"]["other"] >= 1024 * 1024
    assert report["total_bytes"] == sum(report["by_kind"].values())
    assert report["high"] is False


def test_a_follow_up_run_inside_a_folder_keeps_it(tmp_path):
    root = tmp_path / "root"
    parent = _item("done", 10)
    follow_up = _item("needs_you", 10)
    unit = _folder(root / "worktrees" / "app" / f"work-{parent}", 64, 10)
    work_store.add_run(follow_up, "sid-follow", f"work-{follow_up}", str(unit / "app"))

    report = disk_usage.scan(NOW, base=root)

    assert report["suggestions"] == []


def test_counts_uncommitted_files_of_a_worktree(tmp_path):
    root = tmp_path / "root"
    repo = make_repo(tmp_path)
    done = _item("done", 10)
    unit = root / "worktrees" / "app" / f"work-{done}"
    unit.mkdir(parents=True)
    _git(repo, "worktree", "add", "-b", "task", str(unit / "app"))
    (unit / "app" / "draft.txt").write_text("unsaved work\n")
    _folder(unit, 8, 10)

    report = disk_usage.scan(NOW, base=root)

    assert report["suggestions"][0]["uncommitted"] == 1


def test_high_when_frshty_holds_too_much_or_the_disk_is_full(tmp_path):
    root = tmp_path / "root"
    _folder(root / "launch", 64, 0)
    with patch.object(disk_usage, "WARN_BYTES", 1024):
        report = disk_usage.scan(NOW, base=root)
    assert report["high"] is True
    assert report["reasons"][0].startswith("frshty holds")

    with patch.object(disk_usage.shutil, "disk_usage", return_value=Usage(100, 95, 5)):
        report = disk_usage.scan(NOW, base=root)
    assert report["high"] is True
    assert report["reasons"] == ["the disk has 5.0% free, under 10%"]


def test_refresh_alerts_once_when_high_and_once_when_it_clears(tmp_path):
    root = tmp_path / "root"
    _folder(root / "artifacts" / "work-5", 64, 10)
    full = Usage(100, 95, 5)
    with patch.object(disk_usage, "frshty_root", return_value=root), \
            patch.object(disk_usage.log, "emit") as emit:
        with patch.object(disk_usage.shutil, "disk_usage", return_value=full):
            disk_usage.refresh(now=NOW)
            disk_usage.refresh(force=True, now=NOW + 1)
        disk_usage.refresh(force=True, now=NOW + 2)
        disk_usage.refresh(force=True, now=NOW + 3)

    events = [call.args[0] for call in emit.call_args_list]
    assert events == ["disk_usage_high", "disk_usage_ok"]
    assert "artifact work-5" in emit.call_args_list[0].args[1]


def test_refresh_reuses_a_recent_scan(tmp_path):
    root = tmp_path / "root"
    root.mkdir()
    with patch.object(disk_usage, "frshty_root", return_value=root), \
            patch.object(disk_usage, "scan", wraps=disk_usage.scan) as scan:
        first = disk_usage.refresh(now=NOW)
        assert disk_usage.refresh(now=NOW + 60) is first
        disk_usage.refresh(now=NOW + disk_usage.SCAN_INTERVAL_S + 1)
    assert scan.call_count == 2


def test_scan_deletes_nothing(tmp_path):
    root = tmp_path / "root"
    unit = _folder(root / "worktrees" / "app" / "work-777", 64, 30)
    disk_usage.scan(NOW, base=root)
    assert (unit / "blob.bin").exists()


def test_a_folder_without_git_reports_no_uncommitted_count(tmp_path):
    root = tmp_path / "root"
    _folder(root / "worktrees" / "app" / "work-778", 64, 30)
    report = disk_usage.scan(NOW, base=root)
    assert "uncommitted" not in report["suggestions"][0]


def test_an_unfinished_run_inside_an_orphaned_folder_keeps_it(tmp_path):
    root = tmp_path / "root"
    waiting = _item("needs_you", 10)
    unit = _folder(root / "orphaned-worktrees" / "recovered", 64, 10)
    work_store.add_run(waiting, "sid-orphan", f"work-{waiting}", str(unit))

    report = disk_usage.scan(NOW, base=root)

    assert report["suggestions"] == []


def test_suggests_only_folders_over_the_minimum_size(tmp_path):
    root = tmp_path / "root"
    _folder(root / "artifacts" / "work-901", 64, 10)
    _folder(root / "artifacts" / "work-902", 256, 10)
    with patch.object(disk_usage, "MIN_SUGGEST_BYTES", 128 * 1024):
        report = disk_usage.scan(NOW, base=root)
    assert [u["name"] for u in report["suggestions"]] == ["work-902"]
    assert report["reclaimable_count"] == 1
    assert report["reclaimable_bytes"] == report["suggestions"][0]["bytes"]
    assert report["min_bytes"] == 128 * 1024


def test_suggests_nothing_of_100_mb_or_less(tmp_path):
    root = tmp_path / "root"
    _folder(root / "artifacts" / "work-903", 1, 10)
    _folder(root / "artifacts" / "work-904", 1, 10)
    sizes = {"work-903": 100 * 1024 ** 2, "work-904": 100 * 1024 ** 2 + 1}

    def measure(path):
        return sizes.get(path.name, 0), NOW - 10 * DAY

    with patch.object(disk_usage, "MIN_SUGGEST_BYTES", MIN_SUGGEST_BYTES), \
            patch.object(disk_usage, "_measure", side_effect=measure):
        report = disk_usage.scan(NOW, base=root)
    assert [u["name"] for u in report["suggestions"]] == ["work-904"]
