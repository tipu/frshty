import os
import time

from features import tmp_gc

OLD = time.time() - tmp_gc.STALE_S - 3600
SO = ".bcfbd5ebf9df23c0-00000000.so"


def _age(path, age=OLD):
    os.utime(path, (age, age))


def _tree(tmp_path):
    root = tmp_path / "tmp"
    root.mkdir()
    proc = tmp_path / "proc"
    proc.mkdir()
    runs = root / "pytest-of-claude"
    runs.mkdir()
    for n in (1, 2, 3):
        run = runs / f"pytest-{n}"
        (run / "test_x0").mkdir(parents=True)
        (run / "test_x0" / "f").write_bytes(b"x" * 100)
        _age(run)
    os.symlink(runs / "pytest-3", runs / "pytest-current")
    fresh = runs / "pytest-4"
    fresh.mkdir()
    for name in (SO, ".bcfbf79eff85b2fe-00000000.so"):
        (root / name).write_bytes(b"x" * 10)
        _age(root / name)
    (root / ".bcfbe71eb595f77e-00000000.so").write_bytes(b"x")
    for name in ("claude-1000", "cc-socks", "frshty-xauth", ".X11-unix", "other-dir"):
        (root / name).mkdir()
        _age(root / name)
    (root / "plain.so").write_bytes(b"x")
    _age(root / "plain.so")
    config = {"job": {"key": "test"}, "_state_dir": tmp_path / "state"}
    return root, proc, config


def _process(proc, pid, cwd=None, fds=(), maps=()):
    base = proc / str(pid)
    (base / "fd").mkdir(parents=True)
    if cwd:
        os.symlink(cwd, base / "cwd")
    for i, target in enumerate(fds):
        os.symlink(target, base / "fd" / str(i))
    (base / "maps").write_text("".join(
        f"7f00-7f01 r-xp 00000000 00:2b 12 {m}\n" for m in maps)
        + "7f02-7f03 rw-p 00000000 00:00 0 \n")


def test_removes_stale_runs_and_libraries_and_keeps_everything_else(tmp_path):
    root, proc, config = _tree(tmp_path)
    result = tmp_gc.sweep(config, force=True, root=str(root), proc=str(proc))
    assert result["removed"] == sorted([str(root / SO), str(root / ".bcfbf79eff85b2fe-00000000.so"),
                                        str(root / "pytest-of-claude" / "pytest-1"),
                                        str(root / "pytest-of-claude" / "pytest-2")])
    assert result["freed_bytes"] == 220
    left = sorted(os.listdir(root))
    assert left == sorted([".X11-unix", ".bcfbe71eb595f77e-00000000.so", "cc-socks", "claude-1000",
                           "frshty-xauth", "other-dir", "plain.so", "pytest-of-claude"])
    assert sorted(os.listdir(root / "pytest-of-claude")) == ["pytest-3", "pytest-4", "pytest-current"]


def test_keeps_paths_a_live_process_uses(tmp_path):
    root, proc, config = _tree(tmp_path)
    runs = root / "pytest-of-claude"
    _process(proc, 10, cwd=str(runs / "pytest-1" / "test_x0"))
    _process(proc, 11, fds=[str(runs / "pytest-2" / "test_x0" / "f")])
    _process(proc, 12, maps=[str(root / SO)])
    result = tmp_gc.sweep(config, force=True, root=str(root), proc=str(proc))
    assert result["removed"] == [str(root / ".bcfbf79eff85b2fe-00000000.so")]
    assert set(result["kept"]) == {str(runs / "pytest-1"), str(runs / "pytest-2"), str(root / SO)}
    assert (runs / "pytest-1").exists() and (runs / "pytest-2").exists() and (root / SO).exists()


def test_dry_run_removes_nothing(tmp_path):
    root, proc, config = _tree(tmp_path)
    result = tmp_gc.sweep(config, dry_run=True, root=str(root), proc=str(proc))
    assert len(result["removed"]) == 4
    assert (root / SO).exists() and (root / "pytest-of-claude" / "pytest-1").exists()


def test_throttles_within_the_interval(tmp_path):
    root, proc, config = _tree(tmp_path)
    tmp_gc.sweep(config, root=str(root), proc=str(proc))
    assert tmp_gc.sweep(config, root=str(root), proc=str(proc)) == {"throttled": True}


def test_keeps_a_run_whose_lock_names_a_live_pid(tmp_path):
    root, proc, config = _tree(tmp_path)
    runs = root / "pytest-of-claude"
    (runs / "pytest-1" / ".lock").write_text(str(os.getpid()))
    (runs / "pytest-2" / ".lock").write_text("999999999")
    _age(runs / "pytest-1")
    _age(runs / "pytest-2")
    result = tmp_gc.sweep(config, force=True, root=str(root), proc=str(proc))
    assert str(runs / "pytest-2") in result["removed"]
    assert str(runs / "pytest-1") not in result["removed"]
    assert (runs / "pytest-1").exists()


def test_removes_nothing_when_a_live_process_is_unreadable(tmp_path):
    root, proc, config = _tree(tmp_path)
    _process(proc, 10)
    os.remove(proc / "10" / "maps")
    (proc / "10" / "maps").mkdir()
    result = tmp_gc.sweep(config, force=True, root=str(root), proc=str(proc))
    assert result["removed"] == []
    assert "stopped" in result
    assert (root / SO).exists() and (root / "pytest-of-claude" / "pytest-1").exists()


def test_reads_maps_with_a_non_utf8_path(tmp_path):
    root, proc, config = _tree(tmp_path)
    _process(proc, 10, maps=[str(root / SO)])
    with open(proc / "10" / "maps", "ab") as f:
        f.write(b"7f04-7f05 r-xp 00000000 00:2b 13 /tmp/bad\xff.so\n")
    (root / "pytest-of-claude" / "pytest-1" / ".lock").write_text("99999999999999999999999")
    _age(root / "pytest-of-claude" / "pytest-1")
    result = tmp_gc.sweep(config, force=True, root=str(root), proc=str(proc))
    assert str(root / SO) in result["kept"]
    assert str(root / "pytest-of-claude" / "pytest-1") in result["removed"]
