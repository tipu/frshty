import importlib.util
import sqlite3
import subprocess
from pathlib import Path

SPEC = importlib.util.spec_from_file_location(
    "instance_script", Path(__file__).resolve().parent.parent / "scripts" / "instance.py")
instance = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(instance)


def write_config(root: Path, key: str) -> Path:
    path = root / key / "config" / f"{key}.toml"
    path.parent.mkdir(parents=True)
    path.write_text(f'[job]\nkey = "{key}"\nport = 7131\n')
    return path


def setup(monkeypatch, tmp_path, existing: str, up_code: int = 0):
    monkeypatch.setattr(instance, "CONTAINERS_ROOT", tmp_path)
    monkeypatch.setattr(instance.subprocess, "run", lambda cmd, **kw: subprocess.CompletedProcess(
        cmd, 0, stdout=existing, stderr=""))
    ups = []

    def start(config, config_path, capture=False):
        ups.append((config["job"]["key"], config_path, capture))
        return subprocess.CompletedProcess([], up_code, stdout="", stderr="boom" if up_code else "")

    monkeypatch.setattr(instance, "start", start)
    return ups


def test_ensure_runs_up_only_for_a_container_that_does_not_exist(monkeypatch, tmp_path, capsys):
    for key in ("alpha", "beta", "gamma"):
        write_config(tmp_path, key)
    ups = setup(monkeypatch, tmp_path, "frshty-alpha\nfrshty-gamma\n")
    assert instance.ensure() == 0
    assert ups == [("beta", tmp_path / "beta" / "config" / "beta.toml", True)]
    out = capsys.readouterr().out.strip().splitlines()
    assert out == [f"ensure: Container frshty-beta did not exist; ensure recreated it from "
                   f"{tmp_path / 'beta' / 'config' / 'beta.toml'}"]


def test_ensure_skips_an_instance_with_the_down_marker(monkeypatch, tmp_path):
    write_config(tmp_path, "alpha")
    (tmp_path / "alpha" / instance.DOWN_MARKER).write_text("x\n")
    ups = setup(monkeypatch, tmp_path, "")
    assert instance.ensure() == 0
    assert ups == []


def test_ensure_fails_when_up_fails(monkeypatch, tmp_path, capsys):
    write_config(tmp_path, "alpha")
    setup(monkeypatch, tmp_path, "", up_code=125)
    assert instance.ensure() == 1
    assert "frshty-alpha did not exist" in capsys.readouterr().err


def test_ensure_writes_a_feed_event(monkeypatch, tmp_path):
    write_config(tmp_path, "alpha")
    state = tmp_path / "alpha" / "state"
    state.mkdir()
    db = sqlite3.connect(state / "frshty.db")
    db.execute((Path(__file__).resolve().parent.parent / "migrations" / "002_log_supervisor.sql").read_text().split(";")[0])
    db.close()
    monkeypatch.setattr(instance, "db_volume", lambda config: None)
    setup(monkeypatch, tmp_path, "")
    assert instance.ensure() == 0
    rows = sqlite3.connect(state / "frshty.db").execute(
        "SELECT instance_key, job, event FROM log_events").fetchall()
    assert rows == [("alpha", "alpha", "container_recreated")]


def test_down_writes_the_marker_and_up_removes_it(monkeypatch, tmp_path):
    monkeypatch.setattr(instance, "CONTAINERS_ROOT", tmp_path)
    calls = []
    monkeypatch.setattr(instance.subprocess, "run", lambda cmd, **kw: calls.append(cmd) or
                        subprocess.CompletedProcess(cmd, 0, stdout="", stderr=""))
    monkeypatch.setattr(instance, "run_args", lambda config, path, check: ["docker", "run"])
    monkeypatch.setattr(instance, "prepare_db_volume", lambda *a, **kw: None)
    config = {"job": {"key": "alpha"}}
    marker = tmp_path / "alpha" / instance.DOWN_MARKER
    assert instance.down(config) == 0
    assert marker.exists()
    assert ["docker", "rm", "-f", "frshty-alpha"] in calls
    assert instance.up(config, tmp_path / "alpha.toml").returncode == 0
    assert not marker.exists()
