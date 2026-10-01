from pathlib import Path

from core.paths import db_path


def test_db_path_follows_frshty_db(monkeypatch):
    monkeypatch.setenv("FRSHTY_ROOT", "/boxes/mac/state")
    monkeypatch.setenv("FRSHTY_DB", "/var/lib/frshty/frshty.db")
    assert db_path() == Path("/var/lib/frshty/frshty.db")


def test_db_path_defaults_to_the_state_tree(monkeypatch):
    monkeypatch.setenv("FRSHTY_ROOT", "/boxes/mac/state")
    monkeypatch.delenv("FRSHTY_DB", raising=False)
    assert db_path() == Path("/boxes/mac/state/frshty.db")
