"""The plugin config check: which files it calls unreadable, and when it
tells the feed."""
import json
from unittest.mock import patch

import pytest

import core.state as state
from services import claude_config


@pytest.fixture(autouse=True)
def _isolated(tmp_path, monkeypatch):
    state.init(tmp_path / "state")
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(tmp_path / "claude"))
    (tmp_path / "claude" / "plugins").mkdir(parents=True)


def _write(name, text):
    (claude_config.config_dir() / "plugins" / name).write_text(text)


def test_valid_and_missing_files_are_fine():
    _write("known_marketplaces.json", json.dumps({"m": {"installLocation": "/x"}}))
    assert claude_config.problems() == []


@pytest.mark.parametrize("text,needle", [(b"", "empty"), (b'{"m": ', "invalid JSON"),
                                         (b"[]", "JSON list"), (b'{"m": NaN}', "invalid JSON"),
                                         (b'{"m": "\xff"}', "invalid JSON")])
def test_unreadable_file_is_named(text, needle):
    (claude_config.config_dir() / "plugins" / "known_marketplaces.json").write_bytes(text)
    found = claude_config.problems()
    assert len(found) == 1
    assert "known_marketplaces.json" in found[0] and needle in found[0]


def test_feed_hears_once_when_broken_and_once_when_fixed():
    _write("known_marketplaces.json", "")
    with patch.object(claude_config.log, "emit") as emit:
        claude_config.check()
        claude_config.check()
        _write("known_marketplaces.json", "{}")
        claude_config.check()
        claude_config.check()
    assert [c.args[0] for c in emit.call_args_list] == ["claude_config_invalid",
                                                        "claude_config_ok"]
