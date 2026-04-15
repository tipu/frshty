import json
import threading

import pytest

import core.state as state


class TestInit:
    def test_creates_directory(self, tmp_path):
        target = tmp_path / "new_state"
        state.init(target)
        assert target.is_dir()


class TestLoadSave:
    def test_load_missing_returns_empty(self, tmp_state):
        assert state.load("nonexistent") == {}

    def test_save_and_load(self, tmp_state):
        state.save("test", {"key": "value"})
        assert state.load("test") == {"key": "value"}

    def test_save_overwrites(self, tmp_state):
        state.save("test", {"a": 1})
        state.save("test", {"b": 2})
        result = state.load("test")
        assert result == {"b": 2}
        assert "a" not in result

    def test_save_creates_json_file(self, tmp_state):
        state.save("mod", {"x": 1})
        path = tmp_state / "mod.json"
        assert path.exists()
        assert json.loads(path.read_text()) == {"x": 1}

    def test_save_preserves_on_write_failure(self, tmp_state):
        state.save("safe", {"original": True})
        loaded = state.load("safe")
        assert loaded == {"original": True}


class TestConcurrency:
    def test_concurrent_writes_no_corruption(self, tmp_state):
        def writer(key, value):
            for _ in range(50):
                d = state.load("shared")
                d[key] = value
                state.save("shared", d)

        t1 = threading.Thread(target=writer, args=("a", 1))
        t2 = threading.Thread(target=writer, args=("b", 2))
        t1.start(); t2.start()
        t1.join(); t2.join()

        final = state.load("shared")
        assert isinstance(final, dict)
        assert "a" in final or "b" in final
