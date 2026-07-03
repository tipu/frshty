import json
import time

import pytest

from features import presentation


@pytest.fixture
def cache(tmp_path):
    return tmp_path / "presentation_cache.json"


def _spawn(cache_file, sha="abc", auto=True, build_fn=None, key=None):
    calls = []

    def default_build():
        calls.append(1)
        return {"steps": []}

    result = presentation._spawn(
        key or ("ticket", "T-1"), cache_file, sha, auto, build_fn or default_build)
    for _ in range(200):
        if not presentation.is_building(key or ("ticket", "T-1")):
            break
        time.sleep(0.02)
    return result, calls


class TestSpawnDecision:
    def test_missing_cache_builds_and_writes_meta(self, cache):
        result, calls = _spawn(cache)
        assert result == "started"
        assert calls
        assert json.loads(cache.read_text()) == {"steps": []}
        meta = presentation.read_meta(cache)
        assert meta["status"] == "ok"
        assert meta["sha"] == "abc"

    def test_auto_skips_when_sha_matches(self, cache):
        _spawn(cache, sha="abc")
        result, calls = _spawn(cache, sha="abc")
        assert result == "cached"
        assert not calls

    def test_auto_rebuilds_when_sha_moves(self, cache):
        _spawn(cache, sha="abc")
        result, calls = _spawn(cache, sha="def")
        assert result == "started"
        assert calls
        assert presentation.read_meta(cache)["sha"] == "def"

    def test_auto_adopts_pre_meta_cache_without_rebuild(self, cache):
        cache.write_text("{}")
        result, calls = _spawn(cache, sha="abc")
        assert result == "cached"
        assert not calls
        assert presentation.read_meta(cache)["sha"] == "abc"

    def test_failed_build_not_retried_for_same_sha(self, cache):
        def boom():
            raise RuntimeError("llm down")
        result, _ = _spawn(cache, sha="abc", build_fn=boom)
        assert result == "started"
        assert presentation.read_meta(cache)["status"] == "failed"
        result, calls = _spawn(cache, sha="abc")
        assert result == "failed"
        assert not calls

    def test_failed_build_retried_on_new_sha(self, cache):
        def boom():
            raise RuntimeError("llm down")
        _spawn(cache, sha="abc", build_fn=boom)
        result, calls = _spawn(cache, sha="def")
        assert result == "started"
        assert calls

    def test_manual_spawn_ignores_failed_meta(self, cache):
        def boom():
            raise RuntimeError("llm down")
        _spawn(cache, sha="abc", build_fn=boom)
        result, calls = _spawn(cache, sha="abc", auto=False)
        assert result == "started"
        assert calls

    def test_manual_spawn_respects_existing_cache(self, cache):
        cache.write_text("{}")
        result, calls = _spawn(cache, auto=False)
        assert result == "cached"
        assert not calls

    def test_empty_build_result_marks_failed(self, cache):
        result, _ = _spawn(cache, sha="abc", build_fn=lambda: {})
        assert result == "started"
        assert not cache.exists()
        assert presentation.read_meta(cache)["status"] == "failed"

    def test_unavailable_without_cache_path(self):
        result = presentation._spawn(("ticket", "T-2"), None, "abc", True, lambda: {})
        assert result == "unavailable"


class TestEnabled:
    def test_defaults_off(self):
        assert not presentation.enabled({})
        assert not presentation.enabled({"features": {}})

    def test_flag_on(self):
        assert presentation.enabled({"features": {"presentations": True}})
