import json
import time
from unittest.mock import MagicMock, patch

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


class TestResolveTicketGoal:
    def _patches(self, own_ticket=None, system_ticket=None, pr_info=None):
        from unittest.mock import patch, MagicMock
        sys_mock = MagicMock()
        sys_mock.fetch_ticket.return_value = system_ticket
        platform = MagicMock()
        platform.get_pr_info.return_value = pr_info or {}
        return (
            patch("core.state.load", return_value={"DEV-9": own_ticket} if own_ticket else {}),
            patch("features.presentation.make_ticket_system",
                  return_value=sys_mock if system_ticket is not None else None),
            patch("features.presentation.make_platform", return_value=platform),
        )

    def test_prefers_own_ticket_state(self):
        p1, p2, p3 = self._patches(own_ticket={"summary": "our goal", "description": ""})
        with p1, p2, p3:
            assert presentation.resolve_ticket_goal({}, "DEV-9-x", "r", 1) == "our goal"

    def test_falls_back_to_ticket_system_for_foreign_tickets(self):
        p1, p2, p3 = self._patches(system_ticket={"summary": "Chunk documents", "description": "Split uploads"})
        with p1, p2, p3:
            goal = presentation.resolve_ticket_goal({}, "jwd/dev-9-chunking", "r", 1)
        assert goal == "DEV-9: Chunk documents\nSplit uploads"

    def test_falls_back_to_pr_info_when_system_has_nothing(self):
        p1, p2, p3 = self._patches(pr_info={"title": "the pr title", "description": "d"})
        with p1, p2, p3:
            assert presentation.resolve_ticket_goal({}, "jwd/dev-9-x", "r", 1) == "the pr title\nd"


class TestTicketPresentation:
    def test_meta_file_follows_cache_name(self, tmp_path):
        assert presentation.meta_path(tmp_path / "presentation_cache.json").name == "presentation_meta.json"
        assert presentation.meta_path(
            tmp_path / "ticket_presentation_cache.json").name == "ticket_presentation_meta.json"

    def test_ticket_cache_file_is_separate_from_review_deck(self, tmp_path):
        config = {"workspace": {"root": tmp_path, "tickets_dir": "tickets"}}
        path = presentation.ticket_cache_file(config, {"slug": "dev-9"})
        assert path == tmp_path / "tickets" / "dev-9" / "ticket_presentation_cache.json"

    def test_ticket_diffs_label_each_pr(self):
        platform = MagicMock()
        platform.get_pr_diff.side_effect = lambda repo, pr_id: f"diff {repo} {pr_id}"
        ts = {"prs": [{"repo": "api", "id": 4}, {"repo": "web", "id": 7}]}
        with patch("features.presentation.make_platform", return_value=platform):
            diffs = presentation._ticket_diffs({}, ts)
        assert diffs == [("api", "api#4", "diff api 4"), ("web", "web#7", "diff web 7")]

    def test_prompt_lists_criteria_and_labels_every_pr(self):
        seen = {}

        def fake_run(prompt, **kwargs):
            seen["prompt"] = prompt
            seen["schema"] = kwargs["schema"]
            return json.dumps({
                "problem": "p", "approach": "a", "steps": [],
                "requirements": [{"id": "c1", "requirement": "x", "status": "met", "how": "h",
                                  "prs": ["api#4"], "steps": [], "gap": ""}],
                "verdict": {"solves_problem": True, "summary": "s", "concerns": []},
                "visual": {"flow": [], "before": [], "after": []}})

        criteria = [{"id": "c1", "criterion": "Export the report as CSV"},
                    {"id": "c2", "criterion": "Email the report nightly"}]
        diffs = [("api", "api#4", "+export"), ("web", "web#7", "+button"), ("cli", "cli#1", "")]
        with patch("features.presentation.run_balanced", side_effect=fake_run):
            out = presentation.build_ticket_presentation("Reports", criteria, diffs, None)
        assert out["requirements"][0]["status"] == "met"
        assert seen["schema"] == presentation.TICKET_PRESENTATION_SCHEMA
        prompt = seen["prompt"]
        assert "- c1: Export the report as CSV" in prompt
        assert "- c2: Email the report nightly" in prompt
        assert "=== PR api#4 START ===\n+export" in prompt
        assert "=== PR web#7 START ===\n+button" in prompt
        assert "cli#1" not in prompt

    def test_prompt_extracts_requirements_without_criteria(self):
        seen = {}

        def fake_run(prompt, **kwargs):
            seen["prompt"] = prompt
            return "{}"

        with patch("features.presentation.run_balanced", side_effect=fake_run):
            presentation.build_ticket_presentation("Reports", [], [("api", "api#4", "+x")], None)
        assert "Extract the requirements from the ticket text" in seen["prompt"]

    def test_no_diffs_skips_the_model(self):
        with patch("features.presentation.run_balanced") as run:
            assert presentation.build_ticket_presentation("g", [], [("api", "api#4", "")], None) == {}
        run.assert_not_called()
