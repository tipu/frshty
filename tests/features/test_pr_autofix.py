from unittest.mock import patch, MagicMock

from features import pr_autofix
from tests.conftest import make_pr


def _config(**overrides):
    base = {"job": {"key": "clarivis"}, "features": {"pr_autofix": True}}
    base.update(overrides)
    return base


class TestCheck:
    def test_first_run_baselines_without_queueing(self):
        prs = [make_pr(id=1), make_pr(id=2)]
        platform = MagicMock()
        platform.list_open_prs.return_value = prs
        store = {}
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix.state.load", return_value=store), \
             patch("features.pr_autofix.state.save") as mock_save, \
             patch("features.pr_autofix.q.enqueue_job") as mock_enqueue, \
             patch("features.pr_autofix.log.emit"):
            pr_autofix.check(_config())
        mock_enqueue.assert_not_called()
        saved = mock_save.call_args[0][1]
        assert saved[pr_autofix.SEEDED_KEY]
        assert saved["myrepo/1"]["status"] == "baselined"
        assert saved["myrepo/2"]["status"] == "baselined"

    def test_new_pr_queues_one_job(self):
        store = {pr_autofix.SEEDED_KEY: "2026-01-01T00:00:00+00:00",
                 "myrepo/1": {"status": "baselined"}}
        platform = MagicMock()
        platform.list_open_prs.return_value = [make_pr(id=1), make_pr(id=2)]
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix.state.load", return_value=store), \
             patch("features.pr_autofix.state.save") as mock_save, \
             patch("features.pr_autofix.q.enqueue_job") as mock_enqueue, \
             patch("features.pr_autofix.log.emit"):
            pr_autofix.check(_config())
        assert mock_enqueue.call_count == 1
        assert mock_enqueue.call_args[0][1] == "pr_autofix_run"
        assert mock_enqueue.call_args[1]["payload"]["pr"]["id"] == 2
        saved = mock_save.call_args[0][1]
        assert saved["myrepo/2"]["status"] == "queued"

    def test_closed_pr_record_dropped(self):
        store = {pr_autofix.SEEDED_KEY: "2026-01-01T00:00:00+00:00",
                 "myrepo/9": {"status": "clean"}}
        platform = MagicMock()
        platform.list_open_prs.return_value = []
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix.state.load", return_value=store), \
             patch("features.pr_autofix.state.save") as mock_save, \
             patch("features.pr_autofix.q.enqueue_job") as mock_enqueue, \
             patch("features.pr_autofix.log.emit"):
            pr_autofix.check(_config())
        mock_enqueue.assert_not_called()
        saved = mock_save.call_args[0][1]
        assert "myrepo/9" not in saved
        assert pr_autofix.SEEDED_KEY in saved

    def test_error_record_requeues_until_cap(self):
        store = {pr_autofix.SEEDED_KEY: "2026-01-01T00:00:00+00:00",
                 "myrepo/1": {"status": "error", "attempts": 1}}
        platform = MagicMock()
        platform.list_open_prs.return_value = [make_pr(id=1)]
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix.state.load", return_value=store), \
             patch("features.pr_autofix.state.save"), \
             patch("features.pr_autofix.q.enqueue_job") as mock_enqueue, \
             patch("features.pr_autofix.log.emit"):
            pr_autofix.check(_config())
        assert mock_enqueue.call_count == 1

    def test_error_record_at_cap_stays(self):
        store = {pr_autofix.SEEDED_KEY: "2026-01-01T00:00:00+00:00",
                 "myrepo/1": {"status": "error", "attempts": pr_autofix.MAX_ATTEMPTS}}
        platform = MagicMock()
        platform.list_open_prs.return_value = [make_pr(id=1)]
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix.state.load", return_value=store), \
             patch("features.pr_autofix.state.save"), \
             patch("features.pr_autofix.q.enqueue_job") as mock_enqueue, \
             patch("features.pr_autofix.log.emit"):
            pr_autofix.check(_config())
        mock_enqueue.assert_not_called()

    def test_platform_without_list_open_prs_skips(self):
        platform = MagicMock(spec=[])
        with patch("features.pr_autofix.make_platform", return_value=platform), \
             patch("features.pr_autofix.state.load") as mock_load, \
             patch("features.pr_autofix.log.emit"):
            pr_autofix.check(_config())
        mock_load.assert_not_called()


class TestNormalize:
    def test_drops_findings_without_body(self):
        data = {"findings": [
            {"severity": "critical", "path": "a.py", "line": 1, "title": "t", "body": "real"},
            {"severity": "high", "path": "b.py", "line": 2, "title": "t", "body": ""},
        ]}
        out = pr_autofix._normalize_findings(data, "claude")
        assert len(out) == 1
        assert out[0]["providers"] == ["claude"]

    def test_none_data_returns_empty(self):
        assert pr_autofix._normalize_findings(None, "codex") == []


class TestConsolidate:
    def _finding(self, severity="critical", path="a.py", line=1, provider="claude"):
        return {"severity": severity, "path": path, "line": line,
                "title": "t", "body": "b", "providers": [provider]}

    def test_no_actionable_returns_empty_without_llm(self):
        by_provider = {"claude": [self._finding(severity="medium")],
                       "codex": [self._finding(severity="low", provider="codex")]}
        with patch("features.pr_autofix.run_balanced") as mock_llm:
            out = pr_autofix._consolidate({}, make_pr(), by_provider)
        assert out == []
        mock_llm.assert_not_called()

    def test_single_provider_skips_llm(self):
        by_provider = {"claude": [self._finding()], "codex": []}
        with patch("features.pr_autofix.run_balanced") as mock_llm:
            out = pr_autofix._consolidate({}, make_pr(), by_provider)
        assert len(out) == 1
        mock_llm.assert_not_called()

    def test_llm_merge_filters_to_actionable(self):
        by_provider = {"claude": [self._finding()],
                       "codex": [self._finding(provider="codex")]}
        merged = ('{"findings": ['
                  '{"severity": "critical", "path": "a.py", "line": 1, "title": "t",'
                  ' "body": "b", "providers": ["claude", "codex"]},'
                  '{"severity": "medium", "path": "c.py", "line": 3, "title": "t", "body": "b"}]}')
        with patch("features.pr_autofix.run_balanced", return_value=merged), \
             patch("features.pr_autofix.log.emit"):
            out = pr_autofix._consolidate({}, make_pr(), by_provider)
        assert len(out) == 1
        assert out[0]["providers"] == ["claude", "codex"]

    def test_llm_failure_falls_back_to_mechanical_dedupe(self):
        by_provider = {"claude": [self._finding()],
                       "codex": [self._finding(provider="codex"),
                                 self._finding(path="other.py", line=9, provider="codex")]}
        with patch("features.pr_autofix.run_balanced", return_value=None), \
             patch("features.pr_autofix.log.emit") as mock_emit:
            out = pr_autofix._consolidate({}, make_pr(), by_provider)
        assert len(out) == 2
        same_line = next(f for f in out if f["path"] == "a.py")
        assert sorted(same_line["providers"]) == ["claude", "codex"]
        assert any(c[0][0] == "pr_autofix_merge_fallback" for c in mock_emit.call_args_list)


class TestReviewPrompt:
    def test_truncation_marker_present_only_when_capped(self):
        pr = make_pr()
        small = pr_autofix._review_prompt(pr, "x" * 100, has_tools=True)
        assert "truncated" not in small
        big = pr_autofix._review_prompt(pr, "x" * (pr_autofix.DIFF_CHAR_CAP + 1), has_tools=False)
        assert "truncated" in big
