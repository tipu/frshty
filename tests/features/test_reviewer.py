from pathlib import Path
from unittest.mock import patch, MagicMock

from features import reviewer
from tests.conftest import make_pr


class TestExtractChangedPaths:
    def test_extracts_paths(self):
        diff = "diff --git a/src/main.py b/src/main.py\n+++ b/src/main.py\n"
        assert reviewer._extract_changed_paths(diff) == ["src/main.py"]

    def test_multiple_files(self):
        diff = (
            "diff --git a/a.py b/a.py\n"
            "diff --git a/b.py b/b.py\n"
        )
        assert reviewer._extract_changed_paths(diff) == ["a.py", "b.py"]

    def test_empty_diff(self):
        assert reviewer._extract_changed_paths("") == []


class TestReadFunctionContext:
    def test_reads_around_line(self, tmp_path):
        f = tmp_path / "test.py"
        lines = [f"line {i}" for i in range(200)]
        f.write_text("\n".join(lines))
        context = reviewer._read_function_context(tmp_path, "test.py", 100)
        assert "line 100" in context
        assert "line 40" in context
        assert "line 159" in context

    def test_missing_file(self, tmp_path):
        assert reviewer._read_function_context(tmp_path, "nope.py", 10) == ""

    def test_near_start(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("a\nb\nc\n")
        context = reviewer._read_function_context(tmp_path, "test.py", 1)
        assert "a" in context


class TestReadChangedFiles:
    def test_reads_files(self, tmp_path):
        (tmp_path / "a.py").write_text("content a")
        diff = "diff --git a/a.py b/a.py\n"
        result = reviewer._read_changed_files(diff, tmp_path)
        assert "content a" in result

    def test_skips_large_files(self, tmp_path):
        (tmp_path / "big.py").write_text("x" * 70_000)
        diff = "diff --git a/big.py b/big.py\n"
        result = reviewer._read_changed_files(diff, tmp_path)
        assert result == ""

    def test_skips_missing_files(self, tmp_path):
        diff = "diff --git a/gone.py b/gone.py\n"
        result = reviewer._read_changed_files(diff, tmp_path)
        assert result == ""


class TestBuildPersonaPrompt:
    def test_includes_persona_text(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("PERSONA TEXT", pr, "diff", "", "", False)
        assert "PERSONA TEXT" in prompt

    def test_includes_diff(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "my diff content", "", "", False)
        assert "my diff content" in prompt

    def test_includes_conventions(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "diff", "CONV TEXT", "", False)
        assert "CONV TEXT" in prompt

    def test_includes_tool_hint_when_tools(self):
        pr = make_pr()
        prompt = reviewer._build_persona_prompt("p", pr, "diff", "", "", True)
        assert "read-only access" in prompt


class TestMergeReviews:
    def test_single_result_wraps_agreed_by(self):
        data = {"verdict": "approved", "issues": [{"body": "issue1", "severity": "suggestion"}]}
        result = reviewer._merge_reviews([("spec", data)])
        assert result["issues"][0]["agreed_by"] == ["spec"]

    def test_multiple_results_fallback_on_haiku_failure(self):
        data1 = {"verdict": "approved", "issues": [{"body": "a", "severity": "suggestion"}]}
        data2 = {"verdict": "changes_requested", "issues": [{"body": "b", "severity": "blocking"}]}
        with patch("features.reviewer.run_haiku", return_value=None):
            result = reviewer._merge_reviews([("spec", data1), ("breakage", data2)])
        assert len(result["issues"]) == 2


class TestValidateSingle:
    def test_no_path_returns_issue(self):
        issue = {"body": "problem", "severity": "blocking"}
        result = reviewer._validate_single((issue, None))
        assert result == issue

    def test_false_positive_returns_none(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("\n".join([f"line{i}" for i in range(200)]))
        issue = {"body": "problem", "severity": "blocking", "path": "test.py", "line": 50}
        with patch("features.reviewer.run_sonnet", return_value='{"decision": "false_positive", "reason": "guard clause"}'), \
             patch("features.reviewer.extract_json", return_value={"decision": "false_positive", "reason": "guard clause"}), \
             patch("features.reviewer.log"):
            result = reviewer._validate_single((issue, tmp_path))
        assert result is None

    def test_valid_returns_issue(self, tmp_path):
        f = tmp_path / "test.py"
        f.write_text("\n".join([f"line{i}" for i in range(200)]))
        issue = {"body": "problem", "severity": "blocking", "path": "test.py", "line": 50}
        with patch("features.reviewer.run_sonnet", return_value='{"decision": "valid", "reason": "real"}'), \
             patch("features.reviewer.extract_json", return_value={"decision": "valid", "reason": "real"}):
            result = reviewer._validate_single((issue, tmp_path))
        assert result == issue


class TestSimplifyBody:
    def test_returns_simplified(self):
        with patch("features.reviewer.run_haiku", return_value="simplified text"):
            assert reviewer._simplify_body("verbose body") == "simplified text"

    def test_fallback_on_none(self):
        with patch("features.reviewer.run_haiku", return_value=None):
            assert reviewer._simplify_body("original") == "original"


class TestStyleMatch:
    def test_no_examples_returns_body(self):
        assert reviewer._style_match("body", "") == "body"

    def test_with_examples_calls_haiku(self):
        with patch("features.reviewer.run_haiku", return_value="styled"):
            assert reviewer._style_match("body", "example1\nexample2") == "styled"


class TestReviewPr:
    def test_no_diff_returns_none(self, tmp_path):
        mock_platform = MagicMock()
        mock_platform.get_pr_diff.return_value = None
        pr = make_pr()
        config = {"_state_dir": tmp_path, "workspace": {"root": tmp_path, "repos": []}}
        result = reviewer.review_pr(config, mock_platform, pr)
        assert result is None

    def test_all_personas_fail_returns_none(self, tmp_path):
        mock_platform = MagicMock()
        mock_platform.get_pr_diff.return_value = "diff content"
        pr = make_pr()
        config = {"_state_dir": tmp_path, "workspace": {"root": tmp_path, "repos": []}}

        with patch("features.reviewer._ensure_review_worktree", return_value=None), \
             patch("features.reviewer._load_conventions", return_value=""), \
             patch("features.reviewer._run_all_personas", return_value=[("spec", None), ("breakage", None), ("maint", None)]):
            result = reviewer.review_pr(config, mock_platform, pr)
        assert result is None
