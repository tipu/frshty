"""Tests for TESTING.md handling.

TESTING.md is NOT a gate — every ticket that passes tri-review enters the
testing state regardless of whether the file exists. The file is only a hint
inlined into the plan_tests prompt: when substantive, it's authoritative;
when missing or trivial, the prompt tells claude to investigate the codebase.
"""
import pytest

from core.tasks.tickets import (
    _testing_md_is_substantive,
    _render_testing_md_section,
    MIN_TESTING_MD_BYTES,
)


class TestSubstantiveCheck:
    def test_missing_file_returns_false(self, tmp_path):
        assert _testing_md_is_substantive(tmp_path / "TESTING.md") is False

    def test_empty_file_returns_false(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("")
        assert _testing_md_is_substantive(path) is False

    def test_pure_whitespace_returns_false(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("\n\n  \t  \n" * 100)
        assert _testing_md_is_substantive(path) is False

    def test_short_content_below_threshold_returns_false(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("TODO: write this later")
        assert _testing_md_is_substantive(path) is False

    def test_content_at_threshold_returns_true(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("x" * MIN_TESTING_MD_BYTES)
        assert _testing_md_is_substantive(path) is True

    def test_content_above_threshold_returns_true(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("# Testing\n\n" + "real content " * 50)
        assert _testing_md_is_substantive(path) is True

    def test_directory_instead_of_file_returns_false(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.mkdir()
        assert _testing_md_is_substantive(path) is False

    def test_threshold_constant_is_200(self):
        """Lock the threshold so an accidental edit triggers a test failure
        that surfaces the question 'why are we changing the trivial-vs-
        substantive cutoff?'"""
        assert MIN_TESTING_MD_BYTES == 200


class TestPromptSectionRendering:
    def test_substantive_guide_inlined_with_authoritative_framing(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("# Testing\n\nWe use vitest. Fixtures live in __fixtures__/.\n"
                        + ("padding line\n" * 30))
        section = _render_testing_md_section(path)
        assert "PROJECT TESTING GUIDE" in section
        assert "We use vitest" in section
        assert "authoritative" in section.lower()

    def test_missing_guide_falls_back_to_investigate_prompt(self, tmp_path):
        section = _render_testing_md_section(tmp_path / "TESTING.md")
        assert "NO PROJECT TESTING GUIDE" in section
        assert "Investigate" in section or "investigate" in section
        # The fallback must mention concrete detection signals so claude knows
        # what to grep for
        assert "package.json" in section
        assert "pyproject.toml" in section

    def test_trivial_guide_falls_back_to_investigate_prompt(self, tmp_path):
        path = tmp_path / "TESTING.md"
        path.write_text("TODO")
        section = _render_testing_md_section(path)
        assert "NO PROJECT TESTING GUIDE" in section
        # And the trivial content must NOT be inlined as if it were authoritative
        assert "TODO" not in section or "authoritative" not in section.lower()
