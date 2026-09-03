"""Both review paths must honour reviewer.providers.

The A/B experiment was only ever wired into review_ticket, the path for PRs that
belong to a frshty ticket. review_pr, which handles peer PRs and carries most of
the review volume, reviewed with Claude alone. Asking the page for a codex review
of a peer PR therefore returned nothing, and the config setting silently did
nothing on that path.
"""
import json
from pathlib import Path
from unittest.mock import patch

from features import reviewer


def _cfg(tmp_path, providers=("claude", "codex")):
    return {"_state_dir": Path(tmp_path), "workspace": {"root": Path(tmp_path)},
            "reviewer": {"providers": list(providers)}}


def _pr():
    return {"repo": "quill", "id": 4536, "branch": "claude/team-checkout-redirect",
            "url": "https://example/pr/4536"}


def _review(body):
    return {"summary": "s", "verdict": "approved",
            "issues": [{"path": "a.ts", "line": 1, "body": body, "severity": "suggestion"}]}


def _artifacts(tmp_path):
    d = Path(tmp_path) / "reviews" / "quill" / "claude-team-checkout-redirect"
    return sorted(p.name for p in d.iterdir()) if d.is_dir() else []


class TestReviewPrRunsEveryConfiguredProvider:
    def _run(self, tmp_path, providers, claude=True, codex=True):
        cfg = _cfg(tmp_path, providers)
        with patch.object(reviewer, "_ensure_review_worktree", return_value=None), \
             patch.object(reviewer, "_load_conventions", return_value=""), \
             patch.object(reviewer, "run_haiku", return_value=""), \
             patch.object(reviewer, "_merge_reviews", side_effect=lambda r: r[0][1]), \
             patch.object(reviewer, "_validate_issues", side_effect=lambda i, w: i), \
             patch.object(reviewer, "_simplify_all_issues", side_effect=lambda i: i), \
             patch.object(reviewer, "_run_single_persona",
                          side_effect=lambda t: (t[0], _review("from claude") if claude else None)), \
             patch.object(reviewer, "_run_codex_persona",
                          side_effect=lambda t: (t[0], _review("from codex") if codex else None)), \
             patch.object(reviewer, "log"):
            reviewer.review_pr(cfg, None, _pr(), prefetched_diff="diff --git a/a.ts b/a.ts\n")
        return _artifacts(tmp_path)

    def test_both_providers_write_their_own_artifacts(self, tmp_path):
        names = self._run(tmp_path, ("claude", "codex"))
        assert "review.json" in names
        assert "review.codex.json" in names
        assert "queued_comments.json" in names
        assert "queued_comments.codex.json" in names

    def test_claude_only_config_writes_no_codex_artifacts(self, tmp_path):
        names = self._run(tmp_path, ("claude",))
        assert "review.json" in names
        assert not any("codex" in n for n in names)

    def test_each_provider_keeps_its_own_findings(self, tmp_path):
        self._run(tmp_path, ("claude", "codex"))
        d = Path(tmp_path) / "reviews" / "quill" / "claude-team-checkout-redirect"
        assert json.loads((d / "review.json").read_text())["provider"] == "claude"
        assert json.loads((d / "review.codex.json").read_text())["provider"] == "codex"
        codex_body = json.loads((d / "queued_comments.codex.json").read_text())[0]["body"]
        assert codex_body == "from codex"

    def test_a_provider_that_returns_nothing_writes_nothing(self, tmp_path):
        """One provider failing must not deny the other its review."""
        names = self._run(tmp_path, ("claude", "codex"), codex=False)
        assert "review.json" in names
        assert not any("codex" in n for n in names)

    def test_claude_failing_does_not_deny_codex_its_review(self, tmp_path):
        """The reverse of the case above, and the one that catches an early
        return: claude is tried first, so bailing on its failure would silently
        drop the codex review that did succeed. The surviving provider's
        findings are the primary review, so review.json is written from them."""
        names = self._run(tmp_path, ("claude", "codex"), claude=False)
        assert "review.codex.json" in names
        assert "queued_comments.codex.json" in names
        assert "review.json" in names
        d = Path(tmp_path) / "reviews" / "quill" / "claude-team-checkout-redirect"
        assert json.loads((d / "queued_comments.json").read_text())[0]["body"] == "from codex"

    def test_the_fan_out_asks_no_model_of_its_own(self, tmp_path):
        """Every model call on this path is stubbed, so nothing may reach a CLI.

        The merge was not stubbed, so these cases ran the real merge model.
        They passed only on a host that had the claude binary, spawned it on
        every run, and failed on CI where the binary is absent."""
        with patch.object(reviewer, "run_balanced", return_value=None) as balanced, \
             patch.object(reviewer, "run_agentic", return_value=None) as agentic:
            self._run(tmp_path, ("claude", "codex"))
        balanced.assert_not_called()
        agentic.assert_not_called()

    def test_no_provider_succeeding_writes_no_review_at_all(self, tmp_path):
        """The staged diff is the reviewers' input, so it is there either way.
        No review and no comment queue is what a failed review must leave."""
        names = self._run(tmp_path, ("claude", "codex"), claude=False, codex=False)
        assert names == ["diff.txt"]


class TestFanOutIsShared:
    def test_both_paths_use_the_same_helper(self):
        """Kept shared so the experiment cannot cover one path and miss the other."""
        import inspect
        src = inspect.getsource(reviewer)
        assert src.count("def _run_personas_for_providers") == 1
        for fn in ("review_pr", "review_ticket"):
            body = inspect.getsource(getattr(reviewer, fn))
            assert "_run_personas_for_providers" in body, f"{fn} must use the shared fan-out"


class TestCodexPersonaWithoutAWorktree:
    def test_it_does_not_crash_when_there_is_no_worktree(self, tmp_path):
        """Peer PRs can be reviewed without a checkout; the codex run directory
        has to land somewhere regardless."""
        with patch.object(reviewer, "run_external_model", return_value=("{}", 0)), \
             patch.object(reviewer, "log"):
            name, _data = reviewer._run_codex_persona(("spec", "prompt", None, "quill-4536"))
        assert name == "spec"


def _blocking(body, path="b.ts", line=9):
    return {"summary": "s", "verdict": "changes_requested",
            "issues": [{"path": path, "line": line, "body": body, "severity": "blocking"}]}


class TestEveryProvidersBlockersReachThePrimaryReview:
    """The A/B ran both providers and then posted claude's list alone, so a
    defect only codex found reached nobody and the verdict was computed as if
    it did not exist. That is how django-drf-app#175 was approved while codex
    held eleven blocking findings on the same diff."""

    def _run(self, tmp_path, claude_review, codex_review):
        cfg = _cfg(tmp_path, ("claude", "codex"))
        with patch.object(reviewer, "_ensure_review_worktree", return_value=None), \
             patch.object(reviewer, "_load_conventions", return_value=""), \
             patch.object(reviewer, "_merge_reviews", side_effect=lambda r: r[0][1]), \
             patch.object(reviewer, "_validate_issues", side_effect=lambda i, w: i), \
             patch.object(reviewer, "_simplify_all_issues", side_effect=lambda i: i), \
             patch.object(reviewer, "_run_single_persona",
                          side_effect=lambda t: (t[0], claude_review)), \
             patch.object(reviewer, "_run_codex_persona",
                          side_effect=lambda t: (t[0], codex_review)), \
             patch.object(reviewer, "log"):
            result = reviewer.review_pr(cfg, None, _pr(), prefetched_diff="diff --git a/a.ts b/a.ts\n")
        d = Path(tmp_path) / "reviews" / "quill" / "claude-team-checkout-redirect"
        return result, json.loads((d / "queued_comments.json").read_text())

    def test_a_blocker_only_codex_found_flips_the_verdict(self, tmp_path):
        result, queued = self._run(tmp_path, _review("from claude"), _blocking("from codex"))
        assert result["verdict"] == "changes_requested"
        assert "from codex" in [c["body"] for c in queued]

    def test_a_blocker_only_codex_found_is_queued_as_a_comment(self, tmp_path):
        _result, queued = self._run(tmp_path, _review("from claude"), _blocking("from codex"))
        codex_comment = next(c for c in queued if c["body"] == "from codex")
        assert codex_comment["severity"] == "blocking"
        assert codex_comment["path"] == "b.ts"

    def test_the_same_defect_from_both_providers_is_one_comment(self, tmp_path):
        """Two providers that found the same defect must not leave the author
        two comments on the same line."""
        _result, queued = self._run(tmp_path, _review("from claude"), _review("from codex"))
        assert len(queued) == 1
        assert queued[0]["found_by"] == ["claude", "codex"]

    def test_two_nearby_defects_from_one_provider_stay_two(self, tmp_path):
        """Collapsing is for the same defect seen by two providers. Two distinct
        defects a few lines apart in one review must both survive."""
        two = {"summary": "s", "verdict": "approved", "issues": [
            {"path": "a.ts", "line": 10, "body": "first", "severity": "suggestion"},
            {"path": "a.ts", "line": 12, "body": "second", "severity": "suggestion"}]}
        _result, queued = self._run(tmp_path, two, None)
        assert sorted(c["body"] for c in queued) == ["first", "second"]

    def test_the_more_severe_rating_wins_on_a_shared_finding(self, tmp_path):
        """Claude graded the S3 orphan on django-drf-app#175 a suggestion and
        codex graded the same defect blocking. The worse rating decides."""
        result, queued = self._run(tmp_path, _review("from claude"),
                                   _blocking("from codex", path="a.ts", line=1))
        assert len(queued) == 1
        assert queued[0]["severity"] == "blocking"
        assert result["verdict"] == "changes_requested"


class TestSeverityRulesAreInEveryPersonaPrompt:
    """No prompt defined 'blocking', so severity rested entirely on the model's
    prior, and claude's prior filed silent data loss as a suggestion."""

    def test_the_peer_pr_prompt_carries_them(self):
        for persona in reviewer.PERSONAS.values():
            prompt = reviewer._build_persona_prompt(
                persona, _pr(), Path("/tmp/diff.txt"), ["a.ts"], "", None)
            assert "Silent data loss is blocking." in prompt

    def test_the_ticket_prompt_carries_them(self):
        for persona in reviewer.PERSONAS.values():
            prompt = reviewer._build_ticket_persona_prompt(
                persona, "DEV-1", "goal", ["section"], True)
            assert "Silent data loss is blocking." in prompt

    def test_no_persona_still_invites_an_approval(self):
        """Three personas each ended with an instruction to approve. An empty
        finding list is the neutral way to say the same thing."""
        for persona in reviewer.PERSONAS.values():
            assert "say so and approve" not in persona
