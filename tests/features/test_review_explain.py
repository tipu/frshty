"""Every stored review comment gets a plain "is this a valid concern" explainer.

The explainer sits under the comment on the review detail page. It is asked
once per distinct comment, outside the sandbox, so the model can open the
checkout and check the claim itself.
"""
import json
from pathlib import Path
from unittest.mock import patch

from features import reviewer


def _comment(body, path="a.py", line=3, **extra):
    return {"pr_id": 7, "repo": "raven", "path": path, "line": line, "body": body,
            "severity": "blocking", "status": "pending", **extra}


def _write(branch_dir: Path, name: str, comments: list[dict]) -> Path:
    f = branch_dir / name
    f.write_text(json.dumps(comments))
    return f


def _answer(valid="yes", text="The loop stops one row early."):
    return json.dumps({"valid": valid, "explanation": text})


class TestExplainComment:
    def test_runs_outside_the_sandbox_in_the_checkout(self, tmp_path):
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        with patch.object(reviewer, "run_balanced", return_value=_answer()) as balanced, \
             patch.object(reviewer, "log"):
            out = reviewer.explain_comment(_comment("off by one"), tmp_path, worktree)
        assert out == {"valid": "yes", "text": "The loop stops one row early."}
        assert balanced.call_args.kwargs["worktree"] == worktree
        prompt = balanced.call_args.args[0]
        assert "Is this a valid concern?" in prompt
        assert "off by one" in prompt
        assert str(tmp_path / "diff.txt") in prompt

    def test_without_a_checkout_it_runs_in_the_review_dir(self, tmp_path):
        with patch.object(reviewer, "run_balanced", return_value=_answer()) as balanced, \
             patch.object(reviewer, "log"):
            reviewer.explain_comment(_comment("x"), tmp_path, None)
        assert balanced.call_args.kwargs["worktree"] == tmp_path

    def test_unknown_verdict_reads_as_partly(self, tmp_path):
        with patch.object(reviewer, "run_balanced", return_value=_answer(valid="maybe")), \
             patch.object(reviewer, "log"):
            assert reviewer.explain_comment(_comment("x"), tmp_path, None)["valid"] == "partly"

    def test_no_answer_is_logged_and_returns_none(self, tmp_path):
        with patch.object(reviewer, "run_balanced", return_value="not json"), \
             patch.object(reviewer, "log") as log:
            assert reviewer.explain_comment(_comment("x"), tmp_path, None) is None
        assert log.emit.call_args.args[0] == "review_explain_failed"


class TestExplainReview:
    def test_fills_every_provider_queue_and_asks_once_per_distinct_comment(self, tmp_path):
        main = _write(tmp_path, "queued_comments.json", [_comment("shared"), _comment("only claude", line=9)])
        codex = _write(tmp_path, "queued_comments.codex.json", [_comment("shared")])
        with patch.object(reviewer, "run_balanced", return_value=_answer()) as balanced, \
             patch.object(reviewer, "log"):
            filled = reviewer.explain_review(tmp_path, None)
        assert balanced.call_count == 2
        assert filled == 3
        for f in (main, codex):
            for c in json.loads(f.read_text()):
                assert c["explainer"] == {"valid": "yes", "text": "The loop stops one row early."}

    def test_an_explained_comment_is_never_asked_again(self, tmp_path):
        done = {"valid": "no", "text": "already"}
        f = _write(tmp_path, "queued_comments.json", [_comment("old", explainer=done)])
        _write(tmp_path, "queued_comments.tasks.json", [_comment("old")])
        with patch.object(reviewer, "run_balanced") as balanced, patch.object(reviewer, "log"):
            filled = reviewer.explain_review(tmp_path, None)
        balanced.assert_not_called()
        assert filled == 1
        assert json.loads(f.read_text())[0]["explainer"] == done
        tasks = json.loads((tmp_path / "queued_comments.tasks.json").read_text())
        assert tasks[0]["explainer"] == done

    def test_a_failed_answer_leaves_the_comment_for_the_next_run(self, tmp_path):
        f = _write(tmp_path, "queued_comments.json", [_comment("x")])
        with patch.object(reviewer, "run_balanced", return_value=None), \
             patch.object(reviewer, "log"):
            assert reviewer.explain_review(tmp_path, None) == 0
        assert "explainer" not in json.loads(f.read_text())[0]

    def test_a_delete_made_while_claude_ran_is_kept(self, tmp_path):
        f = _write(tmp_path, "queued_comments.json", [_comment("keep"), _comment("drop", line=8)])

        def answer_and_delete(prompt, **kwargs):
            f.write_text(json.dumps([_comment("keep")]))
            return _answer()

        with patch.object(reviewer, "run_balanced", side_effect=answer_and_delete), \
             patch.object(reviewer, "log"):
            reviewer.explain_review(tmp_path, None)
        stored = json.loads(f.read_text())
        assert [c["body"] for c in stored] == ["keep"]
        assert stored[0]["explainer"]["valid"] == "yes"
