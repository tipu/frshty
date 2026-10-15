"""Every stored review comment gets a plain "is this a valid concern" explainer.

The explainer sits under the comment on the review detail page. It is asked
once per distinct comment, outside the sandbox, so the model can open the
checkout and check the claim itself.
"""
import json
import threading
from pathlib import Path
from unittest.mock import patch

import pytest

import core.llm as llm
import core.state as state
from features import reviewer
from services import review_store


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


def _models(claude=None, codex=None):
    """Stub both models. Each argument is a return value or a side effect."""
    def stub(name, value):
        if callable(value):
            return patch.object(reviewer, name, side_effect=value)
        return patch.object(reviewer, name, return_value=value)
    return stub("run_balanced", claude), stub("_ask_codex", codex)


CODEX = {"valid": "partly", "text": "Codex says partly."}


def _codex_answer():
    return json.dumps({"valid": "partly", "explanation": "Codex says partly."})


class TestExplainCommentWithCodex:
    def test_codex_runs_in_the_checkout(self, tmp_path):
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        claude, codex = _models(codex=_codex_answer())
        with claude as balanced, codex as ask, patch.object(reviewer, "log"):
            out = reviewer.explain_comment(_comment("x"), tmp_path, worktree, "codex")
        assert out == CODEX
        balanced.assert_not_called()
        assert ask.call_args.args[1] == worktree
        assert "Is this a valid concern?" in ask.call_args.args[0]

    def test_codex_asks_codex_outside_the_sandbox(self, tmp_path):
        with patch.object(reviewer, "run_external_model", return_value=(_codex_answer(), 0)) as run:
            out = reviewer._ask_codex("prompt", tmp_path)
        assert out == _codex_answer()
        cmd = run.call_args.args[0]
        assert cmd[:2] == ["codex", "exec"]
        assert "--dangerously-bypass-approvals-and-sandbox" in cmd
        assert run.call_args.kwargs["cwd"] == tmp_path

    def test_a_failed_codex_exit_gives_no_text(self, tmp_path):
        with patch.object(reviewer, "run_external_model", return_value=("partial", 1)):
            assert reviewer._ask_codex("prompt", tmp_path) is None


@pytest.fixture
def no_summary():
    with patch.object(reviewer, "summarize_comment", return_value=None) as summarize:
        yield summarize


@pytest.mark.usefixtures("no_summary")
class TestExplainReview:
    def test_fills_both_models_in_every_queue_and_asks_once_per_distinct_comment(self, tmp_path):
        main = _write(tmp_path, "queued_comments.json", [_comment("shared"), _comment("only claude", line=9)])
        other = _write(tmp_path, "queued_comments.codex.json", [_comment("shared")])
        claude, codex = _models(_answer(), _codex_answer())
        with claude as balanced, codex as ask, patch.object(reviewer, "log"):
            filled = reviewer.explain_review(tmp_path, None)
        assert balanced.call_count == 2
        assert ask.call_count == 2
        assert filled == 6
        for f in (main, other):
            for c in json.loads(f.read_text()):
                assert c["explainer"] == {"valid": "yes", "text": "The loop stops one row early."}
                assert c["explainer_codex"] == CODEX

    def test_an_explained_comment_is_never_asked_again(self, tmp_path):
        done = {"valid": "no", "text": "already"}
        f = _write(tmp_path, "queued_comments.json",
                   [_comment("old", explainer=done, explainer_codex=CODEX)])
        _write(tmp_path, "queued_comments.tasks.json", [_comment("old")])
        claude, codex = _models()
        with claude as balanced, codex as ask, patch.object(reviewer, "log"):
            filled = reviewer.explain_review(tmp_path, None)
        balanced.assert_not_called()
        ask.assert_not_called()
        assert filled == 2
        assert json.loads(f.read_text())[0]["explainer"] == done
        tasks = json.loads((tmp_path / "queued_comments.tasks.json").read_text())
        assert tasks[0]["explainer"] == done
        assert tasks[0]["explainer_codex"] == CODEX

    def test_only_the_missing_model_is_asked(self, tmp_path):
        done = {"valid": "no", "text": "already"}
        f = _write(tmp_path, "queued_comments.json", [_comment("old", explainer=done)])
        claude, codex = _models(codex=_codex_answer())
        with claude as balanced, codex as ask, patch.object(reviewer, "log"):
            assert reviewer.explain_review(tmp_path, None) == 1
        balanced.assert_not_called()
        assert ask.call_count == 1
        stored = json.loads(f.read_text())[0]
        assert stored["explainer"] == done
        assert stored["explainer_codex"] == CODEX

    def test_a_failed_answer_leaves_that_model_for_the_next_run(self, tmp_path):
        f = _write(tmp_path, "queued_comments.json", [_comment("x")])
        claude, codex = _models(None, _codex_answer())
        with claude, codex, patch.object(reviewer, "log"):
            assert reviewer.explain_review(tmp_path, None) == 1
        stored = json.loads(f.read_text())[0]
        assert "explainer" not in stored
        assert stored["explainer_codex"] == CODEX

    def test_a_delete_made_while_the_models_ran_is_kept(self, tmp_path):
        f = _write(tmp_path, "queued_comments.json", [_comment("keep"), _comment("drop", line=8)])

        def answer_and_delete(prompt, **kwargs):
            f.write_text(json.dumps([_comment("keep")]))
            return _answer()

        claude, codex = _models(answer_and_delete, _codex_answer())
        with claude, codex, patch.object(reviewer, "log"):
            reviewer.explain_review(tmp_path, None)
        stored = json.loads(f.read_text())
        assert [c["body"] for c in stored] == ["keep"]
        assert stored[0]["explainer"]["valid"] == "yes"
        assert stored[0]["explainer_codex"] == CODEX


@pytest.mark.usefixtures("no_summary")
class TestExplainReviewIsolation:
    def test_the_workers_run_under_the_callers_instance(self, tmp_path):
        """A pool thread starts with empty context variables, so without the
        copy the model call ran under the primary instance's configuration."""
        _write(tmp_path, "queued_comments.json", [_comment("x")])
        seen = []

        def record_claude(prompt, **kwargs):
            seen.append(("claude", llm._active_instance_key()))
            return _answer()

        def record_codex(prompt, cwd):
            seen.append(("codex", llm._active_instance_key()))
            return _codex_answer()

        token = state._instance_key_cv.set("secondary")
        try:
            claude, codex = _models(record_claude, record_codex)
            with claude, codex, patch.object(reviewer, "log"):
                reviewer.explain_review(tmp_path, None)
        finally:
            state._instance_key_cv.reset(token)
        assert sorted(seen) == [("claude", "secondary"), ("codex", "secondary")]

    def test_the_write_back_waits_for_a_page_edit_in_progress(self, tmp_path):
        """A delete on the page holds the queue lock, so the write-back reads
        the queue after the delete and cannot bring the comment back."""
        f = _write(tmp_path, "queued_comments.json", [_comment("keep"), _comment("drop", line=8)])
        done = threading.Event()

        def run():
            claude, codex = _models(_answer(), _codex_answer())
            with claude, codex, patch.object(reviewer, "log"):
                reviewer.explain_review(tmp_path, None)
            done.set()

        with review_store.queue_lock(tmp_path):
            worker = threading.Thread(target=run)
            worker.start()
            assert not done.wait(0.5)
            f.write_text(json.dumps([_comment("keep")]))
        worker.join(5)
        stored = json.loads(f.read_text())
        assert [c["body"] for c in stored] == ["keep"]
        assert stored[0]["explainer"]["valid"] == "yes"


CLAUDE = {"valid": "yes", "text": "The loop stops one row early."}


def _summary_answer(code=None, **extra):
    return json.dumps({"valid": "yes", "summary": "Both agree.", "plain": "A box holds 3 eggs.",
                       "code": code if code is not None else [], **extra})


class TestSummarizeComment:
    def test_merges_both_answers_and_reads_the_code_from_the_checkout(self, tmp_path):
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        (worktree / "a.py").write_text("one\ntwo\nthree\nfour\n")
        code = [{"path": "a.py", "start": 2, "end": 3, "code": "made up", "note": "Reads rows."}]
        with patch.object(reviewer, "run_balanced", return_value=_summary_answer(code)) as balanced, \
             patch.object(reviewer, "log"):
            out = reviewer.summarize_comment(
                _comment("off by one"), {"explainer": CLAUDE, "explainer_codex": CODEX},
                tmp_path, worktree)
        assert out == {"valid": "yes", "summary": "Both agree.", "plain": "A box holds 3 eggs.",
                       "code": [{"path": "a.py", "start": 2, "end": 3, "code": "two\nthree",
                                 "note": "Reads rows."}],
                       "models": {"claude": "yes", "codex": "partly"}}
        prompt = balanced.call_args.args[0]
        assert balanced.call_args.kwargs["worktree"] == worktree
        assert "off by one" in prompt
        assert "The loop stops one row early." in prompt
        assert "Codex says partly." in prompt

    def test_a_path_outside_the_checkout_is_dropped(self, tmp_path):
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        (tmp_path / "secret.txt").write_text("key\n")
        code = [{"path": "../secret.txt", "start": 1, "end": 1, "note": "x"},
                {"path": "missing.py", "start": 1, "end": 1, "note": "x"}]
        with patch.object(reviewer, "run_balanced", return_value=_summary_answer(code)), \
             patch.object(reviewer, "log"):
            out = reviewer.summarize_comment(
                _comment("x"), {"explainer": CLAUDE, "explainer_codex": CODEX}, tmp_path, worktree)
        assert out["code"] == []

    def test_a_symlink_loop_is_dropped(self, tmp_path):
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        (worktree / "loop").symlink_to("loop")
        code = [{"path": "loop", "start": 1, "end": 1, "note": "x"}]
        with patch.object(reviewer, "run_balanced", return_value=_summary_answer(code)), \
             patch.object(reviewer, "log"):
            out = reviewer.summarize_comment(
                _comment("x"), {"explainer": CLAUDE, "explainer_codex": CODEX}, tmp_path, worktree)
        assert out["code"] == []

    def test_the_snippet_is_cut_to_the_line_limit_and_the_file_end(self, tmp_path):
        worktree = tmp_path / "worktree"
        worktree.mkdir()
        (worktree / "a.py").write_text("\n".join(str(n) for n in range(1, 101)))
        code = [{"path": "a.py", "start": 10, "end": 90, "note": ""},
                {"path": "a.py", "start": 99, "end": 120, "note": ""}]
        with patch.object(reviewer, "run_balanced", return_value=_summary_answer(code)), \
             patch.object(reviewer, "log"):
            out = reviewer.summarize_comment(
                _comment("x"), {"explainer": CLAUDE, "explainer_codex": CODEX}, tmp_path, worktree)
        first, last = out["code"]
        assert (first["start"], first["end"]) == (10, 10 + reviewer.SUMMARY_SNIPPET_LINES - 1)
        assert (last["start"], last["end"], last["code"]) == (99, 100, "99\n100")

    def test_without_a_checkout_the_models_copy_is_kept(self, tmp_path):
        code = [{"path": "a.py", "start": 4, "end": 5, "code": "x = 1\ny = 2", "note": "Sets x."}]
        with patch.object(reviewer, "run_balanced", return_value=_summary_answer(code)), \
             patch.object(reviewer, "log"):
            out = reviewer.summarize_comment(
                _comment("x"), {"explainer": CLAUDE, "explainer_codex": CODEX}, tmp_path, None)
        assert out["code"] == [{"path": "a.py", "start": 4, "end": 5, "code": "x = 1\ny = 2",
                                "note": "Sets x."}]

    def test_no_plain_breakdown_is_logged_and_returns_none(self, tmp_path):
        with patch.object(reviewer, "run_balanced", return_value=_summary_answer(plain="")), \
             patch.object(reviewer, "log") as log:
            assert reviewer.summarize_comment(
                _comment("x"), {"explainer": CLAUDE, "explainer_codex": CODEX},
                tmp_path, None) is None
        assert log.emit.call_args.args[0] == "review_explain_failed"


def _claude_and_summary(prompt, **kwargs):
    if kwargs["function_name"] == "review_explain_summary":
        return _summary_answer()
    return _answer()


class TestExplainReviewSummary:
    def test_both_explainers_are_merged_into_one_summary_in_every_queue(self, tmp_path):
        main = _write(tmp_path, "queued_comments.json", [_comment("shared")])
        other = _write(tmp_path, "queued_comments.codex.json", [_comment("shared")])
        claude, codex = _models(_claude_and_summary, _codex_answer())
        with claude as balanced, codex, patch.object(reviewer, "log"):
            filled = reviewer.explain_review(tmp_path, None)
        assert filled == 6
        assert [k.kwargs["function_name"] for k in balanced.call_args_list].count(
            "review_explain_summary") == 1
        for f in (main, other):
            stored = json.loads(f.read_text())[0]
            assert stored[reviewer.SUMMARY_FIELD]["plain"] == "A box holds 3 eggs."
            assert stored[reviewer.SUMMARY_FIELD]["models"] == {"claude": "yes", "codex": "partly"}

    def test_no_summary_while_one_explainer_is_missing(self, tmp_path):
        f = _write(tmp_path, "queued_comments.json", [_comment("x")])
        claude, codex = _models(_claude_and_summary, None)
        with claude as balanced, codex, patch.object(reviewer, "log"):
            assert reviewer.explain_review(tmp_path, None) == 1
        assert [k.kwargs["function_name"] for k in balanced.call_args_list] == ["review_explain"]
        assert reviewer.SUMMARY_FIELD not in json.loads(f.read_text())[0]

    def test_an_explained_comment_gets_only_the_summary(self, tmp_path):
        f = _write(tmp_path, "queued_comments.json",
                   [_comment("old", explainer=CLAUDE, explainer_codex=CODEX)])
        claude, codex = _models(_claude_and_summary)
        with claude as balanced, codex as ask, patch.object(reviewer, "log"):
            assert reviewer.explain_review(tmp_path, None) == 1
        ask.assert_not_called()
        assert [k.kwargs["function_name"] for k in balanced.call_args_list] == [
            "review_explain_summary"]
        assert json.loads(f.read_text())[0][reviewer.SUMMARY_FIELD]["summary"] == "Both agree."

    def test_a_summarized_comment_is_never_asked_again(self, tmp_path):
        done = {"valid": "no", "summary": "s", "plain": "p", "code": []}
        _write(tmp_path, "queued_comments.json",
               [_comment("old", explainer=CLAUDE, explainer_codex=CODEX, explainer_summary=done)])
        tasks = _write(tmp_path, "queued_comments.tasks.json", [_comment("old")])
        claude, codex = _models()
        with claude as balanced, codex as ask, patch.object(reviewer, "log"):
            assert reviewer.explain_review(tmp_path, None) == 3
        balanced.assert_not_called()
        ask.assert_not_called()
        assert json.loads(tasks.read_text())[0][reviewer.SUMMARY_FIELD] == done
