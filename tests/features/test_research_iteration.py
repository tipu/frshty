"""Iterating on a research spike.

The loop the operator uses is: read docs/research.md on the ticket page, add a
note with new specifications, press "Add note & restart". That resets the
ticket to new and re-runs do_research. These tests cover the three things that
loop needs: the note reset must keep the prior document and archive the plan
documents, the re-run must see the note and the prior document, a run that
leaves the document untouched must fail, and a finished research ticket must
stay done instead of being reopened into the code pipeline.
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import core.state as state
from core.tasks.registry import TaskContext
from core.tasks.tickets import apply_note_reset, do_research
from features import tickets
from tests.conftest import make_ticket, make_ticket_state


SLUG = "PROJ-1-do-the-thing"


def _ctx(tmp_path):
    ticket_dir = tmp_path / "tickets" / SLUG
    (ticket_dir / "docs").mkdir(parents=True, exist_ok=True)
    config = {
        "workspace": {"root": tmp_path, "tickets_dir": "tickets"},
        "_base_url": "http://localhost:8000",
    }
    return TaskContext(
        instance_key="inst",
        ticket_key="PROJ-1",
        task="do_research",
        payload={},
        job_id=0,
        triggering_event_id=None,
        config=config,
        registry=None,
        now=None,
    )


def _run(ctx, side_effect=None, returns="done"):
    ts = {"work_type": "research", "slug": SLUG,
          "summary": "Do the thing", "description": "Description text"}
    runner = MagicMock(return_value=returns, side_effect=side_effect)
    with patch("core.tasks.tickets.state.load_ticket", return_value=ts), \
         patch("core.tasks.tickets.get_repos", return_value=[]), \
         patch("core.tasks.tickets.log", MagicMock()), \
         patch("core.tasks.tickets.run_claude_code", runner):
        result = do_research(ctx)
    prompt = runner.call_args.args[0] if runner.call_args else ""
    return result, prompt


class TestResearchRerunReadsTheNote:
    def test_first_run_prompt_has_no_revision_block(self, tmp_path):
        ctx = _ctx(tmp_path)
        docs = tmp_path / "tickets" / SLUG / "docs"
        result, prompt = _run(ctx, side_effect=lambda *a, **k: (
            (docs / "research.md").write_text("first answer\n"), "done")[1])
        assert result.status == "ok"
        assert result.artifacts["revised"] is False
        assert "docs/research.md already exists" not in prompt
        assert "docs/notes/" not in prompt

    def test_rerun_prompt_names_the_note_and_the_prior_doc(self, tmp_path):
        ctx = _ctx(tmp_path)
        docs = tmp_path / "tickets" / SLUG / "docs"
        (docs / "research.md").write_text("first answer\n")
        (docs / "notes").mkdir()
        (docs / "notes" / "note-2026-09-02.md").write_text("also cost the option\n")
        result, prompt = _run(ctx, side_effect=lambda *a, **k: (
            (docs / "research.md").write_text("second answer\n"), "done")[1])
        assert result.status == "ok"
        assert result.artifacts["revised"] is True
        assert "docs/research.md already exists" in prompt
        assert "docs/notes/note-2026-09-02.md" in prompt
        assert "outrank the original ticket" in prompt

    def test_unchanged_document_fails_the_run(self, tmp_path):
        ctx = _ctx(tmp_path)
        docs = tmp_path / "tickets" / SLUG / "docs"
        (docs / "research.md").write_text("first answer\n")
        result, _ = _run(ctx)
        assert result.status == "failed"
        assert "unchanged" in result.reason

    def test_missing_document_still_fails_the_run(self, tmp_path):
        ctx = _ctx(tmp_path)
        result, _ = _run(ctx)
        assert result.status == "failed"
        assert "not produced" in result.reason


def _check_done_ticket(fake_config, tmp_state, work_type):
    state.save("tickets", {"PROJ-1": make_ticket_state(
        status="done", work_type=work_type, discovered_at="2026-09-01T00:00:00Z")})
    with patch("features.tickets._fetch_tickets", return_value=[make_ticket()]), \
         patch("features.tickets._fetch_open_prs", return_value=[]), \
         patch("features.tickets.get_repos",
               return_value=[{"name": "myrepo", "path": tmp_state / "repo"}]), \
         patch("core.queue.jobs_for_ticket", return_value=[]), \
         patch("features.tickets._reconcile_prs", side_effect=lambda ts, *_a: ts), \
         patch("features.tickets._create_pr", side_effect=lambda _c, _t, ts, *_a: ts), \
         patch("features.tickets._enqueue_stage"):
        tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="inst")
    return state.load_ticket("PROJ-1")


class TestFinishedResearchTicketStaysDone:
    def test_research_ticket_is_not_reopened(self, fake_config, tmp_state):
        saved = _check_done_ticket(fake_config, tmp_state, "research")
        assert saved["status"] == "done"

    def test_code_ticket_is_still_reopened(self, fake_config, tmp_state):
        saved = _check_done_ticket(fake_config, tmp_state, "code")
        assert saved["status"] == "pr_ready"


def _note_reset(tmp_path, fake_config, note, files):
    ticket_dir = fake_config["workspace"]["root"] / "tickets" / SLUG
    docs = ticket_dir / "docs"
    docs.mkdir(parents=True, exist_ok=True)
    for name, body in files.items():
        target = docs / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body)
    state.save("tickets", {"PROJ-1": make_ticket_state(status="reviewing", slug=SLUG)})
    ctx = TaskContext(
        instance_key="inst",
        ticket_key="PROJ-1",
        task="apply_note_reset",
        payload={"note": note},
        job_id=0,
        triggering_event_id=None,
        config=fake_config,
        registry=None,
        now=None,
    )
    return apply_note_reset(ctx), docs


class TestApplyNoteReset:
    def test_note_is_written_and_ticket_resets_to_new(self, tmp_path, fake_config, tmp_state):
        result, docs = _note_reset(tmp_path, fake_config, "also cost the option", {})
        assert result.status == "ok"
        notes = list((docs / "notes").glob("note-*.md"))
        assert len(notes) == 1
        assert "also cost the option" in notes[0].read_text()
        assert state.load_ticket("PROJ-1")["status"] == "new"

    def test_research_is_copied_not_moved(self, tmp_path, fake_config, tmp_state):
        result, docs = _note_reset(tmp_path, fake_config, "revise it",
                                   {"research.md": "first answer\n"})
        assert result.status == "ok"
        assert result.artifacts["copied"] == ["research.md"]
        assert (docs / "research.md").read_text() == "first answer\n"
        archive = Path(result.artifacts["archived_to"])
        assert (archive / "research.md").read_text() == "first answer\n"

    def test_plan_documents_are_moved_away(self, tmp_path, fake_config, tmp_state):
        result, docs = _note_reset(tmp_path, fake_config, "revise it",
                                   {"technical-plan.md": "plan\n",
                                    "tri-review.md": "review\n"})
        assert result.status == "ok"
        assert sorted(result.artifacts["moved"]) == ["technical-plan.md", "tri-review.md"]
        assert not (docs / "technical-plan.md").exists()
        assert not (docs / "tri-review.md").exists()
        archive = Path(result.artifacts["archived_to"])
        assert (archive / "technical-plan.md").read_text() == "plan\n"

    def test_nothing_to_archive_leaves_no_archive_directory(self, tmp_path, fake_config, tmp_state):
        result, docs = _note_reset(tmp_path, fake_config, "revise it", {})
        assert result.status == "ok"
        assert result.artifacts["archived_to"] is None
        assert result.artifacts["moved"] == []
        assert result.artifacts["copied"] == []
        assert not (docs / "archive").exists()

    def test_missing_ticket_fails(self, tmp_path, fake_config, tmp_state):
        state.save("tickets", {})
        ctx = TaskContext(
            instance_key="inst", ticket_key="NOPE-1", task="apply_note_reset",
            payload={"note": "x"}, job_id=0, triggering_event_id=None,
            config=fake_config, registry=None, now=None,
        )
        result = apply_note_reset(ctx)
        assert result.status == "failed"
        assert result.reason == "ticket not found"
