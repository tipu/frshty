"""Content prompts pass a JSON schema to the tier and read the answer with
parse_structured, so an answer outside the schema reads as no answer."""
import json
from unittest.mock import patch

import core.llm as llm
import features.acceptance as acceptance
import features.pr_autofix as pr_autofix
import features.timesheet as timesheet
import pm.release_runner as release_runner
import prd.generator as generator
import web.reviews as reviews


def test_daily_summary_list_becomes_the_day_ticket_map():
    answer = json.dumps({"days": [
        {"date": "2026-10-01", "tickets": [{"ticket": "PROJ-1", "summary": "fixed the login"}]},
        {"date": "2026-10-02", "tickets": [{"ticket": "PROJ-2", "summary": "added a test"}]},
    ]})
    grouped = {"2026-10-01": {"PROJ-1": ["commit: a"]}, "2026-10-02": {"PROJ-2": ["commit: b"]}}
    with patch.object(timesheet, "run_haiku", return_value=answer) as haiku:
        out = timesheet._summarize_daily_activity(grouped)
    assert out == {"2026-10-01": {"PROJ-1": "fixed the login"},
                   "2026-10-02": {"PROJ-2": "added a test"}}
    assert haiku.call_args.kwargs["schema"] == timesheet.DAILY_SUMMARY_SCHEMA


def test_daily_summary_outside_the_schema_falls_back_to_the_first_item():
    grouped = {"2026-10-03": {"PROJ-3": ["commit: the first item"]}}
    with patch.object(timesheet, "run_haiku", return_value='{"2026-10-03": {"PROJ-3": "x"}}'), \
         patch.object(llm, "log"):
        out = timesheet._summarize_daily_activity(grouped)
    assert out == {"2026-10-03": {"PROJ-3": "commit: the first item"}}


def test_file_summaries_keep_only_the_changed_files():
    answer = json.dumps({"files": [{"path": "a.py", "summary": "adds the parser"},
                                   {"path": "made_up.py", "summary": "invented"},
                                   {"path": "b.py", "summary": ""}]})
    with patch.object(reviews, "run_haiku", return_value=answer) as haiku:
        out = reviews._generate_file_summaries("goal", {"a.py": "+x", "b.py": "+y"})
    assert out == {"a.py": "adds the parser"}
    assert haiku.call_args.kwargs["schema"] == reviews.FILE_SUMMARIES_SCHEMA


def test_enrich_rejects_a_criterion_without_its_fields():
    answer = json.dumps({"criteria": [{"id": "c1", "criterion": "export works"}]})
    with patch.object(acceptance, "run_haiku", return_value=answer), \
         patch.object(acceptance, "log") as log, patch.object(llm, "log"):
        assert acceptance.enrich("Export the report") is None
    assert log.emit.call_args.args[0] == "enrich_parse_failed"


def test_generator_sets_the_source_text_on_each_ticket():
    criteria = {"criteria": [{"id": "c1", "criterion": "x", "playwright": [],
                              "tests_required": []}]}
    answer = json.dumps({"tickets": [{"title": "t", "summary": "s", "description": "d",
                                      "acceptance_criteria": criteria}]})
    section = {"header": "Export", "content": "Export a CSV.", "stable_key": "k"}
    with patch.object(generator, "run_haiku", return_value=answer) as haiku:
        out = generator.generate(section)
    assert out[0]["acceptance_criteria"]["source_text"] == "## Export\nExport a CSV."
    assert haiku.call_args.kwargs["schema"] == generator.GENERATOR_SCHEMA


def test_release_inspection_rejects_an_unknown_verdict():
    with patch.object(release_runner._releases, "get_release_by_id",
                      return_value={"release_key": "r1", "ticket_set_hash": ""}), \
         patch.object(release_runner._releases, "list_release_tickets", return_value=[{"key": "T-1"}]), \
         patch.object(release_runner._releases, "all_terminal", return_value=True), \
         patch.object(release_runner._releases, "read_release_md", return_value=("", "h")), \
         patch.object(release_runner._releases, "compute_ticket_set_hash", return_value="new"), \
         patch.object(release_runner._releases, "build_inspection_payload", return_value="p"), \
         patch.object(release_runner._releases, "insert_release_review") as insert, \
         patch.object(release_runner, "run_haiku",
                      return_value='{"verdict": "maybe", "findings": []}'), \
         patch.object(release_runner, "log") as log, patch.object(llm, "log"):
        assert release_runner.run_release_inspection("i", 1, {}) is None
    insert.assert_not_called()
    assert log.emit.call_args.args[0] == "release_inspect_parse_failed"


def test_consolidation_drops_a_finding_outside_the_schema():
    by_provider = {"claude": [{"severity": "high", "path": "a.py", "line": 3, "title": "t",
                               "body": "b", "providers": ["claude"]}],
                   "codex": [{"severity": "critical", "path": "a.py", "line": 3, "title": "t",
                              "body": "b", "providers": ["codex"]}]}
    off_schema = json.dumps({"findings": [{"severity": "medium", "path": "a.py", "line": 3,
                                           "title": "t", "body": "b", "providers": ["claude"]}]})
    pr = {"repo": "app", "id": 1}
    with patch.object(pr_autofix, "run_balanced", return_value=off_schema) as balanced, \
         patch.object(pr_autofix, "log") as log, patch.object(llm, "log"):
        out = pr_autofix._consolidate({}, pr, by_provider)
    assert balanced.call_args.kwargs["schema"] == pr_autofix.CONSOLIDATE_SCHEMA
    assert log.emit.call_args.args[0] == "pr_autofix_merge_fallback"
    assert out
