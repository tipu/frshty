"""The /tickets review runs the fan-out arm the experiment measured.

Arm C recalled 8 of 12 pre-registered defects against 7 for the persona-split
prompt it replaces. Its three parts are the fan-out with no lane split, the
provenance step, and the clearance step. A part dropped here is the arm's
recall gone, so each one is pinned.
"""
from unittest.mock import MagicMock, patch

import core.tasks.tickets as T
from tests.core.test_pipeline_task_bodies import SLUG, _ctx, _seed, _ticket_dir


def _prompt(fake_config, repos=()):
    _seed(status="reviewing")
    d = _ticket_dir(fake_config)
    (d / "docs" / "tri-review.md").write_text("VERDICT: PASS\n")
    runner = MagicMock(return_value="done")
    with patch("core.tasks.tickets.repos_with_branch_diff", return_value=list(repos)), \
         patch("core.tasks.tickets.run_claude_code", runner), \
         patch("core.tasks.tickets.log.emit"):
        T.start_reviewing(_ctx(fake_config, "start_reviewing"))
    return runner.call_args.args[0]


class TestTheFanOutHasNoLaneSplit:
    def test_it_asks_for_three_independent_reviewers(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "Run three independent reviewers in parallel as sub-agents" in prompt

    def test_every_reviewer_gets_the_whole_diff_and_every_question(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "Every reviewer reviews the ENTIRE diff and answers all three questions" in prompt
        assert "There is no persona split and no lane" in prompt

    def test_no_finding_is_graded_down_for_belonging_to_another_lane(
            self, fake_config, tmp_state):
        """The persona prompts each told the model to leave the other two lanes
        alone, which is how a defect seen through the wrong lens was filed as a
        suggestion."""
        prompt = _prompt(fake_config)
        assert "no finding is graded down because it belongs to somebody else's question" in prompt

    def test_it_no_longer_delegates_to_the_slash_command(self, fake_config, tmp_state):
        """The prompt is self-contained, so the arm cannot be changed out from
        under the pipeline by an edit to a command file outside the repo."""
        prompt = _prompt(fake_config)
        assert "Run /tri-review" not in prompt
        assert "Run three independent reviewers" in prompt


class TestProvenanceDemotesAndNeverDrops:
    """One of the twelve ground-truth defects lived in already-merged peer code.
    A provenance rule that drops such a finding loses a real defect, so the rule
    demotes it instead."""

    def test_it_names_both_provenance_commands(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "git -C <worktree> log -1 --format='%h %an %cI %s' -- <path>" in prompt
        assert "merge-base --is-ancestor" in prompt
        assert "ALREADY-ON-BASE" in prompt

    def test_an_already_merged_defect_is_demoted_not_dropped(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "Demote that finding. Never drop it." in prompt
        assert "lower its severity to [suggestion]" in prompt

    def test_it_never_tells_the_reviewer_to_withhold_the_finding(self, fake_config, tmp_state):
        """The measured arm said 'Do not file it as a finding for this ticket'.
        That sentence is what drops the defect."""
        prompt = _prompt(fake_config)
        assert "Do not file it as a finding" not in prompt

    def test_a_demoted_finding_keeps_its_evidence_and_its_provenance(
            self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "Keep the finding and its evidence" in prompt
        assert "record the commit and the author the check named" in prompt

    def test_a_demoted_finding_cannot_block_the_ticket(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "A demoted finding is never tagged [blocking]." in prompt


class TestClearancesAreAnOutput:
    def test_it_asks_for_one_line_per_cleared_part(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "For every non-trivial part of the diff you examined and cleared" in prompt
        assert "### Cleared" in prompt

    def test_it_says_why_an_empty_review_is_not_enough(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert ("An empty findings list with no clearances cannot be told apart from never "
                "having looked.") in prompt


class TestTheOutputContractIsUnchanged:
    """features.tickets._VERDICT_RE and the reviewing state handler read this
    document. The arm swap must not move the line they match."""

    def test_the_verdict_line_is_still_asked_for_verbatim(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        assert "A line reading exactly 'VERDICT: PASS'" in prompt
        assert "'VERDICT: FAIL' otherwise" in prompt

    def test_the_document_and_its_first_two_sections_are_unchanged(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        for section in ("## Tri-Review: <short description>", "### Verdict", "### Findings"):
            assert section in prompt

    def test_the_verdict_the_prompt_asks_for_is_the_one_the_parser_reads(
            self, fake_config, tmp_state):
        from features.tickets import _VERDICT_RE
        for verdict in ("VERDICT: PASS", "VERDICT: FAIL"):
            assert _VERDICT_RE.search(verdict), verdict

    def test_the_new_sections_are_added_not_substituted(self, fake_config, tmp_state):
        prompt = _prompt(fake_config)
        for section in ("### Cleared", "### Pre-existing", "### Reviewer Disagreements"):
            assert section in prompt


class TestTheRepoScopeSurvivedTheSwap:
    """DEV-635 was reviewed in four of five repos because the prompt left the
    choice of worktrees to the model. The repo block has to survive the swap."""

    def _repos(self, config):
        root = config["workspace"]["root"] / "tickets" / SLUG
        return [("windows-rpa-client", root / "windows-rpa-client", "main")]

    def test_the_repo_block_still_leads_the_prompt(self, fake_config, tmp_state):
        prompt = _prompt(fake_config, self._repos(fake_config))
        assert "must be named in docs/tri-review.md" in prompt
        assert "windows-rpa-client" in prompt

    def test_the_provenance_step_knows_the_base_branch_without_a_repo_block(
            self, fake_config, tmp_state):
        """With no repo block there is no per-repo base branch, so the workspace
        base branch has to reach the provenance command or the check silently
        compares against nothing."""
        base = fake_config["workspace"].get("base_branch", "main")
        prompt = _prompt(fake_config)
        assert f"The base branch is origin/{base}" in prompt
