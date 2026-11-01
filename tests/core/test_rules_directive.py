from unittest.mock import patch

import core.llm as llm
from core.llm import TICKET_WORKSPACE_RULE, rules_directive


def test_no_rules_file_still_names_the_ticket_workspace():
    assert rules_directive([]) == TICKET_WORKSPACE_RULE


def test_rules_files_come_first_then_the_ticket_workspace(tmp_path):
    rules = tmp_path / "CLAUDE.md"
    rules.write_text("plain rules")

    text = rules_directive([rules])

    assert f"- {rules}\n" in text
    assert text.endswith(TICKET_WORKSPACE_RULE)
    assert "secret.py" in TICKET_WORKSPACE_RULE
    assert "git with -C" in TICKET_WORKSPACE_RULE


def test_rules_file_with_a_secret_emits_once(tmp_path):
    llm._secret_warned.clear()
    rules = tmp_path / "CLAUDE.md"
    rules.write_text("Jira API token: abc123def456\n")

    with patch.object(llm.log, "emit") as emit:
        rules_directive([rules])
        rules_directive([rules])

    assert [c.args[0] for c in emit.call_args_list] == ["rules_file_holds_secret"]
    assert "abc123def456" not in str(emit.call_args_list)


def test_rules_file_without_a_secret_emits_nothing(tmp_path):
    llm._secret_warned.clear()
    rules = tmp_path / "CLAUDE.md"
    rules.write_text("Read the token with `python3 /app/scripts/secret.py jira-api-token`.\n")

    with patch.object(llm.log, "emit") as emit:
        rules_directive([rules])

    emit.assert_not_called()
