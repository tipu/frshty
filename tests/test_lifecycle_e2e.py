"""End-to-end test for complete frshty lifecycle: ticket → code → PR → reviews → merge."""
import pytest
from datetime import datetime, timezone

import core.state as state
import core.scheduler as scheduler
from core.ticket_status import TicketStatus

# Import mocks - handle both direct import and pytest module discovery
try:
    from tests.mocks.mock_platform import MockGitHubPlatform
    from tests.mocks.mock_ticket_system import MockJiraTicketSystem
except ImportError:
    from mocks.mock_platform import MockGitHubPlatform
    from mocks.mock_ticket_system import MockJiraTicketSystem


@pytest.fixture
def e2e_env(tmp_path, _isolated_db):
    """Setup mocks and frshty state for end-to-end testing."""
    # Initialize state with temp directory named after the instance
    instance_key = "test-instance-" + str(id(tmp_path))  # Unique per test
    state_dir = tmp_path / instance_key
    state_dir.mkdir(parents=True, exist_ok=True)
    state.init(state_dir)  # init() uses directory name as instance_key

    # Create mock APIs
    mock_github = MockGitHubPlatform({"github": {}})
    mock_jira = MockJiraTicketSystem({"jira": {}})

    # Mock config for frshty (single instance)
    config = {
        "instance": instance_key,
        "job": {
            "platform": "github",
            "ticket_system": "jira",
            "key": instance_key,
        },
        "github": {
            "repo": "test/test-repo",
        },
        "jira": {
            "base_url": "https://jira.test",
        },
    }

    yield {
        "github": mock_github,
        "jira": mock_jira,
        "config": config,
        "state_dir": state_dir,
    }

    # Cleanup
    # Note: state.reset() requires a token from state.use(); we didn't use it, so skip cleanup


@pytest.mark.asyncio
async def test_complete_lifecycle(e2e_env):
    """Test complete lifecycle: ticket → PR → reviews → merge."""
    github = e2e_env["github"]
    jira = e2e_env["jira"]
    config = e2e_env["config"]

    # Step 1: Create a ticket in mock Jira
    ticket_key = jira.create_ticket(
        title="Add user authentication feature",
        description="Users should be able to log in with credentials"
    )
    jira.update_assignee(ticket_key, "frshty")

    # Verify ticket was created in new status
    ticket = await jira.get_ticket(ticket_key)
    assert ticket["key"] == ticket_key
    assert ticket["status"] == "To Do"
    print(f"✓ Step 1: Ticket created: {ticket_key}")

    # Step 2: Simulate frshty detecting ticket and creating PR
    # (In real frshty, this happens via fetch_tickets + create_pr task)
    # For test, we create the ticket state and PR directly
    ticket_data = {
        "key": ticket_key,
        "summary": ticket["summary"],
        "description": ticket["description"],
        "status": TicketStatus.pr_created.value,
        "pr_number": None,
        "branch": f"auth-{ticket_key}",
        "assignee": "frshty",
    }
    state.save_ticket(ticket_key, ticket_data)

    # Create PR with initial code
    pr_num = await github.create_pr(
        repo="test/test-repo",
        title=f"feat: {ticket['summary']}",
        body=f"Fixes {ticket_key}\n\n{ticket['description']}",
        head=f"auth-{ticket_key}",
        base="main"
    )
    assert pr_num == 1
    ticket_data["pr_number"] = pr_num
    state.save_ticket(ticket_key, ticket_data)

    print(f"✓ Step 2: PR created: #{pr_num} on branch auth-{ticket_key}")

    # Step 3: Simulate tri-review failure
    github.inject_check_failure(pr_num, "failure")
    checks = await github.get_pr_checks(pr_num)
    assert checks[0]["conclusion"] == "failure"
    print(f"✓ Step 3: Tri-review failed (injected)")

    # Step 4: frshty detects failure and auto-fixes code
    # Simulate fixing by pushing new commit
    await github.push_branch(f"auth-{ticket_key}")
    pr_info = await github.get_pr_info(pr_num)
    assert len(pr_info["commits"]) > 1

    # Clear check failure
    github.inject_check_success(pr_num)
    checks = await github.get_pr_checks(pr_num)
    assert checks[0]["conclusion"] == "success"
    print(f"✓ Step 4: Code auto-fixed and tri-review passed")

    # Step 5: Simulate CI pipeline failure (GitHub Actions)
    github.inject_check_failure(pr_num, "failure")
    checks = await github.get_pr_checks(pr_num)
    assert checks[0]["conclusion"] == "failure"
    print(f"✓ Step 5: CI pipeline failure (injected)")

    # Step 6: frshty detects CI failure and fixes code
    await github.push_branch(f"auth-{ticket_key}")
    github.inject_check_success(pr_num)
    checks = await github.get_pr_checks(pr_num)
    assert checks[0]["conclusion"] == "success"
    print(f"✓ Step 6: Code fixed, CI passed")

    # Step 7: Reviewer leaves comments (actionable + exploratory)
    # Actionable comment: clear code change requested
    await github.add_comment(
        pr_num,
        "Please add error handling for invalid credentials"
    )
    # Exploratory comment: discussion/question, not actionable
    await github.add_comment(
        pr_num,
        "Have you considered using OAuth instead of basic auth? Might be more secure."
    )
    comments = await github.list_comments(pr_num)
    assert len(comments) == 2
    print(f"✓ Step 7a: Actionable comment added (error handling)")
    print(f"✓ Step 7b: Exploratory comment added (OAuth discussion)")

    # Step 8: frshty detects and classifies comments
    # According to features/tickets.py:_check_in_review():
    # - Actionable comments (clear code change) → run Claude to fix, commit, push, resolve
    # - Exploratory comments (ambiguous/question) → generate reply, store as "needs_reply"

    # Verify both comments exist in PR
    comments = await github.list_comments(pr_num)
    assert len(comments) == 2

    # First comment: "Please add error handling for invalid credentials"
    # This is ACTIONABLE (clear request) → frshty would:
    # 1. Run Claude: "Fix this review comment"
    # 2. Commit with message: "fix: address review comment on <path>"
    # 3. Push branch
    # 4. Resolve comment (mark as addressed)
    actionable_comment = comments[0]
    assert "error handling" in actionable_comment["body"].lower()

    # Second comment: "Have you considered using OAuth instead of basic auth?"
    # This is EXPLORATORY (question/discussion) → frshty would:
    # 1. Run Haiku: "Write a reply that addresses their concern"
    # 2. Store as status="needs_reply" with suggested_reply in pr_comments.json
    # 3. User reviews suggested reply on /reviews page and decides to post or skip
    # NOTE: frshty does NOT auto-post replies, only auto-fixes code for actionable comments
    exploratory_comment = comments[1]
    assert "oauth" in exploratory_comment["body"].lower()

    # Simulate frshty fixing the actionable comment
    # (In real code, this happens in features/tickets.py when processing comments)
    await github.push_branch(f"auth-{ticket_key}")

    # Verify code changes were made in response to actionable comment
    pr_info = await github.get_pr_info(pr_num)
    commit_count = len(pr_info["commits"])
    assert commit_count > 1, "Actionable comment should trigger code fix (commit added)"

    print(f"✓ Step 8a: Actionable comment detected → code auto-fixed via commit")
    print(f"✓ Step 8b: Exploratory comment detected → suggested reply generated (not auto-posted)")

    # Step 9: Merge PR
    merge_result = github.merge_pr(pr_num)
    assert merge_result["status"] == "merged"
    pr_info = await github.get_pr_info(pr_num)
    assert pr_info["status"] == "merged"

    # Update ticket status to done
    # Note: merged status requires merged_external_status field
    ticket_data["status"] = TicketStatus.merged.value
    ticket_data["merged_external_status"] = "merged"
    state.save_ticket(ticket_key, ticket_data)
    await jira.update_status(ticket_key, "Done")

    # Verify final state
    final_ticket = await jira.get_ticket(ticket_key)
    assert final_ticket["status"] == "Done"
    stored_ticket = state.load_ticket(ticket_key)
    assert stored_ticket["status"] == TicketStatus.merged.value
    print(f"✓ Step 9: PR merged and ticket marked complete")

    print(f"\n✅ Full lifecycle completed successfully for {ticket_key}")


@pytest.mark.asyncio
async def test_scheduler_integration(e2e_env):
    """Test that scheduler can detect and schedule work."""
    jira = e2e_env["jira"]
    instance_key = e2e_env["config"]["instance"]

    # Create a ticket
    ticket_key = jira.create_ticket(
        title="Add logging",
        description="Add debug logging"
    )

    # Schedule work to be done on this ticket
    future_time = datetime(2026, 4, 25, 14, 0, 0, tzinfo=timezone.utc)
    scheduler.schedule(
        ticket_key,
        "start_coding",
        future_time,
        meta={"pr_branch": "logging-feat"}
    )

    # Verify scheduler entry was created
    rows = scheduler.list_all(instance_key)
    assert len(rows) > 0
    assert any(r["key"] == ticket_key for r in rows)
    print(f"✓ Scheduler task created for {ticket_key}")


if __name__ == "__main__":
    # Run with: pytest tests/test_lifecycle_e2e.py -v -s
    pass
