"""Tests for staleness.merge_ready_ticket_prs.

The bucket answers 'get my approved PRs merged'. It used to qualify a ticket
as soon as one of its PRs had an approver, so a three-PR ticket with one
approval was reported merge ready and the operator had to notice the other two
himself."""
import pytest

import core.db as db
import core.state as state
import features.platforms as platforms
import manager.staleness as staleness


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "test"
    state._instance_key_cv.set("test")
    yield


def _ticket(key, prs, ci_passed=True, status="in_review"):
    state.save_ticket(key, {
        "status": status,
        "slug": key.lower(),
        "summary": f"summary for {key}",
        "discovered_at": "2026-09-01T00:00:00Z",
        "ci_passed": ci_passed,
        "prs": prs,
    })


def _pr(repo, pr_id, approvers, pr_state="OPEN"):
    return {"repo": repo, "id": pr_id, "url": f"http://pr/{repo}/{pr_id}",
            "author": "danial", "approvers": approvers, "pr_state": pr_state}


def test_every_open_pr_approved_qualifies():
    _ticket("DEV-100", [_pr("api", 1, ["reviewer"]), _pr("web", 2, ["reviewer"])])

    out = staleness.merge_ready_ticket_prs("test")

    assert [r["ticket_key"] for r in out] == ["DEV-100"]
    assert [(p["repo"], p["id"]) for p in out[0]["prs"]] == [("api", 1), ("web", 2)]


def test_one_unapproved_sibling_disqualifies_the_ticket():
    """The reported defect: any_approved reported this ticket as merge ready."""
    _ticket("DEV-101", [_pr("api", 3, ["reviewer"]), _pr("web", 4, [])])

    assert staleness.merge_ready_ticket_prs("test") == []


def test_a_merged_sibling_does_not_block_and_is_not_listed():
    _ticket("DEV-102", [_pr("api", 5, [], pr_state="MERGED"),
                        _pr("web", 6, ["reviewer"])])

    out = staleness.merge_ready_ticket_prs("test")

    assert [r["ticket_key"] for r in out] == ["DEV-102"]
    assert [(p["repo"], p["id"]) for p in out[0]["prs"]] == [("web", 6)]


def test_a_ticket_with_no_open_pr_left_does_not_qualify():
    _ticket("DEV-103", [_pr("api", 7, ["reviewer"], pr_state="MERGED")])

    assert staleness.merge_ready_ticket_prs("test") == []


def test_a_pr_never_polled_is_treated_as_open():
    _ticket("DEV-104", [{"repo": "api", "id": 8, "url": "", "approvers": ["reviewer"]}])

    assert [r["ticket_key"] for r in staleness.merge_ready_ticket_prs("test")] == ["DEV-104"]


def test_red_ci_disqualifies_even_when_every_pr_is_approved():
    _ticket("DEV-105", [_pr("api", 9, ["reviewer"])], ci_passed=False)

    assert staleness.merge_ready_ticket_prs("test") == []


def test_live_refresh_uses_the_platform_state_and_approvers():
    _ticket("DEV-106", [_pr("api", 10, ["stale-cache"]), _pr("web", 11, ["stale-cache"])])

    class _Platform:
        def get_pr_info(self, repo, pr_id):
            if repo == "web":
                return {"state": "OPEN", "approvers": []}
            return {"state": "OPEN", "approvers": ["reviewer"]}

    saved = platforms.make_platform
    platforms.make_platform = lambda config: _Platform()
    try:
        out = staleness.merge_ready_ticket_prs("test", {"job": {}}, live=True)
    finally:
        platforms.make_platform = saved

    assert out == []


def test_another_instances_ticket_is_not_reported():
    """Negative control: the bucket is per instance."""
    _ticket("DEV-107", [_pr("api", 12, ["reviewer"])])

    assert staleness.merge_ready_ticket_prs("other") == []
    assert db.query_one("SELECT COUNT(*) AS n FROM tickets")["n"] == 1
