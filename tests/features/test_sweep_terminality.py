"""A ticket that leaves the ticket query has not necessarily finished.

DEV-636 was closed out from pr_ready while Jira held it at "Changes Requested".
The sweep read absence from the query as proof of completion. These pin the
replacement rule: ask the source, and only close the ticket when the source says
it is finished.
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import features.tickets as tickets


def _config():
    return {"workspace": {"root": Path("/tmp/x"), "tickets_dir": "tickets", "repos": []},
            "_base_url": "http://base", "_state_dir": Path("/tmp/x")}


def _sweep(local_status: str, upstream: str | None, prs=None):
    """Run one check() cycle where the ticket is absent from the query result."""
    cfg = _config()
    state = {"DEV-1": {"status": local_status, "slug": "DEV-1-x", "prs": prs or []}}
    saved = {}

    system = MagicMock()
    system.fetch_ticket.return_value = ({"key": "DEV-1", "status": upstream}
                                        if upstream is not None else None)

    with patch.object(tickets, "_fetch_tickets", return_value=[{"key": "DEV-OTHER", "summary": "s"}]), \
         patch.object(tickets, "state") as st, \
         patch.object(tickets, "make_ticket_system", return_value=system), \
         patch.object(tickets, "get_repos", return_value=[]), \
         patch.object(tickets, "enqueue_prd_backfill"), \
         patch.object(tickets, "log"), \
         patch("features.ticket_states._PRE_DISPATCH_HANDLERS", ()), \
         patch("features.ticket_states._STATUS_HANDLERS", ()):
        st.load.return_value = state
        st.save_ticket.side_effect = lambda k, v: saved.__setitem__(k, dict(v))
        tickets.check(cfg, "aimyable")
    return saved.get("DEV-1", state["DEV-1"])


class TestSweepAsksTheSource:
    def test_pr_ready_is_not_closed_when_upstream_wants_rework(self):
        """The DEV-636 case. Changes Requested is not finished."""
        out = _sweep("pr_ready", "Changes Requested")
        assert out["status"] == "pr_ready"
        assert "done_at" not in out

    def test_blocked_is_not_closed_when_upstream_is_still_active(self):
        out = _sweep("blocked", "Ready for Deploy")
        assert out["status"] == "blocked"

    def test_gate_holding_status_is_not_closed_while_upstream_is_active(self):
        """planning holds the repo gate, so a wrong close here is not the only risk;
        a wrong hold would stall the repo. Upstream is what decides."""
        out = _sweep("planning", "In Progress")
        assert out["status"] == "planning"

    def test_ticket_is_closed_when_upstream_says_done(self):
        out = _sweep("pr_ready", "Done")
        assert out["status"] == "done"
        assert out["done_at"]

    def test_ticket_is_closed_when_upstream_says_cancelled(self):
        """Cancelled must still close, or a gate-holding ticket strands forever."""
        out = _sweep("planning", "Cancelled")
        assert out["status"] == "done"

    def test_unreadable_upstream_leaves_the_ticket_alone(self):
        """A failed lookup is not evidence of completion."""
        out = _sweep("pr_ready", None)
        assert out["status"] == "pr_ready"
        assert "done_at" not in out

    def test_terminal_match_ignores_case_and_spacing(self):
        assert tickets._is_terminal_upstream({}, "  CANCELLED ")
        assert tickets._is_terminal_upstream({}, "Won't Do")
        assert not tickets._is_terminal_upstream({}, "In Review")

    def test_terminal_names_are_configurable(self):
        cfg = {"tickets": {"terminal_statuses": ["Shipped"]}}
        assert tickets._is_terminal_upstream(cfg, "shipped")
        assert not tickets._is_terminal_upstream(cfg, "Done")


class TestUnresolvedIsLoggedOnce:
    def test_repeat_scans_with_the_same_status_log_once(self):
        """A line every cycle is how this codebase has produced dispatcher loops."""
        ts = {"status": "pr_ready"}
        with patch.object(tickets, "log") as lg:
            tickets._note_unresolved_sweep("DEV-1", ts, "Changes Requested")
            tickets._note_unresolved_sweep("DEV-1", ts, "Changes Requested")
        assert lg.emit.call_count == 1
        assert ts["sweep_unresolved_status"] == "Changes Requested"

    def test_a_changed_status_logs_again(self):
        ts = {"status": "pr_ready", "sweep_unresolved_status": "Changes Requested"}
        with patch.object(tickets, "log") as lg:
            tickets._note_unresolved_sweep("DEV-1", ts, "Ready for Testing")
        assert lg.emit.call_count == 1

    def test_unknown_status_is_recorded_as_unknown(self):
        ts = {"status": "pr_ready"}
        with patch.object(tickets, "log"):
            tickets._note_unresolved_sweep("DEV-1", ts, None)
        assert ts["sweep_unresolved_status"] == "unknown"


class TestUpstreamStatusLookup:
    def test_a_raising_ticket_system_returns_none_rather_than_propagating(self):
        system = MagicMock()
        system.fetch_ticket.side_effect = RuntimeError("boom")
        with patch.object(tickets, "make_ticket_system", return_value=system), \
             patch.object(tickets, "log"):
            assert tickets._upstream_status({}, "DEV-1") is None

    def test_a_blank_status_is_treated_as_undeterminable(self):
        system = MagicMock()
        system.fetch_ticket.return_value = {"key": "DEV-1", "status": "  "}
        with patch.object(tickets, "make_ticket_system", return_value=system):
            assert tickets._upstream_status({}, "DEV-1") is None


class TestNoDiffIsNotAssumedFromAFailedDiff:
    """A git diff that errors returns empty stdout. Before this guard, that read
    as "no code changes" and marked the ticket merged. The base-branch mismatch
    class (base_branch=main against a repo whose default is master) produces
    exactly that failure."""

    def _run(self, rc: int, stdout: str):
        import subprocess as sp
        cfg = _config()
        cfg["workspace"]["repos"] = [{"name": "r", "path": "/tmp/r"}]
        ts = {"slug": "DEV-1-x", "branch": "b", "status": "pr_ready"}
        ticket = {"key": "DEV-1", "summary": "s", "url": ""}
        marked = {}

        def fake_run(cmd, *a, **k):
            if cmd[:2] == ["git", "diff"]:
                return sp.CompletedProcess(cmd, rc, stdout, "fatal: bad revision")
            return sp.CompletedProcess(cmd, 0, "", "")

        with patch.object(tickets, "make_platform"), \
             patch.object(tickets, "get_repos", return_value=[{"name": "r", "path": "/tmp/r"}]), \
             patch.object(tickets, "ticket_worktree_path") as wtp, \
             patch.object(tickets, "base_branch_for", return_value="main"), \
             patch.object(tickets, "subprocess") as subp, \
             patch.object(tickets, "_mark_ticket_merged",
                          side_effect=lambda c, t, s: marked.setdefault("merged", True) or s), \
             patch.object(tickets, "log"):
            wtp.return_value = MagicMock(is_dir=lambda: True)
            subp.run.side_effect = fake_run
            tickets._create_pr(cfg, ticket, ts, "http://base")
        return marked

    def test_failed_diff_does_not_mark_the_ticket_merged(self):
        assert self._run(rc=128, stdout="") == {}

    def test_genuine_empty_diff_still_marks_merged(self):
        assert self._run(rc=0, stdout="") == {"merged": True}


class TestSweepSeesTheMerge:
    """DEV-604's PRs merged after Jira had already moved it past the query
    window. The sweep read only the upstream status, found "Ready for Testing"
    non-terminal, and parked the ticket at in_review for a week. Merged PRs are
    the same proof _check_in_review acts on."""

    def _sweep_with_pr_states(self, local_status: str, upstream: str | None, pr_states: list[str]):
        cfg = _config()
        cfg["workspace"]["repos"] = [{"name": "r", "path": "/tmp/r"}]
        prs = [{"repo": "r", "id": i} for i in range(len(pr_states))]
        state = {"DEV-1": {"status": local_status, "slug": "DEV-1-x", "prs": prs}}
        saved = {}

        system = MagicMock()
        system.fetch_ticket.return_value = ({"key": "DEV-1", "status": upstream}
                                            if upstream is not None else None)
        platform = MagicMock()
        platform.get_pr_state.side_effect = lambda repo, pr_id: pr_states[pr_id]

        with patch.object(tickets, "_fetch_tickets", return_value=[{"key": "DEV-OTHER", "summary": "s"}]), \
             patch.object(tickets, "state") as st, \
             patch.object(tickets, "make_ticket_system", return_value=system), \
             patch.object(tickets, "make_platform", return_value=platform), \
             patch.object(tickets, "get_repos", return_value=[{"name": "r", "path": "/tmp/r"}]), \
             patch.object(tickets, "_fetch_open_prs", return_value=[]), \
             patch.object(tickets, "_fetch_ticket_comments", return_value=[]), \
             patch.object(tickets, "enqueue_prd_backfill"), \
             patch.object(tickets, "_maybe_enqueue_ranker"), \
             patch.object(tickets, "log"), \
             patch("features.ticket_states._PRE_DISPATCH_HANDLERS", ()), \
             patch("features.ticket_states._STATUS_HANDLERS", ()):
            st.load.return_value = state
            st.save_ticket.side_effect = lambda k, v: saved.__setitem__(k, dict(v))
            tickets.check(cfg, "aimyable")
        return saved.get("DEV-1", state["DEV-1"])

    def test_all_prs_merged_moves_the_ticket_to_merged(self):
        out = self._sweep_with_pr_states("in_review", "Ready for Testing", ["MERGED", "MERGED"])
        assert out["status"] == "merged"
        assert out["merged_at"]

    def test_a_merged_sweep_clears_the_unresolved_marker(self):
        out = self._sweep_with_pr_states("in_review", "Ready for Deploy", ["MERGED"])
        assert "sweep_unresolved_status" not in out

    def test_an_open_pr_still_leaves_the_ticket_alone(self):
        out = self._sweep_with_pr_states("in_review", "Ready for Testing", ["OPEN", "MERGED"])
        assert out["status"] == "in_review"

    def test_a_declined_pr_is_not_a_merge(self):
        """DECLINED alongside MERGED is a split outcome, not a landed change."""
        out = self._sweep_with_pr_states("in_review", "Ready for Testing", ["DECLINED", "MERGED"])
        assert out["status"] == "in_review"
        assert out["sweep_unresolved_status"] == "Ready for Testing"

    def test_a_status_that_cannot_reach_merged_is_left_alone(self):
        out = self._sweep_with_pr_states("planning", "Ready for Testing", ["MERGED"])
        assert out["status"] == "planning"

    def test_upstream_done_still_wins_when_no_prs_are_tracked(self):
        out = _sweep("pr_ready", "Done")
        assert out["status"] == "done"
