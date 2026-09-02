"""A ticket that leaves the ticket query has not necessarily finished.

DEV-636 was closed out from pr_ready while Jira held it at "Changes Requested".
The sweep read absence from the query as proof of completion. These pin the
replacement rule: ask the source, and only close the ticket when the source says
it is finished.
"""
from pathlib import Path
from unittest.mock import MagicMock, patch

import features.tickets as tickets
from tests.conftest import make_ticket


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

    def test_the_merge_records_the_upstream_status_it_reads_now(self):
        """merged_external_status decides whether a later upstream move is a
        reopen. The cached external_status is stale by the time the sweep
        sees the ticket."""
        out = self._sweep_with_pr_states("in_review", "Ready for Deploy", ["MERGED"])
        assert out["merged_external_status"] == "Ready for Deploy"
        assert out["external_status"] == "Ready for Deploy"

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


class TestDev604Timeline:
    """The DEV-604 sequence, replayed cycle by cycle against the real state store.

    Cycle 1: the ticket is in the ticket query at "In Review" and both PRs are
    open. _check_in_review holds it at in_review.
    Cycle 2: Jira moves the ticket to "Ready for Testing". That status is
    outside the ticket query, so the query stops returning the ticket and no
    status handler runs for it again. The PRs are still open.
    Cycle 3: the PRs merge. The ticket is still outside the query.

    Before the fix, cycle 3 asked only whether "Ready for Testing" was terminal,
    read no, and left the ticket at in_review. The tickets board renders one
    column per stored status, so DEV-604 sat under "In review" with both PRs
    merged, and no later cycle could move it. The cycles below run the real
    check() against the real state store, so they also cover the durable write
    and the second-cycle behaviour, which a hand-built state dict does not.
    """

    KEY = "DEV-604"
    SLUG = "DEV-604-move-the-thing"
    URL = "https://jira.example.com/browse/DEV-604"

    def _prs(self):
        return [{"repo": "repo", "id": 11, "branch": self.SLUG, "url": "http://pr/11"},
                {"repo": "repo", "id": 12, "branch": self.SLUG, "url": "http://pr/12"}]

    def _seed(self, tmp_state):
        import core.state as state
        state.save("tickets", {
            self.KEY: {"status": "in_review", "slug": self.SLUG, "branch": self.SLUG,
                       "url": self.URL, "summary": "Move the thing",
                       "external_status": "In Review", "prs": self._prs()},
            # A second ticket keeps the query result non-empty; check() returns
            # early on an empty query. ignored is skipped by both loops.
            "DEV-OTHER": {"status": "ignored", "slug": "DEV-OTHER-x"},
        })

    def _cycle(self, config, tmp_state, *, in_query: bool, upstream: str, pr_state: str):
        """One check() pass over a world described by the ticket query result,
        the upstream ticket status, and the state of every tracked PR."""
        import core.state as state

        platform = MagicMock()
        platform.get_pr_state.return_value = pr_state
        platform.get_pr_info.return_value = {"state": pr_state, "approvers": [],
                                             "mergeable": "MERGEABLE"}
        platform.get_pr_comments.return_value = []
        platform.monitor_ci.side_effect = lambda _ticket, ts, _base_url: ts

        system = MagicMock()
        system.fetch_ticket.return_value = {"key": self.KEY, "status": upstream}

        assigned = [make_ticket(key="DEV-OTHER", summary="other")]
        if in_query:
            assigned.append(make_ticket(key=self.KEY, summary="Move the thing",
                                        status=upstream, url=self.URL))

        with patch.object(tickets, "_fetch_tickets", return_value=assigned), \
             patch.object(tickets, "_fetch_open_prs", return_value=[]), \
             patch.object(tickets, "_fetch_ticket_comments", return_value=[]), \
             patch.object(tickets, "_process_ticket_comments"), \
             patch.object(tickets, "enqueue_prd_backfill"), \
             patch.object(tickets, "run_haiku", return_value="code"), \
             patch.object(tickets, "get_repos",
                          return_value=[{"name": "repo", "path": tmp_state / "repo"}]), \
             patch.object(tickets, "make_platform", return_value=platform), \
             patch.object(tickets, "make_ticket_system", return_value=system), \
             patch("core.queue.jobs_for_ticket", return_value=[]), \
             patch("core.queue.enqueue_job"):
            tickets.check({**config, "_base_url": "http://base"}, instance_key="test")
        return state.load_ticket(self.KEY)

    def _merged_events(self):
        import core.db as db
        import core.state as state
        return db.query_all(
            "SELECT event FROM log_events WHERE instance_key=? "
            "AND json_extract(meta, '$.ticket')=? AND event='ticket_merged'",
            (state.active_instance_key(), self.KEY),
        )

    def test_the_ticket_reaches_merged_and_stays_there(self, fake_config, tmp_state, tmp_log):
        self._seed(tmp_state)

        in_query = self._cycle(fake_config, tmp_state,
                               in_query=True, upstream="In Review", pr_state="OPEN")
        assert in_query["status"] == "in_review"

        left_query = self._cycle(fake_config, tmp_state,
                                 in_query=False, upstream="Ready for Testing", pr_state="OPEN")
        assert left_query["status"] == "in_review", "an open PR is not a finished ticket"
        assert "merged_at" not in left_query

        merged = self._cycle(fake_config, tmp_state,
                             in_query=False, upstream="Ready for Testing", pr_state="MERGED")
        assert merged["status"] == "merged", (
            "DEV-604: both PRs merged after the ticket left the query; the board "
            "column comes straight from this status"
        )
        assert merged["merged_at"]

        steady = self._cycle(fake_config, tmp_state,
                             in_query=False, upstream="Ready for Testing", pr_state="MERGED")
        assert steady["status"] == "merged"
        assert steady["merged_at"] == merged["merged_at"]
        assert len(self._merged_events()) == 1, "the merge must be announced once"

    def test_the_merge_records_the_status_upstream_holds_now(self, fake_config, tmp_state, tmp_log):
        """external_status is whatever the ticket showed when it last appeared
        in the query, so on this path it is stale by definition. Cycle 1 caches
        "In Review"; the merge happens while upstream reads "Ready for Testing".
        _handle_merged_ticket reads merged_external_status to decide whether a
        later upstream move is a reopen, so the stale value would hide a real
        reopen back to "Ready for Testing"."""
        self._seed(tmp_state)
        self._cycle(fake_config, tmp_state,
                    in_query=True, upstream="In Review", pr_state="OPEN")
        merged = self._cycle(fake_config, tmp_state,
                             in_query=False, upstream="Ready for Testing", pr_state="MERGED")
        assert merged["merged_external_status"] == "Ready for Testing"
        assert merged["external_status"] == "Ready for Testing"

    def test_an_upstream_lookup_that_fails_does_not_block_the_merge(self, fake_config, tmp_state, tmp_log):
        """Merged PRs are the proof. A Jira outage at the moment of the sweep
        must not park the ticket at in_review again."""
        self._seed(tmp_state)
        import core.state as state

        platform = MagicMock()
        platform.get_pr_state.return_value = "MERGED"
        system = MagicMock()
        system.fetch_ticket.side_effect = RuntimeError("jira down")

        with patch.object(tickets, "_fetch_tickets",
                          return_value=[make_ticket(key="DEV-OTHER", summary="other")]), \
             patch.object(tickets, "_fetch_open_prs", return_value=[]), \
             patch.object(tickets, "_fetch_ticket_comments", return_value=[]), \
             patch.object(tickets, "enqueue_prd_backfill"), \
             patch.object(tickets, "get_repos",
                          return_value=[{"name": "repo", "path": tmp_state / "repo"}]), \
             patch.object(tickets, "make_platform", return_value=platform), \
             patch.object(tickets, "make_ticket_system", return_value=system):
            tickets.check({**fake_config, "_base_url": "http://base"}, instance_key="test")

        ts = state.load_ticket(self.KEY)
        assert ts["status"] == "merged"
        assert ts["merged_external_status"] == "In Review"
