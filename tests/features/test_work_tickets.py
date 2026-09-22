"""A task dispatched on /tasks is coupled to the ticket its objective names.

The couple is derived, not recorded: a ticket key that exists, standing on its
own in the objective. Both ends read the same rule, so the pill on a task card
and the task list on a ticket page always agree.
"""
import pytest

import core.db as db
import core.state as state
from services import work_store, work_tickets
from web import work as work_routes


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    yield


def _mkitem(objective, **kw):
    return work_store.create_item(objective, **kw)


def _mkticket(instance, key, status="new", slug=""):
    token = state.use(instance)
    try:
        state.save_ticket(key, {"status": status, "slug": slug or key.lower()})
    finally:
        state.reset(token)


def _row(item_id, objective, contexts="", **kw):
    return {"id": item_id, "objective": objective, "contexts": contexts, **kw}


class TestKeysIn:
    def test_finds_every_known_key_once_and_in_order(self):
        _mkticket("aimyable", "DEV-635")
        _mkticket("aimyable", "AB-7")
        idx = work_tickets.index()
        assert work_tickets.keys_in(
            "merge DEV-635 after AB-7, then DEV-635 again", idx) == ["DEV-635", "AB-7"]

    def test_a_key_that_names_no_ticket_is_not_found(self):
        _mkticket("aimyable", "DEV-635")
        idx = work_tickets.index()
        assert work_tickets.keys_in("re-encode UTF-8 and finish DEV-999", idx) == []

    def test_ignores_a_key_glued_to_other_words(self):
        _mkticket("aimyable", "DEV-635")
        idx = work_tickets.index()
        assert work_tickets.keys_in("xDEV-635 and DEV-635x and DEV-6355", idx) == []

    def test_finds_a_key_no_project_number_pattern_matches(self):
        _mkticket("bh", "PRD-6_FUNCTIONAL_REQUIREMENTS-3")
        idx = work_tickets.index()
        assert work_tickets.keys_in("finish PRD-6_FUNCTIONAL_REQUIREMENTS-3 today", idx) \
            == ["PRD-6_FUNCTIONAL_REQUIREMENTS-3"]

    def test_the_long_key_wins_over_the_short_one_it_starts_with(self):
        _mkticket("bh", "PRD-6")
        _mkticket("bh", "PRD-6_FUNCTIONAL_REQUIREMENTS-3")
        idx = work_tickets.index()
        assert work_tickets.keys_in("finish PRD-6_FUNCTIONAL_REQUIREMENTS-3", idx) \
            == ["PRD-6_FUNCTIONAL_REQUIREMENTS-3"]
        assert work_tickets.keys_in("finish PRD-6", idx) == ["PRD-6"]

    def test_no_tickets_at_all_is_no_keys(self):
        assert work_tickets.keys_in("finish DEV-635", work_tickets.index()) == []


def _loaded(monkeypatch, *keys, base_url=""):
    class _Entry:
        config = {"_base_url": base_url}

    class _Instances:
        def keys(self):
            return list(keys)

        def get(self, key):
            return _Entry() if key in keys else None

    monkeypatch.setattr(work_tickets.runtime, "instances", lambda: _Instances())


class TestUnloadedInstances:
    def test_a_ticket_on_an_unloaded_instance_is_not_a_couple_by_itself(self, monkeypatch):
        _mkticket("aimyable", "DEV-635")
        _mkticket("leftover", "DEV-777")
        _loaded(monkeypatch, "aimyable")
        idx = work_tickets.index()
        assert work_tickets.links_for(_row(1, "fix DEV-777"), idx) == []
        assert [link["instance"] for link in
                work_tickets.links_for(_row(1, "fix DEV-635"), idx)] == ["aimyable"]

    def test_a_selected_project_that_is_not_loaded_never_becomes_another_instance(self, monkeypatch):
        _mkticket("aimyable", "DEV-635")
        _mkticket("someclient", "DEV-635")
        _loaded(monkeypatch, "aimyable")
        links = work_tickets.links_for(_row(1, "fix DEV-635", "someclient"), work_tickets.index())
        assert [link["instance"] for link in links] == []

    def test_every_reported_ticket_has_an_absolute_link(self, monkeypatch):
        _mkticket("aimyable", "DEV-635")
        _mkticket("someclient", "DEV-635")
        _loaded(monkeypatch, "aimyable", base_url="http://aimyable.local")
        for contexts in ("", "someclient", "aimyable"):
            for link in work_tickets.links_for(
                    _row(1, "fix DEV-635", contexts), work_tickets.index()):
                assert link["url"] == "http://aimyable.local/tickets/DEV-635"

    def test_every_instance_counts_when_no_registry_is_loaded(self):
        _mkticket("aimyable", "DEV-635")
        _mkticket("leftover", "DEV-777")
        idx = work_tickets.index()
        assert [link["instance"] for link in
                work_tickets.links_for(_row(1, "fix DEV-777"), idx)] == ["leftover"]


class TestLinksForATask:
    def test_a_named_ticket_that_exists_becomes_a_link(self):
        _mkticket("aimyable", "DEV-635", status="planning")
        item = _mkitem("finish DEV-635 and open the PR", contexts="aimyable")
        links = work_tickets.for_items(
            [_row(item, "finish DEV-635 and open the PR", "aimyable")])[item]
        assert [(link["key"], link["instance"], link["status"]) for link in links] == [
            ("DEV-635", "aimyable", "planning")]
        assert links[0]["url"] == "/tickets/DEV-635"

    def test_a_named_key_with_no_ticket_is_not_a_link(self):
        item = _mkitem("finish DEV-999 today")
        assert work_tickets.for_items([_row(item, "finish DEV-999 today")])[item] == []

    def test_a_task_that_names_nothing_has_no_link(self):
        _mkticket("aimyable", "DEV-635")
        item = _mkitem("tidy the board")
        assert work_tickets.for_items([_row(item, "tidy the board")])[item] == []

    def test_the_selected_project_wins_a_key_two_instances_hold(self):
        _mkticket("aimyable", "DEV-635")
        _mkticket("someclient", "DEV-635")
        links = work_tickets.links_for(_row(1, "fix DEV-635", "someclient"), work_tickets.index())
        assert [link["instance"] for link in links] == ["someclient"]

    def test_a_key_two_instances_hold_and_no_project_reports_both(self):
        _mkticket("aimyable", "DEV-635")
        _mkticket("someclient", "DEV-635")
        links = work_tickets.links_for(_row(1, "fix DEV-635"), work_tickets.index())
        assert [link["instance"] for link in links] == ["aimyable", "someclient"]

    def test_a_today_task_links_through_scope_ref(self):
        _mkticket("aimyable", "DEV-700")
        row = _row(1, "Fix the failing build", "aimyable",
                   scope="ticket", scope_ref="DEV-700")
        assert [link["key"] for link in work_tickets.links_for(row, work_tickets.index())] \
            == ["DEV-700"]

    def test_a_scope_ref_naming_no_ticket_is_not_a_link(self):
        row = _row(1, "Fix the failing build", "aimyable",
                   scope="ticket", scope_ref="DEV-700")
        assert work_tickets.links_for(row, work_tickets.index()) == []

    def test_the_link_points_at_the_host_that_owns_the_ticket(self, monkeypatch):
        _mkticket("aimyable", "DEV-635")
        _loaded(monkeypatch, "aimyable", base_url="http://aimyable.local/")
        links = work_tickets.links_for(_row(1, "fix DEV-635", "aimyable"), work_tickets.index())
        assert links[0]["url"] == "http://aimyable.local/tickets/DEV-635"


class TestTasksForATicket:
    def test_lists_the_tasks_that_named_the_ticket(self):
        _mkticket("aimyable", "DEV-635")
        mine = _mkitem("finish DEV-635 and open the PR", contexts="aimyable")
        _mkitem("unrelated cleanup", contexts="aimyable")
        rows = work_tickets.tasks_for("aimyable", "DEV-635")
        assert [r["id"] for r in rows] == [mine]
        assert rows[0]["url"] == f"/tasks/{mine}"
        assert rows[0]["projects"] == ["aimyable"]

    def test_a_substring_match_is_not_a_task_of_this_ticket(self):
        _mkticket("aimyable", "DEV-63")
        _mkticket("aimyable", "DEV-635")
        long_one = _mkitem("finish DEV-635", contexts="aimyable")
        assert [r["id"] for r in work_tickets.tasks_for("aimyable", "DEV-63")] == []
        assert [r["id"] for r in work_tickets.tasks_for("aimyable", "DEV-635")] == [long_one]

    def test_an_underscore_in_the_key_is_not_a_wildcard(self):
        _mkticket("bh", "PRD-6_FUNCTIONAL_REQUIREMENTS-3")
        _mkitem("finish PRD-6xFUNCTIONALxREQUIREMENTS-3", contexts="bh")
        named = _mkitem("finish PRD-6_FUNCTIONAL_REQUIREMENTS-3", contexts="bh")
        rows = work_tickets.tasks_for("bh", "PRD-6_FUNCTIONAL_REQUIREMENTS-3")
        assert [r["id"] for r in rows] == [named]

    def test_another_instances_ticket_does_not_claim_the_task(self):
        _mkticket("aimyable", "DEV-635")
        _mkticket("someclient", "DEV-635")
        _mkitem("fix DEV-635", contexts="someclient")
        assert work_tickets.tasks_for("aimyable", "DEV-635") == []
        assert len(work_tickets.tasks_for("someclient", "DEV-635")) == 1

    def test_newest_first(self):
        _mkticket("aimyable", "DEV-635")
        first = _mkitem("start DEV-635", contexts="aimyable")
        second = _mkitem("finish DEV-635", contexts="aimyable")
        assert [r["id"] for r in work_tickets.tasks_for("aimyable", "DEV-635")] == [second, first]

    def test_carries_the_state_and_the_report(self):
        _mkticket("aimyable", "DEV-635")
        item = _mkitem("finish DEV-635", contexts="aimyable")
        db.execute("UPDATE work_items SET state = 'needs_ack', summary = 'PR opened' WHERE id = ?",
                   (item,))
        row = work_tickets.tasks_for("aimyable", "DEV-635")[0]
        assert row["state"] == "needs_ack"
        assert row["note"] == "PR opened"

    def test_lists_every_task_the_board_would_pill(self):
        _mkticket("aimyable", "DEV-635")
        ids = [_mkitem(f"step {n} of DEV-635", contexts="aimyable") for n in range(101)]
        listed = {r["id"] for r in work_tickets.tasks_for("aimyable", "DEV-635")}
        pilled = {i for i, links in work_tickets.for_items(
            [_row(i, f"step {n} of DEV-635", "aimyable") for n, i in enumerate(ids)]).items()
            if links}
        assert listed == pilled == set(ids)

    def test_a_padded_scope_ref_agrees_with_the_board(self):
        _mkticket("aimyable", "DEV-700")
        item = _mkitem("Fix the failing build", contexts="aimyable")
        db.execute("UPDATE work_items SET scope = 'ticket', scope_ref = ' DEV-700 ' WHERE id = ?",
                   (item,))
        row = db.query_one(
            "SELECT id, objective, contexts, scope, scope_ref FROM work_items WHERE id = ?",
            (item,))
        assert [link["key"] for link in
                work_tickets.links_for(row, work_tickets.index())] == ["DEV-700"]
        assert [r["id"] for r in work_tickets.tasks_for("aimyable", "DEV-700")] == [item]

    def test_a_tab_padded_scope_ref_agrees_with_the_board(self):
        _mkticket("aimyable", "DEV-700")
        item = _mkitem("Fix the failing build", contexts="aimyable")
        db.execute("UPDATE work_items SET scope = 'ticket', scope_ref = ? WHERE id = ?",
                   ("\tDEV-700\n", item))
        row = db.query_one(
            "SELECT id, objective, contexts, scope, scope_ref FROM work_items WHERE id = ?",
            (item,))
        assert [link["key"] for link in
                work_tickets.links_for(row, work_tickets.index())] == ["DEV-700"]
        assert [r["id"] for r in work_tickets.tasks_for("aimyable", "DEV-700")] == [item]

    def test_an_empty_key_asks_nothing(self):
        assert work_tickets.tasks_for("aimyable", "") == []

    def test_an_unknown_key_asks_nothing(self):
        assert work_tickets.tasks_for("aimyable", "DEV-999") == []


class TestBoardApi:
    def test_the_board_row_carries_its_tickets(self):
        _mkticket("aimyable", "DEV-635", status="in_review")
        item = _mkitem("finish DEV-635", contexts="aimyable")
        body = work_routes.api_work_items()
        rows = {r["id"]: r for g in body["groups"].values() for r in g}
        assert [t["key"] for t in rows[item]["tickets"]] == ["DEV-635"]
        assert rows[item]["tickets"][0]["status"] == "in_review"

    def test_a_row_naming_no_ticket_carries_an_empty_list(self):
        item = _mkitem("tidy the board")
        body = work_routes.api_work_items()
        rows = {r["id"]: r for g in body["groups"].values() for r in g}
        assert rows[item]["tickets"] == []

    def test_the_task_detail_carries_its_tickets(self):
        _mkticket("aimyable", "DEV-635")
        item = _mkitem("finish DEV-635", contexts="aimyable")
        assert [t["key"] for t in work_routes.api_work_detail(item)["tickets"]] == ["DEV-635"]


def _mkticket_prs(instance, key, prs, status="in_review"):
    token = state.use(instance)
    try:
        state.save_ticket(key, {"status": status, "slug": key.lower(), "prs": prs})
    finally:
        state.reset(token)


def _pr(repo, pr_id, approvers=(), pr_state="OPEN", host="bitbucket.org", owner="acme"):
    segment = "pull" if host == "github.com" else "pull-requests"
    return {"repo": repo, "id": pr_id, "pr_state": pr_state,
            "url": f"https://{host}/{owner}/{repo}/{segment}/{pr_id}",
            "approvers": list(approvers)}


class TestMergeScope:
    """A follow-up the board wrote names a pull request by address. The ticket
    behind that address decides whether the whole ticket can be merged."""

    def test_text_that_names_no_pull_request_has_no_scope(self):
        _mkticket_prs("aimyable", "DEV-728", [_pr("django-drf-app", 198, ["Jawad"])])
        assert work_tickets.merge_scope("push the branch and open a pull request") == {}

    def test_the_ticket_behind_the_named_address_is_resolved(self):
        _mkticket_prs("aimyable", "DEV-728", [
            _pr("django-drf-app", 198, ["Jawad"]),
            _pr("websocket-server", 129, ["Jawad"]),
        ])
        scope = work_tickets.merge_scope(
            "Merge PR #198 (https://bitbucket.org/acme/django-drf-app/"
            "pull-requests/198/overview) into main.")
        assert scope["ticket_key"] == "DEV-728"
        assert scope["instance_key"] == "aimyable"
        assert [p["id"] for p in scope["named"]] == [198]
        assert [p["id"] for p in scope["siblings"]] == [129]
        assert scope["unapproved"] == []

    def test_a_sibling_nobody_approved_is_reported(self):
        _mkticket_prs("aimyable", "DEV-728", [
            _pr("django-drf-app", 198, ["Jawad"]),
            _pr("websocket-server", 129),
            _pr("windows-rpa-client", 60, ["Jawad"]),
        ])
        scope = work_tickets.merge_scope(
            "Merge https://bitbucket.org/acme/django-drf-app/pull-requests/198")
        assert [p["id"] for p in scope["unapproved"]] == [129]

    def test_a_sibling_that_is_no_longer_open_is_not_counted(self):
        _mkticket_prs("aimyable", "DEV-728", [
            _pr("django-drf-app", 198, ["Jawad"]),
            _pr("websocket-server", 129, pr_state="MERGED"),
        ])
        scope = work_tickets.merge_scope(
            "Merge https://bitbucket.org/acme/django-drf-app/pull-requests/198")
        assert scope["siblings"] == [] and scope["unapproved"] == []

    def test_the_named_pull_request_is_never_reported_unapproved(self):
        """Its own approval is what the run that wrote the follow-up
        established. The cache here can be older than that run."""
        _mkticket_prs("aimyable", "DEV-728", [_pr("django-drf-app", 198)])
        scope = work_tickets.merge_scope(
            "Merge https://bitbucket.org/acme/django-drf-app/pull-requests/198")
        assert [p["id"] for p in scope["named"]] == [198]
        assert scope["unapproved"] == []

    def test_a_named_pull_request_already_merged_still_resolves_the_ticket(self):
        """The operator can merge the named pull request by hand while the
        follow-up waits. The siblings it waits for are still unapproved."""
        _mkticket_prs("aimyable", "DEV-728", [
            _pr("django-drf-app", 198, ["Jawad"], pr_state="MERGED"),
            _pr("websocket-server", 129),
        ])
        scope = work_tickets.merge_scope(
            "Merge https://bitbucket.org/acme/django-drf-app/pull-requests/198")
        assert scope["ticket_key"] == "DEV-728"
        assert scope["named"] == []
        assert [p["id"] for p in scope["unapproved"]] == [129]

    def test_a_ticket_that_left_review_is_not_resolved(self):
        """in_review is the only status whose poll refreshes the approver
        cache, so it is the only status the cache answers for."""
        _mkticket_prs("aimyable", "DEV-728", [
            _pr("django-drf-app", 198, ["Jawad"]),
            _pr("websocket-server", 129),
        ], status="done")
        assert work_tickets.merge_scope(
            "Merge https://bitbucket.org/acme/django-drf-app/pull-requests/198") == {}

    def test_a_github_address_resolves_the_same_way(self):
        _mkticket_prs("someclient", "SC-3064", [
            _pr("someclient-app-backend", 692, ["Ana"], host="github.com",
                owner="SomeClientLabs"),
            _pr("someclient-app-web", 12, host="github.com", owner="SomeClientLabs"),
        ])
        scope = work_tickets.merge_scope(
            "Merge https://github.com/SomeClientLabs/someclient-app-backend/pull/692 first.")
        assert scope["ticket_key"] == "SC-3064"
        assert [p["id"] for p in scope["unapproved"]] == [12]

    def test_a_pull_request_no_ticket_holds_has_no_scope(self):
        _mkticket_prs("aimyable", "DEV-728", [_pr("django-drf-app", 198, ["Jawad"])])
        assert work_tickets.merge_scope(
            "Merge https://bitbucket.org/acme/other-repo/pull-requests/5") == {}
