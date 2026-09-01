import pathlib
from datetime import datetime, timedelta, timezone

import core.db as db
from services import work_store


def _client(follow_redirects=True):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from web.work import router
    app = FastAPI()
    app.include_router(router)
    return TestClient(app, follow_redirects=follow_redirects)


def _chain(objectives):
    ids = []
    source = None
    for objective in objectives:
        source = work_store.create_item(objective, source_item_id=source)
        ids.append(source)
    return ids


class TestThreadDerivation:
    def test_a_lone_task_is_not_a_thread(self):
        work_store.create_item("stands alone")
        titles = [t["objective"] for t in work_store.threads()]
        assert "stands alone" not in titles

    def test_follow_up_chain_becomes_one_thread(self):
        ids = _chain(["design the task model", "reconcile with the rail", "port the templates"])
        threads = [t for t in work_store.threads() if t["root_id"] == ids[0]]
        assert len(threads) == 1
        thread = threads[0]
        assert thread["title"] == "design the task model"
        assert [t["id"] for t in thread["tasks"]] == ids
        assert thread["task_count"] == 3

    def test_thread_map_names_every_member(self):
        ids = _chain(["root objective", "follow up"])
        mapping = work_store.thread_map()
        assert mapping[ids[0]]["root_id"] == ids[0]
        assert mapping[ids[1]]["root_id"] == ids[0]
        assert mapping[ids[1]]["title"] == "root objective"

    def test_long_objective_is_truncated_for_the_pill(self):
        long = "word " * 40
        ids = _chain([long.strip(), "follow up"])
        title = work_store.thread_map()[ids[0]]["title"]
        assert title.endswith("…")
        assert len(title) <= work_store.THREAD_TITLE_CHARS + 1

    def test_thread_counts_artifacts_and_providers(self):
        ids = _chain(["root with output", "follow up with output"])
        work_store.add_run(ids[0], "sid-thread-1", "work-t1", "/tmp", provider="claude")
        work_store.add_run(ids[1], "sid-thread-2", "work-t2", "/tmp", provider="codex")
        now = datetime.now(timezone.utc).isoformat()
        with db.tx() as c:
            for item_id, path in ((ids[0], "/tmp/a.md"), (ids[1], "/tmp/b.png")):
                c.execute("INSERT INTO work_artifacts(work_item_id, work_run_id, path, created_at) "
                          "VALUES (?, ?, ?, ?)", (item_id, item_id, path, now))
        thread = next(t for t in work_store.threads() if t["root_id"] == ids[0])
        assert thread["artifact_count"] == 2
        assert sorted(thread["providers"]) == ["claude", "codex"]

    def test_thread_reports_the_projects_of_its_members(self):
        root = work_store.create_item("root in quill", contexts="quill")
        work_store.create_item("follow up in quill and clarivis",
                               contexts="quill,clarivis,slack_int",
                               source_item_id=root)
        thread = next(t for t in work_store.threads() if t["root_id"] == root)
        assert thread["projects"] == ["quill", "clarivis"]

    def test_thread_without_a_project_context_reports_none(self):
        ids = _chain(["root with no project", "follow up with no project"])
        thread = next(t for t in work_store.threads() if t["root_id"] == ids[0])
        assert thread["projects"] == []

    def test_stale_agent_working_member_reports_as_stale(self):
        ids = _chain(["root stale", "follow up stale"])
        old = (datetime.now(timezone.utc) - timedelta(
            minutes=work_store.STALE_AFTER_MINUTES + 5)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'agent_working', updated_at = ? WHERE id = ?",
                      (old, ids[1]))
        thread = next(t for t in work_store.threads() if t["root_id"] == ids[0])
        states = {t["id"]: t["state"] for t in thread["tasks"]}
        assert states[ids[1]] == "failed_stale"


class TestArchive:
    def test_archive_returns_done_items_older_than_the_board_window(self):
        item_id = work_store.create_item("finished long ago")
        old = (datetime.now(timezone.utc) - timedelta(
            days=work_store.DONE_WINDOW_DAYS + 3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'done', updated_at = ? WHERE id = ?",
                      (old, item_id))
        board = work_store.grouped_items()
        archive = work_store.grouped_items(all_done=True)
        assert item_id not in [row["id"] for row in board["done"]]
        assert item_id in [row["id"] for row in archive["done"]]

    def test_items_endpoint_honours_the_archive_flag(self):
        # The done group is paginated, so count the group rather than read page one.
        client = _client()
        board_before = client.get("/api/work/items").json()["counts"]["done"]
        archive_before = client.get("/api/work/items?archive=1").json()["counts"]["done"]
        item_id = work_store.create_item("archived by the endpoint")
        old = (datetime.now(timezone.utc) - timedelta(
            days=work_store.DONE_WINDOW_DAYS + 3)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'done', updated_at = ? WHERE id = ?",
                      (old, item_id))
        assert client.get("/api/work/items").json()["counts"]["done"] == board_before
        assert client.get("/api/work/items?archive=1").json()["counts"]["done"] == archive_before + 1


class TestNeedsYouCount:
    def test_count_matches_the_board_group(self):
        work_store.create_item("waiting on the operator")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_you' WHERE objective = ?",
                      ("waiting on the operator",))
        assert work_store.needs_you_count() == len(work_store.grouped_items()["needs_you"])

    def test_an_expired_snooze_counts_the_same_way_the_board_counts_it(self):
        item_id = work_store.create_item("snoozed and now due")
        past = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'waiting_external', snoozed_until = ? "
                      "WHERE id = ?", (past, item_id))
        assert item_id in [row["id"] for row in work_store.grouped_items()["needs_you"]]
        assert work_store.needs_you_count() == len(work_store.grouped_items()["needs_you"])

    def test_a_live_snooze_is_not_counted(self):
        item_id = work_store.create_item("snoozed until later")
        future = (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()
        before = work_store.needs_you_count()
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'waiting_external', snoozed_until = ? "
                      "WHERE id = ?", (future, item_id))
        assert work_store.needs_you_count() == before


class TestRoutes:
    def test_tasks_page_renders_the_shell(self):
        r = _client().get("/tasks")
        assert r.status_code == 200
        assert "<frshty-shell" in r.text
        assert "frshtyApp({" in r.text
        assert 'href="/threads"' in r.text
        assert 'href="/tasks?view=archive"' in r.text

    def test_task_detail_page_renders_the_shell(self):
        r = _client().get("/tasks/1")
        assert r.status_code == 200
        assert "/static/frshty-shell.js" in r.text
        assert "<frshty-shell" in r.text
        assert "frshtyApp({" in r.text

    def test_threads_page_renders(self):
        r = _client().get("/threads")
        assert r.status_code == 200
        assert "<frshty-shell" in r.text
        assert "/api/work/threads" in r.text

    def test_work_routes_redirect_to_tasks(self):
        client = _client(follow_redirects=False)
        assert client.get("/work").headers["location"] == "/tasks"
        assert client.get("/work/7").headers["location"] == "/tasks/7"
        assert client.get("/work/7/terminal").headers["location"] == "/tasks/7/terminal"
        assert client.get("/work/7/summary").headers["location"] == "/tasks/7/summary"

    def test_threads_endpoint_reports_threads_and_the_needs_you_count(self):
        ids = _chain(["root for endpoint", "follow up for endpoint"])
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'needs_you' WHERE id = ?", (ids[1],))
        d = _client().get("/api/work/threads").json()
        thread = next(t for t in d["threads"] if t["root_id"] == ids[0])
        assert thread["counts"]["needs_you"] == 1
        assert d["needs_you"] >= 1

    def test_threads_endpoint_carries_the_project_context(self):
        root = work_store.create_item("root for project endpoint", contexts="aimyable")
        work_store.create_item("follow up for project endpoint", contexts="aimyable",
                               source_item_id=root)
        d = _client().get("/api/work/threads").json()
        thread = next(t for t in d["threads"] if t["root_id"] == root)
        assert thread["projects"] == ["aimyable"]

    def test_threads_page_shows_the_project_context(self):
        assert 'v-for="p in t.projects"' in _client().get("/threads").text

    def test_board_items_carry_their_thread(self):
        ids = _chain(["root on the board", "follow up on the board"])
        groups = _client().get("/api/work/items").json()["groups"]
        rows = {row["id"]: row for rows in groups.values() for row in rows}
        assert rows[ids[1]]["thread"]["title"] == "root on the board"

    def test_detail_endpoint_carries_the_thread(self):
        ids = _chain(["root for detail", "follow up for detail"])
        d = _client().get(f"/api/work/items/{ids[1]}/detail").json()
        assert d["thread"]["root_id"] == ids[0]
        assert d["needs_you"] == work_store.needs_you_count()

    def test_task_detail_page_shows_the_rail_badge(self):
        r = _client().get("/tasks/1")
        assert ':counts="railCounts"' in r.text
        assert "this.needsYou = d.needs_you || 0;" in r.text


class TestShellRail:
    def _shell(self):
        return pathlib.Path("static/frshty-shell.js").read_text()

    def test_tasks_is_the_first_item_in_the_work_section(self):
        shell = self._shell()
        work = shell.split("{ title: 'Work', links: [")[1].split("]}")[0]
        assert work.index("label: 'Tasks'") < work.index("label: 'Reviews'")
        assert "href: '/tasks'" in work

    def test_tasks_stays_active_on_threads(self):
        shell = self._shell()
        assert "match: ['/tasks', '/threads']" in shell
        assert "function linkActive(" in shell
        assert "active: linkActive(path, l)" in shell

    def test_crumbs_cover_tasks_and_threads(self):
        shell = self._shell()
        assert "'/tasks': ['Work', 'Tasks']" in shell
        assert "'/threads': ['Work', 'Tasks', 'Threads']" in shell

    def test_thread_pills_do_not_capitalise(self):
        css = pathlib.Path("static/frshty-v2.css").read_text()
        assert ".ln-thread-pill { text-transform: none;" in css


def _artifact(item_id, path, note=""):
    now = datetime.now(timezone.utc).isoformat()
    with db.tx() as c:
        c.execute("INSERT INTO work_artifacts(work_item_id, work_run_id, path, note, created_at) "
                  "VALUES (?, ?, ?, ?, ?)", (item_id, None, path, note, now))


def _set(item_id, **fields):
    assigns = ", ".join(f"{k} = ?" for k in fields)
    with db.tx() as c:
        c.execute(f"UPDATE work_items SET {assigns} WHERE id = ?",
                  (*fields.values(), item_id))


class TestThreadDetail:
    def test_unknown_thread_is_an_error(self):
        assert "error" in work_store.thread_detail(999999)

    def test_a_lone_task_has_no_thread_page(self):
        item_id = work_store.create_item("stands alone, no page")
        assert "error" in work_store.thread_detail(item_id)

    def test_detail_lists_members_oldest_first(self):
        ids = _chain(["thread root", "second task", "third task"])
        detail = work_store.thread_detail(ids[0])
        assert [t["id"] for t in detail["tasks"]] == ids
        assert detail["task_count"] == 3
        assert detail["title"] == "thread root"
        assert detail["objective"] == "thread root"

    def test_detail_rolls_up_artifacts_with_their_task(self):
        ids = _chain(["root that produced files", "follow up that produced files"])
        _artifact(ids[0], "/tmp/design.md", "the design")
        _artifact(ids[1], "/tmp/port.md")
        detail = work_store.thread_detail(ids[0])
        assert detail["artifact_count"] == 2
        assert [a["work_item_id"] for a in detail["artifacts"]] == ids
        assert detail["artifacts"][0]["note"] == "the design"
        counts = {t["id"]: t["artifact_count"] for t in detail["tasks"]}
        assert counts == {ids[0]: 1, ids[1]: 1}

    def test_detail_counts_completion_and_attention(self):
        ids = _chain(["root to finish", "follow up to answer"])
        _set(ids[0], state="done")
        _set(ids[1], state="needs_you")
        detail = work_store.thread_detail(ids[0])
        assert detail["done_count"] == 1
        assert detail["needs_you"] == 1
        assert detail["status"] == "active"

    def test_a_thread_is_complete_when_every_task_is_done(self):
        ids = _chain(["root done", "follow up done"])
        for item_id in ids:
            _set(item_id, state="done")
        assert work_store.thread_detail(ids[0])["status"] == "complete"

    def test_a_stale_run_shows_as_stale_on_the_thread_page(self):
        ids = _chain(["root for staleness", "follow up gone quiet"])
        old = (datetime.now(timezone.utc)
               - timedelta(minutes=work_store.STALE_AFTER_MINUTES + 5)).isoformat()
        _set(ids[1], state="agent_working", updated_at=old)
        states = {t["id"]: t["state"] for t in work_store.thread_detail(ids[0])["tasks"]}
        assert states[ids[1]] == "failed_stale"

    def test_an_expired_snooze_shows_as_needs_you_on_the_thread_page(self):
        ids = _chain(["root for snooze", "follow up snoozed"])
        past = (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
        _set(ids[1], state="waiting_external", snoozed_until=past)
        detail = work_store.thread_detail(ids[0])
        states = {t["id"]: t["state"] for t in detail["tasks"]}
        assert states[ids[1]] == "needs_you"
        assert detail["needs_you"] == 1

    def test_the_summary_is_the_newest_member_summary(self):
        ids = _chain(["root with a summary", "follow up with a summary"])
        _set(ids[0], summary="the first synthesis")
        _set(ids[1], summary="the latest synthesis")
        detail = work_store.thread_detail(ids[0])
        assert detail["summary"] == "the latest synthesis"
        assert detail["summary_from"] == ids[1]

    def test_a_thread_with_no_summary_says_so(self):
        ids = _chain(["root with no summary", "follow up with no summary"])
        detail = work_store.thread_detail(ids[0])
        assert detail["summary"] == ""
        assert detail["summary_from"] is None

    def test_continue_from_is_the_newest_done_task(self):
        ids = _chain(["root done first", "middle done later", "still running"])
        _set(ids[0], state="done")
        _set(ids[1], state="done")
        assert work_store.thread_detail(ids[0])["continue_from"] == ids[1]

    def test_continue_from_is_empty_when_no_task_is_done(self):
        ids = _chain(["root not done", "follow up not done"])
        assert work_store.thread_detail(ids[0])["continue_from"] is None


class TestThreadDetailRoutes:
    def test_thread_page_renders_the_shell(self):
        ids = _chain(["root for the page", "follow up for the page"])
        r = _client().get(f"/threads/{ids[0]}")
        assert r.status_code == 200
        assert "<frshty-shell" in r.text
        assert "/api/work/threads/" in r.text

    def test_thread_endpoint_returns_the_roll_up(self):
        ids = _chain(["root for the endpoint", "follow up for the endpoint"])
        _artifact(ids[1], "/tmp/out.md")
        d = _client().get(f"/api/work/threads/{ids[0]}").json()
        assert d["root_id"] == ids[0]
        assert [t["id"] for t in d["tasks"]] == ids
        assert d["artifact_count"] == 1
        assert d["rail_needs_you"] == work_store.needs_you_count()
        assert d["agents"]

    def test_unknown_thread_endpoint_is_404(self):
        item_id = work_store.create_item("lonely task with no thread")
        assert _client().get(f"/api/work/threads/{item_id}").status_code == 404

    def test_launching_into_a_thread_needs_a_completed_task(self):
        ids = _chain(["root still running", "follow up still running"])
        r = _client().post(f"/api/work/threads/{ids[0]}/tasks", json={"text": "next step"})
        assert r.status_code == 409
        assert "completed task" in r.json()["error"]

    def test_launching_into_an_unknown_thread_is_404(self):
        item_id = work_store.create_item("another lonely task")
        r = _client().post(f"/api/work/threads/{item_id}/tasks", json={"text": "next step"})
        assert r.status_code == 404

    def test_launching_into_a_thread_continues_its_newest_done_task(self, monkeypatch):
        ids = _chain(["root that finished", "follow up that finished"])
        for item_id in ids:
            _set(item_id, state="done")
        seen = {}

        def fake_launch(objective, cwd="", contexts=None, slack=False,
                        source_item_id=None, agent="claude"):
            seen.update(objective=objective, source_item_id=source_item_id, agent=agent)
            return {"item_id": 4242}

        from services import work_launch
        monkeypatch.setattr(work_launch, "launch", fake_launch)
        r = _client().post(f"/api/work/threads/{ids[0]}/tasks",
                           json={"text": "the next task", "agent": "codex"})
        assert r.status_code == 200
        assert seen == {"objective": "the next task", "source_item_id": ids[1], "agent": "codex"}


class TestBoardUi:
    def _board(self):
        return pathlib.Path("templates/work.html").read_text()

    def test_the_board_uses_task_cards(self):
        board = self._board()
        assert 'class="ln-task"' in board
        assert 'class="ln-task-title"' in board
        assert 'class="ln-task-foot"' in board

    def test_the_board_has_the_active_threads_panel(self):
        board = self._board()
        assert "Active threads" in board
        assert 'class="ln-board"' in board
        assert "loadThreads()" in board

    def test_thread_pills_link_to_the_thread_page(self):
        for name in ("templates/work.html", "templates/work_detail.html"):
            text = pathlib.Path(name).read_text()
            assert "'/threads/' + " in text, name

    def test_the_task_card_css_carries_a_state_stripe(self):
        css = pathlib.Path("static/frshty-v2.css").read_text()
        assert ".ln-task::before" in css
        assert ".ln-task.needs_you { --ln-state: var(--ln-amber); }" in css
        assert ".ln-tl-node" in css
