"""The board filters on the projects a launch recorded, not on tags.

A task carries its projects in the item contexts, so the chips the board
shows are the dispatch project list and a chip matches a task that names that
project. The tags column is gone; migration 043 keeps the project of an item
that carried one only as a tag."""
import pathlib
import sqlite3

import pytest

import core.db as db
import core.state as state
from services import work_store
from web import work as work_routes


@pytest.fixture(autouse=True)
def _clean(fresh_db, tmp_path):
    state.init(tmp_path)
    state._default_instance_key = "personal"
    state._instance_key_cv.set("personal")
    yield


def _mkitem(objective="do the thing", **kw):
    return work_store.create_item(objective, **kw)


class TestMigration:
    def test_the_tags_column_is_gone(self):
        cols = {r["name"] for r in db.query_all("PRAGMA table_info(work_items)")}
        assert "tags" not in cols

    def test_an_item_with_no_contexts_keeps_its_tags_as_contexts(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "m.db")
        for name in ("014_work_layer.sql", "019_work_contexts.sql", "022_work_tags.sql"):
            conn.executescript(pathlib.Path("migrations", name).read_text())
        conn.execute(
            "INSERT INTO work_items(objective, contexts, tags, created_at, updated_at) "
            "VALUES ('a', '', 'quill', 't', 't'), ('b', 'frshty', 'frshty,codex', 't', 't'), "
            "('c', '', '', 't', 't')")
        conn.executescript(pathlib.Path("migrations/043_drop_work_tags.sql").read_text())
        rows = dict(conn.execute("SELECT objective, contexts FROM work_items").fetchall())
        assert rows == {"a": "quill", "b": "frshty", "c": ""}
        cols = {r[1] for r in conn.execute("PRAGMA table_info(work_items)")}
        assert "tags" not in cols


class TestGroupedItemsProjectFilter:
    def test_filters_by_project(self):
        a = _mkitem("quill sync", contexts="quill")
        b = _mkitem("frshty report", contexts="frshty")
        groups = work_store.grouped_items(projects="quill")
        ids = {r["id"] for g in groups.values() for r in g}
        assert a in ids
        assert b not in ids

    def test_matches_one_project_of_several(self):
        a = _mkitem("two projects", contexts="aimyable,frshty")
        groups = work_store.grouped_items(projects="frshty")
        assert a in {r["id"] for g in groups.values() for r in g}

    def test_or_semantics_across_projects(self):
        a = _mkitem("quill sync", contexts="quill")
        b = _mkitem("frshty report", contexts="frshty")
        c = _mkitem("no project")
        groups = work_store.grouped_items(projects="quill,frshty")
        ids = {r["id"] for g in groups.values() for r in g}
        assert {a, b} <= ids
        assert c not in ids

    def test_no_filter_keeps_every_task(self):
        a = _mkitem("quill sync", contexts="quill")
        b = _mkitem("no project")
        ids = {r["id"] for g in work_store.grouped_items().values() for r in g}
        assert {a, b} <= ids

    def test_the_project_filter_applies_to_the_archive(self):
        kept = _mkitem("ancient quill job", contexts="quill")
        other = _mkitem("ancient frshty job", contexts="frshty")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'done' WHERE id IN (?, ?)", (kept, other))
        work_store.apply_action(kept, "archive")
        work_store.apply_action(other, "archive")
        archive = work_store.grouped_items(projects="quill", archived=True)
        assert kept in {r["id"] for r in archive["done"]}
        assert other not in {r["id"] for r in archive["done"]}


class TestBoardRoute:
    def test_the_route_filters_on_the_projects_parameter(self):
        a = _mkitem("quill sync", contexts="quill")
        b = _mkitem("frshty report", contexts="frshty")
        body = work_routes.api_work_items(projects="quill")
        ids = {r["id"] for g in body["groups"].values() for r in g}
        assert a in ids
        assert b not in ids

    def test_the_route_reports_no_tags(self):
        assert "all_tags" not in work_routes.api_work_items()

    def test_the_route_echoes_the_filter_it_applied(self):
        body = work_routes.api_work_items(q="sync", projects="quill,frshty")
        assert body["filter"] == {"q": "sync", "projects": "quill,frshty"}
