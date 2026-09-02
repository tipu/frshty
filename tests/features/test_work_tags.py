import pathlib
import sqlite3

import core.db as db
from services import work_store, work_tags


def _mkitem(objective="do the thing", **kw):
    return work_store.create_item(objective, **kw)


class TestMigration:
    def test_tags_column_exists(self):
        cols = {r["name"] for r in db.query_all("PRAGMA table_info(work_items)")}
        assert "tags" in cols

    def test_backfill_caps_contexts_at_two(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "t.db")
        conn.executescript(pathlib.Path("migrations/014_work_layer.sql").read_text())
        conn.executescript(pathlib.Path("migrations/019_work_contexts.sql").read_text())
        conn.execute(
            "INSERT INTO work_items(objective, contexts, created_at, updated_at) "
            "VALUES ('a', 'x,y,z', 't', 't'), ('b', 'x,y', 't', 't'), "
            "('c', 'x', 't', 't'), ('d', '', 't', 't')")
        conn.executescript(pathlib.Path("migrations/022_work_tags.sql").read_text())
        rows = dict(conn.execute("SELECT objective, tags FROM work_items").fetchall())
        assert rows == {"a": "x,y", "b": "x,y", "c": "x", "d": ""}


class TestNormalize:
    def test_accepts_simple_tags(self):
        assert work_tags.normalize(" Turo ") == "turo"
        assert work_tags.normalize("slack_int") == "slack_int"
        assert work_tags.normalize("my-client2") == "my-client2"

    def test_rejects_junk(self):
        assert work_tags.normalize("") == ""
        assert work_tags.normalize("x") == ""
        assert work_tags.normalize("has space") == ""
        assert work_tags.normalize("a" * 30) == ""
        assert work_tags.normalize("-lead") == ""


class TestDeriveTags:
    def test_labels_become_tags(self):
        assert work_tags.derive_tags("fix the build", ["aimyable"], []) == ["aimyable"]

    def test_labels_capped_at_two(self):
        tags = work_tags.derive_tags("x", ["a1", "b1", "c1"], [])
        assert tags == ["a1", "b1"]

    def test_objective_matches_known_vocabulary(self):
        _mkitem("old turo job", tags="turo")
        tags = work_tags.derive_tags("check my turo bookings", [], ["aimyable"])
        assert tags == ["turo"]

    def test_objective_matches_project_key(self):
        tags = work_tags.derive_tags("restart aimyable daemon", [], ["aimyable"])
        assert tags == ["aimyable"]

    def test_labels_take_precedence_over_vocabulary(self):
        tags = work_tags.derive_tags("restart aimyable and nectar", ["quill"],
                                     ["aimyable", "nectar"])
        assert tags[0] == "quill"
        assert len(tags) == 2

    def test_no_duplicate_tags(self):
        tags = work_tags.derive_tags("work on aimyable", ["aimyable"], ["aimyable"])
        assert tags == ["aimyable"]


class TestMergeImplicit:
    def test_fills_up_to_cap(self):
        item_id = _mkitem(tags="turo")
        added = work_tags.merge_implicit(item_id, ["billing", "extra"])
        assert added == ["billing"]
        row = db.query_one("SELECT tags FROM work_items WHERE id = ?", (item_id,))
        assert row["tags"] == "turo,billing"

    def test_skips_invalid_and_duplicate(self):
        item_id = _mkitem(tags="turo")
        added = work_tags.merge_implicit(item_id, ["TURO", "has space", "ok-tag"])
        assert added == ["ok-tag"]

    def test_full_item_unchanged(self):
        item_id = _mkitem(tags="a1,b1")
        assert work_tags.merge_implicit(item_id, ["c1"]) == []
        row = db.query_one("SELECT tags FROM work_items WHERE id = ?", (item_id,))
        assert row["tags"] == "a1,b1"

    def test_records_event(self):
        item_id = _mkitem(tags="")
        work_tags.merge_implicit(item_id, ["turo"])
        events = db.query_all(
            "SELECT kind FROM work_events WHERE work_item_id = ?", (item_id,))
        assert "tags_derived" in {e["kind"] for e in events}


class TestParseTags:
    def test_parses_array(self):
        assert work_tags._parse_tags('noise ["turo", "billing"] tail') == ["turo", "billing"]

    def test_rejects_no_array(self):
        try:
            work_tags._parse_tags("no json here")
        except ValueError:
            return
        raise AssertionError("expected ValueError")


class TestGroupedItemsTagFilter:
    def test_filters_by_tag(self):
        a = _mkitem("turo sync", tags="turo")
        b = _mkitem("quill report", tags="quill")
        groups = work_store.grouped_items(tags="turo")
        ids = {r["id"] for g in groups.values() for r in g}
        assert a in ids
        assert b not in ids

    def test_or_semantics_across_tags(self):
        a = _mkitem("turo sync", tags="turo")
        b = _mkitem("quill report", tags="quill")
        c = _mkitem("untagged")
        groups = work_store.grouped_items(tags="turo,quill")
        ids = {r["id"] for g in groups.values() for r in g}
        assert {a, b} <= ids
        assert c not in ids

    def test_tag_filter_applies_to_the_archive(self):
        kept = _mkitem("ancient turo job", tags="turo")
        other = _mkitem("ancient quill job", tags="quill")
        with db.tx() as c:
            c.execute("UPDATE work_items SET state = 'done' WHERE id IN (?, ?)", (kept, other))
        work_store.apply_action(kept, "archive")
        work_store.apply_action(other, "archive")
        archive = work_store.grouped_items(tags="turo", archived=True)
        assert kept in {r["id"] for r in archive["done"]}
        assert other not in {r["id"] for r in archive["done"]}

    def test_known_tags_lists_distinct(self):
        _mkitem("one", tags="turo,billing")
        _mkitem("two", tags="turo")
        known = work_tags.known_tags()
        assert "turo" in known
        assert "billing" in known
        assert known.count("turo") == 1
