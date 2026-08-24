import sqlite3
from pathlib import Path

import pytest

import core.db as db


REPO_MIGRATIONS = Path(__file__).resolve().parent.parent.parent / "migrations"


@pytest.fixture
def fresh(tmp_path, monkeypatch):
    def _init(migrations_dir):
        monkeypatch.setattr(db, "_DB_PATH", tmp_path / "m.db")
        monkeypatch.setattr(db, "_MIGRATIONS_DIR", migrations_dir)
        db._apply_migrations()
        return sqlite3.connect(tmp_path / "m.db")
    return _init


class TestStatements:
    def test_splits_on_statement_boundaries(self):
        out = db._statements("CREATE TABLE a (x);\nCREATE TABLE b (y);\n")
        assert len(out) == 2
        assert out[0].startswith("CREATE TABLE a")

    def test_semicolon_inside_comment_does_not_split(self):
        out = db._statements("CREATE TABLE a (\n  x TEXT -- one; two\n);\n")
        assert len(out) == 1

    def test_semicolon_inside_string_does_not_split(self):
        out = db._statements("INSERT INTO a VALUES ('x;y');\n")
        assert len(out) == 1

    def test_trailing_comment_dropped(self):
        out = db._statements("CREATE TABLE a (x);\n-- done\n")
        assert len(out) == 1

    def test_all_repo_migrations_split_and_run(self, tmp_path):
        conn = sqlite3.connect(tmp_path / "s.db")
        for sql_file in sorted(REPO_MIGRATIONS.glob("*.sql")):
            for statement in db._statements(sql_file.read_text()):
                conn.execute(statement)
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        assert {"tickets", "jobs", "work_items", "work_followups"} <= tables


class TestApplyMigrations:
    def test_applies_and_records(self, tmp_path, fresh):
        mdir = tmp_path / "mig"
        mdir.mkdir()
        (mdir / "001_a.sql").write_text("CREATE TABLE a (x);\n")
        conn = fresh(mdir)
        assert conn.execute("SELECT name FROM _migrations").fetchall() == [("001_a.sql",)]
        db._apply_migrations()

    def test_failed_migration_rolls_back_whole_script(self, tmp_path, fresh):
        mdir = tmp_path / "mig"
        mdir.mkdir()
        (mdir / "001_a.sql").write_text("CREATE TABLE a (x);\n")
        (mdir / "002_b.sql").write_text(
            "ALTER TABLE a ADD COLUMN y INTEGER;\nALTER TABLE nope ADD COLUMN z INTEGER;\n")
        with pytest.raises(sqlite3.OperationalError):
            fresh(mdir)
        conn = sqlite3.connect(tmp_path / "m.db")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(a)")]
        assert cols == ["x"], "partial migration must not persist"
        recorded = [r[0] for r in conn.execute("SELECT name FROM _migrations")]
        assert recorded == ["001_a.sql"]
        (mdir / "002_b.sql").write_text("ALTER TABLE a ADD COLUMN y INTEGER;\n")
        db._apply_migrations()
        conn = sqlite3.connect(tmp_path / "m.db")
        cols = [r[1] for r in conn.execute("PRAGMA table_info(a)")]
        assert cols == ["x", "y"]

    def test_already_applied_by_racer_is_skipped(self, tmp_path, fresh):
        mdir = tmp_path / "mig"
        mdir.mkdir()
        (mdir / "001_a.sql").write_text("CREATE TABLE a (x);\n")
        conn = fresh(mdir)
        (mdir / "002_b.sql").write_text("ALTER TABLE a ADD COLUMN y INTEGER;\n")
        conn.execute("INSERT INTO _migrations(name, applied_at) VALUES ('002_b.sql', datetime('now'))")
        conn.commit()
        db._apply_migrations()
        cols = [r[1] for r in sqlite3.connect(tmp_path / "m.db").execute("PRAGMA table_info(a)")]
        assert cols == ["x"]

    def test_leading_pragma_applies(self, tmp_path, fresh):
        mdir = tmp_path / "mig"
        mdir.mkdir()
        (mdir / "001_a.sql").write_text("PRAGMA journal_mode = WAL;\nCREATE TABLE a (x);\n")
        conn = fresh(mdir)
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode == "wal"
        assert conn.execute("SELECT name FROM _migrations").fetchall() == [("001_a.sql",)]
