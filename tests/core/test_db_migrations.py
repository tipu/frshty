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


class TestWatchdogLaunchLedger:
    """The rolling launch cap moved from watchdog_observations, which keeps only
    an entity's latest attempt, to watchdog_launches, one row per attempt. The
    pre-upgrade count cannot be reconstructed, so 029 writes one fence row per
    known instance and the watchdog spends no budget for a day."""

    def _apply(self, tmp_path, name="s.db", stop_before=None, seed=None):
        conn = sqlite3.connect(tmp_path / name)
        for sql_file in sorted(REPO_MIGRATIONS.glob("*.sql")):
            if sql_file.name == stop_before:
                break
            for statement in db._statements(sql_file.read_text()):
                conn.execute(statement)
        if seed:
            seed(conn)
        return conn

    def _run(self, conn, name):
        for statement in db._statements((REPO_MIGRATIONS / name).read_text()):
            conn.execute(statement)

    def _ticket(self, conn, instance_key, key):
        conn.execute(
            "INSERT INTO tickets(instance_key, ticket_key, status, data, updated_at)"
            " VALUES (?, ?, 'in_review', '{}', '2026-09-01T00:00:00Z')",
            (instance_key, key))

    def test_every_known_instance_is_fenced(self, tmp_path):
        """The live shape: 028 already applied, then 029 alone. An instance
        whose pre-upgrade attempts left no trace must still be fenced, and a
        PR-only instance has no ticket rows at all — the watchdog runs for it
        on review_prs alone (core/tasks/routes.py)."""
        def seed(conn):
            self._ticket(conn, "nectar", "NEC-1")
            self._ticket(conn, "nectar", "NEC-2")
            conn.execute(
                "INSERT INTO comment_state(instance_key, resource_type, resource_id,"
                " comment_id, last_checked_at) VALUES ('proxy', 'pr', 'api/1', 'c1', 't0')")
            conn.execute(
                "INSERT INTO kv(instance_key, key, data, updated_at)"
                " VALUES ('quill', 'own_prs', '{}', 't0')")
        conn = self._apply(tmp_path, stop_before="029_watchdog_launch_fence.sql",
                           seed=seed)
        assert conn.execute("SELECT COUNT(*) FROM watchdog_launches").fetchone()[0] == 0

        self._run(conn, "029_watchdog_launch_fence.sql")

        rows = conn.execute(
            "SELECT instance_key, bucket, entity_id FROM watchdog_launches"
            " ORDER BY instance_key").fetchall()
        assert rows == [("nectar", "migration", "upgrade-fence"),
                        ("proxy", "migration", "upgrade-fence"),
                        ("quill", "migration", "upgrade-fence")]

    def test_running_the_fence_twice_leaves_one_row_per_instance(self, tmp_path):
        conn = self._apply(tmp_path, stop_before="029_watchdog_launch_fence.sql",
                           seed=lambda c: self._ticket(c, "nectar", "NEC-1"))
        self._run(conn, "029_watchdog_launch_fence.sql")
        self._run(conn, "029_watchdog_launch_fence.sql")

        assert conn.execute("SELECT COUNT(*) FROM watchdog_launches").fetchone()[0] == 1

    def test_a_fresh_install_is_not_fenced(self, tmp_path):
        """Negative control: a database that has never held any instance state
        has no pre-upgrade activity to fence."""
        conn = self._apply(tmp_path, name="fresh.db")

        assert conn.execute("SELECT COUNT(*) FROM watchdog_launches").fetchone()[0] == 0

    def test_prior_attempts_are_carried_into_the_ledger(self, tmp_path):
        """028's backfill still carries what it can, for the history."""
        def seed(conn):
            conn.execute(
                "INSERT INTO watchdog_observations(instance_key, bucket, entity_id,"
                " first_seen_at, last_seen_at, opened_at, work_item_id)"
                " VALUES ('nectar', 'pr_failed_tickets', 'NEC-1', 't0', 't1',"
                " '2026-09-03T10:00:00Z', 7)")
            conn.execute(
                "INSERT INTO watchdog_observations(instance_key, bucket, entity_id,"
                " first_seen_at, last_seen_at)"
                " VALUES ('nectar', 'pr_failed_tickets', 'NEC-2', 't0', 't1')")
        conn = self._apply(tmp_path, stop_before="028_watchdog_launches.sql",
                           seed=seed)

        self._run(conn, "028_watchdog_launches.sql")

        rows = conn.execute("SELECT entity_id, work_item_id, created_at"
                            " FROM watchdog_launches").fetchall()
        assert rows == [("NEC-1", 7, "2026-09-03T10:00:00Z")]
