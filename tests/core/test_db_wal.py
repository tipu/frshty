from pathlib import Path

import core.db as db


REPO_MIGRATIONS = Path(__file__).resolve().parent.parent.parent / "migrations"


def _wal(path: Path) -> Path:
    return path.with_name(path.name + "-wal")


class TestWalKeptBetweenConnections:
    def test_closing_a_query_connection_keeps_the_wal(self, tmp_path, monkeypatch):
        monkeypatch.setattr(db, "_KEEPER", None)
        path = tmp_path / "w.db"
        db.init(path, REPO_MIGRATIONS)
        try:
            for _ in range(3):
                db.execute("INSERT INTO usage_counters(instance, kind, name, day, count) "
                           "VALUES ('i', 'route', 'GET /x', '2026-01-01', 1) "
                           "ON CONFLICT(instance, kind, name, day) DO UPDATE SET count = count + 1")
                assert _wal(path).exists()
            assert db.query_one("SELECT count FROM usage_counters")["count"] == 3
        finally:
            db._KEEPER.close()

    def test_without_the_held_connection_the_wal_is_deleted(self, tmp_path, monkeypatch):
        monkeypatch.setattr(db, "_KEEPER", None)
        path = tmp_path / "w.db"
        db.init(path, REPO_MIGRATIONS)
        db._KEEPER.close()
        db._KEEPER = None
        db.execute("INSERT INTO usage_counters(instance, kind, name, day, count) "
                   "VALUES ('i', 'route', 'GET /x', '2026-01-01', 1)")
        assert not _wal(path).exists()
