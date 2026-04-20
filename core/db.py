import json
import random
import sqlite3
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any

_DB_PATH: Path | None = None
_MIGRATIONS_DIR: Path | None = None


def init(db_path: Path, migrations_dir: Path) -> None:
    global _DB_PATH, _MIGRATIONS_DIR
    _DB_PATH = db_path
    _MIGRATIONS_DIR = migrations_dir
    db_path.parent.mkdir(parents=True, exist_ok=True)
    _apply_migrations()


def _connect() -> sqlite3.Connection:
    if _DB_PATH is None:
        raise RuntimeError("db not initialized; call core.db.init(path, migrations_dir) first")
    conn = sqlite3.connect(str(_DB_PATH), timeout=30.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def _apply_migrations() -> None:
    if _MIGRATIONS_DIR is None or not _MIGRATIONS_DIR.exists():
        return
    conn = _connect()
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS _migrations (name TEXT PRIMARY KEY, applied_at TEXT NOT NULL)")
        applied = {row["name"] for row in conn.execute("SELECT name FROM _migrations")}
        for sql_file in sorted(_MIGRATIONS_DIR.glob("*.sql")):
            if sql_file.name in applied:
                continue
            conn.executescript(sql_file.read_text())
            conn.execute("INSERT INTO _migrations(name, applied_at) VALUES (?, datetime('now'))", (sql_file.name,))
    finally:
        conn.close()


@contextmanager
def tx():
    conn = _connect()
    attempt = 0
    while True:
        try:
            conn.execute("BEGIN IMMEDIATE")
            break
        except sqlite3.OperationalError as e:
            if "locked" not in str(e).lower() or attempt >= 4:
                conn.close()
                raise
            time.sleep(0.1 + random.random() * 0.4)
            attempt += 1
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


@contextmanager
def cursor():
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def execute(sql: str, params: tuple | dict = ()) -> None:
    with tx() as c:
        c.execute(sql, params)


def query_one(sql: str, params: tuple | dict = ()) -> dict | None:
    with cursor() as c:
        row = c.execute(sql, params).fetchone()
        return dict(row) if row else None


def query_all(sql: str, params: tuple | dict = ()) -> list[dict]:
    with cursor() as c:
        return [dict(row) for row in c.execute(sql, params).fetchall()]


def load_json(row: dict | None, field: str = "data") -> dict:
    if not row:
        return {}
    raw = row.get(field)
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {}


def dump_json(obj: Any) -> str:
    return json.dumps(obj, default=str)
