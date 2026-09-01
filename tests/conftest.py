import json
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest

import core.db as db
import core.state as state
import core.log as log
from services import work_artifacts


_SESSION_DB_PATH = None
_SESSION_MIGRATIONS_DIR = None



def _reinject_core_modules() -> None:
    """Point every loaded frshty module back at the session core.state/db/log.

    test_http_endpoints and test_tz pop core.* out of sys.modules and
    re-import, which leaves already-imported features.* modules bound to a
    core.state that is no longer initialized. Rebinding by name rather than
    from a fixed list keeps new modules covered automatically.
    """
    targets = (("state", state, "core.state"), ("db", db, "core.db"), ("log", log, "core.log"))
    core_pkg = sys.modules.get("core")
    if core_pkg is not None:
        for attr, replacement, _ in targets:
            setattr(core_pkg, attr, replacement)
    for mod_name, mod in list(sys.modules.items()):
        if mod is None:
            continue
        if not (mod_name == "frshty" or mod_name.startswith("features.")
                or mod_name.startswith("core.") or mod_name.startswith("web.")
                or mod_name.startswith("services.")):
            continue
        for attr, replacement, expected in targets:
            current = getattr(mod, attr, None)
            if isinstance(current, types.ModuleType) and current.__name__ == expected:
                setattr(mod, attr, replacement)

@pytest.fixture(scope="session", autouse=True)
def _isolated_artifact_root(tmp_path_factory):
    """Keep a test launch from creating folders in the operator's real
    ~/.frshty/artifacts store. The environment carries the override because
    some tests re-import services.*, which would drop a patched attribute."""
    root = tmp_path_factory.mktemp("frshty-artifacts")
    previous = os.environ.get(work_artifacts.ROOT_ENV)
    os.environ[work_artifacts.ROOT_ENV] = str(root)
    yield root
    if previous is None:
        del os.environ[work_artifacts.ROOT_ENV]
    else:
        os.environ[work_artifacts.ROOT_ENV] = previous


@pytest.fixture(scope="session", autouse=True)
def _isolated_db(tmp_path_factory):
    global _SESSION_DB_PATH, _SESSION_MIGRATIONS_DIR
    db_dir = tmp_path_factory.mktemp("frshty-db")
    db_path = db_dir / "frshty.db"
    migrations = Path(__file__).resolve().parent.parent / "migrations"

    db._DB_PATH = None
    db._MIGRATIONS_DIR = None
    state._DB_INITIALIZED = False
    state._default_instance_key = None
    state._TICKETS_MIGRATED.clear()

    db.init(db_path, migrations)
    state._DB_INITIALIZED = True
    _SESSION_DB_PATH = db_path
    _SESSION_MIGRATIONS_DIR = migrations

    yield db_path

    db._DB_PATH = None
    db._MIGRATIONS_DIR = None
    state._DB_INITIALIZED = False
    state._default_instance_key = None
    state._TICKETS_MIGRATED.clear()
    _SESSION_DB_PATH = None
    _SESSION_MIGRATIONS_DIR = None


@pytest.fixture(autouse=True)
def _restore_session_db(_isolated_db):
    """Some legacy tests (test_billing_preview, test_scheduler_beat,
    test_scheduled_virtual, test_tz, test_http_endpoints, test_worker_smoke)
    `sys.modules.pop` core.* and features.* then `db.init(tmp_path/'t.db')`.
    That leaves fresh core.state / core.db / features.tickets modules in
    sys.modules with their own _DB_PATH pointing at a deleted tmp file, and
    subsequent tests that `from features import tickets` get those stale
    modules. Before every test, restore the originals so the session DB is
    the single source of truth."""
    sys.modules["core.db"] = db
    sys.modules["core.state"] = state
    sys.modules["core.log"] = log
    db._DB_PATH = _SESSION_DB_PATH
    db._MIGRATIONS_DIR = _SESSION_MIGRATIONS_DIR
    state._DB_INITIALIZED = True
    state._TICKETS_MIGRATED.clear()
    state._instance_key_cv.set(None)
    # Clear all test-modifiable tables to prevent state leakage between tests
    for t in ("supervisor_actions", "supervisor_escalations"):
        try:
            db.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    # Re-inject the session db/state module references into modules that
    # cache them at import-time (features.tickets in particular, since
    # hardening tests patch attributes via `features.tickets.state`).
    _reinject_core_modules()
    yield


@pytest.fixture()
def tmp_state(tmp_path, _isolated_db):
    state._default_instance_key = None
    state._TICKETS_MIGRATED.clear()
    state.init(tmp_path)
    yield tmp_path
    state._TICKETS_MIGRATED.clear()


@pytest.fixture()
def fresh_db(tmp_path, _restore_session_db):
    """Per-test fresh DB at tmp_path. Saves the session DB path on entry,
    initializes a new DB at tmp_path/t.db with the standard migrations, and
    restores the session DB on teardown. Use this instead of bare
    `db.init(tmp_path/...)` so module references and instance state are
    properly preserved.

    NOTE: depends on `_restore_session_db` so this fixture's setUp runs AFTER
    the autouse session-restore. Otherwise the autouse fixture would override
    our fresh DB path back to the session DB and the test would silently see
    leaked state.

    Returns the path to the fresh DB. The active state instance_key is reset
    to None so tests can call `state.init(...)` themselves if they need a
    specific key."""
    saved_path = db._DB_PATH
    saved_migrations = db._MIGRATIONS_DIR
    saved_default_key = state._default_instance_key
    saved_initialized = state._DB_INITIALIZED

    migrations = saved_migrations or (
        Path(__file__).resolve().parent.parent / "migrations"
    )
    db_path = tmp_path / "t.db"
    db._DB_PATH = None
    db._MIGRATIONS_DIR = None
    state._DB_INITIALIZED = False
    state._default_instance_key = None
    state._TICKETS_MIGRATED.clear()
    db.init(db_path, migrations)
    state._DB_INITIALIZED = True

    yield db_path

    db._DB_PATH = saved_path
    db._MIGRATIONS_DIR = saved_migrations
    state._DB_INITIALIZED = saved_initialized
    state._default_instance_key = saved_default_key
    state._TICKETS_MIGRATED.clear()


@pytest.fixture()
def tmp_log(tmp_state):
    log.init(tmp_state, "test")
    return tmp_state


@pytest.fixture()
def fake_config(tmp_path):
    ws_root = tmp_path / "workspace"
    ws_root.mkdir()
    config_file = tmp_path / "config.toml"
    config_file.write_text('[job]\nkey = "test"\nport = 8000\nplatform = "github"\nticket_system = "jira"\n\n[workspace]\nroot = "' + str(ws_root) + '"\n\n[features]\n\n[pr]\n\n[github]\nrepo = "org/repo"\n')
    return {
        "job": {"key": "test", "port": 8000, "platform": "github", "ticket_system": "jira"},
        "workspace": {
            "root": ws_root,
            "tickets_dir": "tickets",
            "ticket_layout": "flat",
            "base_branch": "main",
            "branch_prefix": "",
            "exclude": [],
            "dep_commands": [],
        },
        "pr": {"auto_pr": True, "auto_merge": False, "merge_strategy": "squash", "merge_flags": []},
        "features": {},
        "github": {"repo": "org/repo"},
        "bitbucket": {"org": "myorg", "user_account_id": "me123"},
        "jira": {"base_url": "https://jira.example.com", "user_env": "JIRA_USER", "token_env": "JIRA_TOKEN"},
        "slack": {},
        "timesheet": {},
        "_config_path": config_file,
        "_state_dir": tmp_path / ".frshty" / "test",
        "_base_url": "http://localhost:8000",
    }


def make_pr(**overrides):
    base = {
        "id": 1,
        "repo": "myrepo",
        "title": "Fix bug",
        "author": "alice",
        "branch": "fix/bug",
        "base": "main",
        "created_on": "2026-01-01T00:00:00Z",
        "updated_on": "2026-01-02T00:00:00Z",
        "url": "https://example.com/pr/1",
    }
    base.update(overrides)
    return base


def make_ticket(**overrides):
    base = {
        "key": "PROJ-1",
        "summary": "Do the thing",
        "status": "In Progress",
        "description": "Description text",
        "url": "https://jira.example.com/browse/PROJ-1",
        "attachments": [],
        "related": [],
        "parent": None,
        "subtasks": [],
        "estimate_seconds": 28800,
    }
    base.update(overrides)
    return base


def make_ticket_state(**overrides):
    base = {
        "status": "new",
        "slug": "PROJ-1-do-the-thing",
        "branch": "PROJ-1-do-the-thing",
        "url": "https://jira.example.com/browse/PROJ-1",
    }
    base.update(overrides)
    return base


def make_comment(**overrides):
    base = {
        "id": 100,
        "body": "Please fix this",
        "author_id": "reviewer1",
        "author_name": "Bob",
        "path": "src/main.py",
        "line": 42,
        "created_on": "2026-01-01T12:00:00Z",
        "parent_id": None,
    }
    base.update(overrides)
    return base


def make_check(**overrides):
    base = {
        "name": "CI",
        "state": "SUCCESS",
        "url": "https://ci.example.com/run/1",
    }
    base.update(overrides)
    return base


def seed_state(tmp_path, module, data):
    state.init(tmp_path)
    state.save(module, data)
