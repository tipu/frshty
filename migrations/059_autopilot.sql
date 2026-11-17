CREATE TABLE IF NOT EXISTS autopilot_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_key TEXT NOT NULL,
    kind TEXT NOT NULL,
    ask_key TEXT NOT NULL DEFAULT '',
    started_at TEXT NOT NULL,
    asks TEXT NOT NULL DEFAULT '[]',
    work_item_id INTEGER,
    error TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_autopilot_runs_instance ON autopilot_runs(instance_key, id);
CREATE TABLE IF NOT EXISTS autopilot_notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id INTEGER NOT NULL,
    ts TEXT NOT NULL,
    text TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_autopilot_notes_run ON autopilot_notes(run_id, id);
CREATE TABLE IF NOT EXISTS autopilot_asks (
    instance_key TEXT NOT NULL,
    ask_key TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    run_id INTEGER,
    seen_at TEXT NOT NULL,
    PRIMARY KEY (instance_key, ask_key)
);
