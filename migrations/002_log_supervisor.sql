-- Move event logs, read state, and supervisor state from disk files to SQLite

CREATE TABLE log_events (
    id           TEXT NOT NULL,
    instance_key TEXT NOT NULL,
    job          TEXT NOT NULL,
    event        TEXT NOT NULL,
    summary      TEXT NOT NULL,
    links        TEXT NOT NULL DEFAULT '{}',
    meta         TEXT NOT NULL DEFAULT '{}',
    ts           TEXT NOT NULL,
    PRIMARY KEY (instance_key, id)
);

CREATE INDEX idx_log_events_job_ts ON log_events(instance_key, job, ts DESC);

CREATE TABLE log_read_state (
    instance_key TEXT NOT NULL,
    event_id     TEXT NOT NULL,
    PRIMARY KEY  (instance_key, event_id)
);

CREATE TABLE supervisor_actions (
    key   TEXT PRIMARY KEY,
    count INTEGER NOT NULL DEFAULT 0,
    ts    REAL NOT NULL
);

CREATE TABLE supervisor_escalations (
    key TEXT PRIMARY KEY,
    ts  REAL NOT NULL
);
