CREATE TABLE IF NOT EXISTS blocked_attempts (
    instance_key TEXT NOT NULL,
    resource TEXT NOT NULL,
    action TEXT NOT NULL,
    blocker TEXT NOT NULL,
    error TEXT NOT NULL DEFAULT '',
    wake TEXT NOT NULL DEFAULT '',
    attempts INTEGER NOT NULL DEFAULT 0,
    first_at TEXT NOT NULL,
    last_at TEXT NOT NULL,
    PRIMARY KEY (instance_key, resource, action, blocker)
);
