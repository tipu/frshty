CREATE TABLE IF NOT EXISTS usage_counters (
    instance TEXT NOT NULL DEFAULT '',
    kind TEXT NOT NULL,
    name TEXT NOT NULL,
    day TEXT NOT NULL,
    count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (instance, kind, name, day)
);

CREATE INDEX IF NOT EXISTS idx_usage_counters_kind_name ON usage_counters(kind, name);
