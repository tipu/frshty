CREATE TABLE IF NOT EXISTS watchdog_observations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  bucket TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  ticket_key TEXT NOT NULL DEFAULT '',
  first_seen_at TEXT NOT NULL,
  last_seen_at TEXT NOT NULL,
  opened_at TEXT,
  work_item_id INTEGER
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_watchdog_observations_entity
  ON watchdog_observations(instance_key, bucket, entity_id);
