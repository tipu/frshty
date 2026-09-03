CREATE TABLE IF NOT EXISTS watchdog_launches (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  bucket TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  work_item_id INTEGER,
  error TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_watchdog_launches_instance
  ON watchdog_launches(instance_key, created_at DESC);

INSERT INTO watchdog_launches
  (instance_key, bucket, entity_id, work_item_id, error, created_at)
SELECT instance_key, bucket, entity_id, work_item_id, '', opened_at
  FROM watchdog_observations WHERE opened_at IS NOT NULL;
