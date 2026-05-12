CREATE TABLE IF NOT EXISTS today_snoozes (
  instance_key   TEXT NOT NULL,
  loop_type      TEXT NOT NULL,
  entity_id      TEXT NOT NULL,
  snooze_until   TEXT,
  created_at     TEXT NOT NULL,
  reason         TEXT,
  PRIMARY KEY (instance_key, loop_type, entity_id)
);

CREATE INDEX IF NOT EXISTS idx_today_snoozes_lookup
  ON today_snoozes(instance_key, loop_type);
