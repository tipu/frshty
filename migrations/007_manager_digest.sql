CREATE TABLE IF NOT EXISTS manager_digest (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  generated_at TEXT NOT NULL,
  priorities_hash TEXT,
  body_md TEXT NOT NULL,
  candidate_counts_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_manager_digest_instance ON manager_digest(instance_key, generated_at DESC);
