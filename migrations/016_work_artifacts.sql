CREATE TABLE IF NOT EXISTS work_artifacts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_item_id INTEGER REFERENCES work_items(id),
  work_run_id INTEGER,
  path TEXT NOT NULL,
  note TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_work_artifacts_item ON work_artifacts(work_item_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_work_artifacts_run_path ON work_artifacts(work_run_id, path);
