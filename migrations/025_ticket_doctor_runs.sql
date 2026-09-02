CREATE TABLE IF NOT EXISTS ticket_doctor_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  ticket_key TEXT NOT NULL,
  work_item_id INTEGER NOT NULL,
  description TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ticket_doctor_runs_ticket
  ON ticket_doctor_runs(instance_key, ticket_key, id DESC);
