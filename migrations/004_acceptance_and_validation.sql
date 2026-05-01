ALTER TABLE tickets ADD COLUMN source TEXT NOT NULL DEFAULT 'jira';
ALTER TABLE tickets ADD COLUMN approval_status TEXT;
ALTER TABLE tickets ADD COLUMN obsolete_at TEXT;

CREATE INDEX IF NOT EXISTS idx_tickets_approval ON tickets(instance_key, approval_status);
CREATE INDEX IF NOT EXISTS idx_tickets_source ON tickets(instance_key, source);

CREATE TABLE IF NOT EXISTS validation_run (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  ticket_key TEXT NOT NULL,
  criterion_id TEXT NOT NULL,
  phase TEXT NOT NULL,
  result TEXT NOT NULL,
  playwright_output TEXT,
  triggered_by_ticket_key TEXT,
  run_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_validation_run_ticket ON validation_run(instance_key, ticket_key, run_at DESC);

CREATE TABLE IF NOT EXISTS regression_pool (
  instance_key TEXT NOT NULL,
  ticket_key TEXT NOT NULL,
  criterion_id TEXT NOT NULL,
  playwright_steps TEXT NOT NULL,
  added_at TEXT NOT NULL,
  removed_at TEXT,
  PRIMARY KEY (instance_key, ticket_key, criterion_id)
);
CREATE INDEX IF NOT EXISTS idx_regression_pool_active ON regression_pool(instance_key, removed_at);
