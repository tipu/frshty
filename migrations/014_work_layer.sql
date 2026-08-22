CREATE TABLE IF NOT EXISTS work_items (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  objective TEXT NOT NULL,
  definition_of_done TEXT NOT NULL DEFAULT '',
  scope TEXT NOT NULL DEFAULT 'ad-hoc',
  scope_ref TEXT NOT NULL DEFAULT '',
  owner TEXT NOT NULL DEFAULT 'agent',
  state TEXT NOT NULL DEFAULT 'agent_working',
  priority INTEGER NOT NULL DEFAULT 0,
  current_checkpoint TEXT NOT NULL DEFAULT '',
  next_action TEXT NOT NULL DEFAULT '',
  stop_reason TEXT NOT NULL DEFAULT '',
  instance_key TEXT,
  snoozed_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_work_items_state ON work_items(state, updated_at DESC);

CREATE TABLE IF NOT EXISTS work_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_item_id INTEGER NOT NULL REFERENCES work_items(id),
  provider TEXT NOT NULL DEFAULT 'claude',
  session_id TEXT NOT NULL,
  tmux_key TEXT NOT NULL DEFAULT '',
  cwd TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'launched',
  started_at TEXT NOT NULL,
  finished_at TEXT,
  result TEXT NOT NULL DEFAULT '',
  transcript_path TEXT NOT NULL DEFAULT '',
  transcript_cursor INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_work_runs_session ON work_runs(session_id);
CREATE INDEX IF NOT EXISTS idx_work_runs_item ON work_runs(work_item_id, id DESC);

CREATE TABLE IF NOT EXISTS work_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  work_item_id INTEGER NOT NULL REFERENCES work_items(id),
  work_run_id INTEGER,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_work_events_item ON work_events(work_item_id, id);
