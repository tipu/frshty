PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT,
  source TEXT NOT NULL,
  kind TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  created_at TEXT NOT NULL,
  dispatched_at TEXT,
  dispatch_reason TEXT
);
CREATE INDEX IF NOT EXISTS idx_events_pending ON events(dispatched_at, id);

CREATE TABLE IF NOT EXISTS jobs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  ticket_key TEXT,
  task TEXT NOT NULL,
  payload TEXT NOT NULL DEFAULT '{}',
  status TEXT NOT NULL DEFAULT 'queued',
  enqueued_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  response TEXT,
  triggering_event_id INTEGER REFERENCES events(id)
);
CREATE INDEX IF NOT EXISTS idx_jobs_claim ON jobs(status, id);
CREATE INDEX IF NOT EXISTS idx_jobs_ticket ON jobs(instance_key, ticket_key, id DESC);

CREATE TABLE IF NOT EXISTS tickets (
  instance_key TEXT NOT NULL,
  ticket_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'new',
  slug TEXT,
  branch TEXT,
  url TEXT,
  external_status TEXT,
  auto_pr INTEGER,
  data TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (instance_key, ticket_key)
);

CREATE TABLE IF NOT EXISTS own_prs (
  instance_key TEXT NOT NULL,
  repo TEXT NOT NULL,
  pr_id INTEGER NOT NULL,
  data TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (instance_key, repo, pr_id)
);

CREATE TABLE IF NOT EXISTS scheduler (
  instance_key TEXT NOT NULL,
  key TEXT NOT NULL,
  run_at TEXT,
  data TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (instance_key, key)
);

CREATE TABLE IF NOT EXISTS slack_state (
  instance_key TEXT NOT NULL,
  namespace TEXT NOT NULL,
  key TEXT NOT NULL,
  data TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (instance_key, namespace, key)
);

CREATE TABLE IF NOT EXISTS billing_entries (
  instance_key TEXT NOT NULL,
  date TEXT NOT NULL,
  type TEXT NOT NULL,
  hours REAL NOT NULL DEFAULT 0,
  PRIMARY KEY (instance_key, date)
);

CREATE TABLE IF NOT EXISTS billing_invoices (
  instance_key TEXT NOT NULL,
  id TEXT NOT NULL,
  number TEXT,
  status TEXT,
  start_date TEXT,
  end_date TEXT,
  hours REAL,
  amount REAL,
  date TEXT,
  source TEXT,
  data TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (instance_key, id)
);

CREATE TABLE IF NOT EXISTS billing_autogen (
  instance_key TEXT NOT NULL,
  marker TEXT NOT NULL,
  data TEXT NOT NULL DEFAULT '{}',
  PRIMARY KEY (instance_key, marker)
);

CREATE TABLE IF NOT EXISTS kv (
  instance_key TEXT NOT NULL,
  key TEXT NOT NULL,
  data TEXT NOT NULL DEFAULT '{}',
  updated_at TEXT NOT NULL,
  PRIMARY KEY (instance_key, key)
);
