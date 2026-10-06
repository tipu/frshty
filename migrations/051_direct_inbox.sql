CREATE TABLE IF NOT EXISTS direct_followups (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  source       TEXT NOT NULL,
  thread_key   TEXT NOT NULL,
  last_at      TEXT NOT NULL,
  needs_reply  INTEGER NOT NULL DEFAULT 0,
  who          TEXT NOT NULL DEFAULT '',
  reason       TEXT NOT NULL DEFAULT '',
  objective    TEXT NOT NULL DEFAULT '',
  evidence     TEXT NOT NULL DEFAULT '',
  work_item_id INTEGER REFERENCES work_items(id),
  created_at   TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_direct_followups_message
  ON direct_followups(instance_key, source, thread_key, last_at);
