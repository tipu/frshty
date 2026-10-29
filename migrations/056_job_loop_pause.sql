CREATE TABLE IF NOT EXISTS job_pauses (
  instance_key  TEXT NOT NULL,
  task          TEXT NOT NULL,
  ticket_key    TEXT NOT NULL,
  ticket_digest TEXT NOT NULL,
  count         INTEGER NOT NULL,
  reason        TEXT NOT NULL DEFAULT '',
  work_item_id  INTEGER,
  paused_at     TEXT NOT NULL,
  expires_at    TEXT NOT NULL,
  PRIMARY KEY (instance_key, task, ticket_key)
);
