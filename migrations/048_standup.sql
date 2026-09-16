CREATE TABLE IF NOT EXISTS standups (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  day          TEXT NOT NULL,
  state        TEXT NOT NULL DEFAULT 'draft',
  yesterday_md TEXT NOT NULL DEFAULT '',
  created_at   TEXT NOT NULL,
  opened_at    TEXT,
  closed_at    TEXT,
  last_tick_at TEXT
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_standups_day ON standups(day);

CREATE TABLE IF NOT EXISTS standup_items (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  standup_id       INTEGER NOT NULL REFERENCES standups(id),
  text             TEXT NOT NULL,
  contexts         TEXT NOT NULL DEFAULT '',
  state            TEXT NOT NULL DEFAULT 'open',
  origin           TEXT NOT NULL DEFAULT 'operator',
  origin_ref       TEXT NOT NULL DEFAULT '',
  carried_from     INTEGER REFERENCES standup_items(id),
  carry_count      INTEGER NOT NULL DEFAULT 0,
  position         INTEGER NOT NULL DEFAULT 0,
  snoozed_until    TEXT,
  last_nudge_at    TEXT,
  nudge_count      INTEGER NOT NULL DEFAULT 0,
  pending_question TEXT NOT NULL DEFAULT '',
  created_at       TEXT NOT NULL,
  updated_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_standup_items_day ON standup_items(standup_id, position, id);

CREATE TABLE IF NOT EXISTS standup_events (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  standup_item_id INTEGER NOT NULL REFERENCES standup_items(id),
  kind            TEXT NOT NULL,
  payload         TEXT NOT NULL DEFAULT '{}',
  created_at      TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_standup_events_item ON standup_events(standup_item_id, id);

ALTER TABLE work_items ADD COLUMN standup_item_id INTEGER REFERENCES standup_items(id);

CREATE INDEX IF NOT EXISTS idx_work_items_standup ON work_items(standup_item_id)
  WHERE standup_item_id IS NOT NULL;
