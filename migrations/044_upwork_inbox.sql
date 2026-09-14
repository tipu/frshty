CREATE TABLE IF NOT EXISTS upwork_rooms (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  room_id TEXT NOT NULL,
  room_name TEXT NOT NULL DEFAULT '',
  job_title TEXT NOT NULL DEFAULT '',
  job_uid TEXT NOT NULL DEFAULT '',
  client_name TEXT NOT NULL DEFAULT '',
  recent_ts TEXT NOT NULL DEFAULT '',
  first_ts TEXT NOT NULL,
  last_ts TEXT NOT NULL,
  message_count INTEGER NOT NULL DEFAULT 0,
  revision INTEGER NOT NULL DEFAULT 0,
  judged_ts TEXT NOT NULL DEFAULT '',
  judged_at TEXT,
  work_item_id INTEGER REFERENCES work_items(id),
  proposed_ts TEXT NOT NULL DEFAULT '',
  proposed_at TEXT,
  injected INTEGER NOT NULL DEFAULT 0,
  injected_reason TEXT NOT NULL DEFAULT '',
  reply_draft TEXT NOT NULL DEFAULT '',
  reply_sent_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_upwork_rooms_key
  ON upwork_rooms(instance_key, room_id);
CREATE INDEX IF NOT EXISTS idx_upwork_rooms_recent
  ON upwork_rooms(instance_key, last_ts DESC);

CREATE TABLE IF NOT EXISTS upwork_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  room_id INTEGER NOT NULL REFERENCES upwork_rooms(id),
  story_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  user_id TEXT NOT NULL DEFAULT '',
  user_name TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL DEFAULT '',
  is_system INTEGER NOT NULL DEFAULT 0,
  deleted INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_upwork_messages_story
  ON upwork_messages(room_id, story_id);
