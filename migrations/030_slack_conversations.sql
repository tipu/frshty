CREATE TABLE IF NOT EXISTS slack_conversations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  workspace TEXT NOT NULL DEFAULT '',
  thread_ts TEXT NOT NULL,
  channel_id TEXT NOT NULL DEFAULT '',
  channel_name TEXT NOT NULL DEFAULT '',
  first_ts TEXT NOT NULL,
  last_ts TEXT NOT NULL,
  message_count INTEGER NOT NULL DEFAULT 0,
  involves_operator INTEGER NOT NULL DEFAULT 0,
  judged_ts TEXT NOT NULL DEFAULT '',
  judged_at TEXT,
  work_item_id INTEGER REFERENCES work_items(id),
  proposed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_slack_conversations_key
  ON slack_conversations(instance_key, workspace, thread_ts);
CREATE INDEX IF NOT EXISTS idx_slack_conversations_recent
  ON slack_conversations(instance_key, last_ts DESC);

CREATE TABLE IF NOT EXISTS slack_conversation_messages (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  conversation_id INTEGER NOT NULL REFERENCES slack_conversations(id),
  ts TEXT NOT NULL,
  user_id TEXT NOT NULL DEFAULT '',
  user_name TEXT NOT NULL DEFAULT '',
  text TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_slack_conversation_messages_ts
  ON slack_conversation_messages(conversation_id, ts);

ALTER TABLE work_items ADD COLUMN launch_cwd TEXT NOT NULL DEFAULT '';
ALTER TABLE work_items ADD COLUMN launch_brief TEXT NOT NULL DEFAULT '';
