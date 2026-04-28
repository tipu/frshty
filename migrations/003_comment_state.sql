-- Stateful comment tracking for PR and ticket comments
-- Enables detection of new, edited, and deleted comments

CREATE TABLE IF NOT EXISTS comment_state (
  instance_key TEXT NOT NULL,
  resource_type TEXT NOT NULL,  -- 'pr' or 'ticket'
  resource_id TEXT NOT NULL,     -- pr_key (repo/id) or ticket_key
  comment_id TEXT NOT NULL,
  comment_edited_at TEXT,        -- timestamp from platform (updated_at)
  last_checked_at TEXT NOT NULL, -- when we last processed this comment
  state TEXT NOT NULL DEFAULT 'new', -- 'new', 'processing', 'processed', 'deleted'
  error_count INTEGER DEFAULT 0,
  last_error TEXT,
  processed_at TEXT,
  PRIMARY KEY (instance_key, resource_type, resource_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_comment_state_unprocessed
  ON comment_state(instance_key, resource_type, state, resource_id);

CREATE INDEX IF NOT EXISTS idx_comment_state_by_resource
  ON comment_state(instance_key, resource_type, resource_id);
