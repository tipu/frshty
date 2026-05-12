CREATE TABLE IF NOT EXISTS ticket_transitions (
  id               INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key     TEXT NOT NULL,
  ticket_key       TEXT NOT NULL,
  prior_status     TEXT,
  new_status       TEXT NOT NULL,
  rejected         INTEGER NOT NULL DEFAULT 0,
  rejection_reason TEXT,
  actor            TEXT NOT NULL DEFAULT '',
  reason           TEXT NOT NULL DEFAULT '',
  co_field_diff    TEXT NOT NULL DEFAULT '{}',
  ts               TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ticket_transitions_by_ticket
  ON ticket_transitions(instance_key, ticket_key, ts);

CREATE INDEX IF NOT EXISTS idx_ticket_transitions_recent
  ON ticket_transitions(instance_key, ts DESC);
