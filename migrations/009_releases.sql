ALTER TABLE tickets ADD COLUMN release_key TEXT;
CREATE INDEX IF NOT EXISTS idx_tickets_release ON tickets(instance_key, release_key);

CREATE TABLE IF NOT EXISTS releases (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  release_key TEXT NOT NULL,
  title TEXT,
  status TEXT NOT NULL DEFAULT 'open',
  ticket_set_hash TEXT,
  last_inspected_at TEXT,
  created_at TEXT NOT NULL,
  UNIQUE(instance_key, release_key)
);
CREATE INDEX IF NOT EXISTS idx_releases_instance ON releases(instance_key, status);

CREATE TABLE IF NOT EXISTS release_review (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  release_id INTEGER NOT NULL,
  instance_key TEXT NOT NULL,
  verdict TEXT NOT NULL,
  findings TEXT NOT NULL DEFAULT '[]',
  ticket_set_hash TEXT NOT NULL,
  release_md_hash TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (release_id) REFERENCES releases(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_release_review_release ON release_review(release_id, created_at DESC);
