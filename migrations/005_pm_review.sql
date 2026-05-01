CREATE TABLE IF NOT EXISTS pm_review (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL,
  ticket_key TEXT,
  prd_section_id INTEGER,
  checkpoint_type TEXT NOT NULL,
  verdict TEXT NOT NULL,
  findings TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_pm_review_ticket ON pm_review(instance_key, ticket_key, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_pm_review_section ON pm_review(prd_section_id);
