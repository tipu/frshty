CREATE TABLE IF NOT EXISTS prd (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_key TEXT NOT NULL UNIQUE,
  file_path TEXT NOT NULL,
  last_parsed_at TEXT,
  last_file_hash TEXT
);

CREATE TABLE IF NOT EXISTS prd_section (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  prd_id INTEGER NOT NULL,
  stable_key TEXT NOT NULL,
  header TEXT NOT NULL,
  content TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  deleted_at TEXT,
  FOREIGN KEY (prd_id) REFERENCES prd(id)
);
CREATE INDEX IF NOT EXISTS idx_prd_section_prd ON prd_section(prd_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_prd_section_key ON prd_section(prd_id, stable_key);

CREATE TABLE IF NOT EXISTS prd_section_ticket (
  prd_section_id INTEGER NOT NULL,
  instance_key TEXT NOT NULL,
  ticket_key TEXT NOT NULL,
  PRIMARY KEY (prd_section_id, instance_key, ticket_key),
  FOREIGN KEY (prd_section_id) REFERENCES prd_section(id)
);
CREATE INDEX IF NOT EXISTS idx_prd_section_ticket_ticket ON prd_section_ticket(instance_key, ticket_key);
