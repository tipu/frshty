CREATE TABLE IF NOT EXISTS verification_claim (
  instance_key        TEXT NOT NULL,
  ticket_key          TEXT NOT NULL,
  claim               TEXT NOT NULL,
  established_at      TEXT NOT NULL,
  established_against TEXT NOT NULL,
  invalidated_at      TEXT,
  invalidated_by      TEXT,
  PRIMARY KEY (instance_key, ticket_key, claim)
);
