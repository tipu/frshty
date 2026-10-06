ALTER TABLE work_items ADD COLUMN proposal_key TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_work_items_proposal_key ON work_items(proposal_key) WHERE proposal_key != '';
