ALTER TABLE jobs ADD COLUMN root_job_id INTEGER;
ALTER TABLE jobs ADD COLUMN ticket_digest TEXT;
ALTER TABLE events ADD COLUMN root_job_id INTEGER;
CREATE INDEX IF NOT EXISTS idx_log_events_event_ts ON log_events(instance_key, event, ts DESC);
