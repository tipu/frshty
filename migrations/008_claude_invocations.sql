CREATE TABLE IF NOT EXISTS claude_invocations (
  id            TEXT PRIMARY KEY,
  instance_key  TEXT NOT NULL,
  job_key       TEXT NOT NULL DEFAULT '',
  function_name TEXT NOT NULL,
  model         TEXT NOT NULL,
  prompt        TEXT NOT NULL,
  prompt_length INTEGER NOT NULL,
  cwd           TEXT,
  tools         TEXT,
  timeout_s     INTEGER,
  started_at    TEXT NOT NULL,
  finished_at   TEXT,
  duration_ms   INTEGER,
  status        TEXT NOT NULL DEFAULT 'running',
  exit_code     INTEGER,
  output        TEXT,
  output_length INTEGER
);
CREATE INDEX IF NOT EXISTS idx_claude_invocations_instance_started ON claude_invocations(instance_key, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_claude_invocations_job ON claude_invocations(instance_key, job_key, started_at DESC);
