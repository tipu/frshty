CREATE INDEX IF NOT EXISTS idx_jobs_task_recent ON jobs(instance_key, task, id DESC);
