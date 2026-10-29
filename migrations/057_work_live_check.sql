ALTER TABLE work_items ADD COLUMN live_check TEXT NOT NULL DEFAULT '';
ALTER TABLE work_items ADD COLUMN live_check_set_at TEXT;
ALTER TABLE work_items ADD COLUMN live_check_ran_at TEXT;
ALTER TABLE work_items ADD COLUMN live_check_exit INTEGER;
ALTER TABLE work_items ADD COLUMN live_check_output TEXT NOT NULL DEFAULT '';
