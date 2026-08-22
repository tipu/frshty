ALTER TABLE work_items ADD COLUMN tags TEXT NOT NULL DEFAULT '';
UPDATE work_items SET tags = CASE
  WHEN instr(substr(contexts, instr(contexts, ',') + 1), ',') > 0
  THEN substr(contexts, 1, instr(contexts, ',') + instr(substr(contexts, instr(contexts, ',') + 1), ',') - 1)
  ELSE contexts
END WHERE contexts != '';
