UPDATE work_items SET contexts = tags WHERE contexts = '' AND tags != '';
ALTER TABLE work_items DROP COLUMN tags;
