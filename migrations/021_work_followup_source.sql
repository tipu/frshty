ALTER TABLE work_items ADD COLUMN source_item_id INTEGER REFERENCES work_items(id);
