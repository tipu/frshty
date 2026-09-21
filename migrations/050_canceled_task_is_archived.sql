UPDATE work_items SET archived_at = updated_at
WHERE state = 'canceled' AND archived_at IS NULL;
