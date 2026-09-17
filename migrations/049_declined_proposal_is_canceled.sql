UPDATE work_items SET state = 'canceled'
WHERE state = 'done' AND stop_reason = 'Proposal declined';
