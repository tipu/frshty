DELETE FROM watchdog_launches WHERE bucket = 'migration';

INSERT INTO watchdog_launches
  (instance_key, bucket, entity_id, work_item_id, error, created_at)
SELECT instance_key, 'migration', 'upgrade-fence', NULL,
       'the launch ledger replaced a per-entity count that could not be reconstructed',
       strftime('%Y-%m-%dT%H:%M:%S+00:00', 'now')
FROM (SELECT instance_key FROM kv
      UNION SELECT instance_key FROM tickets
      UNION SELECT instance_key FROM comment_state
      UNION SELECT instance_key FROM watchdog_observations);
