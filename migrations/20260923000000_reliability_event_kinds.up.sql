ALTER TABLE shardline_reliability_events
    ADD COLUMN operation_kind TEXT;

UPDATE shardline_reliability_events
SET operation_kind = event_json->'operation'->>'kind'
WHERE operation_kind IS NULL;

ALTER TABLE shardline_reliability_events
    ALTER COLUMN operation_kind SET NOT NULL;

ALTER TABLE shardline_reliability_events
    DROP CONSTRAINT IF EXISTS shardline_reliability_events_pkey;

ALTER TABLE shardline_reliability_events
    ADD CONSTRAINT shardline_reliability_events_pkey
    PRIMARY KEY (operation_kind, operation_id, sequence);
