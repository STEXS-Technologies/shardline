ALTER TABLE shardline_reliability_events
    DROP CONSTRAINT IF EXISTS shardline_reliability_events_pkey;

ALTER TABLE shardline_reliability_events
    DROP COLUMN IF EXISTS operation_kind;

ALTER TABLE shardline_reliability_events
    ADD CONSTRAINT shardline_reliability_events_pkey
    PRIMARY KEY (operation_id, sequence);
