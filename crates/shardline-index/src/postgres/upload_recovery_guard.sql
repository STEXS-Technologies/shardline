-- Older servers recover logical file intents by filename alone. Preserve new
-- intents when that code cannot supply the current writer protocol marker.
-- Scoped native intent IDs also need preservation: older physical-key recovery
-- cannot identify their repository publication or normalized storage outcome.
-- New xorb identities use canonical stored lengths; preserve their histories
-- during rollback rather than allowing writers without this protocol marker.
-- This is a compatibility guard, not authorization or execution ownership.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_upload_recovery_guard_v2()
RETURNS trigger LANGUAGE plpgsql
SET search_path = pg_catalog, @SCHEMA@
AS $$
DECLARE repository TEXT;
BEGIN
    IF NEW.state IS NOT DISTINCT FROM OLD.state OR
       (OLD.object_key NOT LIKE 'record/%' AND OLD.intent_id NOT LIKE 'shard-v2-%'
        AND OLD.intent_id NOT LIKE 'xorb-v2-%') THEN
        RETURN NEW;
    END IF;
    IF current_setting('shardline.upload_intent_writer_v2', true) = OLD.intent_id THEN
        RETURN NEW;
    END IF;
    IF OLD.intent_id LIKE 'shard-v2-%' OR OLD.intent_id LIKE 'xorb-v2-%' THEN
        RETURN NULL;
    END IF;
    -- Pin the trigger table's schema without interpolating catalog text into
    -- this dollar-quoted body or trusting a caller's search_path.
    EXECUTE format(
        'SELECT event_json -> ''operation'' ->> ''repository''
         FROM %I.shardline_reliability_events
         WHERE operation_kind = ''Upload'' AND operation_id = $1
         ORDER BY sequence DESC LIMIT 1', TG_TABLE_SCHEMA)
    INTO repository USING OLD.intent_id;
    IF OLD.intent_id LIKE 'file-v2-%' OR repository LIKE 'scope-json-v1:%' THEN
        -- A zero-row UPDATE makes the released atomic transition roll back
        -- before it appends an event. Startup can continue without false evidence.
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS shardline_upload_recovery_guard_v2 ON @SCHEMA@.shardline_upload_intents;
CREATE TRIGGER shardline_upload_recovery_guard_v2
BEFORE UPDATE OF state ON @SCHEMA@.shardline_upload_intents
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_upload_recovery_guard_v2();
