-- Prevent an older writer from changing a resumable-session lifecycle state
-- after a newer writer has established the reliability protocol. The trigger
-- is deferred because the current writer updates the materialized row and
-- appends its evidence in the same transaction.
CREATE OR REPLACE FUNCTION shardline_require_resumable_session_state_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    observed_after TEXT;
BEGIN
    IF NEW.state IS DISTINCT FROM OLD.state THEN
        SELECT event_json->>'after' INTO observed_after
        FROM shardline_reliability_events
        WHERE operation_kind = 'ResumableSession'
          AND operation_id = NEW.session_id
        ORDER BY sequence DESC
        LIMIT 1;
        IF observed_after IS NULL OR observed_after IS DISTINCT FROM NEW.state THEN
            RAISE EXCEPTION 'resumable session state mutation committed without matching reliability evidence'
                USING ERRCODE = '23514';
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER shardline_resumable_session_reliability_gate
AFTER UPDATE OF state ON shardline_resumable_sessions
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_resumable_session_state_evidence();
