-- Compatibility extension: the released migration history and checksums stay
-- unchanged. Install atomically with the retention trigger in db migrate up.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_guard_v1(
    index_schema TEXT, object_key TEXT
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
AS $guard$
DECLARE pending BOOLEAN;
BEGIN
    -- Grant a boolean safety check, not direct access to other schemas' fences.
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_retention_holds', index_schema), 'SELECT') THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'retention metadata access is required for the remote delete guard';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('shardline:remote-delete:' || object_key, 0));
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I.shardline_resource_fences WHERE domain=$1 AND resource=$2)',
        index_schema
    ) INTO pending USING 'maintenance-remote-delete', object_key;
    IF pending THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD01',
            MESSAGE = 'object has an unresolved remote delete; retry after reconciliation';
    END IF;
END;
$guard$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_retention_remote_delete_v1()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $retention$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM shardline_remote_delete_guard_v1(TG_TABLE_SCHEMA, OLD.object_key);
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE' THEN
        IF OLD.object_key IS DISTINCT FROM NEW.object_key THEN
            PERFORM shardline_remote_delete_guard_v1(TG_TABLE_SCHEMA, OLD.object_key);
        END IF;
    END IF;
    PERFORM shardline_remote_delete_guard_v1(TG_TABLE_SCHEMA, NEW.object_key);
    RETURN NEW;
END;
$retention$;

-- This read-only check validates the caller's existing schema/retention access.
-- Keep it usable even with restrictive default function privileges.
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_guard_v1(TEXT, TEXT) TO PUBLIC;

DROP TRIGGER IF EXISTS shardline_retention_remote_delete_v1 ON @SCHEMA@.shardline_retention_holds;
CREATE TRIGGER shardline_retention_remote_delete_v1
BEFORE INSERT OR UPDATE OR DELETE ON @SCHEMA@.shardline_retention_holds
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_retention_remote_delete_v1();
