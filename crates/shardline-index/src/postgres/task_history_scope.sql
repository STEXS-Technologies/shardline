-- A stable namespace identity distinguishes independently initialized metadata
-- stores behind the same proxy. One row survives owner-run upgrades and restores.
CREATE UNIQUE INDEX IF NOT EXISTS shardline_task_history_scope_v1
ON @SCHEMA@.shardline_resource_fences(domain)
WHERE domain='maintenance-task-history-scope';

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_history_scope_v1(index_schema TEXT)
RETURNS TEXT LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, pg_temp
SET shardline.task_history_schema TO @SCHEMA_LITERAL@
AS $history_scope$
DECLARE identity TEXT;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.task_history_schema')
       OR NOT has_schema_privilege(session_user, index_schema, 'USAGE') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='task history schema access is required';
    END IF;
    -- A common physical schema does not establish a common tenant visibility.
    -- Keep legacy role scopes when metadata uses row-level security.
    IF EXISTS(SELECT 1 FROM pg_class table_entry
              JOIN pg_namespace namespace ON namespace.oid=table_entry.relnamespace
              WHERE namespace.nspname=index_schema AND table_entry.relrowsecurity
                AND table_entry.relname LIKE 'shardline_%') THEN
        RAISE EXCEPTION USING ERRCODE='SLD04', MESSAGE='row-scoped task history grouping is unsupported';
    END IF;
    EXECUTE format(
        'SELECT resource FROM %I.shardline_resource_fences
         WHERE domain=''maintenance-task-history-scope'' AND epoch=1', index_schema
    ) INTO identity;
    IF identity IS NULL OR identity !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE='SLD04', MESSAGE='task history scope requires owner initialization';
    END IF;
    RETURN identity;
END;
$history_scope$;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_history_scope_v1(TEXT) TO PUBLIC;
