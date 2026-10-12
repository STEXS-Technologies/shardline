-- Compatibility extension: the released migration history and checksums stay
-- unchanged. Install atomically with the retention trigger in db migrate up.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_guard_v1(
    index_schema TEXT, object_key TEXT
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $guard$
DECLARE pending BOOLEAN;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    -- Grant a boolean safety check, not direct access to other schemas' fences.
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_retention_holds', index_schema), 'SELECT') THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'retention metadata access is required for the remote delete guard';
    END IF;
    -- Retain the original single-key lock for in-flight retention writers.
    -- The bounded slot also arbitrates large reference publications.
    PERFORM pg_advisory_xact_lock(hashtextextended('shardline:remote-delete:' || object_key, 0));
    PERFORM pg_advisory_xact_lock(1397507140,
        (hashtextextended('shardline:remote-delete:' || object_key, 0) & 255)::INTEGER);
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

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_begin_v1(
    index_schema TEXT, object_key TEXT
) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $begin_delete$
DECLARE previous_epoch BIGINT; next_epoch BIGINT;
BEGIN
    -- The read guard validates the installed schema and the caller's existing
    -- retention access, takes the object lock, and rejects an earlier marker.
    PERFORM shardline_remote_delete_guard_v1(index_schema, object_key);
    IF NOT has_table_privilege(session_user,
           format('%I.shardline_quarantine_candidates', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_quarantine_candidates', index_schema), 'DELETE') THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'garbage collection metadata access is required for remote delete admission';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('shardline:remote-delete-sequence', 0));
    EXECUTE format(
        'SELECT epoch FROM %I.shardline_resource_fences WHERE domain=$1 AND resource=$2 FOR UPDATE',
        index_schema
    ) INTO previous_epoch USING 'maintenance-remote-delete-sequence', 'global';
    IF previous_epoch = 9223372036854775807 THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD02', MESSAGE = 'remote delete epoch exhausted';
    END IF;
    next_epoch := COALESCE(previous_epoch, 0) + 1;
    EXECUTE format(
        'INSERT INTO %I.shardline_resource_fences(domain,resource,epoch) VALUES($1,$2,$3)
         ON CONFLICT(domain,resource) DO UPDATE SET epoch=EXCLUDED.epoch', index_schema
    ) USING 'maintenance-remote-delete-sequence', 'global', next_epoch;
    EXECUTE format(
        'INSERT INTO %I.shardline_resource_fences(domain,resource,epoch) VALUES($1,$2,$3)', index_schema
    ) USING 'maintenance-remote-delete', object_key, next_epoch;
    RETURN next_epoch;
END;
$begin_delete$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_finish_v1(
    index_schema TEXT, object_key TEXT, expected_epoch BIGINT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $finish_delete$
DECLARE changed BIGINT;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_retention_holds', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_quarantine_candidates', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_quarantine_candidates', index_schema), 'DELETE') THEN
        RAISE EXCEPTION USING ERRCODE = '42501',
            MESSAGE = 'garbage collection metadata access is required for remote delete settlement';
    END IF;
    PERFORM pg_advisory_xact_lock(hashtextextended('shardline:remote-delete:' || object_key, 0));
    PERFORM pg_advisory_xact_lock(1397507140,
        (hashtextextended('shardline:remote-delete:' || object_key, 0) & 255)::INTEGER);
    EXECUTE format(
        'DELETE FROM %I.shardline_resource_fences WHERE domain=$1 AND resource=$2 AND epoch=$3', index_schema
    ) USING 'maintenance-remote-delete', object_key, expected_epoch;
    GET DIAGNOSTICS changed = ROW_COUNT;
    IF changed = 1 THEN
        EXECUTE format('DELETE FROM %I.shardline_resource_fences WHERE domain=$1 AND epoch=$2', index_schema)
          USING 'maintenance-remote-delete-receipt-key', expected_epoch;
    END IF;
    RETURN changed = 1;
END;
$finish_delete$;

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

-- A dedupe mapping publishes a physical shard key and identifies a chunk.
-- Protect both keys at the table itself
-- so released writers also participate, without requiring new fence grants
-- or retention-table privileges from an existing mapping writer.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_dedupe_remote_delete_v1()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $dedupe$
DECLARE pending BOOLEAN; keys TEXT[]; lock_slot INTEGER;
BEGIN
    IF TG_TABLE_SCHEMA IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    keys := ARRAY[NEW.shard_object_key];
    -- Preserve inspection/repair of legacy malformed hashes; only canonical
    -- hashes identify a physical chunk in the existing object-key grammar.
    IF NEW.chunk_hash ~ '^[0-9a-f]{64}$' THEN
        keys := array_append(keys, left(NEW.chunk_hash, 2) || '/' || NEW.chunk_hash);
    END IF;
    FOR lock_slot IN SELECT DISTINCT
        (hashtextextended('shardline:remote-delete:' || physical, 0) & 255)::INTEGER AS slot
        FROM unnest(keys) physical ORDER BY slot LOOP
        PERFORM pg_advisory_xact_lock(1397507140, lock_slot);
    END LOOP;
    EXECUTE format(
        'SELECT EXISTS(SELECT 1 FROM %I.shardline_resource_fences WHERE domain=$1 AND resource=ANY($2))',
        TG_TABLE_SCHEMA
    ) INTO pending USING 'maintenance-remote-delete', keys;
    IF pending THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD01',
            MESSAGE = 'object has an unresolved remote delete; retry after reconciliation';
    END IF;
    RETURN NEW;
END;
$dedupe$;

-- Direct references are checked at the SQL publication boundary, including
-- legacy writers. Do not add JSON validation here: corrupt-row inspection and
-- released record defaults retain their existing contract.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_reference_remote_delete_v1()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $references$
DECLARE hashes TEXT[]; keys TEXT[]; lock_slot INTEGER; pending BOOLEAN;
BEGIN
    IF TG_TABLE_SCHEMA IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF TG_TABLE_NAME = 'shardline_file_records' THEN
        SELECT array_agg(DISTINCT term->>'hash') INTO hashes
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(NEW.record->'chunks') = 'array'
             THEN NEW.record->'chunks' ELSE '[]'::jsonb END) term
        WHERE term->>'hash' ~ '^[0-9a-f]{64}$';
        IF NEW.record->>'chunk_size' IS DISTINCT FROM '0' THEN
            SELECT array_agg(physical) INTO keys FROM (
                SELECT left(hash, 2) || '/' || hash AS physical FROM unnest(hashes) hash
                UNION
                SELECT mapping.shard_object_key FROM shardline_dedupe_shards mapping
                WHERE mapping.chunk_hash = ANY(hashes)
            ) direct_chunks;
        END IF;
    ELSIF TG_TABLE_NAME = 'shardline_file_reconstructions' THEN
        SELECT array_agg(DISTINCT term->>'object_hash') INTO hashes
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(NEW.terms->'terms') = 'array'
             THEN NEW.terms->'terms' ELSE '[]'::jsonb END) term
        WHERE term->>'object_hash' ~ '^[0-9a-f]{64}$';
    ELSIF TG_TABLE_NAME = 'shardline_stored_objects' THEN
        IF NEW.object_hash ~ '^[0-9a-f]{64}$' THEN
            hashes := ARRAY[NEW.object_hash];
        END IF;
    ELSE
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'unsupported reference publication table';
    END IF;
    -- Xet containers and their GC-managed chunk-cache sidecars use these same
    -- namespaces. Other enabled frontends cannot make a pending key safe.
    SELECT array_agg(DISTINCT physical ORDER BY physical) INTO keys FROM (
        SELECT unnest(keys) AS physical
        UNION
        SELECT 'xorbs/default/' || left(hash, 2) || '/' || hash || '.xorb'
        FROM unnest(hashes) hash
        UNION
        SELECT '_xorb_chunks/' || left(hash, 2) || '/' || hash
        FROM unnest(hashes) hash
    ) references_to_check;
    -- Bound lock-table usage independently of the number of chunks. Colliding
    -- keys serialize briefly; the pending lookup still checks the exact key.
    -- Acquire slots in a stable order within each publication.
    FOR lock_slot IN SELECT DISTINCT
        (hashtextextended('shardline:remote-delete:' || physical, 0) & 255)::INTEGER AS slot
        FROM unnest(keys) physical ORDER BY slot LOOP
        PERFORM pg_advisory_xact_lock(1397507140, lock_slot);
    END LOOP;
    SELECT EXISTS(SELECT 1 FROM shardline_resource_fences
                  WHERE domain='maintenance-remote-delete' AND resource=ANY(keys))
    INTO pending;
    IF pending THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD01',
            MESSAGE = 'object has an unresolved remote delete; retry after reconciliation';
    END IF;
    RETURN NEW;
END;
$references$;

-- Payload-derived references are supplied by the native xorb validator and
-- checked inside the metadata publication transaction, never as a preflight.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_reference_keys_remote_delete_v1(
    index_schema TEXT, object_keys TEXT[]
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $member_references$
DECLARE keys TEXT[]; lock_slot INTEGER; pending BOOLEAN;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE') OR NOT (
        has_column_privilege(session_user, format('%I.shardline_file_records', index_schema), 'record', 'INSERT')
        OR has_column_privilege(session_user, format('%I.shardline_file_records', index_schema), 'record', 'UPDATE')
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'file publication access is required for reference checks';
    END IF;
    SELECT array_agg(DISTINCT physical) INTO keys FROM (
        SELECT unnest(object_keys) AS physical
        UNION
        SELECT mapping.shard_object_key FROM shardline_dedupe_shards mapping
        JOIN unnest(object_keys) member_key
          ON member_key ~ '^[0-9a-f]{2}/[0-9a-f]{64}$'
         AND mapping.chunk_hash = right(member_key, 64)
    ) references_to_check;
    FOR lock_slot IN SELECT DISTINCT
        (hashtextextended('shardline:remote-delete:' || physical, 0) & 255)::INTEGER AS slot
        FROM unnest(keys) physical ORDER BY slot LOOP
        PERFORM pg_advisory_xact_lock(1397507140, lock_slot);
    END LOOP;
    SELECT EXISTS(SELECT 1 FROM shardline_resource_fences
                  WHERE domain='maintenance-remote-delete' AND resource=ANY(keys)) INTO pending;
    IF pending THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD01',
            MESSAGE = 'object has an unresolved remote delete; retry after reconciliation';
    END IF;
END;
$member_references$;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_reference_keys_remote_delete_v1(TEXT, TEXT[]) TO PUBLIC;

-- Inspection never settles an attempt. Epochs are globally unique and never
-- reused by admission, giving a compact cursor even for long object keys.
CREATE INDEX IF NOT EXISTS shardline_pending_remote_deletes_v1
ON @SCHEMA@.shardline_resource_fences(epoch)
WHERE domain='maintenance-remote-delete';
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_pending_remote_deletes_v1(
    index_schema TEXT, after_epoch BIGINT, page_limit INTEGER
) RETURNS TABLE(object_key TEXT, attempt_epoch BIGINT)
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $inspect_deletes$
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_retention_holds', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_quarantine_candidates', index_schema), 'SELECT') THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD03', MESSAGE = 'maintenance metadata read access is required for inspection';
    END IF;
    IF after_epoch < 0 OR after_epoch IS NULL OR page_limit IS NULL OR page_limit NOT BETWEEN 1 AND 101 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid pending delete page';
    END IF;
    -- Do not broaden a tenant role's row visibility through definer access.
    IF EXISTS(SELECT 1 FROM pg_class WHERE relrowsecurity AND oid IN (
        to_regclass(format('%I.shardline_retention_holds', index_schema)),
        to_regclass(format('%I.shardline_quarantine_candidates', index_schema)),
        to_regclass(format('%I.shardline_resource_fences', index_schema))
    )) THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD03', MESSAGE = 'row-scoped pending delete inspection is unsupported';
    END IF;
    RETURN QUERY EXECUTE format(
        'SELECT resource, epoch FROM %I.shardline_resource_fences
         WHERE domain=''maintenance-remote-delete'' AND epoch > $1
         ORDER BY epoch LIMIT $2', index_schema
    ) USING after_epoch, page_limit;
END;
$inspect_deletes$;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_pending_remote_deletes_v1(TEXT, BIGINT, INTEGER) TO PUBLIC;

-- This read-only check validates the caller's existing schema/retention access.
-- Keep it usable even with restrictive default function privileges.
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_guard_v1(TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_begin_v1(TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_finish_v1(TEXT, TEXT, BIGINT) TO PUBLIC;

DROP TRIGGER IF EXISTS shardline_retention_remote_delete_v1 ON @SCHEMA@.shardline_retention_holds;
CREATE TRIGGER shardline_retention_remote_delete_v1
BEFORE INSERT OR UPDATE OR DELETE ON @SCHEMA@.shardline_retention_holds
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_retention_remote_delete_v1();

DROP TRIGGER IF EXISTS shardline_dedupe_remote_delete_v1 ON @SCHEMA@.shardline_dedupe_shards;
CREATE TRIGGER shardline_dedupe_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_dedupe_shards
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_dedupe_remote_delete_v1();

DROP TRIGGER IF EXISTS shardline_reference_remote_delete_v1 ON @SCHEMA@.shardline_file_records;
CREATE TRIGGER shardline_reference_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_file_records
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_reference_remote_delete_v1();
DROP TRIGGER IF EXISTS shardline_reference_remote_delete_v1 ON @SCHEMA@.shardline_file_reconstructions;
CREATE TRIGGER shardline_reference_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_file_reconstructions
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_reference_remote_delete_v1();
DROP TRIGGER IF EXISTS shardline_reference_remote_delete_v1 ON @SCHEMA@.shardline_stored_objects;
CREATE TRIGGER shardline_reference_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_stored_objects
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_reference_remote_delete_v1();
