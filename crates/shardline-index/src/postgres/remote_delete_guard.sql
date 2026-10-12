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

-- OCI keys use the adapter's SHA-256 repository identifier. Native publication
-- and legacy tag/tombstone SQL writers share the same physical-key boundary.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_oci_publication_references_remote_delete_v1(
    index_schema TEXT, identities JSONB
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $oci_payload_references$
DECLARE keys TEXT[]; lock_slot INTEGER; pending BOOLEAN;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE') OR NOT (
        has_table_privilege(session_user, format('%I.shardline_oci_object_tombstones', index_schema), 'DELETE')
        OR has_any_column_privilege(session_user, format('%I.shardline_oci_tags', index_schema), 'INSERT,UPDATE')
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'OCI publication access is required for reference checks';
    END IF;
    SELECT array_agg(DISTINCT physical) INTO keys FROM (
        SELECT 'protocols/oci/' || scope_namespace || '/repos/' ||
            encode(sha256(convert_to(repository, 'UTF8')), 'hex') || '/' || part || '/' || digest_hex AS physical
        FROM jsonb_to_recordset(identities) AS identity(
            scope_namespace TEXT, repository TEXT, object_kind TEXT, digest_hex TEXT)
        CROSS JOIN LATERAL unnest(CASE object_kind
            WHEN 'blob' THEN ARRAY['blobs']
            WHEN 'manifest' THEN ARRAY['manifests', 'manifest-media-types']
            ELSE ARRAY[]::TEXT[] END) part
        WHERE digest_hex ~ '^[0-9a-f]{64}$'
          AND (scope_namespace='global' OR scope_namespace ~ '^[0-9a-f]{64}$')
    ) physical_references;
    FOR lock_slot IN SELECT DISTINCT
        (hashtextextended('shardline:remote-delete:' || physical, 0) & 255)::INTEGER AS slot
        FROM unnest(keys) physical ORDER BY slot LOOP
        PERFORM pg_advisory_xact_lock(1397507140, lock_slot);
    END LOOP;
    SELECT EXISTS(SELECT 1 FROM shardline_resource_fences
        WHERE domain='maintenance-remote-delete' AND resource=ANY(keys)) INTO pending;
    IF pending THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD01', MESSAGE = 'object has an unresolved remote delete; retry after reconciliation';
    END IF;
END;
$oci_payload_references$;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_oci_publication_references_remote_delete_v1(TEXT,JSONB) TO PUBLIC;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_oci_publication_remote_delete_v1(
    index_schema TEXT, scope_namespace TEXT, repository TEXT, object_kind TEXT, digest_hex TEXT
) RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $oci_publication$
DECLARE base TEXT; keys TEXT[]; lock_slot INTEGER; pending BOOLEAN;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE') OR NOT (
        has_table_privilege(session_user, format('%I.shardline_oci_object_tombstones', index_schema), 'DELETE')
        OR has_any_column_privilege(session_user, format('%I.shardline_oci_tags', index_schema), 'INSERT,UPDATE')
    ) THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'OCI publication access is required for reference checks';
    END IF;
    -- Preserve inspection/repair of malformed legacy metadata. These values
    -- cannot be reclaimed through the validated OCI physical-key adapter.
    IF digest_hex !~ '^[0-9a-f]{64}$'
       OR (scope_namespace <> 'global' AND scope_namespace !~ '^[0-9a-f]{64}$') THEN
        RETURN;
    END IF;
    base := 'protocols/oci/' || scope_namespace || '/repos/' || encode(sha256(convert_to(repository, 'UTF8')), 'hex') || '/';
    IF object_kind = 'blob' THEN
        keys := ARRAY[base || 'blobs/' || digest_hex];
    ELSIF object_kind = 'manifest' THEN
        keys := ARRAY[base || 'manifests/' || digest_hex, base || 'manifest-media-types/' || digest_hex];
    ELSE
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'unsupported OCI publication kind';
    END IF;
    FOR lock_slot IN SELECT DISTINCT
        (hashtextextended('shardline:remote-delete:' || physical, 0) & 255)::INTEGER AS slot
        FROM unnest(keys) physical ORDER BY slot LOOP
        PERFORM pg_advisory_xact_lock(1397507140, lock_slot);
    END LOOP;
    SELECT EXISTS(SELECT 1 FROM shardline_resource_fences
        WHERE domain='maintenance-remote-delete' AND resource=ANY(keys)) INTO pending;
    IF pending THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD01', MESSAGE = 'object has an unresolved remote delete; retry after reconciliation';
    END IF;
END;
$oci_publication$;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_oci_publication_remote_delete_v1(TEXT,TEXT,TEXT,TEXT,TEXT) TO PUBLIC;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_oci_reference_remote_delete_v1()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $oci_reference$
BEGIN
    IF TG_TABLE_SCHEMA IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete guard schema mismatch';
    END IF;
    IF TG_TABLE_NAME = 'shardline_oci_tags' THEN
        PERFORM shardline_oci_publication_remote_delete_v1(TG_TABLE_SCHEMA,
            NEW.scope_namespace, NEW.repository, 'manifest', NEW.digest_hex);
        RETURN NEW;
    ELSIF TG_TABLE_NAME = 'shardline_oci_object_tombstones' THEN
        PERFORM shardline_oci_publication_remote_delete_v1(TG_TABLE_SCHEMA,
            OLD.scope_namespace, OLD.repository, OLD.object_kind, OLD.digest_hex);
        RETURN OLD;
    END IF;
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'unsupported OCI publication table';
END;
$oci_reference$;

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

DROP TRIGGER IF EXISTS shardline_oci_reference_remote_delete_v1 ON @SCHEMA@.shardline_oci_tags;
CREATE TRIGGER shardline_oci_reference_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_oci_tags
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_oci_reference_remote_delete_v1();
DROP TRIGGER IF EXISTS shardline_oci_reference_remote_delete_v1 ON @SCHEMA@.shardline_oci_object_tombstones;
CREATE TRIGGER shardline_oci_reference_remote_delete_v1
BEFORE DELETE ON @SCHEMA@.shardline_oci_object_tombstones
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_oci_reference_remote_delete_v1();
DROP TRIGGER IF EXISTS shardline_reference_remote_delete_v1 ON @SCHEMA@.shardline_file_reconstructions;
CREATE TRIGGER shardline_reference_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_file_reconstructions
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_reference_remote_delete_v1();
DROP TRIGGER IF EXISTS shardline_reference_remote_delete_v1 ON @SCHEMA@.shardline_stored_objects;
CREATE TRIGGER shardline_reference_remote_delete_v1
BEFORE INSERT OR UPDATE ON @SCHEMA@.shardline_stored_objects
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_reference_remote_delete_v1();

-- Materialize verified hidden manifest state into the existing OCI inventory.
-- This is a candidate boundary: Rust still verifies the complete evidence head
-- before GC issues any physical deletion. Malformed legacy events are ignored.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_oci_staged_identity_v1(
    encoded_event TEXT, stored_operation_id TEXT
) RETURNS TABLE(scope_namespace TEXT, repository TEXT, digest_hex TEXT, deleted_at_unix_seconds BIGINT)
LANGUAGE plpgsql IMMUTABLE SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $staged_identity$
DECLARE payload JSONB; epoch_text TEXT;
BEGIN
    BEGIN
        payload := encoded_event::JSONB;
    EXCEPTION WHEN invalid_text_representation THEN RETURN;
    END;
    IF payload #>> '{after,state}' IS DISTINCT FROM 'Deleted'
       OR payload #>> '{after,identity,object_kind}' IS DISTINCT FROM 'manifest' THEN RETURN; END IF;
    scope_namespace := payload #>> '{after,identity,scope_namespace}';
    repository := payload #>> '{after,identity,repository}';
    digest_hex := payload #>> '{after,identity,digest_hex}';
    epoch_text := payload #>> '{after,deleted_at_unix_seconds}';
    IF scope_namespace IS NULL OR repository IS NULL OR digest_hex IS NULL OR epoch_text IS NULL
       OR (scope_namespace <> 'global' AND scope_namespace !~ '^[0-9a-f]{64}$')
       OR digest_hex !~ '^[0-9a-f]{64}$' OR epoch_text !~ '^[0-9]{1,18}$'
       OR stored_operation_id IS DISTINCT FROM scope_namespace || ':' || repository || ':manifest:' || digest_hex THEN RETURN; END IF;
    deleted_at_unix_seconds := epoch_text::BIGINT;
    RETURN NEXT;
END;
$staged_identity$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_oci_staged_tombstone_v1()
RETURNS TRIGGER
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $staged_tombstone$
BEGIN
    IF TG_TABLE_SCHEMA IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'OCI staging schema mismatch';
    END IF;
    IF NEW.operation_kind <> 'Visibility'
       OR NEW.operation_id !~ '^(global|[0-9a-f]{64}):.+:manifest:[0-9a-f]{64}$' THEN RETURN NEW; END IF;
    IF NOT has_schema_privilege(session_user, TG_TABLE_SCHEMA, 'USAGE')
       OR NOT has_table_privilege(session_user,
           format('%I.shardline_oci_object_tombstones', TG_TABLE_SCHEMA), 'DELETE') THEN RETURN NEW; END IF;
    -- Do not broaden row-scoped writers' permissions through definer access.
    IF EXISTS(SELECT 1 FROM pg_class target JOIN pg_roles actor ON actor.rolname=session_user
        WHERE target.oid IN ('shardline_oci_object_tombstones'::regclass, 'shardline_reliability_events'::regclass)
          AND target.relrowsecurity AND NOT (actor.rolsuper OR actor.rolbypassrls
              OR (target.relowner=actor.oid AND NOT target.relforcerowsecurity))) THEN RETURN NEW; END IF;
    -- Replaying an older checkpoint cannot hide a later published generation.
    IF EXISTS(SELECT 1 FROM shardline_reliability_events
        WHERE operation_kind=NEW.operation_kind AND operation_id=NEW.operation_id
          AND sequence>NEW.sequence) THEN RETURN NEW; END IF;
    INSERT INTO shardline_oci_object_tombstones
        (scope_namespace, repository, object_kind, digest_hex, deleted_at_unix_seconds)
    SELECT identity.scope_namespace, identity.repository, 'manifest', identity.digest_hex, identity.deleted_at_unix_seconds
    FROM shardline_oci_staged_identity_v1(NEW.event_json::TEXT, NEW.operation_id) identity
    ON CONFLICT (scope_namespace, repository, object_kind, digest_hex) DO NOTHING;
    RETURN NEW;
END;
$staged_tombstone$;

DROP TRIGGER IF EXISTS shardline_oci_staged_tombstone_v1 ON @SCHEMA@.shardline_reliability_events;
CREATE TRIGGER shardline_oci_staged_tombstone_v1
AFTER INSERT ON @SCHEMA@.shardline_reliability_events
FOR EACH ROW EXECUTE FUNCTION @SCHEMA@.shardline_oci_staged_tombstone_v1();

-- Ordinary GC can discover journal-only stages without asking a publication
-- account to bypass its row policies or acquire tombstone INSERT. Inventory is
-- only a candidate: native GC still verifies the journal before deleting bytes.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_oci_staged_gc_inventory_v1(
    index_schema TEXT, candidate_limit INTEGER
) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $staged_gc_inventory$
DECLARE changed BIGINT;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='remote delete guard schema mismatch';
    END IF;
    IF candidate_limit IS NULL OR candidate_limit NOT BETWEEN 1 AND 64 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid staged manifest inventory limit';
    END IF;
    IF NOT has_schema_privilege(session_user,index_schema,'USAGE')
       OR NOT has_table_privilege(session_user,format('%I.shardline_oci_object_tombstones',index_schema),'SELECT')
       OR NOT has_table_privilege(session_user,format('%I.shardline_oci_object_tombstones',index_schema),'DELETE')
       OR NOT has_table_privilege(session_user,format('%I.shardline_reliability_events',index_schema),'SELECT') THEN
        RETURN -1; -- Preserve existing GC accounts without OCI mutation grants.
    END IF;
    -- Require the caller's own exclusive maintenance session. A separate pool
    -- must not invent a candidate concurrently with a manifest publication.
    IF NOT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory'
       AND pid=pg_backend_pid() AND granted AND mode='ExclusiveLock' AND objsubid=1
       AND classid::bigint=(x'5348415244474301'::bit(64)::bigint >> 32)
       AND objid::bigint=(x'5348415244474301'::bit(64)::bigint & 4294967295)) THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='staged inventory requires the GC lock-owning session';
    END IF;
    IF EXISTS(SELECT 1 FROM pg_class target JOIN pg_roles actor ON actor.rolname=session_user
        WHERE target.oid IN ('shardline_oci_object_tombstones'::regclass,'shardline_reliability_events'::regclass)
          AND target.relrowsecurity AND NOT (actor.rolsuper OR actor.rolbypassrls
              OR (target.relowner=actor.oid AND NOT target.relforcerowsecurity))) THEN
        RETURN -1; -- Do not broaden this GC account's row visibility either.
    END IF;
    INSERT INTO shardline_oci_object_tombstones
        (scope_namespace,repository,object_kind,digest_hex,deleted_at_unix_seconds)
    SELECT identity.scope_namespace,identity.repository,'manifest',identity.digest_hex,identity.deleted_at_unix_seconds
    FROM (SELECT DISTINCT ON(operation_id) operation_id,event_json
          FROM shardline_reliability_events WHERE operation_kind='Visibility'
            AND operation_id ~ '^(global|[0-9a-f]{64}):.+:manifest:[0-9a-f]{64}$'
          ORDER BY operation_id,sequence DESC) head
    CROSS JOIN LATERAL shardline_oci_staged_identity_v1(head.event_json::TEXT,head.operation_id) identity
    WHERE NOT EXISTS(SELECT 1 FROM shardline_oci_object_tombstones existing
        WHERE existing.scope_namespace=identity.scope_namespace AND existing.repository=identity.repository
          AND existing.object_kind='manifest' AND existing.digest_hex=identity.digest_hex)
    ORDER BY head.operation_id LIMIT candidate_limit
    ON CONFLICT(scope_namespace,repository,object_kind,digest_hex) DO NOTHING;
    GET DIAGNOSTICS changed=ROW_COUNT;
    RETURN changed;
END;
$staged_gc_inventory$;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_oci_staged_gc_inventory_v1(TEXT,INTEGER) TO PUBLIC;

-- Trigger installation excludes concurrent event inserts until commit. OCI
-- publication tables are already locked by the guards installed above. Restore
-- prior journal-only stages without replacing any existing deletion generation.
DO $materialize_stages$
BEGIN
IF NOT pg_catalog.row_security_active(pg_catalog.to_regclass(pg_catalog.format('%I.shardline_oci_object_tombstones', @SCHEMA_LITERAL@)))
   AND NOT pg_catalog.row_security_active(pg_catalog.to_regclass(pg_catalog.format('%I.shardline_reliability_events', @SCHEMA_LITERAL@))) THEN
INSERT INTO @SCHEMA@.shardline_oci_object_tombstones
    (scope_namespace, repository, object_kind, digest_hex, deleted_at_unix_seconds)
SELECT identity.scope_namespace, identity.repository, 'manifest', identity.digest_hex, identity.deleted_at_unix_seconds
FROM (SELECT DISTINCT ON (operation_id) operation_id, event_json
      FROM @SCHEMA@.shardline_reliability_events WHERE operation_kind='Visibility'
        AND operation_id ~ '^(global|[0-9a-f]{64}):.+:manifest:[0-9a-f]{64}$'
      ORDER BY operation_id, sequence DESC) head
CROSS JOIN LATERAL @SCHEMA@.shardline_oci_staged_identity_v1(head.event_json::TEXT, head.operation_id) identity
ON CONFLICT (scope_namespace, repository, object_kind, digest_hex) DO NOTHING;
END IF;
END;
$materialize_stages$;
