-- One public verifier per pending attempt; no private key is stored here.
CREATE UNIQUE INDEX IF NOT EXISTS shardline_remote_delete_receipt_epoch_v1
ON @SCHEMA@.shardline_resource_fences(domain, epoch)
WHERE domain = 'maintenance-remote-delete-receipt-key';

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_receipt_namespace_v1(
    index_schema TEXT
) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $receipt_namespace$
DECLARE namespace TEXT;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema')
       OR NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user, format('%I.shardline_retention_holds', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user, format('%I.shardline_quarantine_candidates', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user, format('%I.shardline_quarantine_candidates', index_schema), 'DELETE') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'garbage collection metadata access is required for receipt namespace';
    END IF;
    EXECUTE format('SELECT resource FROM %I.shardline_resource_fences WHERE domain=$1 AND epoch=1', index_schema)
      INTO namespace USING 'maintenance-task-history-scope';
    IF namespace IS NULL OR namespace !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD04', MESSAGE = 'remote delete receipt namespace requires owner initialization';
    END IF;
    RETURN namespace;
END;
$receipt_namespace$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_receipt_pending_v1(
    index_schema TEXT, object_key TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $receipt_pending$
DECLARE pending BOOLEAN;
BEGIN
    PERFORM shardline_remote_delete_receipt_namespace_v1(index_schema);
    EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.shardline_resource_fences WHERE domain=$1 AND resource=$2)', index_schema)
      INTO pending USING 'maintenance-remote-delete', object_key;
    RETURN pending;
END;
$receipt_pending$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_receipt_begin_v1(
    index_schema TEXT, object_key TEXT, verification_key TEXT
) RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $receipt_begin$
DECLARE admitted_epoch BIGINT;
BEGIN
    IF verification_key IS NULL OR verification_key !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'invalid remote delete receipt key';
    END IF;
    admitted_epoch := shardline_remote_delete_begin_v1(index_schema, object_key);
    EXECUTE format('INSERT INTO %I.shardline_resource_fences(domain, resource, epoch) VALUES ($1,$2,$3)', index_schema)
      USING 'maintenance-remote-delete-receipt-key', verification_key, admitted_epoch;
    RETURN admitted_epoch;
END;
$receipt_begin$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_receipt_key_v1(
    index_schema TEXT, object_key TEXT, expected_epoch BIGINT
) RETURNS TEXT
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $receipt_key$
DECLARE verification_key TEXT;
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.remote_delete_schema') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'remote delete receipt schema mismatch';
    END IF;
    IF NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user, format('%I.shardline_retention_holds', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user, format('%I.shardline_quarantine_candidates', index_schema), 'SELECT')
       OR NOT has_table_privilege(session_user, format('%I.shardline_quarantine_candidates', index_schema), 'DELETE') THEN
        RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'garbage collection metadata access is required for receipt lookup';
    END IF;
    EXECUTE format('SELECT verifier.resource FROM %I.shardline_resource_fences verifier
        WHERE verifier.domain=$1 AND verifier.epoch=$2
          AND EXISTS(SELECT 1 FROM %I.shardline_resource_fences pending
              WHERE pending.domain=$3 AND pending.resource=$4 AND pending.epoch=$2)', index_schema, index_schema)
      INTO verification_key USING 'maintenance-remote-delete-receipt-key', expected_epoch,
        'maintenance-remote-delete', object_key;
    IF verification_key IS NOT NULL AND verification_key !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION USING ERRCODE = 'SLD04', MESSAGE = 'invalid pinned remote delete receipt key';
    END IF;
    RETURN verification_key;
END;
$receipt_key$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_remote_delete_receipt_finish_v1(
    index_schema TEXT, object_key TEXT, expected_epoch BIGINT, verification_key TEXT
) RETURNS BOOLEAN
LANGUAGE plpgsql SECURITY DEFINER SET search_path TO pg_catalog, @SCHEMA@, pg_temp
SET shardline.remote_delete_schema TO @SCHEMA_LITERAL@
AS $receipt_finish$
BEGIN
    PERFORM shardline_remote_delete_receipt_key_v1(index_schema, object_key, expected_epoch);
    PERFORM pg_advisory_xact_lock(hashtextextended('shardline:remote-delete:' || object_key, 0));
    PERFORM pg_advisory_xact_lock(1397507140,
        (hashtextextended('shardline:remote-delete:' || object_key, 0) & 255)::INTEGER);
    IF verification_key IS NULL OR verification_key IS DISTINCT FROM
        shardline_remote_delete_receipt_key_v1(index_schema, object_key, expected_epoch) THEN
        RETURN FALSE;
    END IF;
    RETURN shardline_remote_delete_finish_v1(index_schema, object_key, expected_epoch);
END;
$receipt_finish$;

GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_receipt_begin_v1(TEXT, TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_receipt_namespace_v1(TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_receipt_pending_v1(TEXT, TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_receipt_key_v1(TEXT, TEXT, BIGINT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_remote_delete_receipt_finish_v1(TEXT, TEXT, BIGINT, TEXT) TO PUBLIC;
