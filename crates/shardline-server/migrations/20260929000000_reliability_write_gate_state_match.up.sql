-- Upgrade pre-release write gates that only required an evidence row to
-- exist. Existing installations must also bind the latest evidence `after`
-- snapshot to the row being written, otherwise an N-1 writer could overwrite
-- the same resource while leaving stale evidence behind.
CREATE OR REPLACE FUNCTION shardline_require_s3_object_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_operation_id TEXT; expected_after JSONB; observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(NEW.scope_namespace)::text || ':' || NEW.scope_namespace
        || octet_length(NEW.object_key)::text || ':' || NEW.object_key;
    expected_after := jsonb_build_object('entry', jsonb_build_object(
        'file_id', NEW.file_id, 'size_bytes', NEW.size_bytes,
        'content_hash', NEW.content_hash, 'etag', NEW.etag,
        'user_metadata', CASE WHEN NEW.user_metadata = '' THEN '[]'::jsonb ELSE NEW.user_metadata::jsonb END,
        'updated_at_unix_seconds', NEW.updated_at_unix_seconds
    ), 'object_key', NEW.object_key, 'scope_namespace', NEW.scope_namespace);
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'S3Object' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'S3 object mutation committed without matching reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION shardline_require_oci_tag_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_operation_id TEXT; expected_after JSONB; observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(NEW.scope_namespace)::text || ':' || NEW.scope_namespace
        || octet_length(NEW.repository)::text || ':' || NEW.repository
        || octet_length(NEW.tag)::text || ':' || NEW.tag;
    expected_after := jsonb_build_object(
        'scope_namespace', NEW.scope_namespace, 'repository', NEW.repository,
        'tag', NEW.tag, 'digest_hex', NEW.digest_hex
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'OciTag' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'OCI tag mutation committed without matching reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION shardline_require_hub_ref_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_operation_id TEXT; expected_after JSONB; observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(NEW.repo_id)::text || ':' || NEW.repo_id
        || octet_length(NEW.ref_name)::text || ':' || NEW.ref_name;
    expected_after := jsonb_build_object(
        'repository', NEW.repo_id, 'ref_name', NEW.ref_name, 'head_sha', NEW.sha
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'MetadataCommit' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'Hub ref mutation committed without matching reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END; $$;
