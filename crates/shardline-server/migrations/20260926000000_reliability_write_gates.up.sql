-- Evidence-bound mutations must be transactionally paired with their
-- reliability event. These deferred triggers are the mixed-version gate:
-- an N-1 writer can read an N resource, but cannot commit a same-resource
-- update without producing the canonical evidence that N verifies.
CREATE OR REPLACE FUNCTION shardline_require_s3_object_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_operation_id TEXT;
    expected_after JSONB;
    observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(NEW.scope_namespace)::text || ':' || NEW.scope_namespace
        || octet_length(NEW.object_key)::text || ':' || NEW.object_key;
    expected_after := jsonb_build_object(
        'entry', jsonb_build_object(
            'file_id', NEW.file_id,
            'size_bytes', NEW.size_bytes,
            'content_hash', NEW.content_hash,
            'etag', NEW.etag,
            'user_metadata', CASE WHEN NEW.user_metadata = '' THEN '[]'::jsonb ELSE NEW.user_metadata::jsonb END,
            'updated_at_unix_seconds', NEW.updated_at_unix_seconds
        ),
        'object_key', NEW.object_key,
        'scope_namespace', NEW.scope_namespace
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'S3Object' AND shardline_reliability_events.operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'S3 object mutation committed without matching reliability evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION shardline_require_oci_tag_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_operation_id TEXT;
    expected_after JSONB;
    observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(NEW.scope_namespace)::text || ':' || NEW.scope_namespace
        || octet_length(NEW.repository)::text || ':' || NEW.repository
        || octet_length(NEW.tag)::text || ':' || NEW.tag;
    expected_after := jsonb_build_object(
        'scope_namespace', NEW.scope_namespace,
        'repository', NEW.repository,
        'tag', NEW.tag,
        'digest_hex', NEW.digest_hex
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'OciTag' AND shardline_reliability_events.operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'OCI tag mutation committed without matching reliability evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION shardline_require_hub_ref_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_operation_id TEXT;
    expected_after JSONB;
    observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(NEW.repo_id)::text || ':' || NEW.repo_id
        || octet_length(NEW.ref_name)::text || ':' || NEW.ref_name;
    expected_after := jsonb_build_object(
        'repository', NEW.repo_id,
        'ref_name', NEW.ref_name,
        'head_sha', NEW.sha
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'MetadataCommit' AND shardline_reliability_events.operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'Hub ref mutation committed without matching reliability evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION shardline_require_s3_object_delete_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_operation_id TEXT;
    expected_after JSONB;
    observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(OLD.scope_namespace)::text || ':' || OLD.scope_namespace
        || octet_length(OLD.object_key)::text || ':' || OLD.object_key;
    expected_after := jsonb_build_object(
        'entry', NULL,
        'object_key', OLD.object_key,
        'scope_namespace', OLD.scope_namespace
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'S3Object' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'S3 object deletion committed without matching reliability evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION shardline_require_oci_tag_delete_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_operation_id TEXT;
    expected_after JSONB;
    observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(OLD.scope_namespace)::text || ':' || OLD.scope_namespace
        || octet_length(OLD.repository)::text || ':' || OLD.repository
        || octet_length(OLD.tag)::text || ':' || OLD.tag;
    expected_after := jsonb_build_object(
        'scope_namespace', OLD.scope_namespace,
        'repository', OLD.repository,
        'tag', OLD.tag,
        'digest_hex', NULL
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'OciTag' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'OCI tag deletion committed without matching reliability evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE FUNCTION shardline_require_hub_ref_delete_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    expected_operation_id TEXT;
    expected_after JSONB;
    observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(OLD.repo_id)::text || ':' || OLD.repo_id
        || octet_length(OLD.ref_name)::text || ':' || OLD.ref_name;
    expected_after := jsonb_build_object(
        'repository', OLD.repo_id,
        'ref_name', OLD.ref_name,
        'head_sha', NULL
    );
    SELECT event_json->'after' INTO observed_after
    FROM shardline_reliability_events
    WHERE operation_kind = 'MetadataCommit' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'Hub ref deletion committed without matching reliability evidence'
            USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END;
$$;

CREATE CONSTRAINT TRIGGER shardline_s3_object_reliability_gate
AFTER INSERT OR UPDATE ON shardline_s3_objects
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_s3_object_evidence();

CREATE CONSTRAINT TRIGGER shardline_oci_tag_reliability_gate
AFTER INSERT OR UPDATE ON shardline_oci_tags
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_oci_tag_evidence();

CREATE CONSTRAINT TRIGGER shardline_hub_ref_reliability_gate
AFTER INSERT OR UPDATE ON shardline_hub_refs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_hub_ref_evidence();

CREATE CONSTRAINT TRIGGER shardline_s3_object_delete_reliability_gate
AFTER DELETE ON shardline_s3_objects
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_s3_object_delete_evidence();

CREATE CONSTRAINT TRIGGER shardline_oci_tag_delete_reliability_gate
AFTER DELETE ON shardline_oci_tags
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_oci_tag_delete_evidence();

CREATE CONSTRAINT TRIGGER shardline_hub_ref_delete_reliability_gate
AFTER DELETE ON shardline_hub_refs
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_hub_ref_delete_evidence();
