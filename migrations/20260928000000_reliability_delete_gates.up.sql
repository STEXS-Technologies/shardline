-- Add delete enforcement for databases that already applied the original
-- reliability write-gate migration before DELETE triggers were included.
DROP TRIGGER IF EXISTS shardline_s3_object_delete_reliability_gate ON shardline_s3_objects;
DROP TRIGGER IF EXISTS shardline_oci_tag_delete_reliability_gate ON shardline_oci_tags;
DROP TRIGGER IF EXISTS shardline_hub_ref_delete_reliability_gate ON shardline_hub_refs;

CREATE OR REPLACE FUNCTION shardline_require_s3_object_delete_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_operation_id TEXT; expected_after JSONB; observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(OLD.scope_namespace)::text || ':' || OLD.scope_namespace
        || octet_length(OLD.object_key)::text || ':' || OLD.object_key;
    expected_after := jsonb_build_object('entry', NULL, 'object_key', OLD.object_key,
        'scope_namespace', OLD.scope_namespace);
    SELECT event_json->'after' INTO observed_after FROM shardline_reliability_events
    WHERE operation_kind = 'S3Object' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'S3 object deletion committed without matching reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END; $$;

CREATE OR REPLACE FUNCTION shardline_require_oci_tag_delete_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_operation_id TEXT; expected_after JSONB; observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(OLD.scope_namespace)::text || ':' || OLD.scope_namespace
        || octet_length(OLD.repository)::text || ':' || OLD.repository
        || octet_length(OLD.tag)::text || ':' || OLD.tag;
    expected_after := jsonb_build_object('scope_namespace', OLD.scope_namespace,
        'repository', OLD.repository, 'tag', OLD.tag, 'digest_hex', NULL);
    SELECT event_json->'after' INTO observed_after FROM shardline_reliability_events
    WHERE operation_kind = 'OciTag' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'OCI tag deletion committed without matching reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END; $$;

CREATE OR REPLACE FUNCTION shardline_require_hub_ref_delete_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE expected_operation_id TEXT; expected_after JSONB; observed_after JSONB;
BEGIN
    expected_operation_id := octet_length(OLD.repo_id)::text || ':' || OLD.repo_id
        || octet_length(OLD.ref_name)::text || ':' || OLD.ref_name;
    expected_after := jsonb_build_object('repository', OLD.repo_id,
        'ref_name', OLD.ref_name, 'head_sha', NULL);
    SELECT event_json->'after' INTO observed_after FROM shardline_reliability_events
    WHERE operation_kind = 'MetadataCommit' AND operation_id = expected_operation_id
    ORDER BY sequence DESC LIMIT 1;
    IF observed_after IS NULL OR observed_after IS DISTINCT FROM expected_after THEN
        RAISE EXCEPTION 'Hub ref deletion committed without matching reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN OLD;
END; $$;

CREATE CONSTRAINT TRIGGER shardline_s3_object_delete_reliability_gate
AFTER DELETE ON shardline_s3_objects DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_s3_object_delete_evidence();
CREATE CONSTRAINT TRIGGER shardline_oci_tag_delete_reliability_gate
AFTER DELETE ON shardline_oci_tags DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_oci_tag_delete_evidence();
CREATE CONSTRAINT TRIGGER shardline_hub_ref_delete_reliability_gate
AFTER DELETE ON shardline_hub_refs DEFERRABLE INITIALLY DEFERRED FOR EACH ROW
EXECUTE FUNCTION shardline_require_hub_ref_delete_evidence();
