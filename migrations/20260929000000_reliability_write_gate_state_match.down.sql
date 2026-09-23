-- Restore the pre-state-match gate semantics when rolling back this
-- compatibility migration. The preceding migration still requires an
-- evidence row, but does not bind it to the materialized after-state.
CREATE OR REPLACE FUNCTION shardline_require_s3_object_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM shardline_reliability_events
        WHERE operation_kind = 'S3Object'
          AND operation_id = octet_length(NEW.scope_namespace)::text || ':' || NEW.scope_namespace
              || octet_length(NEW.object_key)::text || ':' || NEW.object_key
    ) THEN
        RAISE EXCEPTION 'S3 object mutation committed without reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION shardline_require_oci_tag_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM shardline_reliability_events
        WHERE operation_kind = 'OciTag'
          AND operation_id = octet_length(NEW.scope_namespace)::text || ':' || NEW.scope_namespace
              || octet_length(NEW.repository)::text || ':' || NEW.repository
              || octet_length(NEW.tag)::text || ':' || NEW.tag
    ) THEN
        RAISE EXCEPTION 'OCI tag mutation committed without reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END; $$;

CREATE OR REPLACE FUNCTION shardline_require_hub_ref_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM shardline_reliability_events
        WHERE operation_kind = 'MetadataCommit'
          AND operation_id = octet_length(NEW.repo_id)::text || ':' || NEW.repo_id
              || octet_length(NEW.ref_name)::text || ':' || NEW.ref_name
    ) THEN
        RAISE EXCEPTION 'Hub ref mutation committed without reliability evidence' USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END; $$;
