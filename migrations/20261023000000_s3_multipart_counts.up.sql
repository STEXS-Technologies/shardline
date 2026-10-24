CREATE TABLE IF NOT EXISTS shardline_s3_multipart_counts (
    scope_namespace TEXT NOT NULL,
    object_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    part_count INTEGER NOT NULL CHECK (part_count >= 0),
    updated_at_unix_seconds BIGINT NOT NULL,
    proof TEXT NOT NULL,
    PRIMARY KEY (scope_namespace, object_key)
);

-- The released binaries write only shardline_s3_objects. Invalidate any
-- derived part count when such a write changes the authoritative row. Newer
-- binaries write a fresh sidecar later in the same transaction.
CREATE OR REPLACE FUNCTION shardline_invalidate_s3_multipart_count()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM shardline_s3_multipart_counts
        WHERE scope_namespace = OLD.scope_namespace AND object_key = OLD.object_key;
    ELSE
        DELETE FROM shardline_s3_multipart_counts
        WHERE scope_namespace = NEW.scope_namespace AND object_key = NEW.object_key;
        IF TG_OP = 'UPDATE' AND (OLD.scope_namespace, OLD.object_key)
            IS DISTINCT FROM (NEW.scope_namespace, NEW.object_key) THEN
            DELETE FROM shardline_s3_multipart_counts
            WHERE scope_namespace = OLD.scope_namespace AND object_key = OLD.object_key;
        END IF;
    END IF;
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS shardline_s3_multipart_counts_invalidate ON shardline_s3_objects;
CREATE TRIGGER shardline_s3_multipart_counts_invalidate
AFTER INSERT OR UPDATE OR DELETE ON shardline_s3_objects
FOR EACH ROW EXECUTE FUNCTION shardline_invalidate_s3_multipart_count();
