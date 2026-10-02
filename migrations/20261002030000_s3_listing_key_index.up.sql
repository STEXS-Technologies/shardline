CREATE INDEX IF NOT EXISTS shardline_s3_objects_scope_key_c_idx
    ON shardline_s3_objects (scope_namespace, object_key COLLATE "C");
