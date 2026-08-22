CREATE TABLE IF NOT EXISTS shardline_oci_object_tombstones (
    scope_namespace TEXT NOT NULL,
    repository TEXT NOT NULL,
    object_kind TEXT NOT NULL CHECK (object_kind IN ('blob', 'manifest')),
    digest_hex TEXT NOT NULL,
    deleted_at_unix_seconds INTEGER NOT NULL,
    PRIMARY KEY (scope_namespace, repository, object_kind, digest_hex)
) WITHOUT ROWID;
