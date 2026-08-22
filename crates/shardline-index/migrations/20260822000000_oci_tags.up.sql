CREATE TABLE IF NOT EXISTS shardline_oci_tags (
    scope_namespace TEXT NOT NULL,
    repository TEXT NOT NULL,
    tag TEXT NOT NULL,
    digest_hex TEXT NOT NULL,
    PRIMARY KEY (scope_namespace, repository, tag)
) WITHOUT ROWID;

CREATE INDEX IF NOT EXISTS shardline_oci_tags_digest_idx
    ON shardline_oci_tags (scope_namespace, repository, digest_hex, tag);
