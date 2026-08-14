ALTER TABLE shardline_s3_objects ADD COLUMN etag TEXT NOT NULL DEFAULT '';
ALTER TABLE shardline_s3_objects ADD COLUMN user_metadata TEXT NOT NULL DEFAULT '';
-- Backfill: rows created before the ETag column keep their previous opaque
-- ETag semantics (the content hash) so conditional requests keep working.
UPDATE shardline_s3_objects SET etag = content_hash WHERE etag = '';
