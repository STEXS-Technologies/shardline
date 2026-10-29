DROP TRIGGER IF EXISTS shardline_s3_multipart_counts_invalidate ON shardline_s3_objects;
DROP FUNCTION IF EXISTS shardline_invalidate_s3_multipart_count();
DROP TABLE IF EXISTS shardline_s3_multipart_counts;
