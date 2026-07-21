DROP INDEX IF EXISTS shardline_quarantine_delete_after_idx;
DROP INDEX IF EXISTS shardline_file_records_scope_file_idx;
DROP INDEX IF EXISTS shardline_file_records_kind_key_idx;
DROP TABLE IF EXISTS shardline_file_records;
DROP TABLE IF EXISTS shardline_quarantine_candidates;
DROP TABLE IF EXISTS shardline_stored_objects;
DROP TABLE IF EXISTS shardline_file_reconstructions;
