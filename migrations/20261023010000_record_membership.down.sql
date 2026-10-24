DROP TRIGGER IF EXISTS shardline_record_membership_refresh ON shardline_file_records;
DROP FUNCTION IF EXISTS shardline_refresh_record_membership();
DROP TABLE IF EXISTS shardline_record_membership;
