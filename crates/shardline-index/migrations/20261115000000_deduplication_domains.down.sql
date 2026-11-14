DROP TRIGGER IF EXISTS shardline_record_membership_insert;
DROP TRIGGER IF EXISTS shardline_record_membership_update;
CREATE TRIGGER IF NOT EXISTS shardline_record_membership_insert
AFTER INSERT ON shardline_file_records
BEGIN
    INSERT INTO shardline_record_membership (record_key, scope_key, member_hash)
    SELECT DISTINCT NEW.record_key, NEW.scope_key,
           json_extract(CASE WHEN member.type = 'object' THEN member.value ELSE '{}' END, '$.hash')
    FROM json_each(CASE WHEN json_valid(NEW.record) THEN NEW.record ELSE '{}' END, '$.chunks') AS member
    WHERE json_type(CASE WHEN member.type = 'object' THEN member.value ELSE '{}' END, '$.hash') = 'text';
END;

CREATE TRIGGER IF NOT EXISTS shardline_record_membership_update
AFTER UPDATE OF record, scope_key, record_key ON shardline_file_records
WHEN OLD.record IS NOT NEW.record OR OLD.scope_key IS NOT NEW.scope_key
     OR OLD.record_key IS NOT NEW.record_key
BEGIN
    DELETE FROM shardline_record_membership WHERE record_key = OLD.record_key;
    INSERT INTO shardline_record_membership (record_key, scope_key, member_hash)
    SELECT DISTINCT NEW.record_key, NEW.scope_key,
           json_extract(CASE WHEN member.type = 'object' THEN member.value ELSE '{}' END, '$.hash')
    FROM json_each(CASE WHEN json_valid(NEW.record) THEN NEW.record ELSE '{}' END, '$.chunks') AS member
    WHERE json_type(CASE WHEN member.type = 'object' THEN member.value ELSE '{}' END, '$.hash') = 'text';
END;

UPDATE shardline_record_membership SET scope_key = (
    SELECT source.scope_key FROM shardline_file_records source
    WHERE source.record_key = shardline_record_membership.record_key
);
