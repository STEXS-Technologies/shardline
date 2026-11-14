-- Reuse domains do not change record storage keys.
DROP TRIGGER IF EXISTS shardline_record_membership_insert;
DROP TRIGGER IF EXISTS shardline_record_membership_update;
CREATE TRIGGER IF NOT EXISTS shardline_record_membership_insert
AFTER INSERT ON shardline_file_records
BEGIN
    INSERT INTO shardline_record_membership (record_key, scope_key, member_hash)
    SELECT DISTINCT NEW.record_key, COALESCE(json_extract(CASE WHEN json_valid(NEW.record) THEN NEW.record ELSE '{}' END, '$.deduplication_domain'), NEW.scope_key),
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
    SELECT DISTINCT NEW.record_key, COALESCE(json_extract(CASE WHEN json_valid(NEW.record) THEN NEW.record ELSE '{}' END, '$.deduplication_domain'), NEW.scope_key),
           json_extract(CASE WHEN member.type = 'object' THEN member.value ELSE '{}' END, '$.hash')
    FROM json_each(CASE WHEN json_valid(NEW.record) THEN NEW.record ELSE '{}' END, '$.chunks') AS member
    WHERE json_type(CASE WHEN member.type = 'object' THEN member.value ELSE '{}' END, '$.hash') = 'text';
END;


-- Restore explicit domains if this derived index was built by an older owner.
-- Records with no domain remain unchanged; never infer tenant ownership.
UPDATE shardline_record_membership AS membership
SET scope_key = (
    SELECT json_extract(source.record, '$.deduplication_domain')
    FROM shardline_file_records source WHERE source.record_key = membership.record_key
)
WHERE record_key IN (
    SELECT record_key FROM shardline_file_records
    WHERE json_type(CASE WHEN json_valid(record) THEN record ELSE '{}' END,
                    '$.deduplication_domain') = 'text'
);
