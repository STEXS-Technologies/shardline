CREATE TABLE IF NOT EXISTS shardline_xet_file_aliases (
    scope_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    total_bytes INTEGER NOT NULL CHECK (total_bytes >= 0),
    PRIMARY KEY (scope_key, file_id)
) WITHOUT ROWID;

CREATE TRIGGER IF NOT EXISTS shardline_xet_file_aliases_insert
AFTER INSERT ON shardline_xet_file_identities
BEGIN
    INSERT INTO shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
    VALUES (NEW.scope_key,NEW.file_id,NEW.sha256,NEW.total_bytes)
    ON CONFLICT (scope_key,file_id) DO NOTHING;
    SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM shardline_xet_file_aliases WHERE scope_key=NEW.scope_key
        AND file_id=NEW.file_id AND sha256=NEW.sha256 AND total_bytes=NEW.total_bytes
    ) THEN RAISE(ABORT,'conflicting native Xet file alias') END;
END;

CREATE TRIGGER IF NOT EXISTS shardline_xet_file_aliases_update
AFTER UPDATE ON shardline_xet_file_identities
BEGIN
    INSERT INTO shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
    VALUES (OLD.scope_key,OLD.file_id,OLD.sha256,OLD.total_bytes)
    ON CONFLICT (scope_key,file_id) DO NOTHING;
    SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM shardline_xet_file_aliases WHERE scope_key=OLD.scope_key
        AND file_id=OLD.file_id AND sha256=OLD.sha256 AND total_bytes=OLD.total_bytes
    ) THEN RAISE(ABORT,'conflicting native Xet file alias') END;
    INSERT INTO shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
    VALUES (NEW.scope_key,NEW.file_id,NEW.sha256,NEW.total_bytes)
    ON CONFLICT (scope_key,file_id) DO NOTHING;
    SELECT CASE WHEN NOT EXISTS(
        SELECT 1 FROM shardline_xet_file_aliases WHERE scope_key=NEW.scope_key
        AND file_id=NEW.file_id AND sha256=NEW.sha256 AND total_bytes=NEW.total_bytes
    ) THEN RAISE(ABORT,'conflicting native Xet file alias') END;
END;

INSERT INTO shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
SELECT scope_key,file_id,sha256,total_bytes FROM shardline_xet_file_identities WHERE TRUE
ON CONFLICT (scope_key,file_id) DO UPDATE SET sha256 =
    CASE WHEN shardline_xet_file_aliases.sha256=excluded.sha256
          AND shardline_xet_file_aliases.total_bytes=excluded.total_bytes
         THEN shardline_xet_file_aliases.sha256 ELSE NULL END;
