CREATE TABLE IF NOT EXISTS shardline_xet_file_aliases (
    scope_key TEXT NOT NULL,
    file_id TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    total_bytes BIGINT NOT NULL CHECK (total_bytes >= 0),
    PRIMARY KEY (scope_key, file_id)
);

CREATE OR REPLACE FUNCTION shardline_preserve_xet_file_alias_v1()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE inserted bigint; replay boolean;
BEGIN
    IF TG_OP = 'UPDATE' THEN
        EXECUTE format('INSERT INTO %I.shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
            VALUES ($1,$2,$3,$4) ON CONFLICT (scope_key,file_id) DO NOTHING', TG_TABLE_SCHEMA)
            USING OLD.scope_key,OLD.file_id,OLD.sha256,OLD.total_bytes;
        GET DIAGNOSTICS inserted = ROW_COUNT;
        IF inserted = 0 THEN
            EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.shardline_xet_file_aliases
                WHERE scope_key=$1 AND file_id=$2 AND sha256=$3 AND total_bytes=$4)', TG_TABLE_SCHEMA)
                INTO replay USING OLD.scope_key,OLD.file_id,OLD.sha256,OLD.total_bytes;
            IF NOT replay THEN RAISE EXCEPTION 'conflicting native Xet file alias'; END IF;
        END IF;
    END IF;
    EXECUTE format('INSERT INTO %I.shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
        VALUES ($1,$2,$3,$4) ON CONFLICT (scope_key,file_id) DO NOTHING', TG_TABLE_SCHEMA)
        USING NEW.scope_key,NEW.file_id,NEW.sha256,NEW.total_bytes;
    GET DIAGNOSTICS inserted = ROW_COUNT;
    IF inserted = 0 THEN
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.shardline_xet_file_aliases
            WHERE scope_key=$1 AND file_id=$2 AND sha256=$3 AND total_bytes=$4)', TG_TABLE_SCHEMA)
            INTO replay USING NEW.scope_key,NEW.file_id,NEW.sha256,NEW.total_bytes;
        IF NOT replay THEN RAISE EXCEPTION 'conflicting native Xet file alias'; END IF;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS shardline_xet_file_aliases_preserve ON shardline_xet_file_identities;
CREATE TRIGGER shardline_xet_file_aliases_preserve
    AFTER INSERT OR UPDATE ON shardline_xet_file_identities
    FOR EACH ROW EXECUTE FUNCTION shardline_preserve_xet_file_alias_v1();

INSERT INTO shardline_xet_file_aliases(scope_key,file_id,sha256,total_bytes)
SELECT scope_key,file_id,sha256,total_bytes FROM shardline_xet_file_identities WHERE TRUE
ON CONFLICT (scope_key,file_id) DO UPDATE SET sha256 =
    CASE WHEN shardline_xet_file_aliases.sha256=excluded.sha256
          AND shardline_xet_file_aliases.total_bytes=excluded.total_bytes
         THEN shardline_xet_file_aliases.sha256 ELSE NULL END;
