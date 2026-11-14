CREATE OR REPLACE FUNCTION shardline_refresh_record_membership()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF TG_OP = 'UPDATE'
       AND NEW.record IS NOT DISTINCT FROM OLD.record
       AND NEW.scope_key IS NOT DISTINCT FROM OLD.scope_key
       AND NEW.record_key IS NOT DISTINCT FROM OLD.record_key THEN
        RETURN NEW;
    END IF;
    IF TG_OP <> 'INSERT' THEN
        EXECUTE format('DELETE FROM %I.shardline_record_membership WHERE record_key = $1',
                       TG_TABLE_SCHEMA)
            USING OLD.record_key;
    END IF;
    IF TG_OP <> 'DELETE' THEN
        EXECUTE format(
            'INSERT INTO %I.shardline_record_membership (record_key, scope_key, member_hash)
             SELECT DISTINCT $1, $2, member->>''hash''
             FROM jsonb_array_elements(
                 CASE WHEN jsonb_typeof($3->''chunks'') = ''array''
                      THEN $3->''chunks'' ELSE ''[]''::jsonb END) AS member
             WHERE jsonb_typeof(member->''hash'') = ''string''', TG_TABLE_SCHEMA)
            USING NEW.record_key, NEW.scope_key, NEW.record;
        RETURN NEW;
    END IF;
    RETURN OLD;
END;
$$;

CREATE OR REPLACE TRIGGER shardline_record_membership_refresh
    AFTER INSERT OR UPDATE OR DELETE ON shardline_file_records
    FOR EACH ROW EXECUTE FUNCTION shardline_refresh_record_membership();

UPDATE shardline_record_membership m SET scope_key = source.scope_key
FROM shardline_file_records source
WHERE source.record_key = m.record_key AND m.scope_key IS DISTINCT FROM source.scope_key;
