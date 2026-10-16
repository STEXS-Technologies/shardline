-- Optional evidence storage. The released migration ledger is unchanged, and
-- payload enumeration never sees these rows. Runtime roles get no table grants.
CREATE TABLE IF NOT EXISTS @SCHEMA@.shardline_task_artifacts_v1 (
    artifact_key TEXT COLLATE "C" PRIMARY KEY,
    payload BYTEA NOT NULL
);
REVOKE ALL ON @SCHEMA@.shardline_task_artifacts_v1 FROM PUBLIC;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_artifact_access_v1(
    index_schema TEXT, operation TEXT
) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, pg_temp
SET shardline.task_artifact_schema TO @SCHEMA_LITERAL@
AS $access$
BEGIN
    IF index_schema IS DISTINCT FROM current_setting('shardline.task_artifact_schema')
       OR NOT has_schema_privilege(session_user, index_schema, 'USAGE')
       OR NOT has_table_privilege(session_user,
            format('%I.shardline_reliability_events', index_schema), 'SELECT') THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='task evidence requires existing reliability metadata access';
    END IF;
    -- A schema-wide function cannot infer tenant visibility from arbitrary RLS
    -- policies. Do not bypass those policies to expose or publish shared history.
    IF EXISTS(SELECT 1 FROM pg_class target JOIN pg_namespace namespace
              ON namespace.oid=target.relnamespace
              WHERE namespace.nspname=index_schema AND target.relrowsecurity
                AND target.relname LIKE 'shardline_%') THEN
        RAISE EXCEPTION USING ERRCODE='SLD04', MESSAGE='row-scoped task evidence storage is unsupported';
    END IF;
    IF operation IS NULL OR operation NOT IN ('read','publish','delete') THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid task evidence operation';
    END IF;
    IF operation='publish' AND NOT (
        has_table_privilege(session_user,
            format('%I.shardline_reliability_events', index_schema), 'INSERT')
        OR (has_table_privilege(session_user,
                format('%I.shardline_quarantine_candidates', index_schema), 'SELECT')
            AND has_table_privilege(session_user,
                format('%I.shardline_quarantine_candidates', index_schema), 'DELETE'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='task evidence publication requires existing maintenance metadata permissions';
    END IF;
    IF operation='delete' AND NOT (
        has_table_privilege(session_user,
            format('%I.shardline_reliability_events', index_schema), 'DELETE')
        OR (has_table_privilege(session_user,
                format('%I.shardline_quarantine_candidates', index_schema), 'SELECT')
            AND has_table_privilege(session_user,
                format('%I.shardline_quarantine_candidates', index_schema), 'DELETE'))
    ) THEN
        RAISE EXCEPTION USING ERRCODE='42501', MESSAGE='task evidence retention requires existing maintenance deletion permissions';
    END IF;
END;
$access$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_artifact_publish_v1(
    index_schema TEXT, artifact TEXT, body BYTEA
) RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $publish$
DECLARE identical BOOLEAN;
BEGIN
    PERFORM shardline_task_artifact_access_v1(index_schema, 'publish');
    IF artifact IS NULL OR body IS NULL
       OR left(artifact, 20) <> '_shardline_tasks/v1/' THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid task evidence key or body';
    END IF;
    EXECUTE format(
        'INSERT INTO %I.shardline_task_artifacts_v1(artifact_key,payload) VALUES($1,$2)
         ON CONFLICT(artifact_key) DO NOTHING', index_schema
    ) USING artifact, body;
    EXECUTE format(
        'SELECT payload=$2 FROM %I.shardline_task_artifacts_v1 WHERE artifact_key=$1', index_schema
    ) INTO identical USING artifact, body;
    IF identical IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION USING ERRCODE='SLD05', MESSAGE='task evidence publication conflict';
    END IF;
END;
$publish$;

-- Automatic publication has an admission budget. The original primitive remains
-- available for immutable legacy imports; no runtime table grant is added.
CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_history_admission_full_v1(
    index_schema TEXT, maximum_runs BIGINT
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $admission_full$
DECLARE retained BIGINT;
BEGIN
    PERFORM shardline_task_artifact_access_v1(index_schema, 'read');
    IF maximum_runs IS NULL OR maximum_runs < 0 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid automatic history capacity';
    END IF;
    EXECUTE format('SELECT count(*) FROM (SELECT artifact_key FROM %I.shardline_task_artifacts_v1
        WHERE (artifact_key >= ''_shardline_tasks/v1/manifests/'' AND artifact_key < ''_shardline_tasks/v1/manifests0'')
           OR (artifact_key >= ''_shardline_tasks/v1/retiring/'' AND artifact_key < ''_shardline_tasks/v1/retiring0'')
        LIMIT $1) bounded', index_schema) INTO retained USING maximum_runs;
    RETURN retained>=maximum_runs;
END;
$admission_full$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_artifact_publish_bounded_v1(
    index_schema TEXT, artifact TEXT, body BYTEA, maximum_runs BIGINT
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $bounded_publish$
DECLARE identical BOOLEAN; category TEXT; run_id TEXT;
BEGIN
    PERFORM shardline_task_artifact_access_v1(index_schema, 'publish');
    IF maximum_runs IS NULL OR maximum_runs < 0 OR artifact IS NULL OR body IS NULL THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid automatic task evidence publication';
    END IF;
    -- A pre-existing caller snapshot cannot safely count concurrent admissions.
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION USING ERRCODE='SLD04', MESSAGE='automatic history admission requires read committed isolation';
    END IF;
    PERFORM pg_advisory_xact_lock(1397508169, (SELECT (oid::BIGINT-2147483648)::INT FROM pg_namespace WHERE nspname=index_schema));
    -- Keep an exact retry's row alive until this transaction ends. Retirement
    -- must not delete it between verification and the idempotent publication.
    EXECUTE format('SELECT payload=$2 FROM %I.shardline_task_artifacts_v1 WHERE artifact_key=$1 FOR KEY SHARE', index_schema)
        INTO identical USING artifact,body;
    IF identical IS NOT NULL THEN
        PERFORM shardline_task_artifact_publish_v1(index_schema,artifact,body);
        RETURN TRUE;
    END IF;
    IF octet_length(body)>65536 THEN RETURN FALSE; END IF;
    IF left(artifact,20)<>'_shardline_tasks/v1/' THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid automatic task evidence key';
    END IF;
    category := split_part(substr(artifact,21),'/',1);
    run_id := split_part(substr(artifact,21),'/',2);
    IF category NOT IN ('manifests','retiring','scope-bindings','runs') OR run_id !~ '^[A-Za-z0-9_-]{1,128}$'
       OR (category<>'runs' AND substr(artifact,21)<>category||'/'||run_id) THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid automatic task evidence key';
    END IF;
    IF category='manifests' THEN
        IF shardline_task_history_admission_full_v1(index_schema,maximum_runs) THEN RETURN FALSE; END IF;
    ELSE
        -- Admission remains attached to its manifest through publication.
        -- A concurrent retirement either waits or wins before this row check.
        EXECUTE format('SELECT EXISTS(SELECT 1 FROM %I.shardline_task_artifacts_v1 WHERE artifact_key=$1 FOR KEY SHARE)',index_schema)
            INTO identical USING '_shardline_tasks/v1/manifests/'||run_id;
        IF NOT identical THEN RETURN FALSE; END IF;
    END IF;
    PERFORM shardline_task_artifact_publish_v1(index_schema,artifact,body);
    RETURN TRUE;
END;
$bounded_publish$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_artifact_read_v1(
    index_schema TEXT, artifact TEXT, maximum_bytes BIGINT
) RETURNS TABLE(byte_length BIGINT, body BYTEA) LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $read$
BEGIN
    PERFORM shardline_task_artifact_access_v1(index_schema, 'read');
    IF maximum_bytes IS NULL OR maximum_bytes < 0 THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid task evidence read bound';
    END IF;
    -- Return the length with a NULL body when oversized. No oversized bytea is
    -- transferred to the client, and absence remains distinct from oversize.
    RETURN QUERY EXECUTE format(
        'SELECT octet_length(payload)::BIGINT,
         CASE WHEN octet_length(payload)<=$2 THEN payload END
         FROM %I.shardline_task_artifacts_v1 WHERE artifact_key=$1', index_schema
    ) USING artifact, maximum_bytes;
END;
$read$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_artifact_page_v1(
    index_schema TEXT, prefix TEXT, after_key TEXT
) RETURNS TABLE(artifact_key TEXT, byte_length BIGINT) LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $page$
BEGIN
    PERFORM shardline_task_artifact_access_v1(index_schema, 'read');
    IF prefix IS NULL THEN
        RAISE EXCEPTION USING ERRCODE='22023', MESSAGE='invalid task evidence prefix';
    END IF;
    -- Literal prefixes, ordered using the same bytewise collation as the key
    -- index. Callers can stop at the first nonmatching row, without full scans
    -- or SQL wildcard interpretation. No artifact bodies enter inventory pages.
    IF after_key IS NULL THEN
        RETURN QUERY EXECUTE format(
            'SELECT artifact_key,octet_length(payload)::BIGINT
             FROM %I.shardline_task_artifacts_v1 WHERE artifact_key >= $1 COLLATE "C"
             ORDER BY artifact_key COLLATE "C" LIMIT 64', index_schema
        ) USING prefix;
    ELSE
        RETURN QUERY EXECUTE format(
            'SELECT artifact_key,octet_length(payload)::BIGINT
             FROM %I.shardline_task_artifacts_v1
             WHERE artifact_key >= $1 COLLATE "C" AND artifact_key > $2 COLLATE "C"
             ORDER BY artifact_key COLLATE "C" LIMIT 64', index_schema
        ) USING prefix, after_key;
    END IF;
END;
$page$;

CREATE OR REPLACE FUNCTION @SCHEMA@.shardline_task_artifact_delete_v1(
    index_schema TEXT, artifact TEXT
) RETURNS BOOLEAN LANGUAGE plpgsql SECURITY DEFINER
SET search_path TO pg_catalog, @SCHEMA@, pg_temp
AS $delete$
DECLARE removed BIGINT;
BEGIN
    PERFORM shardline_task_artifact_access_v1(index_schema, 'delete');
    EXECUTE format('DELETE FROM %I.shardline_task_artifacts_v1 WHERE artifact_key=$1', index_schema)
        USING artifact;
    GET DIAGNOSTICS removed = ROW_COUNT;
    RETURN removed=1;
END;
$delete$;

GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_artifact_access_v1(TEXT,TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_artifact_publish_v1(TEXT,TEXT,BYTEA) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_artifact_publish_bounded_v1(TEXT,TEXT,BYTEA,BIGINT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_history_admission_full_v1(TEXT,BIGINT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_artifact_read_v1(TEXT,TEXT,BIGINT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_artifact_page_v1(TEXT,TEXT,TEXT) TO PUBLIC;
GRANT EXECUTE ON FUNCTION @SCHEMA@.shardline_task_artifact_delete_v1(TEXT,TEXT) TO PUBLIC;
