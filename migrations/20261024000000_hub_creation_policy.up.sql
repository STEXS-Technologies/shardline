ALTER TABLE shardline_hub_repos ADD COLUMN creation_authorized BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE shardline_hub_repos ADD COLUMN creation_issuer TEXT;
ALTER TABLE shardline_hub_repos ADD COLUMN creation_subject TEXT;

CREATE TABLE shardline_hub_creation_policies (
    namespace TEXT PRIMARY KEY CHECK (length(namespace) > 0 AND position('/' in namespace) = 0),
    configured BOOLEAN NOT NULL DEFAULT FALSE,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    max_repositories BIGINT CHECK (max_repositories IS NULL OR max_repositories >= 0),
    max_creations BIGINT CHECK (max_creations IS NULL OR max_creations >= 0),
    window_seconds BIGINT NOT NULL DEFAULT 3600 CHECK (window_seconds BETWEEN 1 AND 86400)
);
CREATE TABLE shardline_hub_creation_denials (
    namespace TEXT NOT NULL REFERENCES shardline_hub_creation_policies(namespace) ON DELETE CASCADE,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    PRIMARY KEY (namespace, issuer, subject)
);
CREATE TABLE shardline_hub_creation_events (
    namespace TEXT NOT NULL,
    created_at_unix_seconds BIGINT NOT NULL
);
CREATE INDEX shardline_hub_creation_events_window ON shardline_hub_creation_events(namespace, created_at_unix_seconds);
CREATE INDEX shardline_hub_repos_creation_namespace ON shardline_hub_repos((split_part(repo_id, '/', 1)));
INSERT INTO shardline_hub_creation_events(namespace, created_at_unix_seconds)
SELECT split_part(repo_id, '/', 1), created_at_unix_seconds FROM shardline_hub_repos
WHERE created_at_unix_seconds >= EXTRACT(EPOCH FROM clock_timestamp())::bigint - 86400;

-- One namespace lock serializes creation and policy mutation, including the
-- absent-policy case. Never derive a namespace lock from a repository identity.
CREATE FUNCTION shardline_hub_creation_policy_lock() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM pg_advisory_xact_lock(1397506896, hashtext(OLD.namespace));
        RETURN OLD;
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.namespace <> OLD.namespace THEN
        RAISE EXCEPTION 'creation policy namespace is immutable';
    END IF;
    PERFORM pg_advisory_xact_lock(1397506896, hashtext(NEW.namespace));
    RETURN NEW;
END;
$$;
CREATE TRIGGER shardline_hub_creation_policy_lock BEFORE INSERT OR UPDATE OR DELETE ON shardline_hub_creation_policies
FOR EACH ROW EXECUTE FUNCTION shardline_hub_creation_policy_lock();
CREATE TRIGGER shardline_hub_creation_denial_lock BEFORE INSERT OR UPDATE OR DELETE ON shardline_hub_creation_denials
FOR EACH ROW EXECUTE FUNCTION shardline_hub_creation_policy_lock();

CREATE FUNCTION shardline_hub_creation_guard() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    ns TEXT := split_part(NEW.repo_id, '/', 1);
    policy shardline_hub_creation_policies%ROWTYPE;
    now_seconds BIGINT;
BEGIN
    PERFORM pg_advisory_xact_lock(1397506896, hashtext(ns));
    -- Touch a shared row even for the absent-policy case. PostgreSQL rejects
    -- stale repeatable-read snapshots rather than letting them miss a policy
    -- installed after the snapshot. Default rows retain legacy behavior.
    INSERT INTO shardline_hub_creation_policies(namespace) VALUES (ns)
    ON CONFLICT(namespace) DO UPDATE SET namespace = EXCLUDED.namespace
    RETURNING * INTO policy;
    IF FOUND THEN
        IF policy.configured AND (NOT policy.enabled OR NOT NEW.creation_authorized OR EXISTS (
            SELECT 1 FROM shardline_hub_creation_denials
            WHERE namespace = ns AND issuer = NEW.creation_issuer AND subject = NEW.creation_subject
        )) THEN
            RAISE EXCEPTION USING ERRCODE = 'SL403', MESSAGE = 'shardline_hub_creation_denied';
        END IF;
        IF NOT EXISTS (SELECT 1 FROM shardline_hub_repos WHERE repo_id = NEW.repo_id) THEN
            IF policy.max_repositories IS NOT NULL AND policy.max_repositories <= (
                SELECT count(*) FROM shardline_hub_repos WHERE split_part(repo_id, '/', 1) = ns
            ) THEN
                RAISE EXCEPTION USING ERRCODE = 'SL409', MESSAGE = 'shardline_hub_creation_quota';
            END IF;
            now_seconds := EXTRACT(EPOCH FROM clock_timestamp())::bigint;
            IF policy.max_creations IS NOT NULL AND policy.max_creations <= (
                SELECT count(*) FROM shardline_hub_creation_events
                WHERE namespace = ns AND created_at_unix_seconds >= now_seconds - policy.window_seconds
            ) THEN
                RAISE EXCEPTION USING ERRCODE = 'SL429', MESSAGE = 'shardline_hub_creation_rate';
            END IF;
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER shardline_hub_creation_guard BEFORE INSERT ON shardline_hub_repos
FOR EACH ROW EXECUTE FUNCTION shardline_hub_creation_guard();

CREATE FUNCTION shardline_hub_creation_record() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    ns TEXT := split_part(NEW.repo_id, '/', 1);
    now_seconds BIGINT := EXTRACT(EPOCH FROM clock_timestamp())::bigint;
BEGIN
    DELETE FROM shardline_hub_creation_events WHERE namespace = ns AND created_at_unix_seconds < now_seconds - 86400;
    INSERT INTO shardline_hub_creation_events(namespace, created_at_unix_seconds) VALUES (ns, now_seconds);
    RETURN NEW;
END;
$$;
CREATE TRIGGER shardline_hub_creation_record AFTER INSERT ON shardline_hub_repos
FOR EACH ROW EXECUTE FUNCTION shardline_hub_creation_record();

-- Bind table resolution to the owner-installed schema, never a caller's
-- search_path. Functions retain invoker privileges; no elevated role is added.
DO $$
DECLARE
    schema_name TEXT := current_schema();
    function_name TEXT;
BEGIN
    FOREACH function_name IN ARRAY ARRAY['shardline_hub_creation_policy_lock','shardline_hub_creation_guard','shardline_hub_creation_record'] LOOP
        EXECUTE format('ALTER FUNCTION %I.%I() SET search_path TO pg_catalog, %I', schema_name, function_name, schema_name);
    END LOOP;
END;
$$;
