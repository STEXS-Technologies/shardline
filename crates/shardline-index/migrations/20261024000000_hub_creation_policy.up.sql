-- Shared creation policy is opt-in. Existing namespaces keep their behavior.
ALTER TABLE shardline_hub_repos ADD COLUMN creation_authorized INTEGER NOT NULL DEFAULT 0 CHECK (creation_authorized IN (0, 1));
ALTER TABLE shardline_hub_repos ADD COLUMN creation_issuer TEXT;
ALTER TABLE shardline_hub_repos ADD COLUMN creation_subject TEXT;

CREATE TABLE shardline_hub_creation_policies (
    namespace TEXT PRIMARY KEY NOT NULL CHECK (length(namespace) > 0 AND instr(namespace, '/') = 0),
    configured INTEGER NOT NULL DEFAULT 0 CHECK (configured IN (0, 1)),
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    max_repositories INTEGER CHECK (max_repositories IS NULL OR max_repositories >= 0),
    max_creations INTEGER CHECK (max_creations IS NULL OR max_creations >= 0),
    window_seconds INTEGER NOT NULL DEFAULT 3600 CHECK (window_seconds BETWEEN 1 AND 86400)
);
CREATE TABLE shardline_hub_creation_denials (
    namespace TEXT NOT NULL REFERENCES shardline_hub_creation_policies(namespace) ON DELETE CASCADE,
    issuer TEXT NOT NULL,
    subject TEXT NOT NULL,
    PRIMARY KEY (namespace, issuer, subject)
);
CREATE TABLE shardline_hub_creation_events (
    namespace TEXT NOT NULL,
    created_at_unix_seconds INTEGER NOT NULL
);
CREATE INDEX shardline_hub_creation_events_window ON shardline_hub_creation_events(namespace, created_at_unix_seconds);
CREATE INDEX shardline_hub_repos_creation_namespace ON shardline_hub_repos(CASE WHEN instr(repo_id, '/') = 0 THEN repo_id ELSE substr(repo_id, 1, instr(repo_id, '/') - 1) END);
INSERT INTO shardline_hub_creation_events(namespace, created_at_unix_seconds)
SELECT CASE WHEN instr(repo_id, '/') = 0 THEN repo_id ELSE substr(repo_id, 1, instr(repo_id, '/') - 1) END, created_at_unix_seconds
FROM shardline_hub_repos WHERE created_at_unix_seconds >= CAST(strftime('%s', 'now') AS INTEGER) - 86400;

-- SQLite serializes this check, repository publication and policy updates in
-- the same writer transaction, including competing connections and old writers.
CREATE TRIGGER shardline_hub_creation_guard BEFORE INSERT ON shardline_hub_repos
BEGIN
    INSERT INTO shardline_hub_creation_policies(namespace)
    VALUES (CASE WHEN instr(NEW.repo_id, '/') = 0 THEN NEW.repo_id ELSE substr(NEW.repo_id, 1, instr(NEW.repo_id, '/') - 1) END)
    ON CONFLICT(namespace) DO NOTHING;
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM shardline_hub_creation_policies p
        WHERE p.namespace = CASE WHEN instr(NEW.repo_id, '/') = 0 THEN NEW.repo_id ELSE substr(NEW.repo_id, 1, instr(NEW.repo_id, '/') - 1) END
          AND p.configured = 1 AND (p.enabled = 0 OR NEW.creation_authorized = 0 OR EXISTS (
              SELECT 1 FROM shardline_hub_creation_denials d WHERE d.namespace = p.namespace
                AND d.issuer = NEW.creation_issuer AND d.subject = NEW.creation_subject
          ))
    ) THEN RAISE(ABORT, 'shardline_hub_creation_denied') END;
    -- Authorized retries of an existing identity consume neither limit.
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM shardline_hub_repos WHERE repo_id = NEW.repo_id)
        AND EXISTS (
            SELECT 1 FROM shardline_hub_creation_policies p
            WHERE p.namespace = CASE WHEN instr(NEW.repo_id, '/') = 0 THEN NEW.repo_id ELSE substr(NEW.repo_id, 1, instr(NEW.repo_id, '/') - 1) END
              AND p.max_repositories IS NOT NULL AND p.max_repositories <= (
                  SELECT count(*) FROM shardline_hub_repos r
                  WHERE CASE WHEN instr(r.repo_id, '/') = 0 THEN r.repo_id ELSE substr(r.repo_id, 1, instr(r.repo_id, '/') - 1) END = p.namespace
              )
        ) THEN RAISE(ABORT, 'shardline_hub_creation_quota') END;
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM shardline_hub_repos WHERE repo_id = NEW.repo_id)
        AND EXISTS (
            SELECT 1 FROM shardline_hub_creation_policies p
            WHERE p.namespace = CASE WHEN instr(NEW.repo_id, '/') = 0 THEN NEW.repo_id ELSE substr(NEW.repo_id, 1, instr(NEW.repo_id, '/') - 1) END
              AND p.max_creations IS NOT NULL AND p.max_creations <= (
                  SELECT count(*) FROM shardline_hub_creation_events e
                  WHERE e.namespace = p.namespace
                    AND e.created_at_unix_seconds >= CAST(strftime('%s', 'now') AS INTEGER) - p.window_seconds
              )
        ) THEN RAISE(ABORT, 'shardline_hub_creation_rate') END;
END;
CREATE TRIGGER shardline_hub_creation_record AFTER INSERT ON shardline_hub_repos
BEGIN
    DELETE FROM shardline_hub_creation_events
    WHERE namespace = CASE WHEN instr(NEW.repo_id, '/') = 0 THEN NEW.repo_id ELSE substr(NEW.repo_id, 1, instr(NEW.repo_id, '/') - 1) END
      AND created_at_unix_seconds < CAST(strftime('%s', 'now') AS INTEGER) - 86400;
    INSERT INTO shardline_hub_creation_events(namespace, created_at_unix_seconds)
    VALUES (CASE WHEN instr(NEW.repo_id, '/') = 0 THEN NEW.repo_id ELSE substr(NEW.repo_id, 1, instr(NEW.repo_id, '/') - 1) END, CAST(strftime('%s', 'now') AS INTEGER));
END;
