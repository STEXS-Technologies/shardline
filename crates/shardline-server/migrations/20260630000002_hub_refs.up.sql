CREATE TABLE IF NOT EXISTS shardline_hub_refs (
    repo_id TEXT NOT NULL,
    ref_name TEXT NOT NULL,
    sha TEXT NOT NULL,
    PRIMARY KEY (repo_id, ref_name),
    FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
);

-- Preserve existing repositories when upgrading. Branch refs are stored in
-- their short Hub spelling so `main` and Git's `refs/heads/main` are one ref.
INSERT INTO shardline_hub_refs (repo_id, ref_name, sha)
SELECT revision.repo_id,
       CASE
           WHEN revision.ref_name LIKE 'refs/heads/%' THEN substr(revision.ref_name, 12)
           ELSE revision.ref_name
       END,
       revision.sha
FROM shardline_hub_revisions AS revision
WHERE revision.ref_name <> 'HEAD'
  AND NOT EXISTS (
      SELECT 1
      FROM shardline_hub_revisions AS newer
      WHERE newer.repo_id = revision.repo_id
        AND (
            CASE
                WHEN newer.ref_name LIKE 'refs/heads/%' THEN substr(newer.ref_name, 12)
                ELSE newer.ref_name
            END
        ) = (
            CASE
                WHEN revision.ref_name LIKE 'refs/heads/%' THEN substr(revision.ref_name, 12)
                ELSE revision.ref_name
            END
        )
        AND (
            newer.created_at_unix_seconds > revision.created_at_unix_seconds
            OR (
                newer.created_at_unix_seconds = revision.created_at_unix_seconds
                AND newer.sha > revision.sha
            )
        )
  );
