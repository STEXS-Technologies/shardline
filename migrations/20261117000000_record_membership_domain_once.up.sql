-- SQLite-only optimization. PostgreSQL already evaluates the domain once
-- before expanding members; preserve the shared ordered migration history.
SELECT 1;
