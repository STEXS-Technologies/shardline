DROP TRIGGER IF EXISTS shardline_hub_creation_record;
DROP TRIGGER IF EXISTS shardline_hub_creation_guard;
DROP INDEX IF EXISTS shardline_hub_repos_creation_namespace;
DROP TABLE IF EXISTS shardline_hub_creation_events;
DROP TABLE IF EXISTS shardline_hub_creation_denials;
DROP TABLE IF EXISTS shardline_hub_creation_policies;
ALTER TABLE shardline_hub_repos DROP COLUMN creation_subject;
ALTER TABLE shardline_hub_repos DROP COLUMN creation_issuer;
ALTER TABLE shardline_hub_repos DROP COLUMN creation_authorized;
