CREATE INDEX IF NOT EXISTS shardline_hub_repos_search_prefix_idx
    ON shardline_hub_repos (repo_type, repo_id text_pattern_ops);
