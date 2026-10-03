CREATE INDEX IF NOT EXISTS shardline_tree_entries_prefix_pattern_idx
    ON shardline_tree_entries (provider, owner, repo, revision, path text_pattern_ops);
