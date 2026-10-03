CREATE INDEX IF NOT EXISTS shardline_hub_file_entries_page_idx
    ON shardline_hub_file_entries (commit_sha, path COLLATE "C");
