use axum::{
    Json,
    extract::{Path, Query, State},
    http::HeaderMap,
    http::StatusCode,
};
use shardline_index::hub::{BoxedHubStore, HubFileEntry, HubRepo, HubRepoType};

use crate::commit::{CommitInstruction, ParsedCommit};
use crate::error::HubApiError;

use super::*;

#[test]
fn parse_csv_line_simple_fields() {
    let result = parse_csv_line("a,b,c");
    assert_eq!(result, vec!["a", "b", "c"]);
}

#[test]
fn parse_csv_line_quoted_field_with_comma() {
    let result = parse_csv_line(r#""hello, world",b"#);
    assert_eq!(result, vec!["hello, world", "b"]);
}

#[test]
fn parse_csv_line_escaped_quote() {
    let result = parse_csv_line(r#""say ""hello""",done"#);
    assert_eq!(result, vec![r#"say ""hello"""#, "done"]);
}

#[test]
fn parse_csv_line_trailing_comma() {
    let result = parse_csv_line("a,b,");
    assert_eq!(result, vec!["a", "b", ""]);
}

#[test]
fn parse_csv_line_single_field() {
    let result = parse_csv_line("only");
    assert_eq!(result, vec!["only"]);
}

#[test]
fn parse_csv_line_empty_field() {
    let result = parse_csv_line("a,,c");
    assert_eq!(result, vec!["a", "", "c"]);
}

#[test]
fn parse_csv_line_unterminated_quote() {
    let result = parse_csv_line(r#""unterminated,a"#);
    assert_eq!(result, vec!["unterminated,a"]);
}

// --- repo_delete endpoint logic tests ---
// These test the store operations that the repo_delete handler relies on,
// using BoxedHubStore with the same flow the handler follows.

fn make_delete_test_store() -> (tempfile::TempDir, BoxedHubStore) {
    let ts = tempfile::tempdir().expect("tempdir");
    let root = ts.path();

    // Create hub tables using the public API
    shardline_index::hub::ensure_hub_tables(root).expect("ensure hub tables");

    let store = shardline_index::LocalIndexStore::open(root.to_path_buf());
    let boxed = BoxedHubStore::from_store(store);
    (ts, boxed)
}

/// Helper: returns (TempDir, HubState) with no auth.
fn make_test_state() -> (tempfile::TempDir, HubState) {
    let (td, store) = make_delete_test_store();
    let object_store = shardline_server_core::ServerObjectStore::local(td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    (td, state)
}

#[test]
fn repo_delete_cleans_up_revisions() {
    let (_ts, store) = make_delete_test_store();

    use shardline_index::hub::{HubFileEntry, HubRepoType};

    store
        .create_repo(HubRepoType::Model, "org/model", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

    // Add a commit
    store
        .create_revision(
            "org/model",
            Some(initial_sha),
            "sha1",
            "main",
            "first commit",
        )
        .unwrap();

    // Store files for the commit
    let files = vec![HubFileEntry {
        path: "README.md".into(),
        size: 100,
        sha: "sha_readme".into(),
        is_lfs: false,
    }];
    store.store_files("sha1", &files).unwrap();

    // Verify data exists
    assert_eq!(store.list_revisions("org/model").unwrap().len(), 2);
    assert_eq!(store.get_files("sha1").unwrap().len(), 1);

    // Delete — mirrors what repo_delete handler does
    store.delete_repo("org/model").unwrap();

    // Verify everything is gone
    assert!(store.get_repo("org/model").unwrap().is_none());
    assert!(store.list_revisions("org/model").unwrap().is_empty());
    assert!(store.get_files("sha1").unwrap().is_empty());
}

#[test]
fn repo_delete_cleans_up_webhooks() {
    let (_ts, store) = make_delete_test_store();

    use shardline_index::hub::HubRepoType;

    store
        .create_repo(HubRepoType::Model, "org/model", false)
        .unwrap();

    // Create webhook
    store
        .create_webhook(
            "org/model",
            "https://example.com/hook",
            &["push".into()],
            None,
        )
        .unwrap();

    assert_eq!(store.list_webhooks("org/model").unwrap().len(), 1);

    // Delete
    store.delete_repo("org/model").unwrap();

    // Verify webhook is gone
    assert!(store.list_webhooks("org/model").unwrap().is_empty());
}

#[test]
fn repo_delete_idempotent() {
    let (_ts, store) = make_delete_test_store();

    use shardline_index::hub::HubRepoType;

    store
        .create_repo(HubRepoType::Model, "org/model", false)
        .unwrap();

    // First delete succeeds
    store.delete_repo("org/model").unwrap();
    assert!(store.get_repo("org/model").unwrap().is_none());

    // Second delete is also fine (no-op, no rows affected)
    store.delete_repo("org/model").unwrap();
    assert!(store.get_repo("org/model").unwrap().is_none());
}

// -----------------------------------------------------------------------
// repo_type_path
// -----------------------------------------------------------------------

#[test]
fn repo_type_path_model() {
    assert_eq!(repo_type_path(HubRepoType::Model), "models");
}

#[test]
fn repo_type_path_dataset() {
    assert_eq!(repo_type_path(HubRepoType::Dataset), "datasets");
}

#[test]
fn repo_type_path_space() {
    assert_eq!(repo_type_path(HubRepoType::Space), "spaces");
}

// -----------------------------------------------------------------------
// sanitize_log_url
// -----------------------------------------------------------------------

#[test]
fn sanitize_log_url_truncates_long() {
    let base = "https://example.com/";
    let long = format!("{}{}", base, "a".repeat(300));
    let result = sanitize_log_url(&long);
    assert!(result.len() <= 204); // 200 chars + "..."
    assert!(result.ends_with("..."));
}

// -----------------------------------------------------------------------
// repo_response_from_hub
// -----------------------------------------------------------------------

#[test]
fn repo_response_from_hub_model() {
    use shardline_index::hub::HubRepoType;
    let hub_repo = HubRepo {
        repo_id: "org/my-model".to_owned(),
        repo_type: HubRepoType::Model,
        private: false,
        default_branch: "main".to_owned(),
        created_at_unix_seconds: 1_700_000_000,
        updated_at_unix_seconds: 1_700_000_001,
    };
    let resp = repo_response_from_hub(&hub_repo);
    assert_eq!(resp.id, "org/my-model");
    assert_eq!(resp.repo_type, RepoType::Model);
    assert!(!resp.private);
    assert_eq!(resp.url, "/org/my-model");
    assert_eq!(resp.default_branch.as_deref(), Some("main"));
    let lm = resp
        .last_modified
        .as_deref()
        .expect("last_modified should be Some");
    // Should be a valid RFC 3339 timestamp around 2023-11-14
    assert!(
        lm.contains("2023-11-14T22"),
        "expected 2023-11-14T22... got {lm}"
    );
}

#[test]
fn repo_response_from_hub_dataset() {
    use shardline_index::hub::HubRepoType;
    let hub_repo = HubRepo {
        repo_id: "org/data".to_owned(),
        repo_type: HubRepoType::Dataset,
        private: true,
        default_branch: "main".to_owned(),
        created_at_unix_seconds: 0,
        updated_at_unix_seconds: 0,
    };
    let resp = repo_response_from_hub(&hub_repo);
    assert_eq!(resp.repo_type, RepoType::Dataset);
    assert!(resp.private);
    assert_eq!(resp.url, "/datasets/org/data");
}

#[test]
fn repo_response_from_hub_space() {
    use shardline_index::hub::HubRepoType;
    let hub_repo = HubRepo {
        repo_id: "org/space1".to_owned(),
        repo_type: HubRepoType::Space,
        private: false,
        default_branch: "main".to_owned(),
        created_at_unix_seconds: 0,
        updated_at_unix_seconds: 0,
    };
    let resp = repo_response_from_hub(&hub_repo);
    assert_eq!(resp.repo_type, RepoType::Space);
    assert_eq!(resp.url, "/spaces/org/space1");
}

// -----------------------------------------------------------------------
// webhook_response_from_hub
// -----------------------------------------------------------------------

#[test]
fn webhook_response_from_hub_basic() {
    use shardline_index::hub::HubWebhook;
    use shardline_protocol::SecretString;
    let hook = HubWebhook {
        id: "wh_123".to_owned(),
        repo_id: "org/repo".to_owned(),
        url: "https://example.com/hook".to_owned(),
        events: vec!["push".to_owned()],
        secret: Some(SecretString::from_secret("s3cret")),
        active: true,
        created_at_unix_seconds: 42,
    };
    let resp = webhook_response_from_hub(&hook);
    assert_eq!(resp.id, "wh_123");
    assert_eq!(resp.url, "https://example.com/hook");
    assert_eq!(resp.events, vec!["push"]);
    assert!(resp.active);
    assert_eq!(resp.created_at, 42);
}

#[test]
fn webhook_response_from_hub_inactive() {
    use shardline_index::hub::HubWebhook;
    let hook = HubWebhook {
        id: "wh_2".to_owned(),
        repo_id: "org/repo".to_owned(),
        url: "http://hook.example".to_owned(),
        events: vec!["push".to_owned(), "delete".to_owned()],
        secret: None,
        active: false,
        created_at_unix_seconds: 99,
    };
    let resp = webhook_response_from_hub(&hook);
    assert!(!resp.active);
    assert_eq!(resp.events.len(), 2);
}

// -----------------------------------------------------------------------
// parse_yaml_frontmatter
// -----------------------------------------------------------------------

#[test]
fn parse_yaml_frontmatter_valid_simple() {
    let content = b"---\nkey: value\n---\n# README\nHello";
    let result = parse_yaml_frontmatter(content);
    assert!(result.is_some());
    let obj = result.unwrap();
    assert_eq!(obj.get("key").and_then(|v| v.as_str()), Some("value"));
}

#[test]
fn parse_yaml_frontmatter_with_quoted_value() {
    let content = b"---\ntitle: 'My Model'\n---\nbody";
    let result = parse_yaml_frontmatter(content);
    assert!(result.is_some());
    let obj = result.unwrap();
    assert_eq!(obj.get("title").and_then(|v| v.as_str()), Some("My Model"));
}

#[test]
fn parse_yaml_frontmatter_no_frontmatter() {
    let content = b"Just a README\nno frontmatter";
    assert!(parse_yaml_frontmatter(content).is_none());
}

#[test]
fn parse_yaml_frontmatter_empty_yaml() {
    let content = b"---\n---\nbody";
    assert!(parse_yaml_frontmatter(content).is_none());
}

#[test]
fn parse_yaml_frontmatter_only_comments() {
    let content = b"---\n# just a comment\n---\nbody";
    assert!(parse_yaml_frontmatter(content).is_none());
}

#[test]
fn parse_yaml_frontmatter_numeric_value() {
    let content = b"---\nlikes: 42\n---\nbody";
    let result = parse_yaml_frontmatter(content).unwrap();
    assert_eq!(result.get("likes").and_then(|v| v.as_u64()), Some(42));
}

#[test]
fn parse_yaml_frontmatter_boolean_value() {
    let content = b"---\nprivate: true\n---\nbody";
    let result = parse_yaml_frontmatter(content).unwrap();
    assert_eq!(result.get("private").and_then(|v| v.as_bool()), Some(true));
}

#[test]
fn parse_yaml_frontmatter_double_quoted_value() {
    let content = b"---\nname: \"hello world\"\n---\nbody";
    let result = parse_yaml_frontmatter(content).unwrap();
    assert_eq!(
        result.get("name").and_then(|v| v.as_str()),
        Some("hello world")
    );
}

#[test]
fn parse_yaml_frontmatter_not_utf8() {
    let content = b"\xff\xfe\x00\x01";
    assert!(parse_yaml_frontmatter(content).is_none());
}

// -----------------------------------------------------------------------
// tree_entries_at_path
// -----------------------------------------------------------------------

#[test]
fn tree_entries_at_path_root_lists_files_and_dirs() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![
        HubFileEntry {
            path: "README.md".into(),
            size: 100,
            sha: "a".into(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "src/main.rs".into(),
            size: 200,
            sha: "b".into(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "src/lib.rs".into(),
            size: 300,
            sha: "c".into(),
            is_lfs: true,
        },
        HubFileEntry {
            path: "data/big.bin".into(),
            size: 5_000_000,
            sha: "d".into(),
            is_lfs: true,
        },
    ];
    let entries = tree_entries_at_path(&files, "");
    assert_eq!(
        entries.len(),
        3,
        "expected 3 entries: README.md, src/, data/"
    );
    // Directories come before files (sorted by type then path)
    assert_eq!(entries[0].entry_type, "directory");
    assert_eq!(entries[0].path, "data");
    assert_eq!(entries[1].entry_type, "directory");
    assert_eq!(entries[1].path, "src");
    assert_eq!(entries[2].entry_type, "file");
    assert_eq!(entries[2].path, "README.md");
    assert_eq!(entries[2].size, Some(100));
    assert!(entries[2].lfs.is_none());
}

#[test]
fn tree_entries_at_path_nested() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![
        HubFileEntry {
            path: "src/main.rs".into(),
            size: 200,
            sha: "b".into(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "src/lib.rs".into(),
            size: 300,
            sha: "c".into(),
            is_lfs: true,
        },
    ];
    let entries = tree_entries_at_path(&files, "src");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].entry_type, "file");
    assert_eq!(entries[0].path, "lib.rs");
    assert!(entries[0].lfs.is_some());
    assert_eq!(entries[1].entry_type, "file");
    assert_eq!(entries[1].path, "main.rs");
}

#[test]
fn tree_entries_at_path_empty_dir() {
    let entries = tree_entries_at_path(&[], "");
    assert!(entries.is_empty());
}

// -----------------------------------------------------------------------
// tree_entries_recursive
// -----------------------------------------------------------------------

#[test]
fn tree_entries_recursive_root() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![
        HubFileEntry {
            path: "README.md".into(),
            size: 100,
            sha: "a".into(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "src/main.rs".into(),
            size: 200,
            sha: "b".into(),
            is_lfs: false,
        },
    ];
    let entries = tree_entries_recursive(&files, "");
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].path, "README.md");
    assert_eq!(entries[1].path, "src/main.rs");
}

#[test]
fn tree_entries_recursive_nested() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![
        HubFileEntry {
            path: "src/main.rs".into(),
            size: 200,
            sha: "b".into(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "src/lib.rs".into(),
            size: 300,
            sha: "c".into(),
            is_lfs: true,
        },
    ];
    let entries = tree_entries_recursive(&files, "src");
    assert_eq!(entries.len(), 2);
}

#[test]
fn tree_entries_recursive_lfs_shows_lfs_info() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "model.bin".into(),
        size: 5_000_000,
        sha: "abcd".into(),
        is_lfs: true,
    }];
    let entries = tree_entries_recursive(&files, "");
    assert_eq!(entries.len(), 1);
    let lfs = entries[0].lfs.as_ref().expect("expected LFS info");
    assert_eq!(lfs.oid, "abcd");
    assert_eq!(lfs.size, 5_000_000);
}

#[test]
fn tree_entries_recursive_empty() {
    assert!(tree_entries_recursive(&[], "").is_empty());
}

// -----------------------------------------------------------------------
// find_dataset_file
// -----------------------------------------------------------------------

#[test]
fn find_dataset_file_default_train() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "data/train/data.jsonl".into(),
        size: 100,
        sha: "abc".into(),
        is_lfs: false,
    }];
    let result = find_dataset_file(&files, "default", "train");
    assert!(result.is_some());
    assert_eq!(result.unwrap().path, "data/train/data.jsonl");
}

#[test]
fn find_dataset_file_config_split() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "myconfig/test/data.parquet".into(),
        size: 200,
        sha: "def".into(),
        is_lfs: false,
    }];
    let result = find_dataset_file(&files, "myconfig", "test");
    assert!(result.is_some());
    assert_eq!(result.unwrap().path, "myconfig/test/data.parquet");
}

#[test]
fn find_dataset_file_split_only() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "train/data.csv".into(),
        size: 300,
        sha: "ghi".into(),
        is_lfs: false,
    }];
    let result = find_dataset_file(&files, "default", "train");
    assert!(result.is_some());
    assert_eq!(result.unwrap().path, "train/data.csv");
}

#[test]
fn find_dataset_file_root() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "data.parquet".into(),
        size: 400,
        sha: "jkl".into(),
        is_lfs: false,
    }];
    let result = find_dataset_file(&files, "default", "train");
    assert!(result.is_some());
    assert_eq!(result.unwrap().path, "data.parquet");
}

#[test]
fn find_dataset_file_not_found() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "other.txt".into(),
        size: 10,
        sha: "x".into(),
        is_lfs: false,
    }];
    assert!(find_dataset_file(&files, "default", "train").is_none());
}

// -----------------------------------------------------------------------
// parse_jsonl_rows
// -----------------------------------------------------------------------

#[test]
fn parse_jsonl_rows_simple() {
    let text = "{\"a\":1,\"b\":2}\n{\"a\":3,\"b\":4}";
    let rows = parse_jsonl_rows(text, 0, 10).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns.get("a"), Some(&serde_json::json!(1)));
    assert_eq!(rows[1].columns.get("b"), Some(&serde_json::json!(4)));
}

#[test]
fn parse_jsonl_rows_with_offset_and_limit() {
    let text = "{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n{\"n\":5}";
    let rows = parse_jsonl_rows(text, 2, 2).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns.get("n"), Some(&serde_json::json!(3)));
    assert_eq!(rows[1].columns.get("n"), Some(&serde_json::json!(4)));
}

#[test]
fn parse_jsonl_rows_empty_lines() {
    let text = "{\"a\":1}\n\n{\"a\":2}\n";
    let rows = parse_jsonl_rows(text, 0, 10).unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn parse_jsonl_rows_invalid_json() {
    let text = "{\"a\":1}\nnot_json\n{\"a\":2}";
    let result = parse_jsonl_rows(text, 0, 10);
    assert!(result.is_err());
}

#[test]
fn parse_jsonl_rows_empty_input() {
    let rows = parse_jsonl_rows("", 0, 10).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn parse_jsonl_rows_limit_bounds() {
    let text = "{\"x\":1}\n{\"x\":2}\n{\"x\":3}";
    let rows = parse_jsonl_rows(text, 0, 0).unwrap();
    assert!(rows.is_empty());
}

// -----------------------------------------------------------------------
// parse_csv_rows
// -----------------------------------------------------------------------

#[test]
fn parse_csv_rows_simple() {
    let text = "name,age\nAlice,30\nBob,25";
    let rows = parse_csv_rows(text, 0, 10).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].columns.get("name"),
        Some(&serde_json::json!("Alice"))
    );
    assert_eq!(rows[1].columns.get("age"), Some(&serde_json::json!(25)));
}

#[test]
fn parse_csv_rows_with_offset_and_limit() {
    let text = "n\n1\n2\n3\n4\n5";
    let rows = parse_csv_rows(text, 2, 2).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].columns.get("n"), Some(&serde_json::json!(3)));
    assert_eq!(rows[1].columns.get("n"), Some(&serde_json::json!(4)));
}

#[test]
fn parse_csv_rows_quoted_fields() {
    let text = r#"name,description
Alice,"hello, world"
Bob,"say ""hi"""#;
    let rows = parse_csv_rows(text, 0, 10).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].columns.get("description"),
        Some(&serde_json::json!("hello, world"))
    );
}

#[test]
fn parse_csv_rows_empty_input() {
    let result = parse_csv_rows("", 0, 10);
    assert!(result.is_err()); // no header
}

#[test]
fn parse_csv_rows_header_only() {
    let rows = parse_csv_rows("a,b,c", 0, 10).unwrap();
    assert!(rows.is_empty());
}

#[test]
fn parse_csv_rows_skip_empty() {
    let text = "a\n1\n\n2\n";
    let rows = parse_csv_rows(text, 0, 10).unwrap();
    assert_eq!(rows.len(), 2);
}

// -----------------------------------------------------------------------
// parse_rows_from_content (routing function)
// -----------------------------------------------------------------------

#[test]
fn parse_rows_from_content_jsonl() {
    let content = b"{\"x\":1}";
    let rows = parse_rows_from_content(content, "data.jsonl", 0, 10).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn parse_rows_from_content_csv() {
    let content = b"a,b\n1,2";
    let rows = parse_rows_from_content(content, "data.csv", 0, 10).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn parse_rows_from_content_unsupported_format() {
    let content = b"some data";
    let result = parse_rows_from_content(content, "data.txt", 0, 10);
    assert!(result.is_err());
}

#[test]
fn parse_rows_from_content_invalid_utf8() {
    let content = b"\xff\xfe\x00";
    let result = parse_rows_from_content(content, "data.csv", 0, 10);
    assert!(result.is_err());
}

// -----------------------------------------------------------------------
// is_cgnat
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// authorize (route-level helper)
// -----------------------------------------------------------------------

#[test]
fn route_authorize_without_auth_is_permissive() {
    let (_td, state) = make_test_state();
    let headers = HeaderMap::new();
    assert!(authorize(&state, &headers, TokenScope::Write).is_ok());
}

// -----------------------------------------------------------------------
// parse_csv_line — additional edge cases
// -----------------------------------------------------------------------

#[test]
fn parse_csv_line_multiple_quoted_fields() {
    let result = parse_csv_line(r#""a","b","c""#);
    assert_eq!(result, vec!["a", "b", "c"]);
}

#[test]
fn parse_csv_line_escaped_quote_middle() {
    let result = parse_csv_line(r#"a,"say ""hello""",b"#);
    // The escped quote inside: ""hello"" contains two double-quotes
    assert_eq!(result[0], "a");
    assert!(result[1].contains("say"));
    assert_eq!(result[2], "b");
}

#[test]
fn parse_csv_line_consecutive_commas() {
    let result = parse_csv_line("a,,,c");
    assert_eq!(result, vec!["a", "", "", "c"]);
}

#[test]
fn parse_csv_line_starts_with_comma() {
    let result = parse_csv_line(",a,b");
    assert_eq!(result, vec!["", "a", "b"]);
}

#[test]
fn parse_csv_line_ends_with_unterminated_quote_comma() {
    // Input: "unterm,, (starts with quote, no closing quote found, ends with comma)
    // The unterminated quote branch pushes content after opening quote: "unterm,,"
    // Then trailing comma adds an extra empty field
    let result = parse_csv_line(r#""unterm,,"#);
    assert_eq!(result, vec!["unterm,,", ""]);
}

#[test]
fn parse_csv_line_quoted_escaped_quote_at_end() {
    let result = parse_csv_line(r#""say ""hi""",done"#);
    assert_eq!(result.len(), 2);
}

// -----------------------------------------------------------------------
// is_private_ip — IPv4-mapped IPv6
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// validate_webhook_url — edge cases
// -----------------------------------------------------------------------

// -----------------------------------------------------------------------
// HubState debug
// -----------------------------------------------------------------------

#[test]
fn hub_state_debug_redacts_auth() {
    let (_td, state) = make_test_state();
    let debug = format!("{state:?}");
    assert!(debug.contains("auth"));
}

// -----------------------------------------------------------------------
// parse_csv_line — remaining edge cases
// -----------------------------------------------------------------------

#[test]
fn parse_csv_line_quoted_field_with_trailing_comma() {
    // After closing quote, comma should be skipped
    let result = parse_csv_line(r#""a","#);
    assert_eq!(result, vec!["a", ""]);
}

#[test]
fn parse_csv_line_only_commas() {
    let result = parse_csv_line(",,");
    assert_eq!(result, vec!["", "", ""]);
}

#[test]
fn parse_csv_line_unterminated_quote_only() {
    let result = parse_csv_line("\"");
    assert_eq!(result, vec![""]);
}

// -----------------------------------------------------------------------
// parse_yaml_frontmatter — additional edge cases
// -----------------------------------------------------------------------

#[test]
fn parse_yaml_frontmatter_no_closing_delimiter() {
    let content = b"---\nkey: value\n";
    assert!(parse_yaml_frontmatter(content).is_none());
}

#[test]
fn parse_yaml_frontmatter_closing_on_same_line() {
    // Closing --- on its own line (no trailing newline) is valid.
    let content = b"---\nkey: value\n---";
    let result = parse_yaml_frontmatter(content);
    assert!(result.is_some(), "closing --- on its own line is valid");
    assert_eq!(
        result.unwrap().get("key").and_then(|v| v.as_str()),
        Some("value")
    );
}

#[test]
fn parse_yaml_frontmatter_line_without_colon_skipped() {
    let content = b"---\nkey: value\nno_colon_line\nother: val\n---\n";
    let result = parse_yaml_frontmatter(content).unwrap();
    assert_eq!(result.get("key").and_then(|v| v.as_str()), Some("value"));
    assert_eq!(result.get("other").and_then(|v| v.as_str()), Some("val"));
    assert!(result.get("no_colon_line").is_none());
}

#[test]
fn parse_yaml_frontmatter_json_number_and_bool_values() {
    let content = b"---\ncount: 42\nactive: true\n---\n";
    let result = parse_yaml_frontmatter(content).unwrap();
    assert_eq!(result.get("count").and_then(|v| v.as_u64()), Some(42));
    assert_eq!(result.get("active").and_then(|v| v.as_bool()), Some(true));
}

// -----------------------------------------------------------------------
// tree_entries_at_path — LFS file at root level
// -----------------------------------------------------------------------

#[test]
fn tree_entries_at_path_lfs_file_at_root() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "model.bin".into(),
        size: 5_000_000,
        sha: "oid123".into(),
        is_lfs: true,
    }];
    let entries = tree_entries_at_path(&files, "");
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].entry_type, "file");
    assert!(entries[0].lfs.is_some());
    let lfs = entries[0].lfs.as_ref().unwrap();
    assert_eq!(lfs.oid, "oid123");
    assert_eq!(lfs.size, 5_000_000);
}

// -----------------------------------------------------------------------
// tree_entries_recursive — empty prefix edge case
// -----------------------------------------------------------------------

#[test]
fn tree_entries_recursive_non_matching_prefix() {
    use shardline_index::hub::HubFileEntry;
    let files = vec![HubFileEntry {
        path: "other/file.txt".into(),
        size: 10,
        sha: "x".into(),
        is_lfs: false,
    }];
    let entries = tree_entries_recursive(&files, "nonexistent");
    assert!(entries.is_empty());
}

// ====================================================================
// Handler-level integration tests (real LocalIndexStore)
// ====================================================================

/// Helper: creates a model repo + initial revision + optional files in a store.
fn make_store_with_repo(
    repo_type: HubRepoType,
    repo_id: &str,
) -> (tempfile::TempDir, BoxedHubStore) {
    let (td, store) = make_delete_test_store();
    store
        .create_repo(repo_type, repo_id, false)
        .expect("create_repo");
    (td, store)
}

fn make_store_with_revision(
    rt: HubRepoType,
    repo_id: &str,
    rev_sha: &str,
    files: &[HubFileEntry],
) -> (tempfile::TempDir, BoxedHubStore) {
    let (td, store) = make_store_with_repo(rt, repo_id);
    // Parent must match the default_branch SHA set by create_repo (empty tree).
    let parent = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    let _ = store
        .create_revision(repo_id, Some(parent), rev_sha, "main", "first")
        .expect("create_revision");
    if !files.is_empty() {
        store.store_files(rev_sha, files).expect("store_files");
    }
    (td, store)
}

/// Pre-populate ObjectStore with content for a given SHA.
fn store_test_content(td: &tempfile::TempDir, sha: &str, content: &[u8]) {
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
    let object_store = shardline_server_core::ServerObjectStore::local(td.path().join("lfs"))
        .expect("local object store");
    // Global namespace (None scope) — matches the permissive-mode read paths.
    let key = crate::routes::lfs_object_key(
        sha,
        &shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    )
    .expect("valid key");
    let body = ObjectBody::from_slice(content);
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(content).as_bytes()),
        content.len() as u64,
    );
    object_store
        .put_if_absent(&key, body, &integrity)
        .expect("store test content");
}

fn default_headers() -> HeaderMap {
    HeaderMap::new()
}

// ------------------------------------------------------------------
// health
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_health_returns_ok() {
    let result = health().await;
    assert_eq!(result.get("status").and_then(|v| v.as_str()), Some("ok"));
}

// ------------------------------------------------------------------
// whoami
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_whoami_anonymous_without_auth() {
    let (_td, state) = make_test_state();
    let result = whoami(State(state), default_headers()).await;
    assert!(result.is_ok());
    let resp = result.unwrap();
    assert_eq!(resp.name, "anonymous");
    assert!(!resp.is_admin);
}

// ------------------------------------------------------------------
// repo_list
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_list_empty() {
    let (_td, state) = make_test_state();
    let result = repo_list(State(state.clone()), test_repo(&state, &default_headers()))
        .await
        .unwrap();
    assert!(result.repos.is_empty());
}

#[tokio::test]
async fn handler_repo_list_with_repos() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/repo-a");
    store
        .create_repo(HubRepoType::Dataset, "org/data-b", false)
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_list(State(state.clone()), test_repo(&state, &default_headers()))
        .await
        .unwrap();
    assert_eq!(result.repos.len(), 2);
}

// ------------------------------------------------------------------
// repo_search
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_search_short_query_rejected() {
    let (_td, state) = make_test_state();
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "x".into(),
            author: None,
            sort: None,
            direction: None,
            limit: 50,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("at least 2")),
        "expected PathValidation, got {err:?}"
    );
}

#[tokio::test]
async fn handler_repo_search_invalid_type() {
    let (_td, state) = make_test_state();
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("invalid".to_string()),
        Query(RepoSearchQuery {
            q: "test".into(),
            author: None,
            sort: None,
            direction: None,
            limit: 50,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("invalid repo type")),
        "expected PathValidation, got {err:?}"
    );
}

#[tokio::test]
async fn handler_repo_search_finds_matching() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/my-model");
    store
        .create_repo(HubRepoType::Model, "other/other", false)
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // The search uses LIKE 'q%' on repo_id, so prefix-match the full ID.
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "org/my-model".into(),
            author: None,
            sort: None,
            direction: None,
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.repos.len(), 1);
    assert_eq!(result.repos[0].id, "org/my-model");
}

// ------------------------------------------------------------------
// Cross-tenant private-repo leak (F-deep-1)
// ------------------------------------------------------------------

/// Builds a `HubState` with a mock auth provider that always authenticates a
/// caller with repository scope `alice/own` (owner `alice`) and Read scope.
fn make_alice_auth_state() -> (tempfile::TempDir, HubState) {
    use shardline_protocol::TokenScope as ProtoScope;
    make_scoped_alice_auth_state(ProtoScope::Read)
}

/// Like `make_alice_auth_state`, but the minted token grants Write scope, which
/// the repo-create and webhook-create handlers require.
fn make_write_alice_auth_state() -> (tempfile::TempDir, HubState) {
    use shardline_protocol::TokenScope as ProtoScope;
    make_scoped_alice_auth_state(ProtoScope::Write)
}

/// Shared builder for an auth-configured `HubState` whose mock provider always
/// verifies a token scoped to `alice/own` with the given token scope.
fn make_scoped_alice_auth_state(
    scope: shardline_protocol::TokenScope,
) -> (tempfile::TempDir, HubState) {
    use shardline_protocol::{
        RepositoryProvider, RepositoryScope, TokenClaims, TokenScope as ProtoScope,
    };
    use shardline_server_core::{AuthError, AuthProvider};

    struct AliceProvider {
        scope: ProtoScope,
    }
    impl AuthProvider for AliceProvider {
        fn verify_token(
            &self,
            _token: &str,
        ) -> Result<TokenClaims, shardline_server_core::AuthError> {
            let repo = RepositoryScope::new(RepositoryProvider::Generic, "alice", "own", None)
                .map_err(|_err| AuthError::InvalidToken)?;
            let claims = TokenClaims::new("issuer", "alice", self.scope, repo, u64::MAX)
                .map_err(|_err| AuthError::InvalidToken)?;
            Ok(claims)
        }
        fn mint_token(
            &self,
            _claims: &TokenClaims,
        ) -> Result<String, shardline_server_core::AuthError> {
            Ok("alice-token".into())
        }
    }

    let (td, store) = make_delete_test_store();
    store
        .create_repo(HubRepoType::Model, "bob/private-repo", true)
        .expect("create bob private repo");
    store
        .create_repo(HubRepoType::Model, "bob/public-repo", false)
        .expect("create bob public repo");
    store
        .create_repo(HubRepoType::Model, "alice/own", true)
        .expect("create alice own repo");
    store
        .create_repo(HubRepoType::Model, "alice/other", true)
        .expect("create alice other private repo");
    let object_store = shardline_server_core::ServerObjectStore::local(td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: Some(HubAuth::new(Box::new(AliceProvider { scope }))),
        http_client: None,
        webhook_secret_cipher: None,
    };
    (td, state)
}

fn alice_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        "Bearer alice-token".parse().unwrap(),
    );
    headers
}

#[tokio::test]
async fn repo_list_hides_other_tenants_private_repos() {
    let (_td, state) = make_alice_auth_state();
    let result = repo_list(State(state.clone()), test_repo(&state, &alice_headers()))
        .await
        .unwrap();
    let ids: Vec<String> = result.repos.iter().map(|r| r.id.clone()).collect();
    // Alice's own private repo and bob's public repo remain visible.
    assert!(ids.iter().any(|id| id == "alice/own"));
    assert!(ids.iter().any(|id| id == "bob/public-repo"));
    // bob's private repo must be hidden.
    assert!(
        !ids.iter().any(|id| id == "bob/private-repo"),
        "other tenant's private repo leaked: {ids:?}"
    );
    // A same-namespace private repo (alice/other) must also be hidden: the token
    // is scoped to alice/own, not the whole alice namespace.
    assert!(
        !ids.iter().any(|id| id == "alice/other"),
        "same-namespace private repo leaked: {ids:?}"
    );
}

#[tokio::test]
async fn repo_search_hides_other_tenants_private_repos() {
    let (_td, state) = make_alice_auth_state();
    // Search for bob's private repo by its full ID.
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &alice_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "bob/private-repo".into(),
            author: None,
            sort: None,
            direction: None,
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert!(
        result.repos.is_empty(),
        "other tenant's private repo leaked in search: {:?}",
        result
            .repos
            .iter()
            .map(|r| r.id.clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn repo_search_hides_same_namespace_private_repos() {
    let (_td, state) = make_alice_auth_state();
    // Alice's token is scoped to alice/own; alice/other is the same namespace
    // but a different repository, so it must be hidden.
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &alice_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "alice/other".into(),
            author: None,
            sort: None,
            direction: None,
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert!(
        result.repos.is_empty(),
        "same-namespace private repo leaked in search: {:?}",
        result
            .repos
            .iter()
            .map(|r| r.id.clone())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn repo_list_hides_oidc_owner_same_namespace_private_repos() {
    use shardline_protocol::{
        RepositoryProvider, RepositoryScope, TokenClaims, TokenScope as ProtoScope,
    };
    use shardline_server_core::{AuthError, AuthProvider};

    // Under OIDC every subject is scoped to a single owner (`oidc`); a token
    // for `oidc/user1` must not reveal `oidc/user2`'s private repo.
    struct OidcUser1Provider;
    impl AuthProvider for OidcUser1Provider {
        fn verify_token(
            &self,
            _token: &str,
        ) -> Result<TokenClaims, shardline_server_core::AuthError> {
            let repo = RepositoryScope::new(RepositoryProvider::Generic, "oidc", "user1", None)
                .map_err(|_err| AuthError::InvalidToken)?;
            let claims = TokenClaims::new("issuer", "oidc", ProtoScope::Read, repo, u64::MAX)
                .map_err(|_err| AuthError::InvalidToken)?;
            Ok(claims)
        }
        fn mint_token(
            &self,
            _claims: &TokenClaims,
        ) -> Result<String, shardline_server_core::AuthError> {
            Ok("oidc-token".into())
        }
    }

    let (td, store) = make_delete_test_store();
    store
        .create_repo(HubRepoType::Model, "oidc/user1", true)
        .expect("create oidc user1 private repo");
    store
        .create_repo(HubRepoType::Model, "oidc/user2", true)
        .expect("create oidc user2 private repo");
    store
        .create_repo(HubRepoType::Model, "oidc/shared", false)
        .expect("create oidc public repo");
    let object_store = shardline_server_core::ServerObjectStore::local(td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: Some(HubAuth::new(Box::new(OidcUser1Provider))),
        http_client: None,
        webhook_secret_cipher: None,
    };
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        "Bearer oidc-token".parse().unwrap(),
    );
    let result = repo_list(State(state.clone()), test_repo(&state, &headers))
        .await
        .unwrap();
    let ids: Vec<String> = result.repos.iter().map(|r| r.id.clone()).collect();
    // The caller's own private repo and public repos remain visible.
    assert!(ids.iter().any(|id| id == "oidc/user1"));
    assert!(ids.iter().any(|id| id == "oidc/shared"));
    // The other OIDC subject's private repo in the same namespace must be hidden.
    assert!(
        !ids.iter().any(|id| id == "oidc/user2"),
        "same-namespace OIDC private repo leaked: {ids:?}"
    );
}

// ------------------------------------------------------------------
// repo_info
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_info_missing_repo() {
    let (_td, state) = make_test_state();
    let result = repo_info(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "missing".into(), "nope".into())),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
}

#[tokio::test]
async fn handler_repo_info_returns_repo() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/existing");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_info(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "existing".into())),
    )
    .await
    .unwrap();
    assert_eq!(result.id, "org/existing");
    assert_eq!(result.repo_type, RepoType::Model);
}

#[tokio::test]
async fn handler_repo_info_with_card_data() {
    let readme_content =
        b"---\nlanguage: en\npipeline_tag: text-classification\n---\n# Model\nSome text";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/card-model",
        "sha_card",
        &[HubFileEntry {
            path: "README.md".into(),
            size: readme_content.len() as u64,
            sha: "abababababababababababababababababababababababababababababababab".into(),
            is_lfs: false,
        }],
    );
    store_test_content(
        &_td,
        "abababababababababababababababababababababababababababababababab",
        readme_content,
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_info(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "card-model".into())),
    )
    .await
    .unwrap();
    let card = result.card_data.as_ref().expect("expected card_data");
    assert_eq!(card.get("language").and_then(|v| v.as_str()), Some("en"));
    assert_eq!(
        card.get("pipeline_tag").and_then(|v| v.as_str()),
        Some("text-classification")
    );
}

// ------------------------------------------------------------------
// repo_modelcard
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_modelcard_missing_repo() {
    let (_td, state) = make_test_state();
    let result = repo_modelcard(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "such".into())),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
}

#[tokio::test]
async fn handler_repo_modelcard_no_readme() {
    let (_td, store) = make_store_with_revision(HubRepoType::Model, "org/no-readme", "sha_nr", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_modelcard(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "no-readme".into())),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::NotFound));
}

#[tokio::test]
async fn handler_repo_modelcard_with_readme() {
    let content = b"# My Model\n\nThis is a test model.";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/my-model",
        "sha_rm",
        &[HubFileEntry {
            path: "README.md".into(),
            size: content.len() as u64,
            sha: "1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b".into(),
            is_lfs: false,
        }],
    );
    store_test_content(
        &_td,
        "1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b",
        content,
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_modelcard(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "my-model".into())),
    )
    .await
    .unwrap();
    // Should be a text/markdown response
    let status = result.status();
    assert_eq!(status, 200);
    // Check header
    let ct = result
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("text/markdown; charset=utf-8"));
}

// ------------------------------------------------------------------
// repo_revisions
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_revisions_with_revisions() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/has-revs", "sha_rev1", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_revisions(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "has-revs".into())),
    )
    .await
    .unwrap();
    assert!(!result.revisions.is_empty());
    let rev = &result.revisions[0];
    assert_eq!(rev.ref_name, "main");
    assert_eq!(rev.sha, "sha_rev1");
}

// ------------------------------------------------------------------
// repo_create
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_create_model() {
    let (_td, state) = make_test_state();
    let req = RepoCreateRequest {
        repo_type: RepoType::Model,
        name: "ns/new-repo".to_owned(),
        organization: None,
        private: false,
        visibility: None,
    };
    let (status, json) = repo_create(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Json(req),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["id"], "ns/new-repo");
}

// ------------------------------------------------------------------
// repo_create_type
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_create_type_invalid_type() {
    let (_td, state) = make_test_state();
    let result = repo_create_type(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Path(("invalid".into(), "ns".into(), "repo".into())),
        Json(serde_json::json!({})),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(_)),
        "expected PathValidation, got {err:?}"
    );
}

#[tokio::test]
async fn handler_repo_create_type_success() {
    let (_td, state) = make_test_state();
    let (status, json) = repo_create_type(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "ns".into(), "my-repo".into())),
        Json(serde_json::json!({})),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json.id, "ns/my-repo");
    assert_eq!(json.repo_type, RepoType::Model);
}

#[tokio::test]
async fn handler_repo_create_type_private() {
    let (_td, state) = make_test_state();
    let (_, json) = repo_create_type(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Path(("datasets".into(), "ns".into(), "secret-data".into())),
        Json(serde_json::json!({"private": true})),
    )
    .await
    .unwrap();
    assert!(json.private);
    assert_eq!(json.repo_type, RepoType::Dataset);
}

// ------------------------------------------------------------------
// repo_delete (handler-level)
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_delete_missing_repo() {
    let (_td, state) = make_test_state();
    let result = repo_delete(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "exist".into())),
    )
    .await;
    assert!(matches!(result, Err(HubApiError::RepoNotFound)));
}

#[tokio::test]
async fn handler_repo_delete_success() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/to-delete");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_delete(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "to-delete".into())),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
    // Verify it's gone
    assert!(state.store.get_repo("org/to-delete").unwrap().is_none());
}

// ------------------------------------------------------------------
// preupload
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_preupload_too_many_files() {
    let (_td, state) = make_test_state();
    let files: Vec<PreuploadFile> = (0..10_001)
        .map(|i| PreuploadFile {
            path: format!("file_{i}"),
            lfs: false,
        })
        .collect();
    let result = preupload(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "ns".into(), "r".into(), "main".into())),
        Json(PreuploadRequest {
            files,
            git_attributes: None,
            git_ignore: None,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("exceeds maximum")),
        "expected PathValidation, got {err:?}"
    );
}

#[tokio::test]
async fn handler_preupload_checks_existence() {
    let content = b"existing content";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/preupload-test",
        "sha_pre",
        &[HubFileEntry {
            path: "existing.txt".into(),
            size: content.len() as u64,
            sha: "existing_sha".into(),
            is_lfs: false,
        }],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = preupload(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "preupload-test".into(),
            "main".into(),
        )),
        Json(PreuploadRequest {
            files: vec![
                PreuploadFile {
                    path: "existing.txt".into(),
                    lfs: false,
                },
                PreuploadFile {
                    path: "new.txt".into(),
                    lfs: false,
                },
            ],
            git_attributes: None,
            git_ignore: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.result.len(), 2);
    assert!(result.result[0].exists); // existing.txt
    assert!(!result.result[1].exists); // new.txt
}

// ------------------------------------------------------------------
// commit (via handler)
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_commit_wrong_content_type() {
    let (_td, state) = make_test_state();
    // No Content-Type header → rejection
    let result = commit(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "ns".into(), "r".into(), "main".into())),
        "{}".to_string(),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("Content-Type")),
        "expected PathValidation, got {err:?}"
    );
}

fn ndjson_headers() -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert(
        axum::http::header::CONTENT_TYPE,
        "application/x-ndjson".parse().unwrap(),
    );
    h
}

#[tokio::test]
async fn handler_commit_inline_file_success() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/commit-test", "parent_sha_001", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let body = r#"{"header":{"message":"add readme"}}
{"file":{"path":"README.md","content":"SGVsbG8gV29ybGQ="}}
"#;
    let result = commit(
        State(state.clone()),
        ndjson_headers(),
        test_repo(&state, &ndjson_headers()),
        Path((
            "models".into(),
            "org".into(),
            "commit-test".into(),
            "main".into(),
        )),
        body.to_string(),
    )
    .await;
    assert!(result.is_ok(), "commit failed: {:?}", result.err());
    let resp = result.unwrap();
    assert!(!resp.commit_id.is_empty());
    assert_eq!(resp.ref_name.as_deref(), Some("main"));
}

#[tokio::test]
async fn handler_commit_lfs_pointer_success() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/lfs-commit", "parent_lfs", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // Valid SHA-256 OID (64 hex chars)
    let oid = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    let body = format!(
        r#"{{"header":{{"message":"add lfs file"}}}}
{{"lfsFile":{{"path":"big.bin","oid":"{oid}","size":5000000}}}}
"#
    );
    let result = commit(
        State(state.clone()),
        ndjson_headers(),
        test_repo(&state, &ndjson_headers()),
        Path((
            "models".into(),
            "org".into(),
            "lfs-commit".into(),
            "main".into(),
        )),
        body,
    )
    .await;
    assert!(result.is_ok(), "commit failed: {:?}", result.err());
}

#[tokio::test]
async fn handler_commit_delete_file() {
    // Create a repo with a file, then delete it
    let content = b"to be deleted";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/del-test",
        "parent_del",
        &[HubFileEntry {
            path: "old.txt".into(),
            size: content.len() as u64,
            sha: "old_sha".into(),
            is_lfs: false,
        }],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let body = r#"{"header":{"message":"delete file"}}
{"deletedEntry":{"path":"old.txt"}}
"#;
    let result = commit(
        State(state.clone()),
        ndjson_headers(),
        test_repo(&state, &ndjson_headers()),
        Path((
            "models".into(),
            "org".into(),
            "del-test".into(),
            "main".into(),
        )),
        body.to_string(),
    )
    .await;
    assert!(result.is_ok(), "commit failed: {:?}", result.err());
}

#[tokio::test]
async fn handler_commit_parent_mismatch() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/parent-mismatch",
        "actual_parent_sha",
        &[],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // Body specifies parentCommit that does NOT match URL resolution
    let body = r#"{"header":{"message":"mismatch","parentCommit":"wrong_parent_sha"}}
{"file":{"path":"f.txt","content":"dGVzdA=="}}
"#;
    let result = commit(
        State(state.clone()),
        ndjson_headers(),
        test_repo(&state, &ndjson_headers()),
        Path((
            "models".into(),
            "org".into(),
            "parent-mismatch".into(),
            "main".into(),
        )),
        body.to_string(),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::Conflict(msg) if msg.contains("parentCommit mismatch")),
        "expected Conflict, got {err:?}"
    );
}

// ------------------------------------------------------------------
// apply_commit — direct testing of the core logic
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_apply_commit_inline_file() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/apply-test", "parent_apply", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let parsed = ParsedCommit {
        message: "apply inline".into(),
        parent_commit: None,
        instructions: vec![CommitInstruction::InlineFile {
            path: "hello.txt".into(),
            content: b"world".to_vec(),
        }],
    };
    let repo = test_repo::<true, true>(&state, &HeaderMap::new());
    let result = apply_commit(
        &state,
        "org/apply-test",
        "parent_apply",
        &parsed,
        repo.capability(),
    )
    .await
    .unwrap();
    assert!(!result.commit_id.is_empty());
    // Verify the file is stored
    let _files = state.store.get_files("parent_apply").unwrap();
    // The new commit's files would be stored under the new commit SHA, not parent
    // apply_commit calls store_files(&commit_sha, &files) then create_revision
    // So check files under the new commit SHA
    let new_sha = &result.commit_id;
    let new_files = state.store.get_files(new_sha).unwrap();
    assert_eq!(new_files.len(), 1);
    assert_eq!(new_files[0].path, "hello.txt");
}

#[tokio::test]
async fn handler_apply_commit_lfs_pointer() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/apply-lfs", "parent_lfs2", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let oid = "1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff";
    let parsed = ParsedCommit {
        message: "add lfs".into(),
        parent_commit: None,
        instructions: vec![CommitInstruction::LfsPointer {
            path: "model.bin".into(),
            oid: oid.to_owned(),
            size: 2_000_000,
        }],
    };
    let repo = test_repo::<true, true>(&state, &HeaderMap::new());
    let result = apply_commit(
        &state,
        "org/apply-lfs",
        "parent_lfs2",
        &parsed,
        repo.capability(),
    )
    .await
    .unwrap();
    let new_files = state.store.get_files(&result.commit_id).unwrap();
    assert_eq!(new_files.len(), 1);
    assert!(new_files[0].is_lfs);
    assert_eq!(new_files[0].sha, oid);
}

#[tokio::test]
async fn handler_apply_commit_delete() {
    let content = b"delete me";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/apply-del",
        "parent_del2",
        &[HubFileEntry {
            path: "old.txt".into(),
            size: content.len() as u64,
            sha: "old_sha2".into(),
            is_lfs: false,
        }],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let parsed = ParsedCommit {
        message: "delete file".into(),
        parent_commit: None,
        instructions: vec![CommitInstruction::Delete {
            path: "old.txt".into(),
        }],
    };
    let repo = test_repo::<true, true>(&state, &HeaderMap::new());
    let result = apply_commit(
        &state,
        "org/apply-del",
        "parent_del2",
        &parsed,
        repo.capability(),
    )
    .await
    .unwrap();
    let new_files = state.store.get_files(&result.commit_id).unwrap();
    assert!(new_files.is_empty());
}

#[tokio::test]
async fn handler_apply_commit_parent_mismatch() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/apply-mismatch", "actual_sha", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let parsed = ParsedCommit {
        message: "bad parent".into(),
        parent_commit: Some("different_sha".into()),
        instructions: vec![],
    };
    let repo = test_repo::<true, true>(&state, &HeaderMap::new());
    let result = apply_commit(
        &state,
        "org/apply-mismatch",
        "actual_sha",
        &parsed,
        repo.capability(),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::Conflict(msg) if msg.contains("parentCommit mismatch")),
        "expected Conflict, got {err:?}"
    );
}

// ------------------------------------------------------------------
// file_tree
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_file_tree_basic() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/tree-test",
        "sha_tree",
        &[
            HubFileEntry {
                path: "README.md".into(),
                size: 100,
                sha: "a".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "src/main.rs".into(),
                size: 200,
                sha: "b".into(),
                is_lfs: false,
            },
        ],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let entries = file_tree(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "tree-test".into(),
            "main".into(),
            String::new(),
        )),
        Query(TreeQuery {
            limit: None,
            cursor: None,
            recursive: false,
        }),
    )
    .await
    .unwrap();
    // Expect 2 entries: README.md at root and src/ directory
    assert_eq!(entries.len(), 2);
}

#[tokio::test]
async fn handler_file_tree_recursive() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/tree-rec",
        "sha_tree_rec",
        &[
            HubFileEntry {
                path: "src/main.rs".into(),
                size: 200,
                sha: "b".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "src/lib.rs".into(),
                size: 300,
                sha: "c".into(),
                is_lfs: false,
            },
        ],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let entries = file_tree(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "tree-rec".into(),
            "main".into(),
            String::new(),
        )),
        Query(TreeQuery {
            limit: None,
            cursor: None,
            recursive: true,
        }),
    )
    .await
    .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].entry_type, "file");
}

#[tokio::test]
async fn handler_file_tree_with_limit_and_cursor() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/tree-lim",
        "sha_tree_lim",
        &[
            HubFileEntry {
                path: "a.txt".into(),
                size: 1,
                sha: "s1".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "b.txt".into(),
                size: 2,
                sha: "s2".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "c.txt".into(),
                size: 3,
                sha: "s3".into(),
                is_lfs: false,
            },
        ],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let entries = file_tree(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "tree-lim".into(),
            "main".into(),
            String::new(),
        )),
        Query(TreeQuery {
            limit: Some(2),
            cursor: Some("a.txt".into()),
            recursive: false,
        }),
    )
    .await
    .unwrap();
    // After a.txt cursor: first 2 entries from b.txt, c.txt
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].path, "b.txt");
    assert_eq!(entries[1].path, "c.txt");
}

// ------------------------------------------------------------------
// resolve_file
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_resolve_file_inline() {
    let content = b"hello world file content";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/resolve-test",
        "sha_resolve",
        &[HubFileEntry {
            path: "data.txt".into(),
            size: content.len() as u64,
            sha: "2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c".into(),
            is_lfs: false,
        }],
    );
    // Pre-load file content into ObjectStore
    store_test_content(
        &_td,
        "2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c2c",
        content,
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = resolve_file(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "resolve-test".into(),
            "main".into(),
            "data.txt".into(),
        )),
    )
    .await;
    assert!(result.is_ok(), "resolve_file failed: {:?}", result.err());
    let resp = result.unwrap();
    assert_eq!(resp.status(), 200);
    // Should have application/octet-stream content type
    let ct = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok());
    assert_eq!(ct, Some("application/octet-stream"));
}

#[tokio::test]
async fn handler_resolve_file_not_found() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/resolve-miss", "sha_miss", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = resolve_file(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "resolve-miss".into(),
            "main".into(),
            "nope.txt".into(),
        )),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::NotFound));
}

// ------------------------------------------------------------------
// lfs_batch
// ------------------------------------------------------------------

fn make_lfs_state() -> (tempfile::TempDir, HubState) {
    let (td, store) = make_delete_test_store();
    let object_store = shardline_server_core::ServerObjectStore::local(td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    (td, state)
}

#[tokio::test]
async fn handler_lfs_batch_upload_new_object() {
    let (_td, state) = make_lfs_state();
    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Upload,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0".into(),
                size: 1000,
            }],
            transfers: Vec::new(),
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.transfer, "basic");
    assert_eq!(result.objects.len(), 1);
    let obj = &result.objects[0];
    // Upload of new object: upload action present, no error
    assert!(obj.actions.is_some());
    let actions = obj.actions.as_ref().unwrap();
    assert!(actions.upload.is_some());
    assert!(actions.download.is_none());
    assert!(actions.verify.is_none());
    assert!(obj.error.is_none());
}

#[tokio::test]
async fn handler_lfs_batch_download_existing_object() {
    let (_td, state) = make_lfs_state();
    // Store an LFS object in the object store (global namespace, matching the
    // permissive-mode read path).
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
    let key = crate::routes::lfs_object_key(
        "7171717171717171717171717171717171717171717171717171717171717171",
        &shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    )
    .unwrap();
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(b"some data").as_bytes()),
        9,
    );
    state
        .object_store
        .put_if_absent(&key, ObjectBody::from_slice(b"some data"), &integrity)
        .unwrap();
    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Download,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "7171717171717171717171717171717171717171717171717171717171717171".into(),
                size: 9,
            }],
            transfers: Vec::new(),
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.objects.len(), 1);
    let obj = &result.objects[0];
    assert!(obj.actions.is_some());
    assert!(obj.actions.as_ref().unwrap().download.is_some());
    assert!(obj.error.is_none());
}

#[tokio::test]
async fn handler_lfs_batch_download_missing_object() {
    let (_td, state) = make_lfs_state();
    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Download,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "9393939393939393939393939393939393939393939393939393939393939393".into(),
                size: 100,
            }],
            transfers: Vec::new(),
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    let obj = &result.objects[0];
    // Missing download: actions is None, error is Some(404)
    assert!(obj.actions.is_none());
    let err = obj
        .error
        .as_ref()
        .expect("expected error for missing object");
    assert_eq!(err.code, 404);
}

#[tokio::test]
async fn handler_lfs_batch_verify_existing() {
    let (_td, state) = make_lfs_state();
    // Store an LFS object in the object store (global namespace).
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
    let key = crate::routes::lfs_object_key(
        "8282828282828282828282828282828282828282828282828282828282828282",
        &shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    )
    .unwrap();
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(b"data").as_bytes()),
        4,
    );
    state
        .object_store
        .put_if_absent(&key, ObjectBody::from_slice(b"data"), &integrity)
        .unwrap();
    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Verify,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "8282828282828282828282828282828282828282828282828282828282828282".into(),
                size: 4,
            }],
            transfers: Vec::new(),
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    let obj = &result.objects[0];
    assert!(obj.actions.as_ref().unwrap().verify.is_some());
    assert!(obj.error.is_none());
}

#[tokio::test]
async fn handler_lfs_batch_verify_missing() {
    let (_td, state) = make_lfs_state();
    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Verify,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4a4".into(),
                size: 1,
            }],
            transfers: Vec::new(),
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    let obj = &result.objects[0];
    assert!(obj.actions.is_none());
    assert!(obj.error.is_some());
}

#[tokio::test]
async fn handler_lfs_batch_xet_transfer_negotiated_with_auth() {
    use crate::auth::HubAuth;
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope};
    use shardline_server_core::{AuthError, AuthProvider};

    struct XetProvider;
    impl AuthProvider for XetProvider {
        fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
            let repo =
                RepositoryScope::new(RepositoryProvider::Generic, "alice", "own", None).unwrap();
            Ok(TokenClaims::new("issuer", "alice", TokenScope::Write, repo, u64::MAX).unwrap())
        }
        fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
            Ok("alice-token".into())
        }
    }

    let (td, store) = make_delete_test_store();
    let object_store = shardline_server_core::ServerObjectStore::local(td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: Some(HubAuth::new(Box::new(XetProvider))),
        http_client: None,
        webhook_secret_cipher: None,
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        "Bearer alice-token".parse().unwrap(),
    );

    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &headers),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Upload,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0".into(),
                size: 1000,
            }],
            transfers: vec!["xet".into()],
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        result.transfer, "xet",
        "should negotiate xet when auth present"
    );
}

#[tokio::test]
async fn handler_lfs_batch_xet_transfer_falls_back_without_auth() {
    let (_td, state) = make_lfs_state();

    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Upload,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0".into(),
                size: 1000,
            }],
            transfers: vec!["xet".into()],
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        result.transfer, "basic",
        "should fall back to basic without auth"
    );
}

#[tokio::test]
async fn handler_lfs_batch_xet_transfer_with_basic_fallback() {
    let (_td, state) = make_lfs_state();

    let result = lfs_batch(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(LfsBatchRequest {
            operation: LfsBatchOperation::Upload,
            ref_: LfsBatchRef {
                name: "main".into(),
            },
            objects: vec![LfsObjectRequest {
                oid: "c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0c0".into(),
                size: 1000,
            }],
            transfers: vec!["xet".into(), "basic".into()],
            hash_algo: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(
        result.transfer, "basic",
        "should fall back to basic when xet-only without auth"
    );
}

// ------------------------------------------------------------------
// lfs_upload
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_lfs_upload_invalid_oid() {
    let (_td, state) = make_test_state();
    let result = lfs_upload(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("bad-oid".to_string()),
        bytes::Bytes::from_static(b"data"),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(_)),
        "expected PathValidation, got {err:?}"
    );
}

#[tokio::test]
async fn handler_lfs_upload_success() {
    let (_td, state) = make_test_state();
    let oid = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let result = lfs_upload(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(oid.to_string()),
        bytes::Bytes::from_static(b"some lfs data"),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), StatusCode::OK);
    // Verify it's stored in the object store (global namespace).
    use shardline_storage::ObjectStore;
    let key = crate::routes::lfs_object_key(
        oid,
        &shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    )
    .unwrap();
    assert!(state.object_store.contains(&key).unwrap());
    let data = state
        .object_store
        .read_range(&key, shardline_protocol::ByteRange::new(0, 12).unwrap())
        .unwrap();
    assert_eq!(data, b"some lfs data");
}

// ------------------------------------------------------------------
// lfs_download
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_lfs_download_missing() {
    let (_td, state) = make_test_state();
    let result = lfs_download(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5b5".to_string()),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::NotFound));
}

#[tokio::test]
async fn handler_lfs_download_success() {
    let (_td, state) = make_test_state();
    let oid = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    // Store an LFS object in the object store (global namespace).
    use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore};
    let key = crate::routes::lfs_object_key(
        oid,
        &shardline_server_core::AuthorizedRepository::anonymous_full_access(),
    )
    .unwrap();
    let integrity = ObjectIntegrity::new(
        shardline_protocol::ShardlineHash::from_bytes(*blake3::hash(b"download data").as_bytes()),
        13,
    );
    state
        .object_store
        .put_if_absent(&key, ObjectBody::from_slice(b"download data"), &integrity)
        .unwrap();
    let (status, headers, data) = lfs_download(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(oid.to_string()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(data, b"download data");
    // Verify content-type header name
    assert!(!headers.is_empty());
}

// ------------------------------------------------------------------
// git_head
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_revisions_has_initial() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/init-rev");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_revisions(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "init-rev".into())),
    )
    .await
    .unwrap();
    // create_repo always inserts an initial empty-tree revision
    assert_eq!(result.revisions.len(), 1);
    assert_eq!(result.revisions[0].ref_name, "main");
}

#[tokio::test]
async fn handler_git_head_with_revision() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/has-head", "sha_head123", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = git_head(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "has-head".into())),
    )
    .await
    .unwrap();
    assert!(result.contains("sha_head123"));
    assert!(result.contains("refs/heads/main"));
}

// ------------------------------------------------------------------
// dataset_parquet
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_commit_no_revision() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/no-rev");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // "nonexistent_rev" isn't a known ref or SHA → revision not found
    let result = commit(
        State(state.clone()),
        ndjson_headers(),
        test_repo(&state, &ndjson_headers()),
        Path((
            "models".into(),
            "org".into(),
            "no-rev".into(),
            "nonexistent_rev".into(),
        )),
        r#"{"header":{"message":"x"}}"#.to_string(),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::RevisionNotFound));
}

#[tokio::test]
async fn handler_dataset_parquet_lists_files() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/data",
        "sha_data",
        &[
            HubFileEntry {
                path: "data/train/data.parquet".into(),
                size: 5000,
                sha: "pq".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "README.md".into(),
                size: 10,
                sha: "rm".into(),
                is_lfs: false,
            },
        ],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_parquet(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "data".into())),
    )
    .await
    .unwrap();
    assert_eq!(result.files.len(), 1);
    assert!(result.files[0].path.ends_with(".parquet"));
}

#[tokio::test]
async fn handler_dataset_parquet_csv_and_jsonl_included() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/multi",
        "sha_multi",
        &[
            HubFileEntry {
                path: "a.csv".into(),
                size: 100,
                sha: "csv".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "b.jsonl".into(),
                size: 200,
                sha: "jl".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "c.txt".into(),
                size: 50,
                sha: "txt".into(),
                is_lfs: false,
            },
        ],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_parquet(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "multi".into())),
    )
    .await
    .unwrap();
    assert_eq!(result.files.len(), 2);
}

// ------------------------------------------------------------------
// dataset_first_rows
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_first_rows_empty_dataset() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Dataset, "org/empty-ds", "sha_empty_ds", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_first_rows(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "empty-ds".into())),
        Query(DatasetFirstRowsQuery {
            config: "default".into(),
            split: "train".into(),
            limit: 100,
        }),
    )
    .await
    .unwrap();
    assert!(result.columns.is_empty());
    assert!(result.rows.is_empty());
}

#[tokio::test]
async fn handler_dataset_first_rows_with_jsonl() {
    let jsonl_content = b"{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/jsonl-ds",
        "sha_jsonl",
        &[HubFileEntry {
            path: "data/train/data.jsonl".into(),
            size: jsonl_content.len() as u64,
            sha: "3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d".into(),
            is_lfs: false,
        }],
    );
    store_test_content(
        &_td,
        "3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d3d",
        jsonl_content,
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_first_rows(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "jsonl-ds".into())),
        Query(DatasetFirstRowsQuery {
            config: "default".into(),
            split: "train".into(),
            limit: 10,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.columns.len(), 2);
    assert!(result.columns.contains(&"a".to_string()));
    assert!(result.columns.contains(&"b".to_string()));
    assert_eq!(result.rows.len(), 2);
}

// ------------------------------------------------------------------
// dataset_viewer
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_viewer_with_data() {
    let csv_content = b"name,age\nAlice,30\nBob,25\nCharlie,35\n";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/viewer-ds",
        "sha_viewer",
        &[HubFileEntry {
            path: "default/train/data.csv".into(),
            size: csv_content.len() as u64,
            sha: "4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e".into(),
            is_lfs: false,
        }],
    );
    store_test_content(
        &_td,
        "4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e4e",
        csv_content,
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_viewer(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "viewer-ds".into(), "train".into())),
        Query(DatasetViewerQuery {
            config: "default".into(),
            offset: 0,
            length: 10,
        }),
    )
    .await
    .unwrap();
    // Columns are sorted alphabetically (from BTreeMap)
    assert_eq!(result.columns, vec!["age", "name"]);
    assert_eq!(result.rows.len(), 3);
    assert!(result.num_rows_total.is_none());
}

#[tokio::test]
async fn handler_dataset_viewer_pagination() {
    let csv_content = b"n\n1\n2\n3\n4\n5\n";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/viewer-pag",
        "sha_vp",
        &[HubFileEntry {
            path: "data/test/data.csv".into(),
            size: csv_content.len() as u64,
            sha: "5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f".into(),
            is_lfs: false,
        }],
    );
    store_test_content(
        &_td,
        "5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f5f",
        csv_content,
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_viewer(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "viewer-pag".into(), "test".into())),
        Query(DatasetViewerQuery {
            config: "data".into(),
            offset: 2,
            length: 2,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.rows.len(), 2);
    // rows[0] is the 3rd data row (offset 2): n=3 (CSV values are parsed as primitives)
    assert_eq!(result.rows[0].columns.get("n"), Some(&serde_json::json!(3)));
    assert_eq!(result.rows[1].columns.get("n"), Some(&serde_json::json!(4)));
}

// ------------------------------------------------------------------
// webhook_create
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_create_success() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-test");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let (status, resp) = webhook_create(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-test".into())),
        Json(WebhookCreateRequest {
            url: "https://example.com/hook".into(),
            events: vec!["push".into()],
            secret: None,
        }),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(resp.url, "https://example.com/hook");
    assert!(resp.active);
}

#[tokio::test]
async fn handler_webhook_create_invalid_url() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-badurl");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = webhook_create(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-badurl".into())),
        Json(WebhookCreateRequest {
            url: "ftp://bad.com/hook".into(),
            events: vec!["push".into()],
            secret: None,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("scheme")),
        "expected PathValidation, got {err:?}"
    );
}

#[tokio::test]
async fn handler_webhook_create_too_many_events() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-toomany");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let events: Vec<String> = (0..51).map(|i| format!("event_{i}")).collect();
    let result = webhook_create(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-toomany".into())),
        Json(WebhookCreateRequest {
            url: "https://example.com/hook".into(),
            events,
            secret: None,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("exceeds maximum")),
        "expected PathValidation, got {err:?}"
    );
}

// ------------------------------------------------------------------
// webhook_list
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_list_empty() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-list-empty");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = webhook_list(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-list-empty".into())),
    )
    .await
    .unwrap();
    assert!(result.webhooks.is_empty());
}

#[tokio::test]
async fn handler_webhook_list_with_webhooks() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-list-full");
    store
        .create_webhook(
            "org/wh-list-full",
            "https://hook1.example.com",
            &["push".into()],
            None,
        )
        .unwrap();
    store
        .create_webhook(
            "org/wh-list-full",
            "https://hook2.example.com",
            &["push".into(), "delete".into()],
            Some("secret"),
        )
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = webhook_list(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-list-full".into())),
    )
    .await
    .unwrap();
    assert_eq!(result.webhooks.len(), 2);
}

// ------------------------------------------------------------------
// webhook_delete
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_delete_success() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-del");
    let wh = store
        .create_webhook(
            "org/wh-del",
            "https://example.com/hook",
            &["push".into()],
            None,
        )
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = webhook_delete(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "wh-del".into(),
            wh.id.clone(),
        )),
    )
    .await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
    // Verify it's gone
    let hooks = state.store.list_webhooks("org/wh-del").unwrap();
    assert!(hooks.is_empty());
}

// ------------------------------------------------------------------
// authorize — additional coverage
// ------------------------------------------------------------------

#[test]
fn route_authorize_with_auth_and_no_header_is_err() {
    use shardline_protocol::TokenClaims;
    use shardline_server_core::AuthProvider;
    struct MockProvider;
    impl AuthProvider for MockProvider {
        fn verify_token(
            &self,
            _token: &str,
        ) -> Result<TokenClaims, shardline_server_core::AuthError> {
            Err(shardline_server_core::AuthError::InvalidToken)
        }
        fn mint_token(
            &self,
            _claims: &TokenClaims,
        ) -> Result<String, shardline_server_core::AuthError> {
            Ok("token".into())
        }
    }
    let (object_store_td, store) = make_delete_test_store();
    let object_store =
        shardline_server_core::ServerObjectStore::local(object_store_td.path().join("lfs"))
            .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: Some(HubAuth::new(Box::new(MockProvider))),
        http_client: None,
        webhook_secret_cipher: None,
    };
    let headers = HeaderMap::new();
    let result = authorize(&state, &headers, TokenScope::Read);
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::Unauthorized),
        "expected Unauthorized, got {err:?}"
    );
}

// ------------------------------------------------------------------
// apply_commit — empty instructions
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_apply_commit_empty_instructions() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/empty-inst", "parent_empty", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let parsed = ParsedCommit {
        message: "empty commit".into(),
        parent_commit: None,
        instructions: vec![],
    };
    let repo = test_repo::<true, true>(&state, &HeaderMap::new());
    let result = apply_commit(
        &state,
        "org/empty-inst",
        "parent_empty",
        &parsed,
        repo.capability(),
    )
    .await
    .unwrap();
    assert!(!result.commit_id.is_empty());
}

// ------------------------------------------------------------------
// dataset_parquet — non-dataset repo error
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_parquet_non_dataset_repo_errors() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/model-repo", "sha_model", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_parquet(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "model-repo".into())),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("not a dataset")),
        "expected PathValidation, got {err:?}"
    );
}

// ------------------------------------------------------------------
// dataset_first_rows — non-dataset repo error
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_first_rows_non_dataset_errors() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/model-ds", "sha_model_ds", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_first_rows(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "model-ds".into())),
        Query(DatasetFirstRowsQuery {
            config: "default".into(),
            split: "train".into(),
            limit: 100,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("not a dataset")),
        "expected PathValidation, got {err:?}"
    );
}

// ------------------------------------------------------------------
// dataset_viewer — non-dataset repo error
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_viewer_non_dataset_errors() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/model-view", "sha_model_view", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_viewer(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "model-view".into(), "train".into())),
        Query(DatasetViewerQuery {
            config: "default".into(),
            offset: 0,
            length: 10,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("not a dataset")),
        "expected PathValidation, got {err:?}"
    );
}

// ------------------------------------------------------------------
// repo_search — sort and direction edge cases
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_search_sort_by_last_modified_asc() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/model-a");
    store
        .create_repo(HubRepoType::Model, "org/model-b", false)
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "org/".into(),
            author: None,
            sort: Some("lastModified".into()),
            direction: Some("asc".into()),
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.repos.len(), 2);
}

#[tokio::test]
async fn handler_repo_search_sort_likes_noop() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/likes-a");
    store
        .create_repo(HubRepoType::Model, "org/likes-b", false)
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // "likes" sort is currently a no-op — just verify it doesn't error.
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "org/likes".into(),
            author: None,
            sort: Some("likes".into()),
            direction: None,
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.repos.len(), 2);
}

#[tokio::test]
async fn handler_repo_search_sort_downloads_noop() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/dl-a");
    store
        .create_repo(HubRepoType::Model, "org/dl-b", false)
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // "downloads" sort is currently a no-op — just verify it doesn't error.
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "org/dl".into(),
            author: None,
            sort: Some("downloads".into()),
            direction: None,
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.repos.len(), 2);
}

// ------------------------------------------------------------------
// repo_search with unknown sort (should keep default order)
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_search_unknown_sort_keeps_default_order() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/order-a");
    store
        .create_repo(HubRepoType::Model, "org/order-b", false)
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_search(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path("models".to_string()),
        Query(RepoSearchQuery {
            q: "org/order".into(),
            author: None,
            sort: Some("unknown_field".into()),
            direction: Some("asc".into()),
            limit: 50,
        }),
    )
    .await
    .unwrap();
    assert_eq!(result.repos.len(), 2);
}

// ------------------------------------------------------------------
// repo_info with invalid repo type
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_info_invalid_type() {
    let (_td, state) = make_test_state();
    let result = repo_info(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("invalid_type".into(), "ns".into(), "repo".into())),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("invalid repo type")),
        "expected PathValidation, got {err:?}"
    );
}

// ------------------------------------------------------------------
// dataset_first_rows with inline content missing
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_first_rows_content_not_inline() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/no-inline",
        "sha_no_inline",
        &[HubFileEntry {
            path: "data/train/data.jsonl".into(),
            size: 50,
            sha: "no_inline_sha".into(),
            is_lfs: false,
        }],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_first_rows(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "no-inline".into())),
        Query(DatasetFirstRowsQuery {
            config: "default".into(),
            split: "train".into(),
            limit: 100,
        }),
    )
    .await;
    assert!(result.is_err());
}

// ------------------------------------------------------------------
// dataset_viewer with data file not found
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_viewer_split_not_found() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Dataset, "org/no-split", "sha_no_split", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = dataset_viewer(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "no-split".into(), "nonexistent".into())),
        Query(DatasetViewerQuery {
            config: "default".into(),
            offset: 0,
            length: 10,
        }),
    )
    .await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        matches!(&err, HubApiError::PathValidation(msg) if msg.contains("no data file")),
        "expected PathValidation, got {err:?}"
    );
}

// ------------------------------------------------------------------
// webhook_create with no repo (repo not found)
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_create_repo_not_found() {
    let (_td, state) = make_test_state();
    let result = webhook_create(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "repo".into())),
        Json(WebhookCreateRequest {
            url: "https://example.com/hook".into(),
            events: vec!["push".into()],
            secret: None,
        }),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
}

// ------------------------------------------------------------------
// is_private_ip — broadcast and documentation IPs
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// validate_webhook_url — scheme edge cases
// ------------------------------------------------------------------

// ------------------------------------------------------------------
// parse_yaml_frontmatter — key without colon separator
// ------------------------------------------------------------------

#[test]
fn parse_yaml_frontmatter_line_without_colon_skipped_and_map_empty() {
    // Only a line without colon → map is empty → None
    let content = b"---\nno-colon-here\n---\nbody";
    assert!(parse_yaml_frontmatter(content).is_none());
}

// ------------------------------------------------------------------
// webhook_list without creating a repo first (returns empty)
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_list_repo_not_found_returns_empty() {
    let (_td, state) = make_test_state();
    let result = webhook_list(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "repo".into())),
    )
    .await
    .unwrap();
    assert!(result.webhooks.is_empty());
}

// ------------------------------------------------------------------
// repo_revisions with missing repo
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_revisions_missing_repo() {
    let (_td, state) = make_test_state();
    let result = repo_revisions(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "such_repo".into())),
    )
    .await;
    assert!(result.is_err());
    assert!(matches!(result.unwrap_err(), HubApiError::RepoNotFound));
}

// ------------------------------------------------------------------
// repo_revision_info
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_revision_info_returns_siblings() {
    let content = b"some content";
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/rev-info",
        "sha_rev_info",
        &[HubFileEntry {
            path: "data.txt".into(),
            size: content.len() as u64,
            sha: "file_sha".into(),
            is_lfs: false,
        }],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_revision_info(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "rev-info".into(),
            "main".into(),
        )),
    )
    .await
    .unwrap();
    assert_eq!(result.id, "org/rev-info");
    assert!(result.sha.is_some());
    let siblings = result.siblings.as_ref().expect("expected siblings");
    assert_eq!(siblings.len(), 1);
    assert_eq!(siblings[0]["rfilename"], "data.txt");
}

#[tokio::test]
async fn handler_repo_revision_info_missing_repo() {
    let (_td, state) = make_test_state();
    let result = repo_revision_info(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "repo".into(), "main".into())),
    )
    .await;
    assert!(matches!(result, Err(HubApiError::RepoNotFound)));
}

// ------------------------------------------------------------------
// repo_delete_compat
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_delete_compat_success() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/compat-del");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store: store.clone(),
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_delete_compat(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(RepoDeleteRequest {
            repo_type: Some(RepoType::Model),
            name: "org/compat-del".to_owned(),
            organization: None,
        }),
    )
    .await;
    assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
    assert!(store.get_repo("org/compat-del").unwrap().is_none());
}

#[tokio::test]
async fn handler_repo_delete_compat_with_organization() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/compat-org");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store: store.clone(),
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = repo_delete_compat(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Json(RepoDeleteRequest {
            repo_type: Some(RepoType::Model),
            name: "compat-org".to_owned(),
            organization: Some("org".to_owned()),
        }),
    )
    .await;
    assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
    assert!(store.get_repo("org/compat-org").unwrap().is_none());
}

// ------------------------------------------------------------------
// file_tree_at_root
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_file_tree_at_root_returns_files() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Model,
        "org/tree-root",
        "sha_root_tree",
        &[HubFileEntry {
            path: "README.md".into(),
            size: 50,
            sha: "r_sha".into(),
            is_lfs: false,
        }],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let entries = file_tree_at_root(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path((
            "models".into(),
            "org".into(),
            "tree-root".into(),
            "main".into(),
        )),
        Query(TreeQuery {
            limit: None,
            cursor: None,
            recursive: false,
        }),
    )
    .await
    .unwrap();
    assert!(!entries.is_empty());
}

// ------------------------------------------------------------------
// git_head
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_git_head_returns_ref() {
    let (_td, store) =
        make_store_with_revision(HubRepoType::Model, "org/git-head", "sha_head", &[]);
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = git_head(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "git-head".into())),
    )
    .await
    .unwrap();
    assert!(result.contains("ref: refs/heads/main"));
    assert!(result.contains("sha_head"));
}

#[tokio::test]
async fn handler_git_head_nonexistent_repo_returns_zero_sha() {
    let (_td, state) = make_test_state();
    let result = git_head(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "no".into(), "repo".into())),
    )
    .await
    .unwrap();
    // No revisions → falls back to the zero SHA fallback
    assert!(result.contains("0000000000000000000000000000000000000000"));
}

// ------------------------------------------------------------------
// repo_create with organization and conflict
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_repo_create_with_organization() {
    let (_td, state) = make_test_state();
    let req = RepoCreateRequest {
        repo_type: RepoType::Model,
        name: "my-repo".to_owned(),
        organization: Some("org".to_owned()),
        private: false,
        visibility: None,
    };
    let (status, json) = repo_create(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Json(req),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(json["id"], "org/my-repo");
}

#[tokio::test]
async fn handler_repo_create_conflict() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "ns/existing");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let req = RepoCreateRequest {
        repo_type: RepoType::Model,
        name: "ns/existing".to_owned(),
        organization: None,
        private: false,
        visibility: None,
    };
    let result = repo_create(
        State(state.clone()),
        default_headers(),
        test_repo(&state, &default_headers()),
        Json(req),
    )
    .await;
    assert!(result.is_ok());
    let (status, json) = result.unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    // Anonymous (permissive-mode) callers have no identity, so even their
    // conflict body must not disclose the existing repository's metadata.
    let obj = json.as_object().expect("conflict body is an object");
    for key in ["id", "private", "url", "last_modified"] {
        assert!(
            !obj.contains_key(key),
            "conflict body leaked repository metadata key '{key}': {json:?}"
        );
    }
    assert_eq!(obj["message"], "repository already exists");
}

#[tokio::test]
async fn repo_create_conflict_other_tenant_returns_minimal_body() {
    // Alice's Write-scoped token is bound to alice/own; the victim
    // bob/private-repo already exists and is private.
    let (_td, state) = make_write_alice_auth_state();
    let req = RepoCreateRequest {
        repo_type: RepoType::Model,
        name: "bob/private-repo".to_owned(),
        organization: None,
        private: false,
        visibility: None,
    };
    let (status, json) = repo_create(
        State(state.clone()),
        alice_headers(),
        test_repo(&state, &alice_headers()),
        Json(req),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    // The victim's RepoResponse must not leak: no id/private/url/last_modified
    // keys in the conflict body, matching the cross-tenant privacy filter on
    // repo_list/repo_search.
    let obj = json.as_object().expect("conflict body is an object");
    for key in ["id", "private", "url", "last_modified"] {
        assert!(
            !obj.contains_key(key),
            "other-tenant conflict leaked victim metadata key '{key}': {json:?}"
        );
    }
    assert_eq!(obj["message"], "repository already exists");
}

#[tokio::test]
async fn repo_create_conflict_own_repo_keeps_full_body() {
    // alice/own is Alice's own (private) repository, so the conflict body keeps
    // the rich compatibility shape the native client needs (including the URL).
    let (_td, state) = make_write_alice_auth_state();
    let req = RepoCreateRequest {
        repo_type: RepoType::Model,
        name: "alice/own".to_owned(),
        organization: None,
        private: true,
        visibility: None,
    };
    let (status, json) = repo_create(
        State(state.clone()),
        alice_headers(),
        test_repo(&state, &alice_headers()),
        Json(req),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(json["id"], "alice/own");
    assert_eq!(json["private"], true);
    assert!(
        json["url"]
            .as_str()
            .is_some_and(|url| url.ends_with("/alice/own")),
        "own-repo conflict body should keep the repository URL: {json:?}"
    );
}

// ------------------------------------------------------------------
// xet_read_token handler (requires auth)
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_xet_read_token_missing_auth_returns_unauthorized() {
    let (_td, state) = make_test_state();
    let result = xet_read_token(
        State(state),
        default_headers(),
        Path(("models".into(), "ns".into(), "r".into(), "main".into())),
    )
    .await;
    assert!(matches!(result, Err(HubApiError::Unauthorized)));
}

#[tokio::test]
async fn handler_xet_write_token_missing_auth_returns_unauthorized() {
    let (_td, state) = make_test_state();
    let result = xet_write_token(
        State(state),
        default_headers(),
        Path(("models".into(), "ns".into(), "r".into(), "main".into())),
    )
    .await;
    assert!(matches!(result, Err(HubApiError::Unauthorized)));
}

// ------------------------------------------------------------------
// repo_response_for_request — Dataset and Space URL logic
// ------------------------------------------------------------------

#[test]
fn repo_response_for_request_dataset_url() {
    use shardline_index::hub::HubRepo;
    let repo = HubRepo {
        repo_id: "org/mydata".to_owned(),
        repo_type: HubRepoType::Dataset,
        private: false,
        default_branch: "main".to_owned(),
        created_at_unix_seconds: 0,
        updated_at_unix_seconds: 0,
    };
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "https".parse().unwrap());
    headers.insert(axum::http::header::HOST, "hub.example.com".parse().unwrap());
    let resp = repo_response_for_request(&headers, &repo);
    assert_eq!(resp.url, "https://hub.example.com/datasets/org/mydata");
    assert_eq!(resp.repo_type, RepoType::Dataset);
}

#[test]
fn repo_response_for_request_space_url() {
    use shardline_index::hub::HubRepo;
    let repo = HubRepo {
        repo_id: "org/myspace".to_owned(),
        repo_type: HubRepoType::Space,
        private: false,
        default_branch: "main".to_owned(),
        created_at_unix_seconds: 0,
        updated_at_unix_seconds: 0,
    };
    let mut headers = HeaderMap::new();
    headers.insert("x-forwarded-proto", "https".parse().unwrap());
    headers.insert(axum::http::header::HOST, "hub.example.com".parse().unwrap());
    let resp = repo_response_for_request(&headers, &repo);
    assert_eq!(resp.url, "https://hub.example.com/spaces/org/myspace");
    assert_eq!(resp.repo_type, RepoType::Space);
}

// ------------------------------------------------------------------
// webhook_create — duplicate URL and too many events
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_create_duplicate_url() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-dup");
    store
        .create_webhook(
            "org/wh-dup",
            "https://example.com/dup",
            &["push".into()],
            None,
        )
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let result = webhook_create(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-dup".into())),
        Json(WebhookCreateRequest {
            url: "https://example.com/dup".into(),
            events: vec!["push".into()],
            secret: None,
        }),
    )
    .await;
    assert!(matches!(result, Err(HubApiError::Conflict(_))));
}

// ------------------------------------------------------------------
// lfs_upload and lfs_download handler tests
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_lfs_upload_and_download_roundtrip() {
    let oid = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
    let data = b"lfs file content";
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/lfs-io");
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store: store.clone(),
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    // Upload
    let result = lfs_upload(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(oid.to_owned()),
        bytes::Bytes::from_static(data),
    )
    .await;
    assert_eq!(result.unwrap(), StatusCode::OK);

    // Download
    let (status, _headers, downloaded) = lfs_download(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(oid.to_owned()),
    )
    .await
    .unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(downloaded, data);
}

// ------------------------------------------------------------------
// dataset_parquet success path
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_dataset_parquet_finds_data_files() {
    let (_td, store) = make_store_with_revision(
        HubRepoType::Dataset,
        "org/ds-parquet",
        "sha_ds_pq",
        &[
            HubFileEntry {
                path: "data/train/data.parquet".into(),
                size: 1000,
                sha: "6060606060606060606060606060606060606060606060606060606060606060".into(),
                is_lfs: false,
            },
            HubFileEntry {
                path: "README.md".into(),
                size: 50,
                sha: "1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b1b".into(),
                is_lfs: false,
            },
        ],
    );
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let resp = dataset_parquet(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("org".into(), "ds-parquet".into())),
    )
    .await
    .unwrap();
    assert_eq!(resp.files.len(), 1);
    assert_eq!(resp.files[0].path, "data/train/data.parquet");
}

// ------------------------------------------------------------------
// webhook_list with a repo that has webhooks
// ------------------------------------------------------------------

#[tokio::test]
async fn handler_webhook_list_with_hooks() {
    let (_td, store) = make_store_with_repo(HubRepoType::Model, "org/wh-list");
    store
        .create_webhook(
            "org/wh-list",
            "https://example.com/hook1",
            &["push".into()],
            None,
        )
        .unwrap();
    store
        .create_webhook(
            "org/wh-list",
            "https://example.com/hook2",
            &["push".into()],
            None,
        )
        .unwrap();
    let object_store = shardline_server_core::ServerObjectStore::local(_td.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
    };
    let resp = webhook_list(
        State(state.clone()),
        test_repo(&state, &default_headers()),
        Path(("models".into(), "org".into(), "wh-list".into())),
    )
    .await
    .unwrap();
    assert_eq!(resp.webhooks.len(), 2);
}

// ------------------------------------------------------------------
// webhook_delete
// ------------------------------------------------------------------
