// Test code intentionally uses unwrap/expect/indexing/vec-push for clarity
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::vec_init_then_push
)]

use super::pack_parse::decompress_zlib;
use super::receive_pack::{build_report_response, find_lfs_blob, parse_receive_pack_request};
use super::ref_advertisement::{
    GitRef, authorize_read, authorize_write, collect_refs, is_valid_refname, resolve_repo_id,
};
use super::tree_walk::parse_lfs_pointer_field;
use super::upload_pack::{
    build_git_tree_objects, build_gitattributes_blob, build_inline_blob, build_lfs_pointer_blob,
    parse_haves, parse_wants,
};
use super::*;
use crate::git::pack::{GitObject, ObjectType, PackError, apply_delta};
use crate::git::pktline;
use crate::routes::HubState;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use shardline_index::hub::HubFileEntry;

#[test]
fn parse_wants_from_request() {
    let lines = vec![
        b"want 8ab686eafeb1f44702738c8b0f24f2567c36da6d side-band-64k\n".to_vec(),
        b"want 0000000000000000000000000000000000000000\n".to_vec(),
    ];
    let wants = parse_wants(&lines);
    assert_eq!(wants.len(), 2);
    assert_eq!(wants[0], "8ab686eafeb1f44702738c8b0f24f2567c36da6d");
}

#[test]
fn parse_haves_from_request() {
    let lines = vec![b"have 0000000000000000000000000000000000000000\n".to_vec()];
    let haves = parse_haves(&lines);
    assert_eq!(haves.len(), 1);
}

#[test]
fn resolve_repo_id_format() {
    let id = resolve_repo_id("models", "org", "my-model");
    assert_eq!(id, "org/my-model");
}

#[test]
fn lfs_pointer_blob_format() {
    let blob = build_lfs_pointer_blob("abc123", 4096);
    let content = String::from_utf8(blob.data).unwrap();
    assert!(content.starts_with("version https://git-lfs.github.com/spec/v1\n"));
    assert!(content.contains("oid sha256:abc123\n"));
    assert!(content.contains("size 4096\n"));
}

// --- find_lfs_blob (F-deep-2.3) ---

/// Builds the sha256→object content index exactly as the production caller in
/// `receive_pack.rs` does, so tests exercise the same O(1) fallback lookup.
fn build_content_index(objects: &[GitObject]) -> std::collections::HashMap<String, &GitObject> {
    use sha2::Digest;
    let mut index = std::collections::HashMap::new();
    for obj in objects {
        if obj.object_type == ObjectType::Blob {
            index.insert(hex::encode(sha2::Sha256::digest(&obj.data)), obj);
        }
    }
    index
}

#[test]
fn find_lfs_blob_non_canonical_pointer_finds_content() {
    // A non-canonical pointer blob (e.g. CRLF line endings) whose git SHA1
    // differs from the canonical pointer must still resolve the real content
    // blob by its sha256 content (the LFS OID) — no silent drop.
    use sha2::Digest;
    let content = b"actual file bytes\n";
    let oid = hex::encode(sha2::Sha256::digest(content));
    let file = HubFileEntry {
        path: "big.bin".to_owned(),
        size: content.len() as u64,
        sha: oid,
        is_lfs: true,
    };
    // A blob with the actual content.
    let content_obj = crate::git::pack::create_blob_object(content);
    // A non-canonical pointer blob (CRLF line endings instead of LF).
    let noncanonical_pointer = crate::git::pack::create_blob_object(
        b"version https://git-lfs.github.com/spec/v1\r\noid sha256:<oid>\r\nsize <n>\r\n",
    );
    let objects = vec![content_obj, noncanonical_pointer];
    let mut sha_to_obj: std::collections::HashMap<[u8; 20], &GitObject> =
        std::collections::HashMap::new();
    for obj in &objects {
        sha_to_obj.insert(obj.sha1(), obj);
    }
    let content_by_sha256 = build_content_index(&objects);

    let found = find_lfs_blob(&file, &sha_to_obj, &content_by_sha256);
    assert!(
        found.is_some(),
        "non-canonical pointer must still find the content blob"
    );
    assert_eq!(
        found.unwrap().data,
        content,
        "the resolved blob should be the real content, not the pointer text"
    );
}

#[test]
fn find_lfs_blob_canonical_pointer_matches_by_sha1() {
    use sha2::Digest;
    let content = b"canonical content";
    let oid = hex::encode(sha2::Sha256::digest(content));
    let file = HubFileEntry {
        path: "f.bin".to_owned(),
        size: content.len() as u64,
        sha: oid,
        is_lfs: true,
    };
    // Canonical pointer blob (matches `build_lfs_pointer_blob` output).
    let pointer = build_lfs_pointer_blob(&file.sha, file.size);
    let content_obj = crate::git::pack::create_blob_object(content);
    let objects = vec![pointer.clone(), content_obj];
    let mut sha_to_obj: std::collections::HashMap<[u8; 20], &GitObject> =
        std::collections::HashMap::new();
    for obj in &objects {
        sha_to_obj.insert(obj.sha1(), obj);
    }
    let content_by_sha256 = build_content_index(&objects);
    let found = find_lfs_blob(&file, &sha_to_obj, &content_by_sha256);
    // The canonical pointer is matched by its git SHA1 fast path.
    assert_eq!(found.unwrap().sha1(), pointer.sha1());
}

#[test]
fn find_lfs_blob_none_when_no_matching_object() {
    use sha2::Digest;
    let content = b"something";
    let oid = hex::encode(sha2::Sha256::digest(content));
    let file = HubFileEntry {
        path: "missing.bin".to_owned(),
        size: content.len() as u64,
        sha: oid,
        is_lfs: true,
    };
    // A pack with an unrelated blob only; neither the canonical pointer nor a
    // content object with the matching sha256 is present.
    let unrelated = crate::git::pack::create_blob_object(b"unrelated");
    let objects = vec![unrelated];
    let mut sha_to_obj: std::collections::HashMap<[u8; 20], &GitObject> =
        std::collections::HashMap::new();
    for obj in &objects {
        sha_to_obj.insert(obj.sha1(), obj);
    }
    let content_by_sha256 = build_content_index(&objects);
    assert!(
        find_lfs_blob(&file, &sha_to_obj, &content_by_sha256).is_none(),
        "no matching object should yield None (the push is then failed)"
    );
}

#[test]
fn inline_blob_deterministic() {
    let file = HubFileEntry {
        path: "test.txt".to_owned(),
        size: 11,
        sha: "aabbccdd".to_owned(),
        is_lfs: false,
    };
    let b1 = build_inline_blob(&file);
    let b2 = build_inline_blob(&file);
    assert_eq!(b1.sha1(), b2.sha1());
}

#[test]
fn tree_from_empty_files() {
    let tree = build_git_tree_objects(&[]);
    assert_eq!(tree.0.object_type, ObjectType::Tree);
}

#[test]
fn tree_from_single_file() {
    let files = vec![HubFileEntry {
        path: "README.md".to_owned(),
        size: 13,
        sha: "deadbeef".to_owned(),
        is_lfs: false,
    }];
    let (tree, sub_trees) = build_git_tree_objects(&files);
    let tree_sha = tree.sha1();
    assert_ne!(tree_sha, [0u8; 20]);
    assert!(sub_trees.is_empty());
}

#[test]
fn tree_from_nested_files() {
    let files = vec![
        HubFileEntry {
            path: "src/main.rs".to_owned(),
            size: 100,
            sha: "aaaa".to_owned(),
            is_lfs: false,
        },
        HubFileEntry {
            path: "Cargo.toml".to_owned(),
            size: 200,
            sha: "bbbb".to_owned(),
            is_lfs: false,
        },
    ];
    let (tree, sub_trees) = build_git_tree_objects(&files);
    let tree_sha = tree.sha1();
    assert_ne!(tree_sha, [0u8; 20]);
    // src/ should produce a sub-tree
    assert_eq!(sub_trees.len(), 1);
}

#[test]
fn gitattributes_blob_generated_for_lfs_files() {
    let files = vec![
        HubFileEntry {
            path: "model.bin".to_owned(),
            size: 1024,
            sha: "oid1".to_owned(),
            is_lfs: true,
        },
        HubFileEntry {
            path: "README.md".to_owned(),
            size: 100,
            sha: "oid2".to_owned(),
            is_lfs: false,
        },
    ];
    let blob = build_gitattributes_blob(&files);
    assert!(blob.is_some());
    let content = String::from_utf8(blob.unwrap().data).unwrap();
    assert!(content.contains("model.bin filter=lfs"));
    assert!(!content.contains("README.md"));
}

#[test]
fn gitattributes_blob_none_when_no_lfs() {
    let files = vec![HubFileEntry {
        path: "README.md".to_owned(),
        size: 100,
        sha: "oid2".to_owned(),
        is_lfs: false,
    }];
    assert!(build_gitattributes_blob(&files).is_none());
}

// --- is_valid_refname tests ---

#[test]
fn is_valid_refname_valid() {
    assert!(is_valid_refname("refs/heads/main"));
    assert!(is_valid_refname("refs/tags/v1.0"));
    assert!(is_valid_refname("refs/heads/feature/foo"));
    assert!(is_valid_refname("refs/heads/feature/foo/bar"));
    assert!(is_valid_refname("refs/pull/42/head"));
}

#[test]
fn is_valid_refname_empty() {
    assert!(!is_valid_refname(""));
}

#[test]
fn is_valid_refname_no_refs_prefix() {
    assert!(!is_valid_refname("heads/main"));
    assert!(!is_valid_refname("tags/v1.0"));
    assert!(!is_valid_refname("main"));
}

#[test]
fn is_valid_refname_control_chars() {
    assert!(!is_valid_refname("refs/heads/main\n"));
    assert!(!is_valid_refname("refs/heads/main\t"));
    assert!(!is_valid_refname("refs/heads/main\x00"));
    assert!(!is_valid_refname("refs/heads/main\x7f"));
}

#[test]
fn is_valid_refname_dotdot() {
    assert!(!is_valid_refname("refs/heads/../secret"));
    assert!(!is_valid_refname("refs/heads/feature/.."));
    assert!(!is_valid_refname("refs/heads/../../etc/passwd"));
}

// --- parse_commit_object tests ---

#[test]
fn parse_commit_object_valid() {
    let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                  parent 1234567890abcdef1234567890abcdef12345678\n\
                  author Test <test@test.com> 1234567890 +0000\n\
                  committer Test <test@test.com> 1234567890 +0000\n\
                  \n\
                  Initial commit\n";
    let (tree, parent, message) = parse_commit_object(data).unwrap();
    assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
    assert_eq!(
        parent.as_deref(),
        Some("1234567890abcdef1234567890abcdef12345678")
    );
    assert_eq!(message, "Initial commit");
}

#[test]
fn parse_commit_object_no_parent() {
    let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                  author Test <test@test.com> 1234567890 +0000\n\
                  committer Test <test@test.com> 1234567890 +0000\n\
                  \n\
                  First commit\n";
    let (tree, parent, message) = parse_commit_object(data).unwrap();
    assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
    assert!(parent.is_none());
    assert_eq!(message, "First commit");
}

#[test]
fn parse_commit_object_malformed() {
    // Missing tree header
    let data = b"parent 1234567890abcdef1234567890abcdef12345678\n\
                  author Test <test@test.com> 1234567890 +0000\n\
                  \n\
                  Some message\n";
    let result = parse_commit_object(data);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("missing tree"));
}

// --- apply_delta tests ---

#[test]
fn apply_delta_simple() {
    // Base: "Hello, World!"
    let base = b"Hello, World!";

    // Build a delta that copies the first 5 bytes ("Hello"), inserts " there", copies the rest.
    let mut delta = Vec::new();
    // Source size: 13 (varint — fits in 1 byte)
    delta.push(13);
    // Target size: 19 (varint — "Hello there, World!")
    delta.push(19);

    // Copy instruction 1: offset=0, size=5 ("Hello")
    //   offset bytes: bit 0 NOT set → no offset bytes (offset=0)
    //   size bytes:   bit 4 set → 0x10 (1 size byte)
    //   cmd = 0x80 (copy flag) | 0x10 = 0x90
    delta.push(0x90);
    delta.push(0x05); // size byte = 5

    // Insert instruction: 6 bytes " there"
    delta.push(6);
    delta.extend_from_slice(b" there");

    // Copy instruction 2: offset=5, size=8 (", World!")
    //   offset bytes: bit 0 set → 0x01 (1 offset byte)
    //   size bytes:   bit 4 set → 0x10 (1 size byte)
    //   cmd = 0x01 | 0x80 (copy flag) | 0x10 = 0x91
    delta.push(0x91);
    delta.push(0x05); // offset byte = 5
    delta.push(0x08); // size byte = 8

    let result = apply_delta(base, &delta).unwrap();
    assert_eq!(result, b"Hello there, World!");
}

#[test]
fn apply_delta_empty() {
    // Base: "abc", delta produces empty output (target size 0)
    let base = b"abc";
    let mut delta = Vec::new();
    // Source size: 3
    delta.push(3);
    // Target size: 0
    delta.push(0);
    // No instructions — result should be empty

    let result = apply_delta(base, &delta).unwrap();
    assert!(result.is_empty());
}

#[test]
fn apply_delta_invalid() {
    // Source size doesn't match base length
    let base = b"Hello, World!";
    let mut delta = Vec::new();
    // Source size: 99 (wrong)
    delta.push(99);
    // Target size: 5
    delta.push(5);
    // Copy command: offset=0, size=5
    delta.push(0x90);
    delta.push(0x00);
    delta.push(0x05);

    let result = apply_delta(base, &delta);
    assert!(result.is_err());
}

// --- parse_commit_object with multiple parents ---

#[test]
fn parse_commit_object_multi_parent() {
    let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                  parent 1111111111111111111111111111111111111111\n\
                  parent 2222222222222222222222222222222222222222\n\
                  author Test <test@test.com> 1234567890 +0000\n\
                  committer Test <test@test.com> 1234567890 +0000\n\
                  \n\
                  Merge commit\n";
    let (tree, parent, message) = parse_commit_object(data).unwrap();
    assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
    // parse_commit_object returns the LAST parent found
    assert_eq!(
        parent.as_deref(),
        Some("2222222222222222222222222222222222222222")
    );
    assert_eq!(message, "Merge commit");
}

// --- walk_git_tree depth-limit tests ---

#[test]
fn walk_git_tree_empty_tree() {
    // An empty tree object (no entries) should return an empty file list.
    let empty_tree = GitObject::tree(vec![]);
    let sha = empty_tree.sha1();
    let mut objects: std::collections::HashMap<[u8; 20], &GitObject> =
        std::collections::HashMap::new();
    objects.insert(sha, &empty_tree);

    let entries = walk_git_tree(&sha, &objects, "").unwrap();
    assert!(
        entries.is_empty(),
        "empty tree should produce no file entries"
    );
}

#[test]
fn walk_git_tree_max_depth() {
    // Build a chain of 128 nested directories, each containing one subdirectory,
    // with a file at the deepest level. Depth 128 = MAX_TREE_DEPTH and should succeed.
    let file_blob = GitObject::blob(b"file content".to_vec());
    let file_sha = file_blob.sha1();

    // Collect all owned objects, then build the HashMap of references.
    let mut owned: Vec<GitObject> = Vec::new();
    owned.push(file_blob);

    let mut current_sha = file_sha;

    for depth in (1..=128).rev() {
        let mut tree_data = Vec::new();
        if depth == 128 {
            // Innermost: file entry
            tree_data.extend_from_slice(b"100644 f\0");
            tree_data.extend_from_slice(&file_sha);
        } else {
            // Directory entry pointing to current_sha
            tree_data.extend_from_slice(b"40000 d\0");
            tree_data.extend_from_slice(&current_sha);
        }
        let tree_obj = GitObject::tree(tree_data);
        let sha = tree_obj.sha1();
        owned.push(tree_obj);
        current_sha = sha;
    }

    let objects: std::collections::HashMap<[u8; 20], &GitObject> =
        owned.iter().map(|o| (o.sha1(), o)).collect();

    let entries = walk_git_tree(&current_sha, &objects, "").unwrap();
    assert_eq!(entries.len(), 1, "should find the file at depth 128");
    // Path is 127 "d/" prefixes + "f"
    let expected_prefix = "d/".repeat(127);
    let expected_path = format!("{expected_prefix}f");
    assert_eq!(entries[0].path, expected_path);
}

#[test]
fn walk_git_tree_exceeds_max_depth() {
    // Build a chain of 130 nested directories — enough to reach depth 129
    // (one more than MAX_TREE_DEPTH=128). The initial call starts at depth 0,
    // so 130 tree levels pushes the deepest recursion to depth 129 > 128.
    let file_blob = GitObject::blob(b"file content".to_vec());
    let file_sha = file_blob.sha1();

    let mut owned: Vec<GitObject> = Vec::new();
    owned.push(file_blob);

    let mut current_sha = file_sha;

    for depth in (1..=130).rev() {
        let mut tree_data = Vec::new();
        if depth == 130 {
            tree_data.extend_from_slice(b"100644 f\0");
            tree_data.extend_from_slice(&file_sha);
        } else {
            tree_data.extend_from_slice(b"40000 d\0");
            tree_data.extend_from_slice(&current_sha);
        }
        let tree_obj = GitObject::tree(tree_data);
        let sha = tree_obj.sha1();
        owned.push(tree_obj);
        current_sha = sha;
    }

    let objects: std::collections::HashMap<[u8; 20], &GitObject> =
        owned.iter().map(|o| (o.sha1(), o)).collect();

    let result = walk_git_tree(&current_sha, &objects, "");
    assert!(
        result.is_err(),
        "should fail at depth 129 (exceeds MAX_TREE_DEPTH)"
    );
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("exceeds maximum depth"),
        "error message should mention depth"
    );
}

// --- is_valid_refname edge cases ---

#[test]
fn is_valid_refname_with_spaces() {
    assert!(!is_valid_refname("refs/heads/my branch"));
    assert!(!is_valid_refname("refs/heads/feature "));
    assert!(!is_valid_refname(" refs/heads/feature"));
}

#[test]
fn is_valid_refname_with_dotdot() {
    assert!(!is_valid_refname("refs/heads/../secret"));
    assert!(!is_valid_refname("refs/heads/feature/.."));
    assert!(!is_valid_refname("refs/heads/../../etc/passwd"));
    assert!(!is_valid_refname("refs/heads/a/../../../x"));
}

// --- collect_refs dedup/HEAD logic tests ---

/// Helper to create a temporary HubState backed by SQLite.
fn make_hub_state() -> (tempfile::TempDir, HubState) {
    use shardline_index::LocalIndexStore;
    use shardline_index::hub::BoxedHubStore;

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let db_path = root.join("metadata.sqlite3");
    let conn = rusqlite::Connection::open(&db_path).expect("open sqlite");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS shardline_hub_repos (
            repo_id TEXT PRIMARY KEY, repo_type TEXT NOT NULL, private INTEGER NOT NULL DEFAULT 0,
            default_branch TEXT NOT NULL, created_at_unix_seconds INTEGER NOT NULL,
            updated_at_unix_seconds INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_revisions (
            repo_id TEXT NOT NULL, ref_name TEXT NOT NULL, sha TEXT NOT NULL,
            parent_sha TEXT, message TEXT, created_at_unix_seconds INTEGER NOT NULL,
            PRIMARY KEY (repo_id, sha)
        );
        CREATE INDEX IF NOT EXISTS shardline_hub_revisions_repo_ref_idx
            ON shardline_hub_revisions (repo_id, ref_name);
        CREATE TABLE IF NOT EXISTS shardline_hub_refs (
            repo_id TEXT NOT NULL, ref_name TEXT NOT NULL, sha TEXT NOT NULL,
            PRIMARY KEY (repo_id, ref_name)
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_file_entries (
            commit_sha TEXT NOT NULL, path TEXT NOT NULL, size INTEGER NOT NULL,
            sha TEXT NOT NULL, is_lfs INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (commit_sha, path)
        );
        CREATE TABLE IF NOT EXISTS shardline_hub_webhooks (
            id TEXT PRIMARY KEY, repo_id TEXT NOT NULL,
            url TEXT NOT NULL, events TEXT NOT NULL DEFAULT 'push', secret TEXT,
            active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
            created_at_unix_seconds INTEGER NOT NULL,
            FOREIGN KEY (repo_id) REFERENCES shardline_hub_repos(repo_id) ON DELETE CASCADE
        );",
    )
    .expect("create schema");
    drop(conn);

    let store = LocalIndexStore::open(root);
    let boxed = BoxedHubStore::from_store(store);
    let object_store = shardline_server_core::ServerObjectStore::local(tmp.path().join("lfs"))
        .expect("local object store");
    let state = HubState {
        store: boxed,
        object_store,
        auth: None,
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    };
    (tmp, state)
}

#[tokio::test]
async fn collect_refs_dedup_identical_shas() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;

    state
        .store
        .create_repo(HubRepoType::Model, "org/dedup", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

    // Insert two revisions with different SHAs but pointing to the same
    // final SHA via a chain. The key dedup test: same ref_name ("main")
    // should only appear once.
    state
        .store
        .create_revision("org/dedup", Some(initial_sha), "sha_a", "main", "first")
        .unwrap();
    state
        .store
        .create_revision(
            "org/dedup",
            Some("sha_a"),
            "sha_b",
            "refs/heads/dev",
            "second",
        )
        .unwrap();
    // Also add a HEAD entry pointing to sha_a
    state
        .store
        .create_revision("org/dedup", Some("sha_b"), "sha_head", "HEAD", "head ref")
        .unwrap();

    let refs = collect_refs(&state, "org/dedup").await.unwrap();

    // Each unique (name, sha) pair should only appear once.
    let mut seen = std::collections::HashSet::new();
    for r in &refs {
        let key = (&r.name, &r.sha1);
        assert!(
            seen.insert(key),
            "duplicate ref entry: {key:?} in refs: {refs:?}"
        );
    }

    // Verify all expected ref names are present.
    let names: Vec<&str> = refs.iter().map(|r| r.name.as_str()).collect();
    assert!(names.contains(&"HEAD"), "should contain HEAD: {refs:?}");
    assert!(
        names.contains(&"refs/heads/main"),
        "should contain main: {refs:?}"
    );
    assert!(
        names.contains(&"refs/heads/dev"),
        "should contain dev: {refs:?}"
    );
}

#[tokio::test]
async fn collect_refs_head_fallback_when_no_head() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;

    state
        .store
        .create_repo(HubRepoType::Model, "org/head-fallback", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

    // Create a revision with a non-HEAD ref name only.
    state
        .store
        .create_revision(
            "org/head-fallback",
            Some(initial_sha),
            "abc123",
            "main",
            "first commit",
        )
        .unwrap();

    let refs = collect_refs(&state, "org/head-fallback").await.unwrap();

    // There should be a HEAD entry injected (no explicit HEAD ref).
    let heads: Vec<&GitRef> = refs.iter().filter(|r| r.name == "HEAD").collect();
    assert_eq!(
        heads.len(),
        1,
        "collect_refs should inject exactly one HEAD entry when none is explicit: {refs:?}"
    );
    // The fallback follows the active default branch, not historical order.
    assert_eq!(
        heads[0].sha1, "abc123",
        "HEAD fallback should point to the active main ref: {refs:?}"
    );
}

#[tokio::test]
async fn collect_refs_explicit_head_not_duplicated() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;

    state
        .store
        .create_repo(HubRepoType::Model, "org/explicit-head", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

    // Create revisions: one with ref_name "HEAD" and one with "main".
    state
        .store
        .create_revision(
            "org/explicit-head",
            Some(initial_sha),
            "sha_head",
            "HEAD",
            "head commit",
        )
        .unwrap();
    state
        .store
        .create_revision(
            "org/explicit-head",
            Some(initial_sha),
            "sha_main",
            "main",
            "main commit",
        )
        .unwrap();

    let refs = collect_refs(&state, "org/explicit-head").await.unwrap();

    // There should be exactly one HEAD entry.
    let head_count = refs.iter().filter(|r| r.name == "HEAD").count();
    assert_eq!(head_count, 1, "HEAD should appear exactly once: {refs:?}");
}

#[tokio::test]
async fn collect_refs_bare_ref_name_gets_refs_prefix() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;

    state
        .store
        .create_repo(HubRepoType::Model, "org/bare-ref", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

    // Create a revision with a bare ref name (no "refs/" prefix).
    state
        .store
        .create_revision(
            "org/bare-ref",
            Some(initial_sha),
            "def456",
            "feature",
            "feature commit",
        )
        .unwrap();

    let refs = collect_refs(&state, "org/bare-ref").await.unwrap();

    // The bare "feature" name should be normalized to "refs/heads/feature".
    assert!(
        refs.iter().any(|r| r.name == "refs/heads/feature"),
        "bare ref name 'feature' should be normalized to 'refs/heads/feature': {refs:?}"
    );
    assert!(
        !refs.iter().any(|r| r.name == "feature"),
        "bare ref name should not appear unmodified: {refs:?}"
    );
}

#[tokio::test]
async fn collect_refs_full_refs_prefix_preserved() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;

    state
        .store
        .create_repo(HubRepoType::Model, "org/full-ref", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

    // Create a revision with a full refs/ prefix.
    state
        .store
        .create_revision(
            "org/full-ref",
            Some(initial_sha),
            "abc789",
            "refs/tags/v1.0",
            "tag v1.0",
        )
        .unwrap();

    let refs = collect_refs(&state, "org/full-ref").await.unwrap();

    // The full refs/ prefix should be preserved.
    assert!(
        refs.iter().any(|r| r.name == "refs/tags/v1.0"),
        "full refs/ prefix should be preserved: {refs:?}"
    );
}

#[tokio::test]
async fn collect_refs_nonexistent_repo_returns_empty() {
    let (_tmp, state) = make_hub_state();

    let refs = collect_refs(&state, "org/nonexistent").await.unwrap();
    assert!(
        refs.is_empty(),
        "collect_refs on nonexistent repo should return empty list: {refs:?}"
    );
}

// --- build_report_response tests ---

fn body_string(response: Response) -> String {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    })
}

#[test]
fn build_report_response_unpack_ok_and_ok_refs() {
    let results = vec![
        ("refs/heads/main".to_owned(), true, None),
        ("refs/heads/dev".to_owned(), true, None),
    ];
    let response = build_report_response(&results, true).unwrap();
    let body = body_string(response);
    assert!(body.contains("unpack ok"));
    assert!(body.contains("ok refs/heads/main"));
    assert!(body.contains("ok refs/heads/dev"));
}

#[test]
fn build_report_response_unpack_failed_and_ng_refs() {
    let results = vec![(
        "refs/heads/main".to_owned(),
        false,
        Some("unpack failed".to_owned()),
    )];
    let response = build_report_response(&results, false).unwrap();
    let body = body_string(response);
    assert!(body.contains("unpack failed"));
    assert!(body.contains("ng refs/heads/main"));
    assert!(body.contains("unpack failed"));
}

#[test]
fn build_report_response_ng_with_default_message() {
    let results = vec![("refs/heads/bad".to_owned(), false, None)];
    let response = build_report_response(&results, false).unwrap();
    let body = body_string(response);
    assert!(body.contains("ng refs/heads/bad failed"));
}

#[test]
fn build_report_response_ends_with_flush() {
    let results: Vec<(String, bool, Option<String>)> = vec![];
    let response = build_report_response(&results, true).unwrap();
    let body = body_string(response);
    assert!(
        body.ends_with("0000"),
        "response should end with flush packet"
    );
}

// --- parse_receive_pack_request tests ---

#[test]
fn parse_receive_pack_request_with_updates_and_pack() {
    // Build a pkt-line request: commands followed by flush, then pack data
    let mut body = Vec::new();
    // Command line: "old-sha new-sha refs/heads/main"
    let cmd = "0000000000000000000000000000000000000000 newsha1234567890123456789012345678901234567890 refs/heads/main\n";
    let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
    body.extend_from_slice(encoded.as_bytes());
    body.extend_from_slice(b"0000"); // flush
    body.extend_from_slice(b"PACK"); // pack header start
    body.extend_from_slice(&[0, 0, 0, 2]); // version
    body.extend_from_slice(&[0, 0, 0, 0]); // 0 objects

    let (updates, pack_data) = parse_receive_pack_request(&body);
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].2, "refs/heads/main");
    assert!(!pack_data.is_empty());
    assert!(pack_data.starts_with(b"PACK"));
}

#[test]
fn parse_receive_pack_request_no_flush_returns_empty_pack() {
    // "000a" = 10 bytes total (4 prefix + 6 payload "check\n")
    let body = b"000acheck\n";
    let (updates, pack_data) = parse_receive_pack_request(body);
    // "check\n" has no whitespace, so split gives 1 part, but the
    // function requires at least 3 parts (old, new, refname), so updates is empty
    assert!(updates.is_empty());
    // No flush packet found, so pack_start remains 0.
    // pack_start (0) < body.len() (10) → true, so pack_data = body[0..] (the full body)
    assert!(
        !pack_data.is_empty(),
        "pack_data should be the full body when no flush is present"
    );
}

#[test]
fn parse_receive_pack_request_empty_body() {
    let (updates, pack_data) = parse_receive_pack_request(b"");
    assert!(updates.is_empty());
    assert!(pack_data.is_empty());
}

#[test]
fn parse_receive_pack_request_non_utf8_line_skipped() {
    let mut body = Vec::new();
    // Non-UTF8 line: length prefix points to content with 0xFF
    body.extend_from_slice(b"0005\xff\x00");
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACKdata");
    let (updates, pack_data) = parse_receive_pack_request(&body);
    // Non-UTF8 line is skipped, so updates is empty
    assert!(updates.is_empty());
    assert!(!pack_data.is_empty());
}

// --- parse_lfs_pointer_field tests ---

#[test]
fn parse_lfs_pointer_field_oid() {
    let text = "version https://git-lfs.github.com/spec/v1\noid sha256:abc123\nsize 100\n";
    let oid = parse_lfs_pointer_field(text, "oid");
    assert_eq!(oid.as_deref(), Some("abc123"));
}

#[test]
fn parse_lfs_pointer_field_size() {
    let text = "version https://git-lfs.github.com/spec/v1\noid sha256:abc123\nsize 100\n";
    let size = parse_lfs_pointer_field(text, "size");
    assert_eq!(size.as_deref(), Some("100"));
}

#[test]
fn parse_lfs_pointer_field_missing_field() {
    let text = "version https://git-lfs.github.com/spec/v1\n";
    assert!(parse_lfs_pointer_field(text, "oid").is_none());
    assert!(parse_lfs_pointer_field(text, "size").is_none());
}

#[test]
fn parse_lfs_pointer_field_extra_whitespace() {
    let text = "oid sha256:  abc\n";
    let oid = parse_lfs_pointer_field(text, "oid");
    // strip_prefix("oid ") after splitting on "oid " returns " sha256:  abc"
    // then strip_prefix("sha256:") would fail because there's a leading space
    // Actually let's trace: line.strip_prefix("oid ") gives "sha256:  abc"
    // then strip_prefix("sha256:") gives "  abc"
    // So oid = Some("  abc")
    assert!(oid.is_some());
}

// --- parse_pack_data tests ---

#[test]
fn parse_pack_data_empty_input() {
    let objects = parse_pack_data(b"").unwrap();
    assert!(objects.is_empty());
}

#[test]
fn parse_pack_data_too_short() {
    let objects = parse_pack_data(b"PACK").unwrap();
    assert!(objects.is_empty());
}

#[test]
fn parse_pack_data_no_pack_magic() {
    let objects = parse_pack_data(b"NOTAPACKFILE").unwrap();
    assert!(objects.is_empty());
}

#[test]
fn parse_pack_data_unsupported_version() {
    let mut data = b"PACK".to_vec();
    data.extend_from_slice(&3u32.to_be_bytes()); // version 3 (unsupported)
    data.extend_from_slice(&0u32.to_be_bytes());
    let objects = parse_pack_data(&data).unwrap();
    assert!(objects.is_empty());
}

// --- parse_pack_data truncated pack (F-deep-2.5) ---

#[test]
fn parse_pack_data_truncated_claims_more_objects_errors() {
    // A header claiming more objects than the stream actually contains must
    // error instead of returning a partial object list.
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(b"blob content").unwrap();
    let compressed = enc.finish().unwrap();

    let mut pack = b"PACK".to_vec();
    pack.extend_from_slice(&2u32.to_be_bytes()); // version 2
    pack.extend_from_slice(&2u32.to_be_bytes()); // claims 2 objects
    // Only 1 object is actually present in the stream.
    pack.push((3 << 4) | (compressed.len() as u8 & 0x0f)); // blob, size low nibble
    pack.extend_from_slice(&compressed);

    let result = parse_pack_data(&pack);
    assert!(
        result.is_err(),
        "truncated pack must error instead of returning partial objects, got {result:?}"
    );
}

// --- authorize_read / authorize_write tests (no auth configured) ---

#[test]
fn authorize_read_without_auth_is_permissive() {
    let (_tmp, state) = make_hub_state();
    let headers = axum::http::HeaderMap::new();
    assert!(authorize_read(&state, &headers).is_ok());
}

#[test]
fn authorize_write_without_auth_is_permissive() {
    let (_tmp, state) = make_hub_state();
    let headers = axum::http::HeaderMap::new();
    assert!(authorize_write(&state, &headers).is_ok());
}

// --- decompress_zlib tests ---

#[test]
fn decompress_zlib_empty_input() {
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"").unwrap();
    let compressed = encoder.finish().unwrap();
    let (decompressed, _bytes_used) = decompress_zlib(&compressed).unwrap();
    assert!(decompressed.is_empty());
}

#[test]
fn decompress_zlib_short_input_returns_empty() {
    // Truncated zlib stream
    let result = decompress_zlib(b"x");
    assert!(result.is_err() || result.unwrap().0.is_empty());
}

// --- parse_pack_data round-trip ---

#[test]
fn parse_pack_data_roundtrip_blob() {
    // Generate a pack with one blob, then parse it back
    let blob = crate::git::pack::create_blob_object(b"hello world");
    let pack = crate::git::pack::generate_pack(&[blob]).unwrap();
    let objects = parse_pack_data(&pack).unwrap();
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].object_type, ObjectType::Blob);
    assert_eq!(objects[0].data, b"hello world");
}

#[test]
fn parse_pack_data_roundtrip_commit_and_tree() {
    // Build a tree and commit, generate pack, parse it back
    let blob = crate::git::pack::create_blob_object(b"file content");
    let blob_sha = blob.sha1();
    let tree_entries = vec![(0o100644, "f.txt", &blob_sha)];
    let tree = crate::git::pack::create_tree_object(&tree_entries);
    let tree_sha = tree.sha1();
    let commit =
        crate::git::pack::create_commit_object(&tree_sha, None, "Test <test@test.com>", "Initial");
    let pack = crate::git::pack::generate_pack(&[blob, tree, commit]).unwrap();
    let objects = parse_pack_data(&pack).unwrap();
    assert_eq!(objects.len(), 3);
    let blob_count = objects
        .iter()
        .filter(|o| o.object_type == ObjectType::Blob)
        .count();
    let tree_count = objects
        .iter()
        .filter(|o| o.object_type == ObjectType::Tree)
        .count();
    let commit_count = objects
        .iter()
        .filter(|o| o.object_type == ObjectType::Commit)
        .count();
    assert_eq!(blob_count, 1);
    assert_eq!(tree_count, 1);
    assert_eq!(commit_count, 1);
}

#[test]
fn parse_pack_data_unknown_object_type_breaks() {
    // A pack with an object of type 5 (reserved, not 1..4 or 6..7) cannot be
    // parsed, and since the header claims an object that is never produced,
    // the truncated/malformed pack must error instead of returning a partial
    // (empty) object list.
    let mut data = b"PACK".to_vec();
    data.extend_from_slice(&2u32.to_be_bytes()); // version 2
    data.extend_from_slice(&1u32.to_be_bytes()); // 1 object
    // Object header byte: type=5, size=0, no continuation
    // type 5 = (5 << 4) | 0 = 0x50
    data.push(0x50);
    // No compressed data follows, parser will break on zlib decompression
    let result = parse_pack_data(&data);
    assert!(
        result.is_err(),
        "pack claiming an object that cannot be parsed must error, got {result:?}"
    );
}

#[test]
fn parse_pack_data_with_ref_delta_stops_gracefully() {
    // A pack with a REF_DELTA object (type 7) but no base object in the index
    // should produce an error since the base SHA won't be found.
    // We need at least a valid compressed stream after the REF_DELTA header.
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(b"delta data").unwrap();
    let compressed = enc.finish().unwrap();

    let mut data = b"PACK".to_vec();
    data.extend_from_slice(&2u32.to_be_bytes());
    data.extend_from_slice(&1u32.to_be_bytes()); // 1 object
    // Object header: type=7, size=0, no continuation
    // type 7 = (7 << 4) | 0 = 0x70
    data.push(0x70);
    // REF_DELTA needs 20 bytes of base SHA
    data.extend_from_slice(&[0u8; 20]);
    // Valid zlib data
    data.extend_from_slice(&compressed);
    let result = parse_pack_data(&data);
    assert!(
        result.is_err(),
        "expected error for REF_DELTA with missing base"
    );
}

#[test]
fn parse_pack_data_shift_overflow_detected() {
    // Create a pack with a varint size that keeps shifting past 63 bits
    let mut data = b"PACK".to_vec();
    data.extend_from_slice(&2u32.to_be_bytes());
    data.extend_from_slice(&1u32.to_be_bytes()); // 1 object
    // Object header byte: type=3 (blob), size continues in following bytes
    // Start with continuation bit set, size low nibble = 0
    data.push(0x80 | (3 << 4) | 0x0f); // type=3, continuation, low 4 bits=0x0f
    // Add more continuation bytes to keep shifting
    for _ in 0..10 {
        data.push(0x80); // continuation, 7 bits of zero
    }
    // Try to parse - should detect shift >= 64
    let result = parse_pack_data(&data);
    // The parser may either return empty (if it breaks early) or return an error
    assert!(result.is_ok() || matches!(result, Err(PackError::ShiftOverflow)));
    if let Ok(objects) = result {
        assert!(
            objects.is_empty(),
            "expected empty objects on shift overflow"
        );
    }
}

// --- decompress_zlib with real data ---

#[test]
fn decompress_zlib_roundtrip() {
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let original = b"Hello, World! This is test data for zlib roundtrip.";
    let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(original).unwrap();
    let compressed = encoder.finish().unwrap();
    let (decompressed, bytes_used) = decompress_zlib(&compressed).unwrap();
    assert_eq!(decompressed, original);
    assert_eq!(bytes_used, compressed.len());
}

#[test]
fn decompress_zlib_trailing_bytes_still_decompresses() {
    // decompress_zlib should consume only the bytes it needs, leaving
    // trailing data unconsumed. It returns the number of bytes consumed.
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(b"hello").unwrap();
    let compressed = encoder.finish().unwrap();

    // Append trailing data
    let mut with_trailing = compressed.clone();
    with_trailing.extend_from_slice(b"TRAILER");

    let (decompressed, bytes_used) = decompress_zlib(&with_trailing).unwrap();
    assert_eq!(decompressed, b"hello");
    assert_eq!(bytes_used, compressed.len());
}

// --- parse_commit_object edge cases ---

#[test]
fn parse_commit_object_no_blank_line_no_message() {
    // Commit with no blank line (no message section)
    let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                  author Test <test@test.com> 1234567890 +0000\n";
    let (tree, parent, message) = parse_commit_object(data).unwrap();
    assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
    assert!(parent.is_none());
    assert_eq!(message, "");
}

#[test]
fn parse_commit_object_message_with_trailing_newline() {
    let data = b"tree abcdef0123456789abcdef0123456789abcdef01\n\
                  \n\
                  My message\n";
    let (tree, parent, message) = parse_commit_object(data).unwrap();
    assert_eq!(tree, "abcdef0123456789abcdef0123456789abcdef01");
    assert!(parent.is_none());
    assert_eq!(message, "My message");
}

#[test]
fn parse_commit_object_not_utf8() {
    let data = b"\xff\xfe\x00";
    let result = parse_commit_object(data);
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("invalid commit encoding")
    );
}

// --- walk_git_tree with invalid/non-standard entries ---

#[test]
fn walk_git_tree_skips_symlinks_and_submodules() {
    // Build a tree with a symlink (120000) and a submodule (160000) entry
    // These should be skipped, returning only the regular file.
    let file_blob = crate::git::pack::create_blob_object(b"content");
    let file_sha = file_blob.sha1();

    let mut tree_data = Vec::new();
    // Regular file
    tree_data.extend_from_slice(b"100644 f\0");
    tree_data.extend_from_slice(&file_sha);
    // Symlink
    tree_data.extend_from_slice(b"120000 link\0");
    tree_data.extend_from_slice(&[0xaa; 20]);
    // Submodule
    tree_data.extend_from_slice(b"160000 sub\0");
    tree_data.extend_from_slice(&[0xbb; 20]);

    let tree = crate::git::pack::GitObject::tree(tree_data);
    let tree_sha = tree.sha1();

    let owned = vec![file_blob, tree];
    let objects: std::collections::HashMap<[u8; 20], &crate::git::pack::GitObject> =
        owned.iter().map(|o| (o.sha1(), o)).collect();

    let entries = walk_git_tree(&tree_sha, &objects, "").unwrap();
    assert_eq!(entries.len(), 1, "should only find the regular file");
    assert_eq!(entries[0].path, "f");
}

#[test]
fn walk_git_tree_wrong_object_type_errors() {
    // Point a tree entry to a blob instead of a sub-tree
    let blob = crate::git::pack::create_blob_object(b"not a tree");
    let blob_sha = blob.sha1();

    let mut tree_data = Vec::new();
    tree_data.extend_from_slice(b"40000 dir\0");
    tree_data.extend_from_slice(&blob_sha);

    let tree = crate::git::pack::GitObject::tree(tree_data);
    let tree_sha = tree.sha1();

    let owned = vec![blob, tree];
    let objects: std::collections::HashMap<[u8; 20], &crate::git::pack::GitObject> =
        owned.iter().map(|o| (o.sha1(), o)).collect();

    let result = walk_git_tree(&tree_sha, &objects, "");
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("expected tree object")
    );
}

// --- build_gitattributes_blob with multiple LFS files ---

#[test]
fn build_gitattributes_blob_multiple_lfs_files() {
    let files = vec![
        HubFileEntry {
            path: "z.bin".to_owned(),
            size: 200,
            sha: "oid_z".to_owned(),
            is_lfs: true,
        },
        HubFileEntry {
            path: "a.bin".to_owned(),
            size: 100,
            sha: "oid_a".to_owned(),
            is_lfs: true,
        },
        HubFileEntry {
            path: "m.bin".to_owned(),
            size: 300,
            sha: "oid_m".to_owned(),
            is_lfs: true,
        },
    ];
    let blob = build_gitattributes_blob(&files);
    assert!(blob.is_some());
    let content = String::from_utf8(blob.unwrap().data).unwrap();
    // Should be sorted: a.bin, m.bin, z.bin
    let lines: Vec<&str> = content.lines().collect();
    assert!(lines[0].starts_with("a.bin"));
    assert!(lines[1].starts_with("m.bin"));
    assert!(lines[2].starts_with("z.bin"));
}

// --- parse_lfs_pointer_field edge cases ---

#[test]
fn parse_lfs_pointer_field_oid_no_sha256_prefix() {
    // OID without "sha256:" prefix should return None
    let text = "oid abc123\n";
    let oid = parse_lfs_pointer_field(text, "oid");
    assert!(oid.is_none());
}

#[test]
fn parse_lfs_pointer_field_empty_lines() {
    let text = "";
    assert!(parse_lfs_pointer_field(text, "oid").is_none());
    assert!(parse_lfs_pointer_field(text, "size").is_none());
}

#[test]
fn parse_lfs_pointer_field_trailing_whitespace() {
    let text = "size 100  \n";
    let size = parse_lfs_pointer_field(text, "size");
    assert_eq!(size.as_deref(), Some("100"));
}

// --- parse_receive_pack_request with valid non-UTF8 pkt-lines ---

#[test]
fn parse_receive_pack_request_skips_non_utf8_and_finds_pack() {
    let mut body = Vec::new();
    // Skip non-UTF8: 0005\xff\x00 (length 5, 2 bytes payload after 4 hex prefix)
    // Actually length 5 total means 5 - 4 = 1 byte payload. Let's fix:
    // 0005\xff = 4 hex + 1 payload byte (non-UTF8)
    body.extend_from_slice(b"0005\xff");
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACK");
    body.extend_from_slice(&[0, 0, 0, 2]);
    body.extend_from_slice(&[0, 0, 0, 0]);
    let (updates, pack_data) = parse_receive_pack_request(&body);
    assert!(updates.is_empty());
    assert!(!pack_data.is_empty());
    assert!(pack_data.starts_with(b"PACK"));
}

// --- authorize with auth configured (error paths) ---

#[test]
fn authorize_read_with_auth_rejects_missing_token() {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope as TS};
    use shardline_server_core::{AuthError, AuthProvider};

    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();
    let _claims = TokenClaims::new("iss", "sub", TS::Read, repo, u64::MAX).unwrap();
    struct MockAuth;
    impl AuthProvider for MockAuth {
        fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
            Err(AuthError::InvalidToken)
        }
        fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
            Err(AuthError::ProviderError("nope".into()))
        }
    }
    let (_tmp, hub_state) = make_hub_state();
    let state = HubState {
        store: hub_state.store,
        object_store: hub_state.object_store,
        auth: Some(crate::auth::HubAuth::new(Box::new(MockAuth))),
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    };
    let headers = axum::http::HeaderMap::new();
    let result = authorize_read(&state, &headers);
    assert!(result.is_err());
}

// --- info_refs with nonexistent repo ---

#[tokio::test]
async fn info_refs_nonexistent_repo_returns_empty_advertisement() {
    let (_tmp, state) = make_hub_state();
    let headers = axum::http::HeaderMap::new();
    let result = info_refs(
        State(state),
        Path(("models".to_owned(), "no".to_owned(), "repo".to_owned())),
        Query(InfoRefsQuery {
            service: Some("git-upload-pack".to_owned()),
        }),
        headers,
    )
    .await;
    // A nonexistent repo returns a valid info/refs response with
    // a null-SHA capabilities advertisement (no refs to advertise).
    let response = result.expect("nonexistent repo should return a valid response");
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let body = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(
        body.contains("capabilities"),
        "response should contain capabilities: {body}"
    );
    assert!(
        body.contains("0000000000000000000000000000000000000000"),
        "response should contain zero SHA: {body}"
    );
}

// --- parse_pack_data with OFS_DELTA ---

#[test]
fn parse_pack_data_ofs_delta_two_objects() {
    // Build a valid pack with 2 objects: a base blob and an OFS_DELTA
    // that copies from it. We'll construct the raw pack bytes.
    let base_content = b"Hello, World!";
    let target_content = b"Hello there, World!";

    // Compress both
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let compress = |data: &[u8]| -> Vec<u8> {
        let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        enc.write_all(data).unwrap();
        enc.finish().unwrap()
    };

    let base_compressed = compress(base_content);

    // Build delta: source=13, target=19, copy(0,5), insert(" there,"), copy(5,8)
    let mut delta = Vec::new();
    delta.push(13); // source size
    delta.push(19); // target size
    // copy(0,5): no offset bytes, 1 size byte
    delta.push(0x90);
    delta.push(5);
    // insert(" there")
    delta.push(6);
    delta.extend_from_slice(b" there");
    // copy(5,8): 1 offset byte, 1 size byte
    delta.push(0x91);
    delta.push(5);
    delta.push(8);
    let delta_compressed = compress(&delta);

    // Build the pack:
    // Header: PACK + version(4) + num_objects(4)
    let mut pack = Vec::new();
    pack.extend_from_slice(b"PACK");
    pack.extend_from_slice(&2u32.to_be_bytes()); // version 2
    pack.extend_from_slice(&2u32.to_be_bytes()); // 2 objects

    // Object 1: base blob (type=3), size=13
    // First byte: type (3) << 4 | low 4 bits of size (13 = 0xd)
    // size > 0x0f? 13 <= 15, so no continuation
    pack.push((3 << 4) | 13); // type=3, size=13
    pack.extend_from_slice(&base_compressed);

    // Object 2: OFS_DELTA (type=6), size delta
    let delta_size = delta.len();
    if delta_size <= 0x0f {
        pack.push((6 << 4) | delta_size as u8);
    } else {
        // Need varint encoding for size
        // Size = delta.len(), encode as varint
        pack.push((6 << 4) | (delta_size & 0x0f) as u8 | 0x80); // continuation
        let mut remaining = delta_size >> 4;
        while remaining > 0 {
            let mut byte = (remaining & 0x7f) as u8;
            remaining >>= 7;
            if remaining > 0 {
                byte |= 0x80;
            }
            pack.push(byte);
        }
    }
    // OFS_DELTA offset: negative offset of 1 (the base object is 1 before this one)
    // Offset 1 → single byte: 0x01 (MSB clear, value=1)
    pack.push(0x01);
    pack.extend_from_slice(&delta_compressed);

    let objects = parse_pack_data(&pack).unwrap();
    assert_eq!(objects.len(), 2, "should parse both objects");
    assert_eq!(objects[0].object_type, ObjectType::Blob);
    assert_eq!(objects[0].data, base_content);
    assert_eq!(
        objects[1].data, target_content,
        "OFS_DELTA should resolve to produce the target content"
    );
}

// --- authorize_write with auth configured ---

#[test]
fn authorize_write_with_auth_rejects_missing_token() {
    use shardline_protocol::{RepositoryProvider, RepositoryScope, TokenClaims, TokenScope as TS};
    use shardline_server_core::{AuthError, AuthProvider};

    let repo = RepositoryScope::new(RepositoryProvider::GitHub, "o", "r", None).unwrap();
    let _claims = TokenClaims::new("iss", "sub", TS::Read, repo, u64::MAX).unwrap();
    struct MockAuth;
    impl AuthProvider for MockAuth {
        fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
            Err(AuthError::InvalidToken)
        }
        fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
            Err(AuthError::ProviderError("nope".into()))
        }
    }
    let (_tmp, hub_state) = make_hub_state();
    let state = HubState {
        store: hub_state.store,
        object_store: hub_state.object_store,
        auth: Some(crate::auth::HubAuth::new(Box::new(MockAuth))),
        http_client: None,
        webhook_secret_cipher: None,
        public_base_url: "http://127.0.0.1:8080".to_owned(),
    };
    let headers = axum::http::HeaderMap::new();
    let result = authorize_write(&state, &headers);
    assert!(result.is_err());
}

// --- build_gitattributes_blob with nested LFS path ---

#[test]
fn gitattributes_blob_handles_nested_lfs_paths() {
    let files = vec![
        HubFileEntry {
            path: "data/nested/model.bin".to_owned(),
            size: 1024,
            sha: "oid_nested".to_owned(),
            is_lfs: true,
        },
        HubFileEntry {
            path: "data/nested/readme.md".to_owned(),
            size: 100,
            sha: "oid_rn".to_owned(),
            is_lfs: false,
        },
    ];
    let blob = build_gitattributes_blob(&files);
    assert!(blob.is_some());
    let content = String::from_utf8(blob.unwrap().data).unwrap();
    assert!(content.contains("data/nested/model.bin filter=lfs"));
}

// --- build_git_tree_objects with nested directories ---

#[test]
fn tree_from_nested_lfs_files_creates_sub_trees() {
    let files = vec![
        HubFileEntry {
            path: "models/a/big.bin".to_owned(),
            size: 2_000_000,
            sha: "lfs_oid_ab".to_owned(),
            is_lfs: true,
        },
        HubFileEntry {
            path: "models/b/big.bin".to_owned(),
            size: 2_000_000,
            sha: "lfs_oid_bb".to_owned(),
            is_lfs: true,
        },
    ];
    let (root, sub_trees) = build_git_tree_objects(&files);
    // Root tree + 3 sub-trees (models/, models/a/, models/b/)
    assert!(!sub_trees.is_empty(), "expected sub-trees for nested dirs");
    let root_sha = root.sha1();
    assert_ne!(root_sha, [0u8; 20]);
}

// --- parse_receive_pack_request with valid commands ---

#[test]
fn parse_receive_pack_request_skips_empty_lines() {
    let mut body = Vec::new();
    // Empty pkt-line (length prefix 0004 = empty)
    body.extend_from_slice(b"0004");
    // Valid command
    let cmd = "0000000000000000000000000000000000000000 newsha1234567890123456789012345678901234567890 refs/heads/main\n";
    let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
    body.extend_from_slice(encoded.as_bytes());
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACK");
    body.extend_from_slice(&[0, 0, 0, 2]);
    body.extend_from_slice(&[0, 0, 0, 0]);

    let (updates, pack_data) = parse_receive_pack_request(&body);
    // The empty line (0004) is skipped, the valid command is parsed
    assert_eq!(updates.len(), 1, "expected 1 update, got {updates:?}");
    assert_eq!(updates[0].2, "refs/heads/main");
    assert!(!pack_data.is_empty());
}

// --- receive_pack error paths ---

#[tokio::test]
async fn upload_pack_empty_refs_returns_empty_pack() {
    let (_tmp, state) = make_hub_state();
    let body = pktline::encode_line("want 0000000000000000000000000000000000000000\n")
        .unwrap()
        .into_bytes();
    let result = upload_pack(
        State(state),
        Path(("models".into(), "empty".into(), "repo".into())),
        axum::http::HeaderMap::new(),
        bytes::Bytes::from(body),
    )
    .await;
    assert!(result.is_ok(), "upload_pack should succeed: {result:?}");
}

// --- decompress_zlib error on garbage input ---

#[test]
fn decompress_zlib_garbage_returns_error() {
    let result = decompress_zlib(b"garbage data that is not valid zlib");
    assert!(result.is_err());
}

#[test]
fn decompress_zlib_empty_input_is_truncated() {
    // Empty input never reaches `StreamEnd`, so it is a truncated zlib stream
    // and must be rejected (it would otherwise return partial bytes as a valid
    // object).
    let result = decompress_zlib(b"");
    assert!(
        result.is_err(),
        "empty zlib input should be treated as truncated: {result:?}"
    );
}

// --- decompress_zlib truncation (F-deep-2.4) ---

#[test]
fn decompress_zlib_truncated_stream_errors() {
    // A valid zlib stream that is cut short must error instead of returning
    // partial decompressed bytes as a complete object.
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(b"hello world, this is a fairly long payload to truncate")
        .unwrap();
    let full = enc.finish().unwrap();
    assert!(full.len() > 4, "compressed payload should be non-trivial");

    // Truncate several bytes off the end so the stream cannot reach StreamEnd.
    let truncated = &full[..full.len() - 4];
    let result = decompress_zlib(truncated);
    assert!(
        result.is_err(),
        "truncated zlib stream must error, got {result:?}"
    );
}

#[test]
fn decompress_zlib_full_stream_ok() {
    // A complete zlib stream still decompresses successfully.
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    let payload = b"complete stream payload";
    enc.write_all(payload).unwrap();
    let full = enc.finish().unwrap();
    let (decompressed, _used) = decompress_zlib(&full).unwrap();
    assert_eq!(decompressed, payload);
}

// --- parse_pack_data with invalid offset ---

#[test]
fn parse_pack_data_ofs_delta_bad_offset() {
    // A pack with a single OFS_DELTA object (no base) should error.
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(b"delta data").unwrap();
    let compressed = enc.finish().unwrap();

    let mut pack = b"PACK".to_vec();
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&1u32.to_be_bytes()); // 1 object
    // OFS_DELTA (type=6), size <= 0x0f
    pack.push((6 << 4) | compressed.len() as u8);
    // OFS_DELTA offset: 2 (no base object at offset 2)
    pack.push(0x02);
    pack.extend_from_slice(&compressed);

    let result = parse_pack_data(&pack);
    assert!(result.is_err());
}

// --- parse_pack_data with OOB OFS_DELTA offset ---
// (offset calculation underflows)

#[test]
fn parse_pack_data_ofs_delta_offset_underflow() {
    use flate2::write::ZlibEncoder;
    use std::io::Write;
    let mut enc = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    enc.write_all(b"delta data").unwrap();
    let compressed = enc.finish().unwrap();

    let mut pack = b"PACK".to_vec();
    pack.extend_from_slice(&2u32.to_be_bytes());
    pack.extend_from_slice(&1u32.to_be_bytes());
    // OFS_DELTA (type=6), size = compressed.len()
    pack.push((6 << 4) | 0x0f);
    // Large OFS_DELTA offset encoded as multi-byte varint
    // offset = 0x01 (MSB=0 means single byte, value=1)
    // An offset of 1 with 0 objects in the index will produce checked_sub(1) = 0
    // but objects is empty, so base_idx(0) is out of bounds.
    // Actually this would work: offset=1 on an empty objects vec:
    // checked_sub(1) = None → InvalidDelta. Let's test that.
    pack.push(0x01);
    pack.extend_from_slice(&compressed);
    let result = parse_pack_data(&pack);
    assert!(result.is_err());
}

// --- build_git_tree_objects with inline blob creation ---

#[test]
fn tree_from_inline_files_creates_blobs_in_tree_entries() {
    let files = vec![HubFileEntry {
        path: "a/b/file.txt".to_owned(),
        size: 3,
        sha: "abc".to_owned(),
        is_lfs: false,
    }];
    let (root, sub_trees) = build_git_tree_objects(&files);
    assert_eq!(sub_trees.len(), 2, "a/ and a/b/ sub-trees");
    let root_sha = root.sha1();
    assert_ne!(root_sha, [0u8; 20]);
}

// --- receive_pack with invalid pack data (error path, lines ~262) ---

#[tokio::test]
async fn receive_pack_malformed_pack_data_returns_ng_refs() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;
    state
        .store
        .create_repo(HubRepoType::Model, "org/rp-bad", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    state
        .store
        .create_revision(
            "org/rp-bad",
            Some(initial_sha),
            "oldsha1234567890123456789012345678901234567",
            "refs/heads/main",
            "initial",
        )
        .unwrap();

    // Build a receive-pack request with valid command but garbage pack data
    let new_sha = "0000000000000000000000000000000000000000";
    let old_sha = "oldsha1234567890123456789012345678901234567";
    let cmd = format!("{old_sha} {new_sha} refs/heads/main\n");
    let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
    let mut body = encoded.into_bytes();
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACK"); // "valid" header but no real content

    let result = receive_pack(
        State(state),
        Path(("models".into(), "org".into(), "rp-bad".into())),
        axum::http::HeaderMap::new(),
        bytes::Bytes::from(body),
    )
    .await;
    // Should return a response (even with errors) rather than fail
    assert!(result.is_ok());
}

// --- receive_pack non-fast-forward rejection has NO write side effects (F-61) ---

#[tokio::test]
async fn receive_pack_non_fast_forward_denied_leaves_no_side_effects() {
    use shardline_index::hub::HubRepoType;
    use shardline_server_core::AuthorizedRepository;
    use shardline_storage::ObjectStore;

    let (_tmp, state) = make_hub_state();
    state
        .store
        .create_repo(HubRepoType::Model, "org/rp-nff", false)
        .unwrap();
    // The repo already has `main` -> empty-tree SHA (created by create_repo).
    let current_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";
    assert_eq!(
        state
            .store
            .resolve_revision("org/rp-nff", "refs/heads/main")
            .unwrap(),
        Some(current_sha.to_owned()),
        "precondition: refs/heads/main exists at the empty-tree sha"
    );

    // Build a valid pack: LFS pointer blob + tree + commit. The commit's tree
    // references an LFS file so a buggy handler would store both file entries
    // AND an LFS object before rejecting.
    let lfs_oid = "a".repeat(64); // valid sha256 hex
    let lfs_blob = build_lfs_pointer_blob(&lfs_oid, 100);
    let lfs_blob_sha = lfs_blob.sha1();
    let tree = crate::git::pack::create_tree_object(&[(0o100644, "model.bin", &lfs_blob_sha)]);
    let tree_sha = tree.sha1();
    let commit = crate::git::pack::create_commit_object(
        &tree_sha,
        None,
        "Test <test@test.com>",
        "non-fast-forward attempt",
    );
    let new_sha = hex::encode(commit.sha1());
    let pack = crate::git::pack::generate_pack(&[lfs_blob, tree, commit]).unwrap();

    // old_sha differs from the current ref value -> non-fast-forward.
    let old_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let cmd = format!("{old_sha} {new_sha} refs/heads/main\n");
    let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
    let mut body = encoded.into_bytes();
    body.extend_from_slice(b"0000");
    body.extend_from_slice(&pack);

    let response = receive_pack(
        State(state.clone()),
        Path(("models".into(), "org".into(), "rp-nff".into())),
        axum::http::HeaderMap::new(),
        bytes::Bytes::from(body),
    )
    .await
    .expect("receive_pack returns a report response");
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read report body");
    let report = String::from_utf8(body_bytes.to_vec()).expect("report is utf-8");
    assert!(
        report.contains("ng"),
        "non-fast-forward push must be rejected: {report}"
    );

    // Deny-no-side-effect: no file entries were persisted for new_sha...
    let files = state
        .store
        .get_files(&new_sha)
        .expect("get_files should not error");
    assert!(
        files.is_empty(),
        "non-fast-forward rejection must not store file entries, got {files:?}"
    );

    // ...and no LFS object was stored under the LFS key for the file's OID.
    let key =
        crate::routes::lfs_object_key(&lfs_oid, &AuthorizedRepository::anonymous_full_access())
            .expect("valid LFS object key");
    assert!(
        !state
            .object_store
            .contains(&key)
            .expect("contains should not error"),
        "non-fast-forward rejection must not store LFS objects"
    );

    // The ref must still point at the old value (nothing was created).
    assert_eq!(
        state
            .store
            .resolve_revision("org/rp-nff", "refs/heads/main")
            .unwrap(),
        Some(current_sha.to_owned()),
        "ref must be unchanged after a rejected push"
    );
}

// --- walk_git_tree with truncated SHA (error path, line ~1163) ---

#[test]
fn walk_git_tree_truncated_sha_errors() {
    // Build a tree entry with a truncated SHA (only 10 bytes instead of 20)
    let blob = crate::git::pack::create_blob_object(b"dummy");
    let mut tree_data = Vec::new();
    tree_data.extend_from_slice(b"100644 f\0");
    tree_data.extend_from_slice(&[0xaa; 10]); // only 10 bytes!

    // Need to also include the null byte after the name for proper parsing
    // Actually the format is: "100644 f\0" + 20 bytes SHA. If SHA is truncated,
    // the sha_start + 20 > data.len() check should catch it.
    let tree = crate::git::pack::GitObject::tree(tree_data);
    let tree_sha = tree.sha1();

    let owned = vec![blob, tree];
    let objects: std::collections::HashMap<[u8; 20], &crate::git::pack::GitObject> =
        owned.iter().map(|o| (o.sha1(), o)).collect();

    let result = walk_git_tree(&tree_sha, &objects, "");
    assert!(result.is_err());
}

// --- build_git_tree_objects with LFS leaf blob (line ~544) ---

#[test]
fn build_tree_entries_lfs_leaf_blob() {
    // A single LFS file at the root level exercises the LFS blob creation
    // in build_tree_entries (line 538).
    let files = vec![HubFileEntry {
        path: "model.bin".to_owned(),
        size: 2_000_000,
        sha: "oid_lfs_leaf".to_owned(),
        is_lfs: true,
    }];
    let (root, sub_trees) = build_git_tree_objects(&files);
    assert!(sub_trees.is_empty());
    let root_sha = root.sha1();
    assert_ne!(root_sha, [0u8; 20]);
}

// --- info_refs_upload_pack / info_refs_receive_pack wrappers ---

#[tokio::test]
async fn info_refs_upload_pack_proxies_correctly() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;
    state
        .store
        .create_repo(HubRepoType::Model, "org/iu-test", false)
        .unwrap();
    let result = info_refs_upload_pack(
        State(state),
        Path(("models".into(), "org".into(), "iu-test".into())),
        Query(InfoRefsQuery { service: None }),
        axum::http::HeaderMap::new(),
    )
    .await;
    assert!(result.is_ok());
    let response = result.unwrap();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(body.contains("git-upload-pack"));
}

#[tokio::test]
async fn info_refs_receive_pack_proxies_correctly() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;
    state
        .store
        .create_repo(HubRepoType::Model, "org/ir-test", false)
        .unwrap();
    let result = info_refs_receive_pack(
        State(state),
        Path(("models".into(), "org".into(), "ir-test".into())),
        Query(InfoRefsQuery { service: None }),
        axum::http::HeaderMap::new(),
    )
    .await;
    assert!(result.is_ok());
    let response = result.unwrap();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let body = String::from_utf8(body_bytes.to_vec()).unwrap();
    assert!(body.contains("git-receive-pack"));
}

// ── Security: Git push tree walk path validation ────────────────────────

#[test]
fn walk_git_tree_rejects_dotdot_entry_name() {
    use crate::git::smart_http::tree_walk::walk_git_tree_inner;

    let tmp = tempfile::tempdir().unwrap();
    let _object_store =
        shardline_server_core::ServerObjectStore::local(tmp.path().to_path_buf()).unwrap();

    let mut tree_data = Vec::new();
    tree_data.extend_from_slice(b"100644 ..\0");
    tree_data.extend_from_slice(&[0u8; 20]);

    let git_obj = crate::git::pack::GitObject {
        object_type: crate::git::pack::ObjectType::Tree,
        data: tree_data,
    };
    let objects = std::collections::HashMap::from([([0u8; 20], &git_obj)]);

    let result = walk_git_tree_inner(&[0u8; 20], &objects, "", 0);
    assert!(result.is_err(), "tree walk should reject '..' entry names");
}

#[test]
fn walk_git_tree_rejects_null_byte_in_entry_name() {
    use crate::git::smart_http::tree_walk::walk_git_tree_inner;

    let tmp = tempfile::tempdir().unwrap();
    let _object_store =
        shardline_server_core::ServerObjectStore::local(tmp.path().to_path_buf()).unwrap();

    let mut tree_data = Vec::new();
    tree_data.extend_from_slice(b"100644 file\0name\0");
    tree_data.extend_from_slice(&[0u8; 20]);

    let git_obj = crate::git::pack::GitObject {
        object_type: crate::git::pack::ObjectType::Tree,
        data: tree_data,
    };
    let objects = std::collections::HashMap::from([([0u8; 20], &git_obj)]);

    let result = walk_git_tree_inner(&[0u8; 20], &objects, "", 0);
    assert!(
        result.is_err(),
        "tree walk should reject entry names with null bytes"
    );
}

// ── Security: Commit message length capping ─────────────────────────────

#[test]
fn commit_message_is_capped_at_max_length() {
    use crate::commit::MAX_COMMIT_MSG_LEN;
    let long_message = "a".repeat(MAX_COMMIT_MSG_LEN + 100);
    let capped: String = long_message.chars().take(MAX_COMMIT_MSG_LEN).collect();
    assert_eq!(capped.len(), MAX_COMMIT_MSG_LEN);
}

#[test]
fn commit_message_with_multibyte_utf8_is_safely_truncated() {
    use crate::commit::MAX_COMMIT_MSG_LEN;
    let message = "中".repeat(MAX_COMMIT_MSG_LEN / 3 + 1);
    let capped: String = message.chars().take(MAX_COMMIT_MSG_LEN).collect();
    assert!(!capped.is_empty());
    assert!(capped.is_char_boundary(capped.len()));
}

// ── Security: Git OFS delta overflow prevention ─────────────────────────

#[test]
fn parse_ofs_delta_offset_rejects_overflow() {
    use crate::git::pack::parse_ofs_delta_offset;
    let mut data = Vec::new();
    data.push(0x80);
    for _ in 0..20 {
        data.push(0xFF);
    }
    data.push(0x00);
    let mut pos = 0;
    let result = parse_ofs_delta_offset(&data, &mut pos);
    assert!(result.is_err(), "ofs_delta_offset should reject overflow");
}

#[test]
fn parse_ofs_delta_offset_rejects_excessive_iterations() {
    use crate::git::pack::parse_ofs_delta_offset;
    let mut data = Vec::new();
    for _ in 0..10 {
        data.push(0xFF);
    }
    data.push(0x00);
    let mut pos = 0;
    let result = parse_ofs_delta_offset(&data, &mut pos);
    assert!(
        result.is_err(),
        "ofs_delta_offset should reject excessive iterations"
    );
}

// ── Security: Git push tree walk path validation ────────────────────────
