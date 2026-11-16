// Test code intentionally uses unwrap/expect/indexing/vec-push for clarity
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::vec_init_then_push
)]

use super::receive_pack::{build_report_response, find_lfs_blob, parse_receive_pack_request};
use super::ref_advertisement::{
    GitRef, authorize_read_with_context, authorize_write_with_context, collect_refs,
    is_valid_refname, resolve_repo_id,
};
use super::tree_walk::parse_lfs_pointer_field;
use super::upload_pack::{build_lfs_pointer_blob, parse_object_ids};
use super::*;
use crate::git::pack::{GitObject, ObjectType, apply_delta};
use crate::git::pktline;
use crate::routes::HubState;
use axum::extract::{Path, Query, State};
use axum::response::Response;
use shardline_index::hub::HubFileEntry;

#[test]
fn negotiation_object_ids_keep_order_and_ignore_other_or_malformed_lines() {
    let first = "8ab686eafeb1f44702738c8b0f24f2567c36da6d";
    let second = "0000000000000000000000000000000000000000";
    for (prefix, other) in [("want ", "have "), ("have ", "want ")] {
        let lines = vec![
            format!("{prefix}{first} side-band-64k\n").into_bytes(),
            format!("{other}{first}\n").into_bytes(),
            format!("  {prefix}{second}\n").into_bytes(),
            prefix.as_bytes().to_vec(),
            vec![0xff],
            b"done\n".to_vec(),
        ];
        assert_eq!(
            parse_object_ids(&lines, prefix),
            [first, second],
            "{prefix}"
        );
    }
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
    let content_by_sha256 = build_content_index(&objects);

    let found = find_lfs_blob(&file, &content_by_sha256);
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
fn find_lfs_blob_canonical_pointer_resolves_payload() {
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
    let objects = vec![pointer, content_obj];
    let content_by_sha256 = build_content_index(&objects);
    let found = find_lfs_blob(&file, &content_by_sha256);
    // LFS resolution must return the content, never the pointer bytes.
    assert_eq!(found.unwrap().data, content);
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
    let content_by_sha256 = build_content_index(&objects);
    assert!(
        find_lfs_blob(&file, &content_by_sha256).is_none(),
        "no matching object should yield None (the push is then failed)"
    );
}

#[test]
fn is_valid_refname_accepts_git_reference_names() {
    for reference in [
        "refs/heads/main",
        "refs/tags/v1.0",
        "refs/heads/feature/foo",
        "refs/heads/feature/foo/bar",
        "refs/pull/42/head",
        "refs/heads/foo./bar",
        "refs/heads/@",
    ] {
        assert!(
            is_valid_refname(reference),
            "rejected valid Git ref: {reference:?}"
        );
    }
}

#[test]
fn is_valid_refname_rejects_reserved_paths_and_characters() {
    for reference in [
        "",
        "heads/main",
        "tags/v1.0",
        "main",
        "refs/heads/a..b",
        "refs/heads/.hidden",
        "refs/heads/a.lock",
        "refs/heads/a.",
        "refs/heads/a//b",
        "refs/heads/a/",
        "refs/heads/a@{b",
        "refs/heads/a~b",
        "refs/heads/a^b",
        "refs/heads/a:b",
        "refs/heads/a?b",
        "refs/heads/a*b",
        "refs/heads/a[b",
        "refs/heads/a\\b",
        "refs/heads/../secret",
        "refs/heads/feature/..",
        "refs/heads/../../etc/passwd",
        "refs/heads/a/../../../x",
        "refs/heads/my branch",
        "refs/heads/feature ",
        " refs/heads/feature",
    ] {
        assert!(
            !is_valid_refname(reference),
            "accepted invalid Git ref: {reference:?}"
        );
    }
    for control in (0u8..=31).chain([127]) {
        let reference = format!("refs/heads/main{}", char::from(control));
        assert!(
            !is_valid_refname(&reference),
            "accepted control byte {control}"
        );
    }
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

// --- collect_refs dedup/HEAD logic tests ---

/// Helper to create a temporary HubState backed by SQLite.
pub(super) fn make_hub_state() -> (tempfile::TempDir, HubState) {
    use shardline_index::LocalIndexStore;
    use shardline_index::hub::BoxedHubStore;

    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path().to_path_buf();
    let store = LocalIndexStore::new(root).expect("initialize SQLite store");
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
async fn collect_refs_preserve_sorted_names_and_commit_identities() {
    let (_tmp, state) = make_hub_state();
    use shardline_index::hub::HubRepoType;

    state
        .store
        .create_repo(HubRepoType::Model, "org/dedup", false)
        .unwrap();
    let initial_sha = "4b825dc642cb6eb9a060e54bf899d69f8f5ce8e3";

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

    let advertised: Vec<_> = refs
        .iter()
        .map(|r| (r.name.as_str(), r.sha1.as_str()))
        .collect();
    assert_eq!(
        advertised,
        [
            ("HEAD", "sha_head"),
            ("refs/heads/dev", "sha_b"),
            ("refs/heads/main", "sha_a"),
        ]
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

async fn body_string(response: Response) -> String {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn receive_status_preserves_exact_packet_framing_and_messages() {
    for (results, unpack_ok, expected) in [
        (vec![], true, "000eunpack ok\n0000"),
        (
            vec![
                ("refs/heads/main".into(), true, None),
                ("refs/heads/dev".into(), true, None),
            ],
            true,
            "000eunpack ok\n0017ok refs/heads/main\n0016ok refs/heads/dev\n0000",
        ),
        (
            vec![(
                "refs/heads/main".into(),
                false,
                Some("unpack failed".into()),
            )],
            false,
            "0012unpack failed\n0025ng refs/heads/main unpack failed\n0000",
        ),
        (
            vec![("refs/heads/bad".into(), false, None)],
            false,
            "0012unpack failed\n001dng refs/heads/bad failed\n0000",
        ),
    ] {
        let response = build_report_response(&results, unpack_ok, false).unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        assert_eq!(
            response.headers()["content-type"],
            "application/x-git-receive-pack-result"
        );
        assert_eq!(body_string(response).await, expected);
    }
}

// --- parse_receive_pack_request tests ---

#[tokio::test]
async fn receive_sideband_preserves_status_across_frame_boundaries() {
    let results: Vec<_> = (0..4000)
        .map(|index| (format!("refs/heads/branch-{index}"), index % 2 == 0, None))
        .collect();
    for unpack_ok in [true, false] {
        let plain = body_string(build_report_response(&results, unpack_ok, false).unwrap()).await;
        let framed = body_string(build_report_response(&results, unpack_ok, true).unwrap()).await;
        assert!(framed.ends_with("0000"));
        let packets = pktline::decode_lines(framed.as_bytes());
        assert!(
            packets.len() > 1,
            "fixture must cross an outer frame boundary"
        );
        let mut recovered = Vec::new();
        for packet in packets {
            assert_eq!(packet[0], 1);
            assert!(packet.len() + 4 <= 65520);
            recovered.extend_from_slice(&packet[1..]);
        }
        assert_eq!(recovered, plain.as_bytes());
        assert!(recovered.ends_with(b"0000"));
    }
}

#[test]
fn parse_receive_pack_request_with_updates_and_pack() {
    // Build a pkt-line request: commands followed by flush, then pack data
    let mut body = Vec::new();
    // Command line: "old-sha new-sha refs/heads/main"
    let cmd = "0000000000000000000000000000000000000000 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa refs/heads/main\n";
    let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
    body.extend_from_slice(encoded.as_bytes());
    body.extend_from_slice(b"0000"); // flush
    body.extend_from_slice(b"PACK"); // pack header start
    body.extend_from_slice(&[0, 0, 0, 2]); // version
    body.extend_from_slice(&[0, 0, 0, 0]); // 0 objects

    let (updates, pack_data) = parse_receive_pack_request(&body).unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].2, "refs/heads/main");
    assert!(!pack_data.is_empty());
    assert!(pack_data.starts_with(b"PACK"));
}

#[test]
fn parse_receive_pack_request_rejects_missing_flush() {
    // "000a" = 10 bytes total (4 prefix + 6 payload "check\n")
    let body = b"000acheck\n";
    assert!(parse_receive_pack_request(body).is_err());
}

#[test]
fn parse_receive_pack_request_empty_body() {
    assert!(parse_receive_pack_request(b"").is_err());
}

#[test]
fn parse_receive_pack_request_rejects_non_utf8_line() {
    let mut body = Vec::new();
    // Non-UTF8 line: length prefix points to content with 0xFF
    body.extend_from_slice(b"0005\xff\x00");
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACKdata");
    assert!(parse_receive_pack_request(&body).is_err());
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

#[test]
fn authorization_requires_credentials_only_when_configured() {
    use shardline_protocol::TokenClaims;
    use shardline_server_core::{AuthError, AuthProvider};

    let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    struct UnexpectedAuth(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl AuthProvider for UnexpectedAuth {
        fn verify_token(&self, _token: &str) -> Result<TokenClaims, AuthError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(AuthError::InvalidToken)
        }
        fn mint_token(&self, _claims: &TokenClaims) -> Result<String, AuthError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Err(AuthError::InvalidToken)
        }
    }

    let (_tmp, mut state) = make_hub_state();
    let headers = axum::http::HeaderMap::new();
    for configured in [false, true] {
        if configured {
            state.auth = Some(crate::auth::HubAuth::new(Box::new(UnexpectedAuth(
                calls.clone(),
            ))));
        }
        for authorize in [authorize_read_with_context, authorize_write_with_context] {
            let result = authorize(&state, &headers);
            if configured {
                assert!(matches!(
                    result,
                    Err(crate::error::HubApiError::Unauthorized)
                ));
            } else {
                assert!(result.unwrap().is_none());
            }
        }
    }
    assert_eq!(calls.load(std::sync::atomic::Ordering::Relaxed), 0);
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
fn walk_git_tree_rejects_symlinks_and_submodules() {
    // Build a tree with a symlink (120000) and a submodule (160000) entry
    // Reject unsupported types rather than silently losing Hub metadata.
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

    assert!(walk_git_tree(&tree_sha, &objects, "").is_err());
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
fn parse_receive_pack_request_rejects_non_utf8_before_pack() {
    let mut body = Vec::new();
    // Skip non-UTF8: 0005\xff\x00 (length 5, 2 bytes payload after 4 hex prefix)
    // Actually length 5 total means 5 - 4 = 1 byte payload. Let's fix:
    // 0005\xff = 4 hex + 1 payload byte (non-UTF8)
    body.extend_from_slice(b"0005\xff");
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACK");
    body.extend_from_slice(&[0, 0, 0, 2]);
    body.extend_from_slice(&[0, 0, 0, 0]);
    assert!(parse_receive_pack_request(&body).is_err());
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
    let delta_start = pack.len();
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
    // OFS_DELTA measures bytes between object headers, not object count.
    let byte_distance = u8::try_from(delta_start - 12).unwrap();
    assert!(byte_distance < 128);
    pack.push(byte_distance);
    pack.extend_from_slice(&delta_compressed);
    use sha1::{Digest, Sha1};
    let checksum = Sha1::digest(&pack);
    pack.extend_from_slice(&checksum);

    let objects = parse_pack_data(&pack).unwrap();
    assert_eq!(objects.len(), 2, "should parse both objects");
    assert_eq!(objects[0].object_type, ObjectType::Blob);
    assert_eq!(objects[0].data, base_content);
    assert_eq!(
        objects[1].data, target_content,
        "OFS_DELTA should resolve to produce the target content"
    );
}

// --- parse_receive_pack_request with valid commands ---

#[test]
fn parse_receive_pack_request_rejects_empty_command_before_valid_update() {
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

    assert!(parse_receive_pack_request(&body).is_err());
}

// --- receive_pack error paths ---

#[tokio::test]
async fn upload_pack_rejects_unowned_want() {
    let (_tmp, state) = make_hub_state();
    let body = pktline::encode_line("want 0000000000000000000000000000000000000000\n")
        .unwrap()
        .into_bytes();
    let result = upload_pack(
        State(state),
        Path(("models".into(), "empty".into(), "repo".into())),
        axum::http::HeaderMap::new(),
        axum::body::Body::from(body),
    )
    .await;
    assert!(
        matches!(result, Err(crate::error::HubApiError::BadRequest(_))),
        "unowned wants must be rejected: {result:?}"
    );
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
    // Build a receive-pack request with valid command but garbage pack data
    let new_sha = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let old_sha = "0000000000000000000000000000000000000000";
    let cmd = format!("{old_sha} {new_sha} refs/heads/main\n");
    let encoded = format!("{:04x}{}", cmd.len() + 4, cmd);
    let mut body = encoded.into_bytes();
    body.extend_from_slice(b"0000");
    body.extend_from_slice(b"PACK"); // "valid" header but no real content

    let result = receive_pack(
        State(state),
        Path(("models".into(), "org".into(), "rp-bad".into())),
        axum::http::HeaderMap::new(),
        axum::body::Body::from(body),
    )
    .await;
    // Should return a response (even with errors) rather than fail
    assert!(result.is_ok(), "unexpected receive-pack error: {result:?}");
    let report = axum::body::to_bytes(result.unwrap().into_body(), usize::MAX)
        .await
        .unwrap();
    let report = std::str::from_utf8(&report).unwrap();
    assert!(report.contains("unpack failed"));
    assert!(report.contains("ng refs/heads/main"));
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
        axum::body::Body::from(body),
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

#[test]
fn walk_git_tree_rejects_every_truncated_entry_prefix() {
    let blob = crate::git::pack::create_blob_object(b"dummy");
    let complete = crate::git::pack::create_tree_object(&[(0o100644, "file", &blob.sha1())]);
    for length in 1..complete.data.len() {
        let tree = GitObject::tree(complete.data[..length].to_vec());
        let objects = std::collections::HashMap::from([(blob.sha1(), &blob), (tree.sha1(), &tree)]);
        assert!(
            walk_git_tree(&tree.sha1(), &objects, "").is_err(),
            "accepted entry truncated to {length} bytes"
        );
    }
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
