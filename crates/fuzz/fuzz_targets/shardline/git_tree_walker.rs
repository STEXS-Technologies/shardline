#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_hub_api::git::pack::{GitObject, ObjectType};
use std::collections::HashMap;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);

    // ── Deterministic, well-formed two-level tree ─────────────────────────
    // Build a populated object map so walk_git_tree actually recurses and
    // parses entries instead of failing fast on an empty map. Blob contents
    // are benign so the walk always succeeds (no LFS-pointer field parsing
    // surprises), and the fuzz input is exercised separately below.
    let root_sha: [u8; 20] = [0u8; 20];
    let sub_sha: [u8; 20] = [1u8; 20];
    let blob1_sha: [u8; 20] = [2u8; 20];
    let blob2_sha: [u8; 20] = [3u8; 20];

    let mut root_data = Vec::new();
    root_data.extend_from_slice(b"40000 dir\0");
    root_data.extend_from_slice(&sub_sha);
    root_data.extend_from_slice(b"100644 file.txt\0");
    root_data.extend_from_slice(&blob1_sha);

    let mut sub_data = Vec::new();
    sub_data.extend_from_slice(b"100755 inner.txt\0");
    sub_data.extend_from_slice(&blob2_sha);

    let blob1 = GitObject {
        object_type: ObjectType::Blob,
        data: b"plain file content\n".to_vec(),
    };
    let blob2 = GitObject {
        object_type: ObjectType::Blob,
        data: b"more content\n".to_vec(),
    };
    let root = GitObject {
        object_type: ObjectType::Tree,
        data: root_data,
    };
    let sub = GitObject {
        object_type: ObjectType::Tree,
        data: sub_data,
    };

    let mut objects: HashMap<[u8; 20], &GitObject> = HashMap::new();
    objects.insert(root_sha, &root);
    objects.insert(sub_sha, &sub);
    objects.insert(blob1_sha, &blob1);
    objects.insert(blob2_sha, &blob2);

    // INVARIANT 1: walking the same populated map twice is deterministic.
    let first = shardline_hub_api::git::smart_http::walk_git_tree(&root_sha, &objects, "");
    let second = shardline_hub_api::git::smart_http::walk_git_tree(&root_sha, &objects, "");
    assert_eq!(format!("{first:?}"), format!("{second:?}"));

    // INVARIANT 2: the well-formed tree parses successfully and recurses into
    // the subtree (both blobs are present and non-LFS).
    assert!(matches!(first, Ok(entries) if !entries.is_empty()));

    // ── Exercise the raw tree-entry parser on arbitrary bytes ─────────────
    // Feed the fuzz input directly as tree-object data with no referenced
    // objects registered. This drives the `<mode> <name>\0<sha>` parse loop on
    // arbitrary/malformed/truncated input without panicking; success is not
    // required (missing blobs/trees legitimately error).
    let direct = GitObject {
        object_type: ObjectType::Tree,
        data: data.to_vec(),
    };
    let mut parse_map: HashMap<[u8; 20], &GitObject> = HashMap::new();
    parse_map.insert(root_sha, &direct);
    let _raw_result = shardline_hub_api::git::smart_http::walk_git_tree(&root_sha, &parse_map, "");
});
