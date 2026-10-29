#![allow(clippy::unwrap_used)]

use super::*;
use crate::FsckIssue;
use shardline_index::{LocalRecordStore, XetXorbChunk};
use shardline_xet_core::merklehash::compute_data_hash;

fn descriptor() -> XetXorbDescriptor {
    let members = [compute_data_hash(b"first"), compute_data_hash(b"second")];
    XetXorbDescriptor {
        hash: xorb_hash(&[(members[0], 5), (members[1], 6)]).hex(),
        total_bytes: 64,
        chunks: vec![
            XetXorbChunk {
                hash: members[0].hex(),
                packed_start: 0,
                packed_end: 16,
                unpacked_start: 0,
                unpacked_end: 5,
            },
            XetXorbChunk {
                hash: members[1].hex(),
                packed_start: 16,
                packed_end: 32,
                unpacked_start: 5,
                unpacked_end: 11,
            },
        ],
    }
}

#[tokio::test]
async fn maximum_member_index_checks_last_and_extra_rows_before_verified_repair() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let members: Vec<_> = (0..shardline_index::MAX_XET_DESCRIPTOR_CHUNKS)
        .map(|index| compute_data_hash(&index.to_le_bytes()))
        .collect();
    let leaves: Vec<_> = members.iter().map(|hash| (*hash, 8)).collect();
    let descriptor = XetXorbDescriptor {
        hash: xorb_hash(&leaves).hex(),
        total_bytes: u64::try_from(members.len()).unwrap() * 16 + 32,
        chunks: members
            .iter()
            .enumerate()
            .map(|(index, hash)| {
                let index = u64::try_from(index).unwrap();
                XetXorbChunk {
                    hash: hash.hex(),
                    packed_start: index * 16,
                    packed_end: (index + 1) * 16,
                    unpacked_start: index * 8,
                    unpacked_end: (index + 1) * 8,
                }
            })
            .collect(),
    };
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    let issues: Vec<_> = descriptor
        .chunks
        .iter()
        .map(|chunk| missing(&chunk.hash, &descriptor.hash))
        .collect();
    let mut findings = report(issues.clone());
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert!(findings.is_clean());

    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "UPDATE shardline_xet_chunks SET chunk_hash=?1 WHERE xorb_hash=?2 AND chunk_index=?3",
            rusqlite::params![
                "0".repeat(64),
                descriptor.hash,
                i64::try_from(members.len() - 1).unwrap()
            ],
        )
        .unwrap();
    let mut damaged = report(issues.clone());
    inspect_missing_copies(&store, &mut damaged).await.unwrap();
    assert_eq!(damaged.issues, issues);
    connection
        .execute_batch(&format!(
            "CREATE TRIGGER fail_final_member BEFORE INSERT ON shardline_xet_chunks
             WHEN NEW.chunk_index={} BEGIN SELECT RAISE(ABORT,'injected repair failure'); END;",
            members.len() - 1
        ))
        .unwrap();
    let mut select = connection
        .prepare("SELECT chunk_index,chunk_hash FROM shardline_xet_chunks WHERE xorb_hash=?1 ORDER BY chunk_index")
        .unwrap();
    let before: Vec<(i64, String)> = select
        .query_map([&descriptor.hash], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert!(
        store
            .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
            .await
            .is_err()
    );
    let after: Vec<(i64, String)> = select
        .query_map([&descriptor.hash], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap()
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    assert_eq!(
        after, before,
        "failed repair must preserve the original index"
    );
    inspect_missing_copies(&store, &mut damaged).await.unwrap();
    assert_eq!(damaged.issues, issues);
    connection
        .execute_batch("DROP TRIGGER fail_final_member")
        .unwrap();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    inspect_missing_copies(&store, &mut damaged).await.unwrap();
    assert!(damaged.is_clean());

    // An extra row must not disappear behind the bounded lookup's LIMIT.
    connection
        .execute(
            "INSERT INTO shardline_xet_chunks(xorb_hash,chunk_index,chunk_hash) VALUES(?1,?2,?3)",
            rusqlite::params![
                descriptor.hash,
                i64::try_from(members.len()).unwrap(),
                "0".repeat(64)
            ],
        )
        .unwrap();
    let mut extra = report(issues.clone());
    inspect_missing_copies(&store, &mut extra).await.unwrap();
    assert_eq!(extra.issues, issues);
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    inspect_missing_copies(&store, &mut extra).await.unwrap();
    assert!(extra.is_clean());
}

fn missing(member: &str, source: &str) -> FsckIssue {
    FsckIssue {
        kind: FsckIssueKind::MissingChunk,
        location: format!("chunks/{member}"),
        detail: FsckIssueDetail::ReferencedByNativeXetXorb {
            xorb_location: format!("xorbs/{source}.xorb"),
        },
    }
}

fn report(issues: Vec<FsckIssue>) -> FsckReport {
    FsckReport {
        latest_records: 1,
        version_records: 0,
        inspected_chunk_references: 2,
        inspected_dedupe_shard_mappings: 0,
        inspected_reconstructions: 0,
        inspected_webhook_deliveries: 0,
        inspected_provider_repository_states: 0,
        issues,
    }
}

#[tokio::test]
async fn indexed_members_resolve_together_without_hiding_other_findings() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let descriptor = descriptor();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    let unknown = missing(&compute_data_hash(b"unrelated").hex(), &descriptor.hash);
    let corrupt = FsckIssue {
        kind: FsckIssueKind::ChunkHashMismatch,
        location: format!("xorbs/{}.xorb", descriptor.hash),
        detail: FsckIssueDetail::XorbHashMismatch {
            expected_hash: descriptor.hash.clone(),
        },
    };
    let mut findings = report(vec![
        missing(&descriptor.chunks[0].hash, &descriptor.hash),
        corrupt.clone(),
        missing(&descriptor.chunks[1].hash, &descriptor.hash),
        unknown.clone(),
    ]);
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert_eq!(findings.issues, vec![corrupt, unknown]);
    assert_eq!(findings.inspected_chunk_references, 2);
}

#[tokio::test]
async fn incomplete_member_index_preserves_every_finding_until_verified_repair() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let descriptor = descriptor();
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "DELETE FROM shardline_xet_chunks WHERE xorb_hash=?1 AND chunk_index=1",
            [&descriptor.hash],
        )
        .unwrap();
    let issues: Vec<_> = descriptor
        .chunks
        .iter()
        .map(|chunk| missing(&chunk.hash, &descriptor.hash))
        .collect();
    let mut findings = report(issues.clone());
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert_eq!(findings.issues, issues);
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert!(findings.is_clean());

    connection
        .execute(
            "UPDATE shardline_xet_xorbs SET descriptor='{}' WHERE xorb_hash=?1",
            [&descriptor.hash],
        )
        .unwrap();
    assert!(
        inspect_missing_copies(&store, &mut report(issues))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn structurally_valid_but_forged_root_cannot_resolve_missing_members() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let mut descriptor = descriptor();
    descriptor.chunks[1].unpacked_end += 1;
    assert!(descriptor.has_valid_structure());
    store
        .cache_xet_xorb_descriptors(std::slice::from_ref(&descriptor))
        .await
        .unwrap();
    let issues: Vec<_> = descriptor
        .chunks
        .iter()
        .map(|chunk| missing(&chunk.hash, &descriptor.hash))
        .collect();
    let mut findings = report(issues.clone());
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert_eq!(findings.issues, issues);
}

#[tokio::test]
async fn switching_sources_never_reuses_another_containers_members() {
    let root = tempfile::tempdir().unwrap();
    let store = LocalRecordStore::new(root.path().to_owned()).unwrap();
    let first = descriptor();
    let mut second = first.clone();
    second.chunks[1].hash = compute_data_hash(b"change").hex();
    second.hash = xorb_hash(&[
        (compute_data_hash(b"first"), 5),
        (compute_data_hash(b"change"), 6),
    ])
    .hex();
    store
        .cache_xet_xorb_descriptors(&[first.clone(), second.clone()])
        .await
        .unwrap();
    let wrong_source = missing(&first.chunks[1].hash, &second.hash);
    let absent_source = missing(&first.chunks[0].hash, &"0".repeat(64));
    let mut findings = report(vec![
        missing(&first.chunks[0].hash, &first.hash),
        wrong_source.clone(),
        missing(&second.chunks[1].hash, &second.hash),
        absent_source.clone(),
        missing(&first.chunks[1].hash, &first.hash),
    ]);
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert_eq!(findings.issues, vec![wrong_source, absent_source]);

    // A new scan must refresh even a source that was previously cached.
    let connection = rusqlite::Connection::open(root.path().join("metadata.sqlite3")).unwrap();
    connection
        .execute(
            "DELETE FROM shardline_xet_chunks WHERE xorb_hash=?1",
            [&first.hash],
        )
        .unwrap();
    let missing_again = missing(&first.chunks[0].hash, &first.hash);
    let mut findings = report(vec![missing_again.clone()]);
    inspect_missing_copies(&store, &mut findings).await.unwrap();
    assert_eq!(findings.issues, vec![missing_again]);
}
