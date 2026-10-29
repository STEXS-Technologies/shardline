//! Share source authentication with ordered file terms without composing hashes.
use super::*;

fn check_terms(ranges: &[(usize, usize)], expected_rechecks: usize) {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let members = [b"first".as_slice(), b"second", b"third", b"fourth"];
    let (hash, leaves) = store_members(&objects, &members);
    let mut selected = Vec::new();
    let mut bytes = Vec::new();
    let segments = ranges
        .iter()
        .map(|&(start, end)| {
            selected.extend_from_slice(&leaves[start..end]);
            for member in &members[start..end] {
                bytes.extend_from_slice(member);
            }
            FileDataSequenceEntry::new(
                hash,
                leaves[start..end].iter().map(|(_, length)| length).sum(),
                start as u64,
                end as u64,
            )
        })
        .collect::<Vec<_>>();
    let expected = format!("{:x}", Sha256::digest(&bytes));
    let file = MDBFileInfo {
        metadata: FileDataSequenceHeader::new(
            file_hash(&selected),
            segments.len() as u64,
            false,
            true,
        ),
        segments,
        verification: Vec::new(),
        metadata_ext: Some(FileMetadataExt::new(
            MerkleHash::from_hex(&expected).unwrap(),
        )),
    };
    let scope = RepositoryScope::new(RepositoryProvider::GitHub, "team", "ordered", None).unwrap();
    let mut verifier = file_digests::FileDigestVerifier::default();
    let (records, identities) = build_file_records_with_cache(
        vec![file],
        &objects,
        Some(&scope),
        None,
        &mut HashMap::new(),
        &mut verifier,
        &|| Ok(()),
    )
    .unwrap();
    assert_eq!(identities[0].sha256, expected);
    assert_eq!(records[0].total_bytes, bytes.len() as u64);
    assert_eq!(records[0].chunks.len(), ranges.len());
    let mut offset = 0;
    for (record, &(start, end)) in records[0].chunks.iter().zip(ranges) {
        assert_eq!(record.offset, offset);
        assert_eq!(record.range_start, start as u64);
        assert_eq!(record.range_end, end as u64);
        offset = offset.checked_add(record.length).unwrap();
    }
    assert_eq!(verifier.source_reads, 1);
    assert_eq!(verifier.member_rechecks, expected_rechecks);
}

#[test]
fn increasing_terms_share_the_authentication_pass_including_gaps() {
    check_terms(&[(0, 1), (1, 2), (3, 4)], 0);
}

#[test]
fn reversed_and_repeated_terms_preserve_logical_digest_order() {
    check_terms(&[(2, 4), (0, 1), (0, 2)], 3);
}

#[test]
fn overlap_is_hashed_again_as_file_content() {
    check_terms(&[(0, 2), (1, 3), (3, 4)], 3);
}
