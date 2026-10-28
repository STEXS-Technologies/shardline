//! Cached metadata must not change authentication or logical file ordering.
use super::*;

#[test]
fn cached_contiguous_terms_preserve_order_and_reject_forged_splits() {
    let root = tempfile::tempdir().unwrap();
    let objects = ServerObjectStore::local(root.path().join("objects")).unwrap();
    let members = [b"first".as_slice(), b"second", b"third"];
    let (hash, leaves) = store_members(&objects, &members);
    for ranges in [
        vec![(0, 1), (1, 3)],
        vec![(0, 1), (2, 3)],
        vec![(1, 3), (0, 1)],
        vec![(0, 2), (1, 3)],
    ] {
        let selected: Vec<_> = ranges
            .iter()
            .flat_map(|&(start, end)| leaves.get(start..end).unwrap().iter().copied())
            .collect();
        let bytes: Vec<_> = ranges
            .iter()
            .flat_map(|&(start, end)| {
                members
                    .get(start..end)
                    .unwrap()
                    .iter()
                    .flat_map(|bytes| bytes.iter().copied())
            })
            .collect();
        let segments: Vec<_> = ranges
            .iter()
            .map(|&(start, end)| {
                FileDataSequenceEntry::new(
                    hash,
                    leaves
                        .get(start..end)
                        .unwrap()
                        .iter()
                        .map(|(_, length)| length)
                        .sum(),
                    start as u64,
                    end as u64,
                )
            })
            .collect();
        let target = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(
                file_hash(&selected),
                segments.len() as u64,
                false,
                true,
            ),
            segments,
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&format!("{:x}", Sha256::digest(&bytes))).unwrap(),
            )),
        };
        let seed = MDBFileInfo {
            metadata: FileDataSequenceHeader::new(file_hash(&leaves), 1, false, true),
            segments: vec![FileDataSequenceEntry::new(
                hash,
                leaves.iter().map(|(_, length)| length).sum(),
                0,
                leaves.len() as u64,
            )],
            verification: Vec::new(),
            metadata_ext: Some(FileMetadataExt::new(
                MerkleHash::from_hex(&format!("{:x}", Sha256::digest(members.concat()))).unwrap(),
            )),
        };
        for corrupt in [false, true] {
            let mut target = target.clone();
            if corrupt {
                // The total stays valid, but each constituent length is false.
                target.segments.first_mut().unwrap().unpacked_segment_bytes += 1;
                target.segments.last_mut().unwrap().unpacked_segment_bytes -= 1;
            }
            let mut verifier = file_digests::FileDigestVerifier::default();
            let result = build_file_records_with_cache(
                vec![seed.clone(), target],
                &objects,
                None,
                None,
                &mut HashMap::new(),
                &mut verifier,
                &|| Ok(()),
            );
            if corrupt {
                assert!(result.is_err());
            } else {
                let (records, _) = result.unwrap();
                assert_eq!(records.last().unwrap().total_bytes, bytes.len() as u64);
                assert_eq!(verifier.source_reads, 1);
                assert_eq!(verifier.member_rechecks, selected.len());
            }
        }
    }
}
