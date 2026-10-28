use super::*;
use shardline_xet_core::xorb_object::{
    CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
};
use std::io::Write;

fn fixture() -> (ShardlineHash, Vec<u8>) {
    let members = [
        b"first member".as_slice(),
        b"second member",
        b"third member",
    ];
    let leaves: Vec<_> = members
        .iter()
        .map(|bytes| (compute_data_hash(bytes), bytes.len() as u64))
        .collect();
    let hash = xorb_hash(&leaves);
    let mut end = 0_u64;
    let boundaries = leaves
        .iter()
        .map(|(hash, length)| {
            end = end.checked_add(*length).unwrap();
            (*hash, end)
        })
        .collect();
    let serialized = serialized_xorb_object_from_components(
        &hash,
        members.concat(),
        boundaries,
        CompressionScheme::LZ4,
    )
    .unwrap();
    (
        merkle_hash_to_shardline_hash(hash),
        serialized.serialized_data,
    )
}

#[test]
fn every_frame_width_preserves_full_validation_and_visitor_bytes() {
    let (hash, bytes) = fixture();
    let expected = validate_serialized_xorb(&mut Cursor::new(&bytes), hash).unwrap();
    for width in 1..=bytes.len() {
        let mut visited = Vec::new();
        let mut visitor = |_, data: &[u8]| {
            visited.extend_from_slice(data);
            Ok(())
        };
        let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
        for frame in bytes.chunks(width) {
            stream.write_all(frame).unwrap();
        }
        assert_eq!(
            stream.finish_full(hash, bytes.len() as u64).unwrap(),
            expected
        );
        assert_eq!(visited, b"first membersecond memberthird member");
    }
}

#[test]
fn every_single_byte_mutation_matches_the_seek_validator() {
    let (hash, bytes) = fixture();
    for index in 0..bytes.len() {
        let mut changed = bytes.clone();
        changed[index] ^= 0x40;
        let ordinary = validate_serialized_xorb(&mut Cursor::new(&changed), hash).is_ok();
        let mut visitor = |_, _: &[u8]| Ok(());
        let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
        let streamed = stream.write_all(&changed).is_ok()
            && stream.finish_full(hash, changed.len() as u64).is_ok();
        assert_eq!(streamed, ordinary, "mutation at {index}");
    }
}

#[test]
fn every_truncation_and_extra_tail_is_rejected() {
    let (hash, bytes) = fixture();
    for end in 0..bytes.len() {
        let mut visitor = |_, _: &[u8]| Ok(());
        let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
        assert!(
            stream.write_all(&bytes[..end]).is_err()
                || stream.finish_full(hash, end as u64).is_err()
        );
    }
    let mut visitor = |_, _: &[u8]| Ok(());
    let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
    stream.write_all(&bytes).unwrap();
    assert!(stream.write_all(&[0; 1024]).is_err());
    assert!(stream.finish_full(hash, bytes.len() as u64).is_err());
}

#[test]
fn authenticated_subrange_requires_exact_members_and_framing() {
    let (hash, bytes) = fixture();
    let verified = validate_serialized_xorb(&mut Cursor::new(&bytes), hash).unwrap();
    let members = &verified.chunks()[1..];
    let start = members[0].packed_start() as usize;
    let end = members.last().unwrap().packed_end() as usize;
    let mut visited = Vec::new();
    let mut visitor = |_, data: &[u8]| {
        visited.extend_from_slice(data);
        Ok(())
    };
    let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
    for frame in bytes[start..end].chunks(7) {
        stream.write_all(frame).unwrap();
    }
    stream.finish_range(members).unwrap();
    assert_eq!(visited, b"second memberthird member");
}

#[test]
fn member_work_completes_before_the_remaining_container_arrives() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (hash, bytes) = fixture();
    let verified = validate_serialized_xorb(&mut Cursor::new(&bytes), hash).unwrap();
    let first_end = verified.chunks()[0].packed_end() as usize;
    let visits = AtomicUsize::new(0);
    let mut visitor = |_, _: &[u8]| {
        visits.fetch_add(1, Ordering::Relaxed);
        Ok(())
    };
    let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
    stream.write_all(&bytes[..first_end]).unwrap();
    assert_eq!(visits.load(Ordering::Relaxed), 1);
    assert!(stream.pending.is_empty());
    assert!(!stream.footer);
    assert_eq!(stream.info.chunk_hashes.len(), 1);
    // Completed member work is provisional: an incomplete source cannot publish.
    assert!(stream.finish_full(hash, first_end as u64).is_err());
}

#[test]
#[allow(deprecated)] // Existing persisted v0 containers remain readable.
fn legacy_footer_matches_the_complete_seek_validator() {
    use shardline_xet_core::xorb_object::XorbObjectInfoV0;
    let (hash, bytes) = fixture();
    let parsed = XorbObject::deserialize(&mut Cursor::new(&bytes)).unwrap();
    let packed = parsed.get_contents_length().unwrap() as usize;
    let mut legacy = XorbObjectInfoV0::default();
    legacy.xorb_hash = parsed.info.xorb_hash;
    legacy.num_chunks = parsed.info.num_chunks as u32;
    legacy.chunk_hashes = parsed.info.chunk_hashes;
    legacy.chunk_boundary_offsets = parsed
        .info
        .chunk_boundary_offsets
        .iter()
        .map(|end| *end as u32)
        .collect();
    let mut footer = Vec::new();
    legacy.serialize(&mut footer).unwrap();
    let footer_length = footer.len() as u64;
    footer.extend_from_slice(&footer_length.to_le_bytes());
    let mut serialized = bytes[..packed].to_vec();
    serialized.extend_from_slice(&footer);
    let expected = validate_serialized_xorb(&mut Cursor::new(&serialized), hash).unwrap();
    let mut visitor = |_, _: &[u8]| Ok(());
    let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
    for frame in serialized.chunks(3) {
        stream.write_all(frame).unwrap();
    }
    assert_eq!(
        stream.finish_full(hash, serialized.len() as u64).unwrap(),
        expected
    );
}

#[test]
fn canonical_member_counts_above_normalization_limit_still_verify() {
    let count = 8193;
    let leaf = compute_data_hash(&[9]);
    let leaves = vec![(leaf, 1); count];
    let hash = xorb_hash(&leaves);
    let serialized = serialized_xorb_object_from_components(
        &hash,
        vec![9; count],
        (1..=count).map(|end| (leaf, end as u64)).collect(),
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    let expected_hash = merkle_hash_to_shardline_hash(hash);
    let expected = validate_serialized_xorb(&mut Cursor::new(&serialized), expected_hash).unwrap();
    let mut visitor = |_, _: &[u8]| Ok(());
    let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
    for frame in serialized.chunks(127) {
        stream.write_all(frame).unwrap();
    }
    assert_eq!(
        stream
            .finish_full(expected_hash, serialized.len() as u64)
            .unwrap(),
        expected
    );
}

#[test]
fn taking_a_writer_error_cannot_reenable_prefix_publication() {
    let (hash, bytes) = fixture();
    let mut visitor = |_, _: &[u8]| Ok(());
    let mut stream = StreamingXorb::new(&|| Ok(()), &mut visitor);
    stream.write_all(&bytes).unwrap();
    assert!(stream.write_all(&[0; 1024]).is_err());
    assert!(stream.take_failure().is_some());
    assert!(stream.write(&[]).is_err());
    assert!(stream.finish_full(hash, bytes.len() as u64).is_err());
}
