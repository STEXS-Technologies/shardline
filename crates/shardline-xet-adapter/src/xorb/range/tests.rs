#![allow(clippy::unwrap_used, clippy::expect_used, clippy::indexing_slicing)]

use super::*;
use shardline_xet_core::{
    merklehash::{compute_data_hash, xorb_hash},
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};
use std::io::{Cursor, Read, Seek, SeekFrom};

struct Counted {
    cursor: Cursor<Vec<u8>>,
    read: usize,
}

impl Read for Counted {
    fn read(&mut self, target: &mut [u8]) -> std::io::Result<usize> {
        let count = self.cursor.read(target)?;
        self.read = self
            .read
            .checked_add(count)
            .expect("bounded fixture read count");
        Ok(count)
    }
}

impl Seek for Counted {
    fn seek(&mut self, position: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(position)
    }
}

fn fixture(compression: CompressionScheme) -> (Counted, ValidatedXorb) {
    let parts: Vec<_> = [11, 27, 43]
        .into_iter()
        .map(|byte| vec![byte; 65536])
        .collect();
    let hashes: Vec<_> = parts.iter().map(|part| compute_data_hash(part)).collect();
    let root = xorb_hash(&hashes.iter().map(|hash| (*hash, 65536)).collect::<Vec<_>>());
    let serialized = serialized_xorb_object_from_components(
        &root,
        parts.concat(),
        hashes.into_iter().zip([65536, 131072, 196608]).collect(),
        compression,
    )
    .unwrap();
    let mut reader = Counted {
        cursor: Cursor::new(serialized.serialized_data),
        read: 0,
    };
    let validated = super::super::validate_serialized_xorb(
        &mut reader,
        super::super::merkle_hash_to_shardline_hash(root),
    )
    .unwrap();
    reader.read = 0;
    (reader, validated)
}

#[test]
fn selected_decode_reads_only_selected_packed_members_after_full_validation() {
    for compression in [
        CompressionScheme::None,
        CompressionScheme::LZ4,
        CompressionScheme::ByteGrouping4LZ4,
    ] {
        let (mut reader, validated) = fixture(compression);
        super::super::try_for_each_serialized_xorb_chunk_trusted(&mut reader, &validated, |_| {
            Ok::<_, ()>(())
        })
        .unwrap();
        assert_eq!(reader.read as u64, validated.packed_content_length());
        reader.read = 0;
        let mut bytes = Vec::new();
        try_for_each_serialized_xorb_chunk_range(&mut reader, &validated, 1..2, |chunk| {
            bytes.extend_from_slice(chunk.data());
            Ok::<_, ()>(())
        })
        .unwrap();
        assert_eq!(bytes, vec![27; 65536]);
        assert_eq!(
            reader.read as u64,
            validated.chunks()[1].packed_end() - validated.chunks()[1].packed_start()
        );
    }
}

#[test]
fn changed_selected_payload_is_rejected_before_visiting_it() {
    let (mut reader, validated) = fixture(CompressionScheme::None);
    let last = usize::try_from(validated.chunks()[1].packed_end() - 1).unwrap();
    reader.cursor.get_mut()[last] ^= 1;
    let mut visits = 0;
    assert!(
        try_for_each_serialized_xorb_chunk_range(&mut reader, &validated, 1..2, |_| {
            visits += 1;
            Ok::<_, ()>(())
        })
        .is_err()
    );
    assert_eq!(visits, 0);
}

#[test]
fn invalid_ranges_do_not_read_or_visit_any_source_bytes() {
    let (mut reader, validated) = fixture(CompressionScheme::None);
    for range in [0..0, Range { start: 2, end: 1 }, 0..4] {
        let mut visits = 0;
        assert!(
            try_for_each_serialized_xorb_chunk_range(&mut reader, &validated, range, |_| {
                visits += 1;
                Ok::<_, ()>(())
            })
            .is_err()
        );
        assert_eq!(reader.read, 0);
        assert_eq!(visits, 0);
    }
}

#[test]
fn rejected_visitor_stops_before_reading_the_next_member() {
    let (mut reader, validated) = fixture(CompressionScheme::None);
    let result =
        try_for_each_serialized_xorb_chunk_range(&mut reader, &validated, 1..3, |_| Err("stop"));
    assert!(matches!(result, Err(XorbVisitError::Visitor("stop"))));
    assert_eq!(
        reader.read as u64,
        validated.chunks()[1].packed_end() - validated.chunks()[1].packed_start()
    );
}
