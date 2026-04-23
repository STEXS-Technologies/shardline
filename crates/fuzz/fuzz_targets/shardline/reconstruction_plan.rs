#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::{FileChunkRecord, FileRecord};
use shardline_protocol::{ByteRange, ChunkRange, ShardlineHash};
use shardline_server::fuzz_reconstruction_response_summary;

const MAX_CHUNKS: usize = 64;

type RawChunk = ([u8; 32], u64, u64, u32, u32, u64, u64);

fuzz_target!(|data: (u64, Vec<RawChunk>)| {
    let (total_bytes, raw_chunks) = data;
    let chunks = raw_chunks
        .into_iter()
        .take(MAX_CHUNKS)
        .map(
            |(hash_bytes, offset, length, range_start, range_end, packed_start, packed_end)| {
                FileChunkRecord {
                    hash: ShardlineHash::from_bytes(hash_bytes).api_hex_string(),
                    offset,
                    length,
                    range_start,
                    range_end,
                    packed_start,
                    packed_end,
                }
            },
        )
        .collect::<Vec<_>>();

    let record = FileRecord {
        file_id: "a".repeat(64),
        content_hash: "b".repeat(64),
        total_bytes,
        chunk_size: 0,
        repository_scope: None,
        chunks,
    };

    let first_validation = record.validate_reconstruction_plan();
    let second_validation = record.validate_reconstruction_plan();
    assert_eq!(first_validation.is_ok(), second_validation.is_ok());
    match (&first_validation, &second_validation) {
        (Ok(()), Ok(())) => {}
        (Err(left), Err(right)) => {
            assert_eq!(left.to_string(), right.to_string());
            return;
        }
        _ => return,
    }

    let mut expected_offset = 0_u64;
    let mut planned_length = 0_u64;
    for chunk in &record.chunks {
        assert_eq!(chunk.offset, expected_offset);
        assert!(chunk.length > 0);
        assert!(chunk.range_end > chunk.range_start);
        assert!(chunk.packed_end > chunk.packed_start);
        assert!(ShardlineHash::parse_api_hex(&chunk.hash).is_ok());
        assert!(ChunkRange::new(chunk.range_start, chunk.range_end).is_ok());
        let next_expected_offset = expected_offset.checked_add(chunk.length);
        assert!(next_expected_offset.is_some());
        let Some(next_expected_offset) = next_expected_offset else {
            return;
        };
        expected_offset = next_expected_offset;
        let next_planned_length = planned_length.checked_add(chunk.length);
        assert!(next_planned_length.is_some());
        let Some(next_planned_length) = next_planned_length else {
            return;
        };
        planned_length = next_planned_length;
    }

    assert_eq!(record.total_bytes, expected_offset);
    assert_eq!(record.total_bytes, planned_length);

    assert_reconstruction_response_is_deterministic(&record, None);

    let Some(last_byte) = record.total_bytes.checked_sub(1) else {
        return;
    };
    let ranged_cases = [
        ByteRange::new(0, last_byte),
        ByteRange::new(0, 0),
        ByteRange::new(last_byte, last_byte),
        ByteRange::new(last_byte / 2, last_byte),
    ];
    for ranged_case in ranged_cases {
        assert!(ranged_case.is_ok());
        let Ok(range) = ranged_case else {
            continue;
        };
        assert_reconstruction_response_is_deterministic(&record, Some(range));
    }

    if let Ok(out_of_bounds_range) = ByteRange::new(record.total_bytes, record.total_bytes) {
        assert_reconstruction_response_rejects_range(&record, out_of_bounds_range);
    }
});

fn assert_reconstruction_response_is_deterministic(
    record: &FileRecord,
    requested_range: Option<ByteRange>,
) {
    let first_response =
        fuzz_reconstruction_response_summary("http://127.0.0.1:8080", record, requested_range);
    let second_response =
        fuzz_reconstruction_response_summary("http://127.0.0.1:8080", record, requested_range);
    assert_eq!(first_response.is_ok(), second_response.is_ok());
    match (&first_response, &second_response) {
        (Ok(left), Ok(right)) => {
            assert_eq!(left, right);
            assert!(left.fetch_ranges <= left.terms);
            assert_eq!(left.fetch_ranges, left.v2_fetches);
            assert_eq!(left.fetch_ranges, left.v2_ranges);
            assert_eq!(left.fetch_xorbs, left.v2_xorbs);
            if !record.chunks.is_empty() {
                assert!(left.terms > 0);
                assert!(left.total_unpacked_length > 0);
            }
        }
        (Err(left), Err(right)) => assert_eq!(left.to_string(), right.to_string()),
        _ => {}
    }
}

fn assert_reconstruction_response_rejects_range(record: &FileRecord, requested_range: ByteRange) {
    let first_response = fuzz_reconstruction_response_summary(
        "http://127.0.0.1:8080",
        record,
        Some(requested_range),
    );
    let second_response = fuzz_reconstruction_response_summary(
        "http://127.0.0.1:8080",
        record,
        Some(requested_range),
    );
    assert!(first_response.is_err());
    assert!(second_response.is_err());
    if let (Err(left), Err(right)) = (&first_response, &second_response) {
        assert_eq!(left.to_string(), right.to_string());
    }
}
