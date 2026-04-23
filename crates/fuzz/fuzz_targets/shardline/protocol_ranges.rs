#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_protocol::{ByteRange, ChunkRange, RangeError, parse_http_byte_range};

const MAX_HEADER_BYTES: usize = 4_096;

fuzz_target!(|data: (u64, u64, u32, u32, u64, String)| {
    let (byte_start, byte_end, chunk_start, chunk_end, resource_length, header) = data;

    if header.len() > MAX_HEADER_BYTES {
        return;
    }

    let byte_range = ByteRange::new(byte_start, byte_end);
    let repeated_byte_range = ByteRange::new(byte_start, byte_end);
    assert_eq!(byte_range, repeated_byte_range);

    match byte_range {
        Ok(range) => {
            assert!(range.start() <= range.end_inclusive());
            assert!(!range.is_empty());
            if let Some(length) = range.len() {
                assert!(length > 0);
            }
        }
        Err(error) => {
            assert_eq!(error, RangeError::Inverted);
            assert!(byte_end < byte_start);
        }
    }

    let chunk_range = ChunkRange::new(chunk_start, chunk_end);
    let repeated_chunk_range = ChunkRange::new(chunk_start, chunk_end);
    assert_eq!(chunk_range, repeated_chunk_range);

    match chunk_range {
        Ok(range) => {
            assert!(range.start() < range.end_exclusive());
        }
        Err(RangeError::Empty) => {
            assert_eq!(chunk_start, chunk_end);
        }
        Err(RangeError::Inverted) => {
            assert!(chunk_end < chunk_start);
        }
    }

    let derived_headers = [
        format!("bytes={byte_start}-{byte_end}"),
        format!("bytes={byte_start}-"),
        format!("items={byte_start}-{byte_end}"),
        format!("bytes=-{byte_end}"),
        format!("bytes={byte_start}-{byte_end},{byte_start}-{byte_end}"),
        header,
    ];

    for candidate in derived_headers {
        let first_parse = parse_http_byte_range(&candidate, resource_length);
        let second_parse = parse_http_byte_range(&candidate, resource_length);
        assert_eq!(first_parse.is_ok(), second_parse.is_ok());
        match (&first_parse, &second_parse) {
            (Ok(left), Ok(right)) => assert_eq!(left, right),
            (Err(left), Err(right)) => assert_eq!(left, right),
            _ => return,
        }

        let Ok(range) = first_parse else {
            continue;
        };

        assert!(range.start() <= range.end_inclusive());
        assert!(!range.is_empty());
        assert!(range.end_inclusive() < resource_length);
        assert_eq!(
            ByteRange::new(range.start(), range.end_inclusive()),
            Ok(range)
        );
        if let Some(length) = range.len() {
            assert!(length > 0);
        }

        let canonical_header = format!("bytes={}-{}", range.start(), range.end_inclusive());
        assert_eq!(
            parse_http_byte_range(&canonical_header, resource_length),
            Ok(range)
        );

        let Some(last_byte) = resource_length.checked_sub(1) else {
            continue;
        };
        let open_ended_header = format!("bytes={}-", range.start());
        let open_ended_range = parse_http_byte_range(&open_ended_header, resource_length);
        let expected_open_ended = ByteRange::new(range.start(), last_byte);
        assert!(expected_open_ended.is_ok());
        let Ok(expected_open_ended) = expected_open_ended else {
            continue;
        };
        assert_eq!(open_ended_range, Ok(expected_open_ended));
    }
});
