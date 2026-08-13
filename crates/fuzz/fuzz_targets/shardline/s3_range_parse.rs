#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_s3_adapter::parse_s3_range;

/// Bound on the fuzzed Range header (the handler bounds the header budget).
const MAX_HEADER_BYTES: usize = 4_096;

fuzz_target!(|data: (String, u64)| {
    let (header, total) = data;
    if header.len() > MAX_HEADER_BYTES {
        return;
    }

    // Determinism: the same header parses identically.
    let first = parse_s3_range(Some(&header), total);
    let second = parse_s3_range(Some(&header), total);
    match (&first, &second) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => {
            assert_eq!(left.code, right.code);
            assert_eq!(left.status, right.status);
        }
        _ => return,
    }

    if let Ok(range) = first {
        // A satisfiable range is always non-empty and within the resource.
        assert!(range.start() <= range.end_inclusive());
        assert!(range.end_inclusive() < total);
    }
});
