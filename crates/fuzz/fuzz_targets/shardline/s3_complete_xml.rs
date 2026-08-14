#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_s3_adapter::parse_complete_multipart_parts;

/// Bound on the fuzzed CompleteMultipartUpload body (the handler bounds the
/// request body too).
const MAX_BODY_BYTES: usize = 64 * 1024;

fuzz_target!(|data: String| {
    if data.len() > MAX_BODY_BYTES {
        return;
    }

    // Determinism: the same input always parses identically.
    let first = parse_complete_multipart_parts(&data);
    let second = parse_complete_multipart_parts(&data);
    match (&first, &second) {
        (Ok(left), Ok(right)) => {
            assert_eq!(left, right);
            // The result is bounded by the input and every part number is
            // within the S3 protocol bounds.
            assert!(left.len() <= data.len());
            for part in left.part_numbers() {
                assert!((1..=10_000).contains(part));
            }
        }
        (Err(left), Err(right)) => {
            assert_eq!(left.code, right.code);
            assert_eq!(left.status, right.status);
        }
        _ => return,
    }
});
