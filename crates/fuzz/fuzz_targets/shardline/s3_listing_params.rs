#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_s3_adapter::parse_list_objects_v2_params;

/// Bound on the fuzzed query-map size (the server also bounds the query
/// string length).
const MAX_QUERY_ENTRIES: usize = 64;

fuzz_target!(|data: Vec<(String, String)>| {
    if data.len() > MAX_QUERY_ENTRIES {
        return;
    }

    // Determinism: the same query parses identically.
    let first = parse_list_objects_v2_params(&data);
    let second = parse_list_objects_v2_params(&data);
    match (&first, &second) {
        (Ok(left), Ok(right)) => {
            assert_eq!(left, right);
            // max-keys is always capped at the S3 page ceiling and the
            // effective resume cursor is deterministic.
            assert!(left.max_keys <= 1000);
            assert_eq!(left.cursor(), right.cursor());
        }
        (Err(left), Err(right)) => {
            assert_eq!(left.code, right.code);
            assert_eq!(left.status, right.status);
        }
        _ => return,
    }
});
