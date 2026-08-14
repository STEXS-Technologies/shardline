#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_s3_adapter::{QueryMap, classify};

/// Bound on the fuzzed query-map size (the server also bounds the query
/// string length).
const MAX_QUERY_ENTRIES: usize = 64;

fuzz_target!(|data: Vec<(String, String)>| {
    if data.len() > MAX_QUERY_ENTRIES {
        return;
    }
    let query: QueryMap = data;

    // Determinism + bounded output: classifying the same query twice yields
    // the same sub-resource set, never larger than the input.
    let first = classify(&query);
    let second = classify(&query);
    assert_eq!(first, second);
    assert!(first.len() <= query.len());

    // An empty query classifies to an empty sub-resource set.
    if query.is_empty() {
        assert!(first.is_empty());
    }
});
