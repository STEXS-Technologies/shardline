#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::hub::HubRepoType;

/// Every token accepted by `parse_str` (singular and plural spellings).
const ACCEPTED_TOKENS: [&str; 6] = ["model", "models", "dataset", "datasets", "space", "spaces"];

fuzz_target!(|data: &str| {
    // INVARIANT 1: parse_str is deterministic for any input.
    let first = HubRepoType::parse_str(data);
    let second = HubRepoType::parse_str(data);
    assert_eq!(first, second);

    // INVARIANT 2: from_api_repo_type delegates to parse_str, so it must agree.
    assert_eq!(HubRepoType::from_api_repo_type(data), first);

    // INVARIANT 3: every accepted token round-trips through as_str() and stays
    // parseable by both entry points.
    for token in ACCEPTED_TOKENS {
        let parsed = HubRepoType::parse_str(token);
        assert!(parsed.is_some());
        if let Some(variant) = parsed {
            let canonical = variant.as_str();
            assert_eq!(HubRepoType::parse_str(canonical), Some(variant));
            assert_eq!(HubRepoType::from_api_repo_type(canonical), Some(variant));
        }
    }
});
