#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);

    // Parse the same bytes twice and assert a deterministic, panic-free
    // outcome. Malformed shards legitimately fail; the key invariant is that
    // the xorb chunk-count allocation cap is honored without panicking.
    let first = sdx::shard::parse_shard_xorbs(data);
    let second = sdx::shard::parse_shard_xorbs(data);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
});
