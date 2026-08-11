#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Parse the same input twice and assert the outcome is deterministic. We do
    // NOT assert success: malformed pack data legitimately fails to parse. The
    // size bound is enforced by the parser itself (ExcessiveDecompressedSize).
    let first = shardline_hub_api::git::smart_http::parse_pack_data(data);
    let second = shardline_hub_api::git::smart_http::parse_pack_data(data);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
});
