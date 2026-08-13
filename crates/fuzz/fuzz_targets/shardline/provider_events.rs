#![no_main]

use libfuzzer_sys::fuzz_target;

const MAX_INPUT: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let data = data.get(..data.len().min(MAX_INPUT)).unwrap_or(data);

    // Parse the same bytes twice and assert a deterministic, panic-free
    // outcome. `shardline_provider_events::records::parse_record_entry` is a
    // `pub(super)` helper in a private module, so this fuzzes its backing
    // implementation (`shardline_server_core::parse_stored_file_record_bytes`)
    // directly. Malformed records legitimately fail; determinism is invariant.
    let first = shardline_server_core::parse_stored_file_record_bytes(data);
    let second = shardline_server_core::parse_stored_file_record_bytes(data);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
});
