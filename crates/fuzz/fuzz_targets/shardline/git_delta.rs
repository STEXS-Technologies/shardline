#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    // Split input into base and delta
    if data.len() < 2 {
        return;
    }
    let mid = data.len() / 2;
    let _ = shardline_hub_api::git::pack::apply_delta(&data[..mid], &data[mid..]);
});
