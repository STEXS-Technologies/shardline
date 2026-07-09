#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = shardline_hub_api::git::smart_http::parse_commit_object(data);
});
