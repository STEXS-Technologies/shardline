#![no_main]
#![allow(clippy::let_underscore_must_use)]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = shardline_hub_api::git::smart_http::parse_pack_data(data);
});
