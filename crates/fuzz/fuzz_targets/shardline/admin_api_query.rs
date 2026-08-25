#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server::fuzz_admin_api_query;

fuzz_target!(|data: &[u8]| {
    if let Ok(query) = std::str::from_utf8(data) {
        let first = fuzz_admin_api_query(query);
        let second = fuzz_admin_api_query(query);
        assert_eq!(format!("{first:?}"), format!("{second:?}"));
    }
});
