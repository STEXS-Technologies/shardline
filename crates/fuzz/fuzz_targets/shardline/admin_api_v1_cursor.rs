#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server::fuzz_admin_api_cursor;

fuzz_target!(|data: &[u8]| {
    let first = fuzz_admin_api_cursor(data);
    let second = fuzz_admin_api_cursor(data);
    assert_eq!(format!("{first:?}"), format!("{second:?}"));
});
