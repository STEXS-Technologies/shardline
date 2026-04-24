#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_server::fuzz_lfs_frontend_summary;

fuzz_target!(|data: &[u8]| {
    let Ok(oid) = from_utf8(data) else {
        return;
    };

    let Ok(summary) = fuzz_lfs_frontend_summary(oid) else {
        return;
    };

    if summary.oid_accepts {
        assert!(summary.key_is_stable);
        assert_eq!(oid.len(), 64);
        assert!(oid.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
});
