#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_server::fuzz_bazel_http_frontend_summary;

fuzz_target!(|data: &[u8]| {
    let Ok(hash_hex) = from_utf8(data) else {
        return;
    };

    let Ok(summary) = fuzz_bazel_http_frontend_summary(hash_hex) else {
        return;
    };

    if summary.ac_accepts || summary.cas_accepts {
        assert!(summary.ac_accepts);
        assert!(summary.cas_accepts);
        assert_eq!(hash_hex.len(), 64);
        assert!(hash_hex.bytes().all(|byte| byte.is_ascii_hexdigit()));
    }
});
