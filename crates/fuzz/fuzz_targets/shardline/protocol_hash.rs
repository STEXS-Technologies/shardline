#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_protocol::ShardlineHash;

fuzz_target!(|data: &[u8]| {
    if let Ok(bytes) = <[u8; 32]>::try_from(data) {
        let hash = ShardlineHash::from_bytes(bytes);
        let hex = hash.hex_string();
        assert_eq!(hex.len(), 64);
        assert!(
            hex.bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(ShardlineHash::parse_hex(&hex), Ok(hash));
    }

    let Ok(raw) = from_utf8(data) else {
        return;
    };

    // INVARIANT 1: Hash parsing is deterministic.
    let first = ShardlineHash::parse_hex(raw);
    let second = ShardlineHash::parse_hex(raw);
    assert_eq!(first, second);

    // INVARIANT 2: Any accepted hash emits canonical lowercase hex.
    if let Ok(hash) = first {
        let hex = hash.hex_string();
        assert_eq!(hex, raw);
        assert_eq!(ShardlineHash::parse_hex(&hex), Ok(hash));
    }
});
