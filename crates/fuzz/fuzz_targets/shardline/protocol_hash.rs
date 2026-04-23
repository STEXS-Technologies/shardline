#![no_main]

use std::str::from_utf8;

use libfuzzer_sys::fuzz_target;
use shardline_protocol::ShardlineHash;

fuzz_target!(|data: &[u8]| {
    if let Ok(bytes) = <[u8; 32]>::try_from(data) {
        let hash = ShardlineHash::from_bytes(bytes);
        let api_hex = hash.api_hex_string();
        assert_eq!(api_hex.len(), 64);
        assert!(
            api_hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        );
        assert_eq!(ShardlineHash::parse_api_hex(&api_hex), Ok(hash));
    }

    let Ok(raw) = from_utf8(data) else {
        return;
    };

    // INVARIANT 1: Hash parsing is deterministic.
    let first = ShardlineHash::parse_api_hex(raw);
    let second = ShardlineHash::parse_api_hex(raw);
    assert_eq!(first, second);

    // INVARIANT 2: Any accepted API hash emits canonical lowercase API hex.
    if let Ok(hash) = first {
        let api_hex = hash.api_hex_string();
        assert_eq!(api_hex, raw);
        assert_eq!(ShardlineHash::parse_api_hex(&api_hex), Ok(hash));
    }
});
