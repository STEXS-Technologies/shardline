#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_s3_adapter::{decode_continuation_token, encode_continuation_token};

/// Bound on the fuzzed cursor/token size.
const MAX_CURSOR_BYTES: usize = 4_096;

fuzz_target!(|data: String| {
    if data.len() > MAX_CURSOR_BYTES {
        return;
    }

    // encode → decode roundtrips any cursor exactly.
    let encoded = encode_continuation_token(&data);
    assert_eq!(
        decode_continuation_token(&encoded).as_deref(),
        Ok(data.as_str())
    );

    // Decoding arbitrary garbage never panics and is deterministic.
    let first = decode_continuation_token(&data);
    let second = decode_continuation_token(&data);
    match (&first, &second) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => {
            assert_eq!(left.code, right.code);
            assert_eq!(left.status, right.status);
        }
        _ => return,
    }
});
