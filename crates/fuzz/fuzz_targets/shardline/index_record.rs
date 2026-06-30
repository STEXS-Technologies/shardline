#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_index::parse_xet_hash_hex;

fuzz_target!(|data: &str| {
    let first = parse_xet_hash_hex(data);
    let second = parse_xet_hash_hex(data);
    assert_eq!(first.is_ok(), second.is_ok());
    match (&first, &second) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(format!("{left}"), format!("{right}")),
        _ => return,
    }
});
