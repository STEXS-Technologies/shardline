#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server_core::{validate_content_hash, validate_identifier};

fuzz_target!(|data: &str| {
    let first_id = validate_identifier(data);
    let second_id = validate_identifier(data);
    assert_eq!(first_id.is_ok(), second_id.is_ok());

    let first_hash = validate_content_hash(data);
    let second_hash = validate_content_hash(data);
    assert_eq!(first_hash.is_ok(), second_hash.is_ok());
});
