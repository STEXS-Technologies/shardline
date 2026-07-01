#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_hub_api::commit::{parse_ndjson_commit, validate_lfs_oid};

fuzz_target!(|data: &str| {
    let first_parse = parse_ndjson_commit(data);
    let second_parse = parse_ndjson_commit(data);
    assert_eq!(first_parse.is_ok(), second_parse.is_ok());

    let first_oid = validate_lfs_oid(data);
    let second_oid = validate_lfs_oid(data);
    assert_eq!(first_oid.is_ok(), second_oid.is_ok());
});
