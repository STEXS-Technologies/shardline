#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server_core::{
    parse_stored_file_record_bytes, validate_content_hash, validate_identifier,
};
use shardline_gc::{quarantine_record_path, quarantine_root};
use std::path::Path;

fuzz_target!(|data: &[u8]| {
    let first_parse = parse_stored_file_record_bytes(data);
    let second_parse = parse_stored_file_record_bytes(data);
    assert_eq!(first_parse.is_ok(), second_parse.is_ok());
    match (&first_parse, &second_parse) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(format!("{left}"), format!("{right}")),
        _ => return,
    }

    let Ok(record) = first_parse else {
        return;
    };

    let first_id = validate_identifier(&record.file_id);
    let second_id = validate_identifier(&record.file_id);
    assert_eq!(first_id.is_ok(), second_id.is_ok());

    let first_hash = validate_content_hash(&record.content_hash);
    let second_hash = validate_content_hash(&record.content_hash);
    assert_eq!(first_hash.is_ok(), second_hash.is_ok());

    let root = Path::new("/tmp/test");
    let q_root = quarantine_root(root);
    let q_record = quarantine_record_path(root, &record.file_id);
    assert!(q_root.starts_with(root));
    assert!(q_record.starts_with(&q_root));
});
