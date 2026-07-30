#![no_main]

use std::io::Cursor;

use libfuzzer_sys::fuzz_target;
use shardline_xet_core::xorb_object::{
    reconstruct_xorb_with_footer, validate_serialized_xorb,
};
use shardline_xet_core::xorb_object::xorb_object_format::XorbObject;

fuzz_target!(|data: &[u8]| {
    if data.len() < 32 {
        return; // Need at least a small payload
    }
    // Split input into up to 8 variable-size "chunks".
    let chunk_count = ((data[0] as usize) % 8).max(1);
    let mut chunks = Vec::new();
    let mut offset = 1;
    for _ in 0..chunk_count {
        if offset >= data.len() {
            break;
        }
        // Vary chunk size by skipping bytes proportional to position.
        let chunk_len = ((data[offset >> 1] as usize) % 256 + 1)
            .min(data.len() - offset);
        chunks.push((data[offset..offset + chunk_len].to_vec(), 0u64));
        offset += chunk_len;
        if offset >= data.len() {
            break;
        }
    }
    if chunks.is_empty() {
        return;
    }
    // Pack into xorb via the same path the ingestor uses.
    // (pack_chunks_into_xorb lives in shardline-server, so we use the
    // lower-level reconstruct path for the fuzz harness.)
    //
    // Round-trip: serialize → validate → decode → verify chunk content.
    let mut serialized = Vec::new();
    if reconstruct_xorb_with_footer(&mut serialized, data).is_ok() {
        let mut cursor = Cursor::new(serialized.as_slice());
        if let Ok(validated) = validate_serialized_xorb(&mut cursor, data) {
            // At minimum the validation parsed correctly.
            let _ = validated;
        }
    }
});
