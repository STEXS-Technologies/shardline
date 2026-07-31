#![no_main]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::assertions_on_constants
)]

use libfuzzer_sys::fuzz_target;
use shardline_server::upload_ingest::xorb_packer::pack_chunks_into_xorb;
use shardline_xet_core::xorb_object::reconstruct_xorb_with_footer;

fuzz_target!(|data: &[u8]| {
    if data.len() < 32 {
        return;
    }
    // Derive chunk boundaries from the fuzz input so we exercise many
    // chunk-size distributions. First byte picks the number of chunks
    // (1..=16), remaining bytes are split evenly among them.
    let num_chunks = (data[0] as usize % 16) + 1;
    let payload = &data[1..];
    if payload.is_empty() {
        return;
    }
    let base = payload.len() / num_chunks;
    let mut chunks: Vec<(Vec<u8>, u64)> = Vec::with_capacity(num_chunks);
    let mut offset: u64 = 0;
    let mut cursor = 0usize;
    for i in 0..num_chunks {
        let end = if i + 1 == num_chunks {
            payload.len()
        } else {
            cursor + base
        };
        if end <= cursor {
            break;
        }
        let slice = payload[cursor..end].to_vec();
        chunks.push((slice, offset));
        offset += (end - cursor) as u64;
        cursor = end;
    }
    if chunks.is_empty() {
        return;
    }

    // Pack: chunks -> serialized xorb.
    let packed = match pack_chunks_into_xorb(&chunks) {
        Ok(p) => p,
        Err(_) => return,
    };

    // Re-parse + re-serialize: for a well-formed xorb this must be
    // byte-identical (headers, payloads, and footer are written back
    // verbatim), and must never fail.
    let mut reserialized = Vec::new();
    match reconstruct_xorb_with_footer(&mut reserialized, &packed.serialized) {
        Ok(_) => {
            assert_eq!(
                reserialized,
                packed.serialized,
                "re-serialization mismatch ({} chunks, {} bytes)",
                chunks.len(),
                packed.serialized.len()
            );
        }
        Err(_) => {
            // A well-formed pack must always re-serialize. Fail loudly.
            assert!(
                false,
                "pack produced xorb that failed to re-serialize ({} chunks, {} bytes)",
                chunks.len(),
                packed.serialized.len()
            );
        }
    }
});
