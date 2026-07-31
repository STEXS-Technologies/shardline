#![no_main]
#![allow(
    clippy::arithmetic_side_effects,
    clippy::indexing_slicing,
    clippy::assertions_on_constants
)]

use libfuzzer_sys::fuzz_target;
use shardline_server::upload_ingest::cdc::CdcChunker;

fuzz_target!(|data: &[u8]| {
    if data.len() < 16 {
        return;
    }
    // Power-of-two target sizes in [256, 2^23): the chunker's documented
    // domain (target_chunk_size must be a power of two greater than 64).
    let target_size = 256_usize << (data[0] as usize % 16);
    let mut chunker = CdcChunker::new(target_size);
    let min_c = chunker.min_chunk();

    if let Some(b) = chunker.find_boundary(data) {
        assert!(b >= min_c, "b {b} < min {min_c}");
        assert!(b <= data.len(), "b {b} > len {}", data.len());
        if b < data.len() {
            let mut c2 = CdcChunker::new(target_size);
            if let Some(b2) = c2.find_boundary(&data[b..]) {
                assert!(b2 > 0, "second boundary must advance");
            }
        }
    }
    // Smaller target size stress test (power of two in [128, 32768)).
    let small_target = 128_usize << (data[0] as usize % 8);
    let mut small_chunker = CdcChunker::new(small_target);
    if let Some(b) = small_chunker.find_boundary(data) {
        assert!(b >= small_chunker.min_chunk());
    }
});
