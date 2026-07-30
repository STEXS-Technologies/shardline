#![no_main]

use libfuzzer_sys::fuzz_target;
use shardline_server::upload_ingest::cdc::CdcChunker;

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    // Fuzz CDC chunker with varying target sizes derived from input.
    let target_size = ((data.len() as u64) % 65536 + 1) as usize;
    let chunker = CdcChunker::new(target_size);
    let initial = chunker.find_boundary(data, 0);
    // Must always return a valid boundary.
    if let Some(boundary) = initial {
        assert!(boundary >= chunker.min_chunk(), "boundary below min");
        assert!(boundary <= data.len(), "boundary past end");
        assert!(boundary <= chunker.max_chunk(), "boundary above max");
        // Resume scan from boundary.
        let second = chunker.find_boundary(data, boundary);
        if let Some(b2) = second {
            assert!(b2 > boundary, "second boundary must advance");
        }
    }
});
