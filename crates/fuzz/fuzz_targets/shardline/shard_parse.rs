#![no_main]

use std::{io::Cursor, num::NonZeroUsize};

use libfuzzer_sys::fuzz_target;
use shardline_server::{
    FuzzRetainedShardSummary, ShardMetadataLimits, fuzz_retained_shard_chunk_hashes,
};
use xet_core_structures::metadata_shard::shard_format::MDBShardInfo;

const MAX_SHARD_BYTES: usize = 1_048_576;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ShardSummary {
    file_entries: usize,
    xorb_entries: usize,
    chunk_entries: usize,
    materialized_bytes: u64,
    stored_bytes: u64,
    files_read: usize,
    xorbs_read: usize,
}

fuzz_target!(|data: Vec<u8>| {
    if data.len() > MAX_SHARD_BYTES {
        return;
    }

    let first_parse = summarize_shard(data.as_slice());
    let second_parse = summarize_shard(data.as_slice());
    assert_eq!(first_parse.is_ok(), second_parse.is_ok());
    match (&first_parse, &second_parse) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left, right),
        _ => return,
    }

    let Ok(summary) = first_parse else {
        return;
    };
    assert!(summary.files_read <= summary.file_entries);
    assert!(summary.xorbs_read <= summary.xorb_entries);

    let default_retained =
        summarize_retained_hashes(data.as_slice(), ShardMetadataLimits::default());
    let repeated_default_retained =
        summarize_retained_hashes(data.as_slice(), ShardMetadataLimits::default());
    assert_eq!(default_retained.is_ok(), repeated_default_retained.is_ok());
    match (&default_retained, &repeated_default_retained) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left, right),
        _ => return,
    }
    if let Ok(retained) = default_retained {
        assert!(retained.dedupe_chunk_hashes.len() <= summary.chunk_entries);
    }

    let minimal_limit = NonZeroUsize::MIN;
    let minimal_limits =
        ShardMetadataLimits::new(minimal_limit, minimal_limit, minimal_limit, minimal_limit);
    let first_limited = summarize_retained_hashes(data.as_slice(), minimal_limits);
    let second_limited = summarize_retained_hashes(data.as_slice(), minimal_limits);
    assert_eq!(first_limited.is_ok(), second_limited.is_ok());
    match (&first_limited, &second_limited) {
        (Ok(left), Ok(right)) => assert_eq!(left, right),
        (Err(left), Err(right)) => assert_eq!(left, right),
        _ => return,
    }
});

fn summarize_shard(data: &[u8]) -> Result<ShardSummary, String> {
    let mut reader = Cursor::new(data);
    let shard = MDBShardInfo::load_from_reader(&mut reader).map_err(|error| format!("{error}"))?;
    let mut file_reader = Cursor::new(data);
    let files = shard
        .read_all_file_info_sections(&mut file_reader)
        .map_err(|error| format!("{error}"))?;
    let mut xorb_reader = Cursor::new(data);
    let xorbs = shard
        .read_all_xorb_blocks_full(&mut xorb_reader)
        .map_err(|error| format!("{error}"))?;

    for file in &files {
        if file.segments.len() != file.metadata.num_entries as usize {
            return Err("file segment count disagreed with metadata".to_owned());
        }
        if file.contains_verification() && file.verification.len() != file.segments.len() {
            return Err("file verification count disagreed with segments".to_owned());
        }
        let file_size = file.file_size();
        let summed = file.segments.iter().try_fold(0_u64, |total, segment| {
            total.checked_add(u64::from(segment.unpacked_segment_bytes))
        });
        if summed != Some(file_size) {
            return Err("file segment lengths disagreed with file size".to_owned());
        }
    }

    for xorb in &xorbs {
        if xorb.chunks.len() != xorb.metadata.num_entries as usize {
            return Err("xorb chunk count disagreed with metadata".to_owned());
        }
    }

    Ok(ShardSummary {
        file_entries: shard.num_file_entries(),
        xorb_entries: shard.num_xorb_entries(),
        chunk_entries: shard.total_num_chunks(),
        materialized_bytes: shard.materialized_bytes(),
        stored_bytes: shard.stored_bytes(),
        files_read: files.len(),
        xorbs_read: xorbs.len(),
    })
}

fn summarize_retained_hashes(
    data: &[u8],
    limits: ShardMetadataLimits,
) -> Result<FuzzRetainedShardSummary, String> {
    fuzz_retained_shard_chunk_hashes(data, limits).map_err(|error| format!("{error}"))
}
