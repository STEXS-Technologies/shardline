use std::io::Cursor;

use shardline_xet_core::merklehash::compute_data_hash;
use shardline_xet_core::metadata_shard::{
    file_structs::{
        FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry,
        MDBFileInfo,
    },
    shard_format::{MDBShardFileFooter, MDBShardFileHeader, MDBShardInfo},
    shard_in_memory::MDBInMemoryShard,
    xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
};

// ============================================================================
// Full shard round-trip: build file info entries → build xorb info entries →
// create in-memory shard → serialize to bytes → load from bytes → verify.
// ============================================================================

fn make_file_info(
    file_hash_data: &[u8],
    num_segments: u64,
    unpacked_per_segment: u64,
    with_verification: bool,
    with_metadata_ext: bool,
) -> MDBFileInfo {
    let segments: Vec<FileDataSequenceEntry> = (0..num_segments)
        .map(|i| {
            FileDataSequenceEntry::new(
                compute_data_hash(b"x"),
                unpacked_per_segment,
                i * unpacked_per_segment,
                (i + 1) * unpacked_per_segment,
            )
        })
        .collect();

    let verification = if with_verification {
        (0..num_segments)
            .map(|_| FileVerificationEntry::new(compute_data_hash(b"v")))
            .collect()
    } else {
        vec![]
    };

    let metadata_ext = if with_metadata_ext {
        Some(FileMetadataExt::new(compute_data_hash(b"ext")))
    } else {
        None
    };

    MDBFileInfo {
        metadata: FileDataSequenceHeader::new(
            compute_data_hash(file_hash_data),
            num_segments,
            with_verification,
            with_metadata_ext,
        ),
        segments,
        verification,
        metadata_ext,
    }
}

fn make_xorb_info(
    xorb_hash_data: &[u8],
    num_chunks: u64,
    bytes_per_chunk: u64) -> MDBXorbInfo {
    let chunks: Vec<XorbChunkSequenceEntry> = (0..num_chunks)
        .map(|i| {
            XorbChunkSequenceEntry::new(
                compute_data_hash(b"c"),
                bytes_per_chunk,
                i * bytes_per_chunk,
            )
        })
        .collect();

    MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(
            compute_data_hash(xorb_hash_data),
            num_chunks,
            num_chunks * bytes_per_chunk,
        ),
        chunks,
    }
}

/// Full shard roundtrip: build entries, serialize, load, verify all match
#[test]
fn shard_full_roundtrip() {
    let mut shard = MDBInMemoryShard::default();

    // Add file infos with various configurations
    shard
        .add_file_reconstruction_info(make_file_info(b"f1", 2, 100, false, false))
        .unwrap();
    shard
        .add_file_reconstruction_info(make_file_info(b"f2", 1, 200, true, false))
        .unwrap();
    shard
        .add_file_reconstruction_info(make_file_info(b"f3", 3, 50, false, true))
        .unwrap();
    shard
        .add_file_reconstruction_info(make_file_info(b"f4", 1, 500, true, true))
        .unwrap();

    // Add xorb infos
    shard.add_xorb_block(make_xorb_info(b"x1", 2, 100)).unwrap();
    shard.add_xorb_block(make_xorb_info(b"x2", 1, 300)).unwrap();
    shard.add_xorb_block(make_xorb_info(b"x3", 0, 0)).unwrap();

    assert_eq!(shard.num_file_entries(), 4);
    assert_eq!(shard.num_xorb_entries(), 3);

    // Serialize to bytes
    let serialized = shard.to_bytes().unwrap();
    assert!(!serialized.is_empty());

    // Load from bytes
    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();

    // Verify counts match
    assert_eq!(loaded.num_file_entries(), 4);
    assert_eq!(loaded.num_xorb_entries(), 3);

    // Verify materialized/stored bytes match
    assert_eq!(loaded.materialized_bytes(), shard.materialized_bytes());
    assert_eq!(loaded.stored_bytes(), shard.stored_bytes());

    // Verify each file info exists
    let file_hashes: Vec<_> = loaded
        .file_infos
        .iter()
        .map(|f| f.metadata.file_hash)
        .collect();
    assert!(file_hashes.contains(&compute_data_hash(b"f1")));
    assert!(file_hashes.contains(&compute_data_hash(b"f2")));
    assert!(file_hashes.contains(&compute_data_hash(b"f3")));
    assert!(file_hashes.contains(&compute_data_hash(b"f4")));

    // Verify each xorb info exists
    let xorb_hashes: Vec<_> = loaded
        .xorb_infos
        .iter()
        .map(|x| x.metadata.xorb_hash)
        .collect();
    assert!(xorb_hashes.contains(&compute_data_hash(b"x1")));
    assert!(xorb_hashes.contains(&compute_data_hash(b"x2")));
    assert!(xorb_hashes.contains(&compute_data_hash(b"x3")));

    // Verify segment counts
    let f1 = loaded
        .file_infos
        .iter()
        .find(|f| f.metadata.file_hash == compute_data_hash(b"f1"))
        .unwrap();
    assert_eq!(f1.segments.len(), 2);
    assert_eq!(f1.verification.len(), 0);
    assert!(f1.metadata_ext.is_none());

    let f2 = loaded
        .file_infos
        .iter()
        .find(|f| f.metadata.file_hash == compute_data_hash(b"f2"))
        .unwrap();
    assert_eq!(f2.segments.len(), 1);
    assert_eq!(f2.verification.len(), 1);
    assert!(f2.metadata_ext.is_none());

    let f3 = loaded
        .file_infos
        .iter()
        .find(|f| f.metadata.file_hash == compute_data_hash(b"f3"))
        .unwrap();
    assert_eq!(f3.segments.len(), 3);
    assert_eq!(f3.verification.len(), 0);
    assert!(f3.metadata_ext.is_some());

    let f4 = loaded
        .file_infos
        .iter()
        .find(|f| f.metadata.file_hash == compute_data_hash(b"f4"))
        .unwrap();
    assert_eq!(f4.segments.len(), 1);
    assert_eq!(f4.verification.len(), 1);
    assert!(f4.metadata_ext.is_some());

    // Verify xorb chunk counts
    let x1 = loaded
        .xorb_infos
        .iter()
        .find(|x| x.metadata.xorb_hash == compute_data_hash(b"x1"))
        .unwrap();
    assert_eq!(x1.chunks.len(), 2);

    let x2 = loaded
        .xorb_infos
        .iter()
        .find(|x| x.metadata.xorb_hash == compute_data_hash(b"x2"))
        .unwrap();
    assert_eq!(x2.chunks.len(), 1);

    let x3 = loaded
        .xorb_infos
        .iter()
        .find(|x| x.metadata.xorb_hash == compute_data_hash(b"x3"))
        .unwrap();
    assert_eq!(x3.chunks.len(), 0);
}

/// Empty shard: no files, no xorbs
#[test]
fn shard_empty_roundtrip() {
    let shard = MDBInMemoryShard::default();
    assert_eq!(shard.num_file_entries(), 0);
    assert_eq!(shard.num_xorb_entries(), 0);

    let serialized = shard.to_bytes().unwrap();
    assert!(!serialized.is_empty()); // header + file bookend + xorb bookend

    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();
    assert_eq!(loaded.num_file_entries(), 0);
    assert_eq!(loaded.num_xorb_entries(), 0);
    assert_eq!(loaded.materialized_bytes(), 0);
    assert_eq!(loaded.stored_bytes(), 0);
}

/// Shard with only files, no xorbs
#[test]
fn shard_files_only() {
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(make_file_info(b"f1", 1, 100, false, false))
        .unwrap();
    shard
        .add_file_reconstruction_info(make_file_info(b"f2", 2, 50, false, false))
        .unwrap();

    let serialized = shard.to_bytes().unwrap();
    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();
    assert_eq!(loaded.num_file_entries(), 2);
    assert_eq!(loaded.num_xorb_entries(), 0);
    assert_eq!(loaded.materialized_bytes(), 200); // 1*100 + 2*50
}

/// Shard with only xorbs, no files
#[test]
fn shard_xorbs_only() {
    let mut shard = MDBInMemoryShard::default();
    shard.add_xorb_block(make_xorb_info(b"x1", 3, 100)).unwrap();
    shard.add_xorb_block(make_xorb_info(b"x2", 1, 500)).unwrap();

    let serialized = shard.to_bytes().unwrap();
    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();
    assert_eq!(loaded.num_file_entries(), 0);
    assert_eq!(loaded.num_xorb_entries(), 2);
    assert_eq!(loaded.stored_bytes(), 800); // 3*100 + 1*500
}

/// Single file, single xorb
#[test]
fn shard_single_entry_each() {
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(make_file_info(b"only_file", 1, 256, false, false))
        .unwrap();
    shard
        .add_xorb_block(make_xorb_info(b"only_xorb", 1, 256))
        .unwrap();

    let serialized = shard.to_bytes().unwrap();
    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();
    assert_eq!(loaded.num_file_entries(), 1);
    assert_eq!(loaded.num_xorb_entries(), 1);

    let file = &loaded.file_infos[0];
    assert_eq!(file.metadata.file_hash, compute_data_hash(b"only_file"));
    assert_eq!(file.segments[0].unpacked_segment_bytes, 256);

    let xorb = &loaded.xorb_infos[0];
    assert_eq!(xorb.metadata.xorb_hash, compute_data_hash(b"only_xorb"));
    assert_eq!(xorb.chunks[0].unpacked_segment_bytes, 256);
}

/// File info with verification + metadata_ext flags
#[test]
fn shard_file_info_with_all_flags() {
    let mut shard = MDBInMemoryShard::default();
    shard
        .add_file_reconstruction_info(make_file_info(b"full", 2, 100, true, true))
        .unwrap();

    let serialized = shard.to_bytes().unwrap();
    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();
    assert_eq!(loaded.num_file_entries(), 1);

    let file = &loaded.file_infos[0];
    assert!(file.contains_verification());
    assert!(file.contains_metadata_ext());
    assert_eq!(file.verification.len(), 2);
    assert!(file.metadata_ext.is_some());
    assert_eq!(file.segments.len(), 2);
    assert_eq!(file.file_size(), 200);
}

/// Serialize from MDBShardInfo directly (not via MDBInMemoryShard)
#[test]
fn shard_serialize_from_info() {
    let info = MDBShardInfo {
        file_infos: vec![make_file_info(b"f1", 1, 100, false, false)],
        xorb_infos: vec![make_xorb_info(b"x1", 2, 50)],
        ..Default::default()
    };

    let mut buf = Vec::new();
    MDBShardFileHeader::default().serialize(&mut buf).unwrap();
    for f in &info.file_infos {
        f.serialize(&mut buf).unwrap();
    }
    FileDataSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();
    for x in &info.xorb_infos {
        x.serialize(&mut buf).unwrap();
    }
    XorbChunkSequenceHeader::bookend()
        .serialize(&mut buf)
        .unwrap();
    MDBShardFileFooter::default().serialize(&mut buf).unwrap();

    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&buf)).unwrap();
    assert_eq!(loaded.num_file_entries(), 1);
    assert_eq!(loaded.num_xorb_entries(), 1);
    assert_eq!(loaded.total_num_chunks(), 2);
    assert_eq!(loaded.materialized_bytes(), 100);
    assert_eq!(loaded.stored_bytes(), 100);
}

/// Shard with many files and xorbs to stress test
#[test]
fn shard_many_entries() {
    let mut shard = MDBInMemoryShard::default();

    // Add 10 files
    for i in 0..10 {
        let hash_data = format!("f{i}");
        shard
            .add_file_reconstruction_info(make_file_info(
                hash_data.as_bytes(),
                1,
                100,
                i % 2 == 0,
                i % 3 == 0,
            ))
            .unwrap();
    }

    // Add 10 xorbs
    for i in 0..10 {
        let hash_data = format!("x{i}");
        shard
            .add_xorb_block(make_xorb_info(hash_data.as_bytes(), i + 1, 50))
            .unwrap();
    }

    assert_eq!(shard.num_file_entries(), 10);
    assert_eq!(shard.num_xorb_entries(), 10);

    let serialized = shard.to_bytes().unwrap();
    let loaded = MDBShardInfo::load_from_reader(&mut Cursor::new(&serialized)).unwrap();
    assert_eq!(loaded.num_file_entries(), 10);
    assert_eq!(loaded.num_xorb_entries(), 10);
    assert_eq!(loaded.total_num_chunks(), (0..10).sum::<usize>() + 10); // 55
    assert_eq!(loaded.stored_bytes(), shard.stored_bytes());
    assert_eq!(loaded.materialized_bytes(), shard.materialized_bytes());
}

/// Test MDBShardInfo::read_all_file_info_sections and read_all_xorb_blocks_full
#[test]
fn shard_read_sections() {
    // Build separate buffers for file section and xorb section to avoid
    // read_all_file_info_sections reading past the file bookend into xorb data.

    // --- File info section ---
    let mut file_buf = Vec::new();
    let f1 = make_file_info(b"fa", 1, 100, false, false);
    let f2 = make_file_info(b"fb", 2, 50, true, false);
    f1.serialize(&mut file_buf).unwrap();
    f2.serialize(&mut file_buf).unwrap();
    FileDataSequenceHeader::bookend()
        .serialize(&mut file_buf)
        .unwrap();

    let info = MDBShardInfo::default();
    let files = info
        .read_all_file_info_sections(&mut Cursor::new(&file_buf), 3)
        .unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].segments[0].unpacked_segment_bytes, 100);
    assert_eq!(files[1].segments.len(), 2);

    // --- Xorb info section ---
    let mut xorb_buf = Vec::new();
    let x1 = make_xorb_info(b"ya", 2, 100);
    let x2 = make_xorb_info(b"yb", 1, 200);
    x1.serialize(&mut xorb_buf).unwrap();
    x2.serialize(&mut xorb_buf).unwrap();
    XorbChunkSequenceHeader::bookend()
        .serialize(&mut xorb_buf)
        .unwrap();

    let xorbs = info
        .read_all_xorb_blocks_full(&mut Cursor::new(&xorb_buf), 3)
        .unwrap();
    assert_eq!(xorbs.len(), 2);
    assert_eq!(xorbs[0].chunks.len(), 2);
    assert_eq!(xorbs[1].chunks.len(), 1);
}
