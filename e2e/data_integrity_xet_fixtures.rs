//! Verified shared-container fixtures; shard claims match actual member bytes.
use shardline_xet_core::{
    merklehash::{compute_data_hash, file_hash, xorb_hash},
    metadata_shard::{
        file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
        shard_format::MDBShardInfo,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
    },
    xorb_object::{
        CompressionScheme, xorb_format_test_utils::serialized_xorb_object_from_components,
    },
};

pub(super) struct SharedXorb {
    pub hash: String,
    pub serialized: Vec<u8>,
    pub files: Vec<File>,
}

pub(super) struct File {
    pub hash: String,
    pub shard: Vec<u8>,
}

pub(super) fn shared_xorb_files(contents: &[&[u8]]) -> SharedXorb {
    let leaves: Vec<_> = contents
        .iter()
        .map(|bytes| {
            (
                compute_data_hash(bytes),
                u64::try_from(bytes.len()).unwrap(),
            )
        })
        .collect();
    let identity = xorb_hash(&leaves);
    let mut total = 0_u64;
    let mut boundaries = Vec::new();
    let mut members = Vec::new();
    for (hash, length) in &leaves {
        members.push(XorbChunkSequenceEntry::new(*hash, *length, total));
        total = total.checked_add(*length).unwrap();
        boundaries.push((*hash, total));
    }
    let serialized = serialized_xorb_object_from_components(
        &identity,
        contents.concat(),
        boundaries,
        CompressionScheme::None,
    )
    .unwrap()
    .serialized_data;
    let cas = MDBXorbInfo {
        metadata: XorbChunkSequenceHeader::new(
            identity,
            u64::try_from(leaves.len()).unwrap(),
            total,
        ),
        chunks: members,
    };
    let files = leaves
        .iter()
        .enumerate()
        .map(|(ordinal, leaf)| {
            let start = u64::try_from(ordinal).unwrap();
            let end = start.checked_add(1).unwrap();
            let hash = file_hash(&[*leaf]);
            let mut shard = MDBInMemoryShard::default();
            shard.add_xorb_block(cas.clone()).unwrap();
            shard
                .add_file_reconstruction_info(MDBFileInfo {
                    metadata: FileDataSequenceHeader::new(hash, 1, false, false),
                    segments: vec![FileDataSequenceEntry::new(identity, leaf.1, start, end)],
                    verification: Vec::new(),
                    metadata_ext: None,
                })
                .unwrap();
            let mut bytes = Vec::new();
            MDBShardInfo::serialize_from(&mut bytes, &shard, None).unwrap();
            File {
                hash: hash.hex(),
                shard: bytes,
            }
        })
        .collect();
    SharedXorb {
        hash: identity.hex(),
        serialized,
        files,
    }
}
