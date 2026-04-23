use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Cursor, Read},
    mem::size_of,
};

use axum::body::Bytes;
use shardline_index::{AsyncIndexStore, DedupeShardMapping, FileChunkRecord, FileRecord};
use shardline_protocol::{
    RepositoryScope, ShardlineHash, ValidatedXorbChunk, validate_serialized_xorb,
};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectKeyError, PutOutcome};
use xet_core_structures::{
    merklehash::{MerkleHash, compute_data_hash},
    metadata_shard::{
        MDBShardFileHeader,
        file_structs::{FileDataSequenceHeader, MDBFileInfo, MDBFileInfoView},
        hash_is_global_dedup_eligible,
        shard_file::MDB_FILE_INFO_ENTRY_SIZE,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{
            MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG, MDBXorbInfo, MDBXorbInfoView, XorbChunkSequenceEntry,
            XorbChunkSequenceHeader,
        },
    },
};

use crate::{
    InvalidSerializedShardError, ServerError,
    config::ShardMetadataLimits,
    local_backend::{chunk_hash, content_hash},
    object_store::{ServerObjectStore, read_full_object},
    validation::validate_content_hash,
    xorb_store::xorb_object_key,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ParsedShardUpload {
    pub(crate) result: u8,
    pub(crate) records: Vec<FileRecord>,
    pub(crate) shard_key: ObjectKey,
    pub(crate) dedupe_chunk_hashes: Vec<String>,
}

pub(crate) fn parse_uploaded_shard(
    object_store: &ServerObjectStore,
    uploaded_shard: &[u8],
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
) -> Result<ParsedShardUpload, ServerError> {
    let parsed_shard = parse_shard_records(uploaded_shard, object_store, repository_scope, limits)?;
    let shard_key = shard_object_key(&parsed_shard.shard_hash_hex)?;
    let was_present = object_store.metadata(&shard_key)?.is_some();
    let shard_length = u64::try_from(parsed_shard.normalized_bytes.len())?;
    let integrity = ObjectIntegrity::new(chunk_hash(&parsed_shard.normalized_bytes), shard_length);
    let stored = object_store.put_if_absent(
        &shard_key,
        ObjectBody::from_slice(&parsed_shard.normalized_bytes),
        &integrity,
    );
    let stored = stored?;

    Ok(ParsedShardUpload {
        result: if was_present || matches!(stored, PutOutcome::AlreadyExists) {
            0
        } else {
            1
        },
        records: parsed_shard.records,
        shard_key,
        dedupe_chunk_hashes: parsed_shard.dedupe_chunk_hashes,
    })
}

pub(crate) async fn resolve_dedupe_shard_object<IndexAdapter>(
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    chunk_hash_hex: &str,
) -> Result<(ObjectKey, u64), ServerError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<ServerError>,
{
    let chunk_hash = ShardlineHash::parse_api_hex(chunk_hash_hex)?;
    let Some(mapping) = index_store
        .dedupe_shard_mapping(&chunk_hash)
        .await
        .map_err(Into::into)?
    else {
        return Err(ServerError::NotFound);
    };
    let shard_key = mapping.shard_object_key();
    let Some(metadata) = object_store.metadata(shard_key)? else {
        return Err(ServerError::NotFound);
    };

    Ok((shard_key.clone(), metadata.length()))
}

pub(crate) fn retained_shard_chunk_hashes(
    shard_bytes: &[u8],
    limits: ShardMetadataLimits,
) -> Result<Vec<String>, ServerError> {
    let mut shard_reader = Cursor::new(shard_bytes);
    MDBShardFileHeader::deserialize(&mut shard_reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    read_bounded_shard_sections(&mut shard_reader, limits)
        .map(|bounded_shard| bounded_shard.dedupe_chunk_hashes)
}

struct NormalizedShardUpload {
    shard_hash_hex: String,
    normalized_bytes: Vec<u8>,
    records: Vec<FileRecord>,
    dedupe_chunk_hashes: Vec<String>,
}

fn parse_shard_records(
    uploaded_shard: &[u8],
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
) -> Result<NormalizedShardUpload, ServerError> {
    let mut shard_reader = Cursor::new(uploaded_shard);
    MDBShardFileHeader::deserialize(&mut shard_reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    let bounded_shard = read_bounded_shard_sections(&mut shard_reader, limits)?;
    let mut in_memory_shard = MDBInMemoryShard::default();
    validate_referenced_xorb_count(&bounded_shard.file_infos, limits.max_xorbs().get())?;

    for file_info in &bounded_shard.file_infos {
        in_memory_shard
            .add_file_reconstruction_info(file_info.clone())
            .map_err(|error| invalid_serialized_shard(&error))?;
    }

    for xorb_info in bounded_shard.xorb_infos {
        in_memory_shard
            .add_xorb_block(xorb_info)
            .map_err(|error| invalid_serialized_shard(&error))?;
    }

    let normalized_bytes = in_memory_shard
        .to_bytes()
        .map_err(|error| invalid_serialized_shard(&error))?;
    let shard_hash_hex = compute_data_hash(&normalized_bytes).hex();
    let records =
        build_file_records_from_infos(bounded_shard.file_infos, object_store, repository_scope)?;

    Ok(NormalizedShardUpload {
        shard_hash_hex,
        normalized_bytes,
        records,
        dedupe_chunk_hashes: bounded_shard.dedupe_chunk_hashes,
    })
}

struct BoundedShardSections {
    file_infos: Vec<MDBFileInfo>,
    xorb_infos: Vec<MDBXorbInfo>,
    dedupe_chunk_hashes: Vec<String>,
}

fn read_bounded_shard_sections<R: Read>(
    reader: &mut R,
    limits: ShardMetadataLimits,
) -> Result<BoundedShardSections, ServerError> {
    let mut file_infos = Vec::new();
    let mut file_start_entries = HashMap::<MerkleHash, HashSet<usize>>::new();
    read_bounded_file_sections(reader, &mut file_infos, &mut file_start_entries, limits)?;

    let mut xorb_infos = Vec::new();
    let dedupe_chunk_hashes =
        read_bounded_xorb_sections(reader, &file_start_entries, &mut xorb_infos, limits)?;

    Ok(BoundedShardSections {
        file_infos,
        xorb_infos,
        dedupe_chunk_hashes,
    })
}

fn read_bounded_file_sections<R: Read>(
    reader: &mut R,
    file_infos: &mut Vec<MDBFileInfo>,
    file_start_entries: &mut HashMap<MerkleHash, HashSet<usize>>,
    limits: ShardMetadataLimits,
) -> Result<(), ServerError> {
    let mut reconstruction_terms = 0_usize;

    loop {
        let header = FileDataSequenceHeader::deserialize(reader)
            .map_err(|error| invalid_serialized_shard(&error))?;
        if header.is_bookend() {
            return Ok(());
        }

        let file_count = checked_increment(file_infos.len())?;
        if file_count > limits.max_files().get() {
            return Err(ServerError::TooManyShardTerms);
        }

        let segment_count = usize::try_from(header.num_entries)?;
        reconstruction_terms = checked_add_limit(
            reconstruction_terms,
            segment_count,
            limits.max_reconstruction_terms().get(),
        )?;

        let followed_entries = file_section_followed_entries(&header, segment_count)?;
        let followed_bytes = checked_mul(followed_entries, MDB_FILE_INFO_ENTRY_SIZE)?;
        let file_view = read_file_info_view(reader, header, followed_bytes)?;

        if segment_count > 0 {
            let first_segment = file_view.entry(0);
            let first_index = usize::try_from(first_segment.chunk_index_start)?;
            file_start_entries
                .entry(first_segment.xorb_hash)
                .or_default()
                .insert(first_index);
        }

        file_infos.push(MDBFileInfo::from(&file_view));
    }
}

fn read_bounded_xorb_sections<R: Read>(
    reader: &mut R,
    file_start_entries: &HashMap<MerkleHash, HashSet<usize>>,
    xorb_infos: &mut Vec<MDBXorbInfo>,
    limits: ShardMetadataLimits,
) -> Result<Vec<String>, ServerError> {
    let mut xorb_chunks = 0_usize;
    let mut dedupe_chunk_hashes = BTreeSet::new();

    loop {
        let header = XorbChunkSequenceHeader::deserialize(reader)
            .map_err(|error| invalid_serialized_shard(&error))?;
        if header.is_bookend() {
            return Ok(dedupe_chunk_hashes.into_iter().collect());
        }

        let xorb_count = checked_increment(xorb_infos.len())?;
        if xorb_count > limits.max_xorbs().get() {
            return Err(ServerError::TooManyShardTerms);
        }

        let chunk_count = usize::try_from(header.num_entries)?;
        xorb_chunks = checked_add_limit(xorb_chunks, chunk_count, limits.max_xorb_chunks().get())?;
        let followed_bytes = checked_mul(chunk_count, size_of::<XorbChunkSequenceEntry>())?;
        let xorb_view = read_xorb_info_view(reader, header, followed_bytes)?;

        collect_dedupe_chunk_hashes(&xorb_view, file_start_entries, &mut dedupe_chunk_hashes);
        xorb_infos.push(MDBXorbInfo::from(&xorb_view));
    }
}

fn file_section_followed_entries(
    header: &FileDataSequenceHeader,
    segment_count: usize,
) -> Result<usize, ServerError> {
    let verification_entries = if header.contains_verification() {
        segment_count
    } else {
        0
    };
    let metadata_entries = if header.contains_metadata_ext() { 1 } else { 0 };

    checked_add(
        checked_add(segment_count, verification_entries)?,
        metadata_entries,
    )
}

fn read_file_info_view<R: Read>(
    reader: &mut R,
    header: FileDataSequenceHeader,
    followed_bytes: usize,
) -> Result<MDBFileInfoView, ServerError> {
    let total_bytes = checked_add(MDB_FILE_INFO_ENTRY_SIZE, followed_bytes)?;
    let mut data = Vec::new();
    data.try_reserve_exact(total_bytes)
        .map_err(|_reserve_error| ServerError::TooManyShardTerms)?;
    header
        .serialize(&mut data)
        .map_err(|error| invalid_serialized_shard(&error))?;
    read_exact_section(reader, followed_bytes, &mut data)?;
    MDBFileInfoView::from_data_and_header(header, Bytes::from(data))
        .map_err(|error| invalid_serialized_shard(&error))
}

fn read_xorb_info_view<R: Read>(
    reader: &mut R,
    header: XorbChunkSequenceHeader,
    followed_bytes: usize,
) -> Result<MDBXorbInfoView, ServerError> {
    let total_bytes = checked_add(size_of::<XorbChunkSequenceHeader>(), followed_bytes)?;
    let mut data = Vec::new();
    data.try_reserve_exact(total_bytes)
        .map_err(|_reserve_error| ServerError::TooManyShardTerms)?;
    header
        .serialize(&mut data)
        .map_err(|error| invalid_serialized_shard(&error))?;
    read_exact_section(reader, followed_bytes, &mut data)?;
    MDBXorbInfoView::from_data_and_header(header, Bytes::from(data))
        .map_err(|error| invalid_serialized_shard(&error))
}

fn read_exact_section<R: Read>(
    reader: &mut R,
    byte_count: usize,
    output: &mut Vec<u8>,
) -> Result<(), ServerError> {
    let start = output.len();
    let end = checked_add(start, byte_count)?;
    output.resize(end, 0);
    let section = output.get_mut(start..end).ok_or(ServerError::Overflow)?;
    reader
        .read_exact(section)
        .map_err(|error| invalid_serialized_shard(&error))
}

fn collect_dedupe_chunk_hashes(
    xorb_view: &MDBXorbInfoView,
    file_start_entries: &HashMap<MerkleHash, HashSet<usize>>,
    dedupe_chunk_hashes: &mut BTreeSet<String>,
) {
    let start_entries = file_start_entries.get(&xorb_view.xorb_hash());
    for chunk_index in 0..xorb_view.num_entries() {
        let chunk = xorb_view.chunk(chunk_index);
        let is_file_start = start_entries.is_some_and(|entries| entries.contains(&chunk_index));
        if is_file_start
            || hash_is_global_dedup_eligible(&chunk.chunk_hash)
            || (chunk.flags & MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG) != 0
        {
            dedupe_chunk_hashes.insert(chunk.chunk_hash.hex());
        }
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, ServerError> {
    left.checked_add(right).ok_or(ServerError::Overflow)
}

fn checked_increment(value: usize) -> Result<usize, ServerError> {
    checked_add(value, 1)
}

fn checked_mul(left: usize, right: usize) -> Result<usize, ServerError> {
    left.checked_mul(right).ok_or(ServerError::Overflow)
}

fn checked_add_limit(left: usize, right: usize, limit: usize) -> Result<usize, ServerError> {
    let value = checked_add(left, right)?;
    if value > limit {
        return Err(ServerError::TooManyShardTerms);
    }
    Ok(value)
}

fn invalid_serialized_shard<T>(_error: &T) -> ServerError {
    InvalidSerializedShardError::ParserRejectedMetadata.into()
}

pub(crate) fn shard_object_key(hash_hex: &str) -> Result<ObjectKey, ServerError> {
    validate_content_hash(hash_hex)?;
    let prefix = hash_hex.get(..2).ok_or(ServerError::InvalidContentHash)?;
    let key = format!("shards/{prefix}/{hash_hex}.shard");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

pub(crate) fn shard_hash_from_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, ServerError> {
    let mut segments = key.as_str().split('/');
    let Some(namespace) = segments.next() else {
        return Ok(None);
    };
    let Some(prefix) = segments.next() else {
        return Ok(None);
    };
    let Some(file_name) = segments.next() else {
        return Ok(None);
    };
    if segments.next().is_some() {
        return Ok(None);
    }
    if namespace != "shards" {
        return Ok(None);
    }
    if prefix.len() != 2 || !prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(None);
    }
    let Some(hash_hex) = file_name.strip_suffix(".shard") else {
        return Ok(None);
    };
    if !hash_hex.starts_with(prefix) {
        return Ok(None);
    }
    validate_content_hash(hash_hex)?;
    Ok(Some(hash_hex))
}

fn build_file_records_from_infos(
    file_infos: Vec<MDBFileInfo>,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
) -> Result<Vec<FileRecord>, ServerError> {
    let mut xorb_cache = HashMap::<String, XorbRangeInfo>::new();
    let mut records = Vec::with_capacity(file_infos.len());

    for file_info in file_infos {
        let file_id = file_info.metadata.file_hash.hex();
        let mut offset = 0_u64;
        let mut chunks = Vec::with_capacity(file_info.segments.len());

        for segment in file_info.segments {
            let hash = segment.xorb_hash.hex();
            let range_start = segment.chunk_index_start;
            let range_end = segment.chunk_index_end;
            if range_end <= range_start {
                return Err(
                    InvalidSerializedShardError::ShardFileTermEmptyOrInvertedChunkRange.into(),
                );
            }

            let xorb_info = if let Some(info) = xorb_cache.get(&hash) {
                info
            } else {
                let loaded = load_xorb_range_info(object_store, &hash)?;
                xorb_cache.insert(hash.clone(), loaded);
                xorb_cache
                    .get(&hash)
                    .ok_or(InvalidSerializedShardError::XorbMetadataCacheInsertionFailed)?
            };
            let packed_start = xorb_info.packed_start(range_start)?;
            let packed_end = xorb_info.packed_end(range_end)?;
            let length = u64::from(segment.unpacked_segment_bytes);
            chunks.push(FileChunkRecord {
                hash,
                offset,
                length,
                range_start,
                range_end,
                packed_start,
                packed_end,
            });
            offset = offset.checked_add(length).ok_or(ServerError::Overflow)?;
        }

        records.push(FileRecord {
            file_id,
            content_hash: content_hash(offset, 0, &chunks),
            total_bytes: offset,
            chunk_size: 0,
            repository_scope: repository_scope.cloned(),
            chunks,
        });
    }

    Ok(records)
}

fn validate_referenced_xorb_count(
    file_infos: &[MDBFileInfo],
    max_xorbs: usize,
) -> Result<(), ServerError> {
    let mut referenced_xorbs = HashSet::new();
    for file_info in file_infos {
        for segment in &file_info.segments {
            referenced_xorbs.insert(segment.xorb_hash.hex());
            if referenced_xorbs.len() > max_xorbs {
                return Err(ServerError::TooManyShardTerms);
            }
        }
    }

    Ok(())
}

fn load_xorb_range_info(
    object_store: &ServerObjectStore,
    hash_hex: &str,
) -> Result<XorbRangeInfo, ServerError> {
    let key = xorb_object_key(hash_hex)?;
    let Some(metadata) = object_store.metadata(&key)? else {
        return Err(ServerError::MissingReferencedXorb);
    };
    let bytes = read_full_object(object_store, &key, metadata.length())?;
    let expected_hash = ShardlineHash::parse_api_hex(hash_hex)?;
    let mut reader = Cursor::new(bytes);
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    let packed_chunk_ends = validated
        .chunks()
        .iter()
        .map(ValidatedXorbChunk::packed_end)
        .collect();
    Ok(XorbRangeInfo { packed_chunk_ends })
}

pub(crate) fn dedupe_shard_mapping(
    chunk_hash_hex: &str,
    shard_key: &ObjectKey,
) -> Result<DedupeShardMapping, ServerError> {
    let chunk_hash = ShardlineHash::parse_api_hex(chunk_hash_hex)?;
    Ok(DedupeShardMapping::new(chunk_hash, shard_key.clone()))
}

const fn map_object_key_error(error: ObjectKeyError) -> ServerError {
    match error {
        ObjectKeyError::Empty
        | ObjectKeyError::UnsafePath
        | ObjectKeyError::ControlCharacter
        | ObjectKeyError::TooLong => ServerError::InvalidContentHash,
    }
}

#[derive(Debug, Clone)]
struct XorbRangeInfo {
    packed_chunk_ends: Vec<u64>,
}

impl XorbRangeInfo {
    fn packed_start(&self, range_start: u32) -> Result<u64, ServerError> {
        let range_start = usize::try_from(range_start)?;
        if range_start == 0 {
            return Ok(0);
        }
        let previous_index = range_start
            .checked_sub(1)
            .ok_or(InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList)?;
        self.packed_chunk_ends
            .get(previous_index)
            .copied()
            .ok_or_else(|| {
                ServerError::from(
                    InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList,
                )
            })
    }

    fn packed_end(&self, range_end: u32) -> Result<u64, ServerError> {
        let range_end = usize::try_from(range_end)?;
        self.packed_chunk_ends
            .get(range_end.saturating_sub(1))
            .copied()
            .ok_or_else(|| {
                ServerError::from(InvalidSerializedShardError::ShardTermRangeEndedPastXorbChunkList)
            })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use xet_core_structures::{
        merklehash::{compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            file_structs::{FileDataSequenceEntry, FileDataSequenceHeader, MDBFileInfo},
            shard_format::MDBShardInfo,
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{MDBXorbInfo, XorbChunkSequenceEntry, XorbChunkSequenceHeader},
        },
    };

    use super::{parse_uploaded_shard, shard_hash_from_object_key_if_present, shard_object_key};
    use crate::{
        ServerError, ShardMetadataLimits, config::DEFAULT_SHARD_METADATA_LIMITS,
        object_store::ServerObjectStore,
    };

    #[test]
    fn shard_object_key_maps_native_hash_into_shard_namespace() {
        let hash = "ab".repeat(32);
        let key = shard_object_key(&hash);

        assert!(key.is_ok());
        if let Ok(key) = key {
            assert_eq!(key.as_str(), format!("shards/ab/{hash}.shard"));
        }
    }

    #[test]
    fn shard_object_key_rejects_non_hash_input() {
        assert!(shard_object_key("asset.bin").is_err());
        assert!(shard_object_key(&"A".repeat(64)).is_err());
    }

    #[test]
    fn shard_hash_from_object_key_extracts_hash_for_retained_shard_layout() {
        let hash = "cd".repeat(32);
        let key = shard_object_key(&hash);

        assert!(key.is_ok());
        let Ok(key) = key else {
            return;
        };
        let extracted = shard_hash_from_object_key_if_present(&key);

        assert!(extracted.is_ok());
        if let Ok(extracted) = extracted {
            assert_eq!(extracted, Some(hash.as_str()));
        }
    }

    #[test]
    fn shard_upload_rejects_excessive_reconstruction_terms_before_xorb_lookup() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let object_store = ServerObjectStore::local(temp.path().join("objects"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };

        let shard = shard_with_reconstruction_terms(
            DEFAULT_SHARD_METADATA_LIMITS
                .max_reconstruction_terms()
                .get()
                + 1,
        );
        let result =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);

        assert!(matches!(result, Err(ServerError::TooManyShardTerms)));
    }

    #[test]
    fn shard_upload_rejects_excessive_xorb_chunks_before_materializing_upload() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let object_store = ServerObjectStore::local(temp.path().join("objects"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };

        let shard =
            shard_with_xorb_chunks(DEFAULT_SHARD_METADATA_LIMITS.max_xorb_chunks().get() + 1);
        let result =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);

        assert!(matches!(result, Err(ServerError::TooManyShardTerms)));
    }

    #[test]
    fn shard_upload_rejects_excessive_unique_referenced_xorbs_before_lookup() {
        let temp = tempfile::tempdir();
        assert!(temp.is_ok());
        let Ok(temp) = temp else {
            return;
        };
        let object_store = ServerObjectStore::local(temp.path().join("objects"));
        assert!(object_store.is_ok());
        let Ok(object_store) = object_store else {
            return;
        };

        let max_one = NonZeroUsize::new(1);
        assert!(max_one.is_some());
        let Some(max_one) = max_one else {
            return;
        };
        let limits = ShardMetadataLimits::new(
            max_one,
            max_one,
            DEFAULT_SHARD_METADATA_LIMITS.max_reconstruction_terms(),
            DEFAULT_SHARD_METADATA_LIMITS.max_xorb_chunks(),
        );
        let first_chunk_hash = compute_data_hash(b"x");
        let second_chunk_hash = compute_data_hash(b"y");
        let first_xorb_hash = xorb_hash(&[(first_chunk_hash, 1_u64)]);
        let second_xorb_hash = xorb_hash(&[(second_chunk_hash, 1_u64)]);
        let file_hash = file_hash(&[(first_chunk_hash, 1_u64), (second_chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 2_usize, false, false),
                segments: vec![
                    FileDataSequenceEntry::new(first_xorb_hash, 1_u32, 0_u32, 1_u32),
                    FileDataSequenceEntry::new(second_xorb_hash, 1_u32, 0_u32, 1_u32),
                ],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            Vec::new(),
        );

        let result = parse_uploaded_shard(&object_store, &shard, None, limits);

        assert!(matches!(result, Err(ServerError::TooManyShardTerms)));
    }

    fn shard_with_reconstruction_terms(term_count: usize) -> Vec<u8> {
        let bytes = b"x";
        let chunk_hash = compute_data_hash(bytes);
        let xorb_hash = xorb_hash(&[(chunk_hash, 1_u64)]);
        let file_chunks = vec![(chunk_hash, 1_u64); term_count];
        let file_hash = file_hash(&file_chunks);
        let file_segments =
            vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32); term_count];
        serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, file_segments.len(), false, false),
                segments: file_segments,
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u32, 1_u32),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u32, 0_u32)],
            }],
        )
    }

    fn shard_with_xorb_chunks(chunk_count: usize) -> Vec<u8> {
        let chunk_hash = compute_data_hash(b"x");
        let chunk_specs = vec![(chunk_hash, 1_u64); chunk_count];
        let xorb_hash = xorb_hash(&chunk_specs);
        let mut chunks = Vec::with_capacity(chunk_count);
        for chunk_index in 0..chunk_count {
            chunks.push(XorbChunkSequenceEntry::new(
                chunk_hash,
                1_u32,
                u32::try_from(chunk_index).unwrap_or(0),
            ));
        }

        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1_usize, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u32, 0_u32, 1_u32)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, chunks.len(), chunks.len()),
                chunks,
            }],
        )
    }

    fn serialize_test_shard(file_infos: Vec<MDBFileInfo>, xorb_infos: Vec<MDBXorbInfo>) -> Vec<u8> {
        let mut shard = MDBInMemoryShard::default();
        for file_info in file_infos {
            assert!(shard.add_file_reconstruction_info(file_info).is_ok());
        }
        for xorb_info in xorb_infos {
            assert!(shard.add_xorb_block(xorb_info).is_ok());
        }

        let mut serialized = Vec::new();
        assert!(MDBShardInfo::serialize_from(&mut serialized, &shard, None).is_ok());
        serialized
    }
}
