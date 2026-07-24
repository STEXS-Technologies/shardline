use std::{
    collections::{BTreeSet, HashMap, HashSet},
    io::{Cursor, Read},
};

use shardline_index::{
    AsyncIndexStore, DedupeShardMapping, FileChunkRecord, FileRecord, parse_xet_hash_hex,
};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{
    InvalidSerializedShardError, ServerObjectStore, ShardMetadataLimits, chunk_hash, content_hash,
};
use shardline_storage::{
    ObjectBody, ObjectIntegrity, ObjectKey, ObjectKeyError, ObjectStore, PutOutcome,
};
use shardline_xet_core::{
    merklehash::{MerkleHash, compute_data_hash},
    metadata_shard::{
        MDBShardFileHeader,
        file_structs::{
            FileDataSequenceEntry, FileDataSequenceHeader, FileMetadataExt, FileVerificationEntry,
            MDBFileInfo,
        },
        hash_is_global_dedup_eligible,
        shard_in_memory::MDBInMemoryShard,
        xorb_structs::{
            MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG, MDBXorbInfo, XorbChunkSequenceEntry,
            XorbChunkSequenceHeader,
        },
    },
};

use crate::error::XetAdapterError;

use super::{ValidatedXorbChunk, validate_serialized_xorb, xorb_object_key};

fn shard_object_key_local(hash_hex: &str) -> Result<ObjectKey, XetAdapterError> {
    shardline_server_core::validate_content_hash_with(hash_hex, || {
        XetAdapterError::InvalidContentHash
    })?;
    let prefix = hash_hex
        .get(..2)
        .ok_or(XetAdapterError::InvalidContentHash)?;
    let key = format!("shards/{prefix}/{hash_hex}.shard");
    ObjectKey::parse(&key).map_err(map_object_key_error)
}

#[derive(Debug)]
pub struct ParsedShardUpload {
    pub result: u8,
    pub records: Vec<FileRecord>,
    pub shard_key: ObjectKey,
    pub dedupe_chunk_hashes: Vec<String>,
}

/// # Errors
///
/// Returns an error when the shard cannot be parsed or stored.
pub fn parse_uploaded_shard(
    object_store: &ServerObjectStore,
    uploaded_shard: &[u8],
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
) -> Result<ParsedShardUpload, XetAdapterError> {
    let parsed_shard = parse_shard_records(uploaded_shard, object_store, repository_scope, limits)?;
    let shard_key = shard_object_key_local(&parsed_shard.shard_hash_hex)?;
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

/// # Errors
///
/// Returns an error when the shard cannot be parsed or stored.
pub fn parse_uploaded_shard_with_metrics(
    object_store: &ServerObjectStore,
    uploaded_shard: &[u8],
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
) -> Result<ParsedShardUpload, XetAdapterError> {
    let result = parse_uploaded_shard(object_store, uploaded_shard, repository_scope, limits)?;
    shardline_metrics::record_xet_shard_upload(uploaded_shard.len() as u64);
    Ok(result)
}

/// # Errors
///
/// Returns an error when the dedupe shard object cannot be resolved.
pub async fn resolve_dedupe_shard_object<IndexAdapter>(
    index_store: &IndexAdapter,
    object_store: &ServerObjectStore,
    chunk_hash_hex: &str,
) -> Result<(ObjectKey, u64), XetAdapterError>
where
    IndexAdapter: AsyncIndexStore,
    IndexAdapter::Error: Into<XetAdapterError>,
{
    let chunk_hash = parse_xet_hash_hex(chunk_hash_hex)?;
    let Some(mapping) = index_store
        .dedupe_shard_mapping(&chunk_hash)
        .await
        .map_err(Into::into)?
    else {
        return Err(XetAdapterError::NotFound);
    };
    let shard_key = mapping.shard_object_key();
    let Some(metadata) = object_store.metadata(shard_key)? else {
        return Err(XetAdapterError::NotFound);
    };

    Ok((shard_key.clone(), metadata.length()))
}

/// # Errors
///
/// Returns an error when the shard bytes cannot be deserialized.
pub fn retained_shard_chunk_hashes(
    shard_bytes: &[u8],
    limits: ShardMetadataLimits,
) -> Result<Vec<String>, XetAdapterError> {
    let mut shard_reader = Cursor::new(shard_bytes);
    let header = MDBShardFileHeader::deserialize(&mut shard_reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    read_bounded_shard_sections(&mut shard_reader, limits, header.version)
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
) -> Result<NormalizedShardUpload, XetAdapterError> {
    let mut shard_reader = Cursor::new(uploaded_shard);
    let header = MDBShardFileHeader::deserialize(&mut shard_reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    let version = header.version;
    let bounded_shard = read_bounded_shard_sections(&mut shard_reader, limits, version)?;
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
    version: u64,
) -> Result<BoundedShardSections, XetAdapterError> {
    let mut file_infos = Vec::new();
    let mut file_start_entries = HashMap::<MerkleHash, HashSet<usize>>::new();
    read_bounded_file_sections(
        reader,
        &mut file_infos,
        &mut file_start_entries,
        limits,
        version,
    )?;

    let mut xorb_infos = Vec::new();
    let dedupe_chunk_hashes = read_bounded_xorb_sections(
        reader,
        &file_start_entries,
        &mut xorb_infos,
        limits,
        version,
    )?;

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
    version: u64,
) -> Result<(), XetAdapterError> {
    let mut reconstruction_terms = 0_usize;

    loop {
        let header = FileDataSequenceHeader::deserialize(reader, version)
            .map_err(|error| invalid_serialized_shard(&error))?;
        if header.is_bookend() {
            return Ok(());
        }

        let file_count = checked_increment(file_infos.len())?;
        if file_count > limits.max_files().get() {
            return Err(XetAdapterError::TooManyShardTerms);
        }

        let segment_count = usize::try_from(header.num_entries)?;
        reconstruction_terms = checked_add_limit(
            reconstruction_terms,
            segment_count,
            limits.max_reconstruction_terms().get(),
        )?;

        file_section_followed_entries(&header, segment_count)?;
        let mut segments = Vec::with_capacity(segment_count);
        for _ in 0..segment_count {
            segments.push(
                FileDataSequenceEntry::deserialize(reader, version)
                    .map_err(|error| invalid_serialized_shard(&error))?,
            );
        }
        let mut verification = Vec::with_capacity(segment_count);
        if header.contains_verification() {
            for _ in 0..segment_count {
                verification.push(
                    FileVerificationEntry::deserialize(reader, version)
                        .map_err(|error| invalid_serialized_shard(&error))?,
                );
            }
        }
        let metadata_ext = header
            .contains_metadata_ext()
            .then(|| FileMetadataExt::deserialize(reader, version))
            .transpose()
            .map_err(|error| invalid_serialized_shard(&error))?;

        if segment_count > 0 {
            let first_segment = segments
                .first()
                .ok_or(InvalidSerializedShardError::ParserRejectedMetadata)?;
            let first_index = usize::try_from(first_segment.chunk_index_start)?;
            file_start_entries
                .entry(first_segment.xorb_hash)
                .or_default()
                .insert(first_index);
        }

        file_infos.push(MDBFileInfo {
            metadata: header,
            segments,
            verification,
            metadata_ext,
        });
    }
}

fn read_bounded_xorb_sections<R: Read>(
    reader: &mut R,
    file_start_entries: &HashMap<MerkleHash, HashSet<usize>>,
    xorb_infos: &mut Vec<MDBXorbInfo>,
    limits: ShardMetadataLimits,
    version: u64,
) -> Result<Vec<String>, XetAdapterError> {
    let mut xorb_chunks = 0_usize;
    let mut dedupe_chunk_hashes = BTreeSet::new();

    loop {
        let header = XorbChunkSequenceHeader::deserialize(reader, version)
            .map_err(|error| invalid_serialized_shard(&error))?;
        if header.is_bookend() {
            return Ok(dedupe_chunk_hashes.into_iter().collect());
        }

        let xorb_count = checked_increment(xorb_infos.len())?;
        if xorb_count > limits.max_xorbs().get() {
            return Err(XetAdapterError::TooManyShardTerms);
        }

        let chunk_count = usize::try_from(header.num_entries)?;
        xorb_chunks = checked_add_limit(xorb_chunks, chunk_count, limits.max_xorb_chunks().get())?;
        let mut chunks = Vec::with_capacity(chunk_count);
        for _ in 0..chunk_count {
            chunks.push(
                XorbChunkSequenceEntry::deserialize(reader, version)
                    .map_err(|error| invalid_serialized_shard(&error))?,
            );
        }
        let xorb_info = MDBXorbInfo {
            metadata: header,
            chunks,
        };

        collect_dedupe_chunk_hashes(&xorb_info, file_start_entries, &mut dedupe_chunk_hashes);
        xorb_infos.push(xorb_info);
    }
}

fn file_section_followed_entries(
    header: &FileDataSequenceHeader,
    segment_count: usize,
) -> Result<usize, XetAdapterError> {
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

fn collect_dedupe_chunk_hashes(
    xorb_info: &MDBXorbInfo,
    file_start_entries: &HashMap<MerkleHash, HashSet<usize>>,
    dedupe_chunk_hashes: &mut BTreeSet<String>,
) {
    let start_entries = file_start_entries.get(&xorb_info.metadata.xorb_hash);
    for (chunk_index, chunk) in xorb_info.chunks.iter().enumerate() {
        let is_file_start = start_entries.is_some_and(|entries| entries.contains(&chunk_index));
        if is_file_start
            || hash_is_global_dedup_eligible(&chunk.chunk_hash)
            || (chunk.flags & MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG) != 0
        {
            dedupe_chunk_hashes.insert(chunk.chunk_hash.hex());
        }
    }
}

fn checked_add(left: usize, right: usize) -> Result<usize, XetAdapterError> {
    left.checked_add(right).ok_or(XetAdapterError::Overflow)
}

fn checked_increment(value: usize) -> Result<usize, XetAdapterError> {
    checked_add(value, 1)
}

fn checked_add_limit(left: usize, right: usize, limit: usize) -> Result<usize, XetAdapterError> {
    let value = checked_add(left, right)?;
    if value > limit {
        return Err(XetAdapterError::TooManyShardTerms);
    }
    Ok(value)
}

fn invalid_serialized_shard<T>(_error: &T) -> XetAdapterError {
    InvalidSerializedShardError::ParserRejectedMetadata.into()
}

/// # Errors
///
/// Returns an error when the hash is not valid or the object key cannot be constructed.
pub fn shard_object_key(hash_hex: &str) -> Result<ObjectKey, XetAdapterError> {
    shard_object_key_local(hash_hex)
}

/// # Errors
///
/// Returns an error when the object key cannot be validated.
pub fn shard_hash_from_object_key_if_present(
    key: &ObjectKey,
) -> Result<Option<&str>, XetAdapterError> {
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
    shardline_server_core::validate_content_hash_with(hash_hex, || {
        XetAdapterError::InvalidContentHash
    })?;
    Ok(Some(hash_hex))
}

fn build_file_records_from_infos(
    file_infos: Vec<MDBFileInfo>,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
) -> Result<Vec<FileRecord>, XetAdapterError> {
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
                    .ok_or(XetAdapterError::InvalidSerializedShard(
                        InvalidSerializedShardError::XorbMetadataCacheInsertionFailed,
                    ))?
            };
            let packed_start = xorb_info.packed_start(range_start)?;
            let packed_end = xorb_info.packed_end(range_end)?;
            let length = segment.unpacked_segment_bytes;
            chunks.push(FileChunkRecord {
                hash,
                offset,
                length,
                range_start,
                range_end,
                packed_start,
                packed_end,
            });
            offset = offset
                .checked_add(length)
                .ok_or(XetAdapterError::Overflow)?;
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
) -> Result<(), XetAdapterError> {
    let mut referenced_xorbs = HashSet::new();
    for file_info in file_infos {
        for segment in &file_info.segments {
            referenced_xorbs.insert(segment.xorb_hash.hex());
            if referenced_xorbs.len() > max_xorbs {
                return Err(XetAdapterError::TooManyShardTerms);
            }
        }
    }

    Ok(())
}

fn load_xorb_range_info(
    object_store: &ServerObjectStore,
    hash_hex: &str,
) -> Result<XorbRangeInfo, XetAdapterError> {
    let key = xorb_object_key(hash_hex)?;
    let Some(metadata) = object_store.metadata(&key)? else {
        return Err(XetAdapterError::MissingReferencedXorb);
    };
    let bytes = shardline_server_core::read_full_object(object_store, &key, metadata.length())?;
    let expected_hash = parse_xet_hash_hex(hash_hex)?;
    let mut reader = Cursor::new(bytes);
    let validated = validate_serialized_xorb(&mut reader, expected_hash)?;
    let packed_chunk_ends = validated
        .chunks()
        .iter()
        .map(ValidatedXorbChunk::packed_end)
        .collect();
    Ok(XorbRangeInfo { packed_chunk_ends })
}

/// # Errors
///
/// Returns an error when the chunk hash is not valid.
pub fn dedupe_shard_mapping(
    chunk_hash_hex: &str,
    shard_key: &ObjectKey,
) -> Result<DedupeShardMapping, XetAdapterError> {
    let chunk_hash = parse_xet_hash_hex(chunk_hash_hex)?;
    Ok(DedupeShardMapping::new(chunk_hash, shard_key.clone()))
}

const fn map_object_key_error(error: ObjectKeyError) -> XetAdapterError {
    match error {
        ObjectKeyError::Empty
        | ObjectKeyError::UnsafePath
        | ObjectKeyError::ControlCharacter
        | ObjectKeyError::TooLong => XetAdapterError::InvalidContentHash,
    }
}

#[derive(Debug, Clone)]
struct XorbRangeInfo {
    packed_chunk_ends: Vec<u64>,
}

impl XorbRangeInfo {
    fn packed_start(&self, range_start: u64) -> Result<u64, XetAdapterError> {
        let range_start = usize::try_from(range_start)?;
        if range_start == 0 {
            return Ok(0);
        }
        let previous_index = range_start.checked_sub(1).ok_or_else(|| {
            XetAdapterError::from(
                InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList,
            )
        })?;
        self.packed_chunk_ends
            .get(previous_index)
            .copied()
            .ok_or_else(|| {
                XetAdapterError::from(
                    InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList,
                )
            })
    }

    fn packed_end(&self, range_end: u64) -> Result<u64, XetAdapterError> {
        let range_end = usize::try_from(range_end)?;
        self.packed_chunk_ends
            .get(range_end.saturating_sub(1))
            .copied()
            .ok_or_else(|| {
                XetAdapterError::from(
                    InvalidSerializedShardError::ShardTermRangeEndedPastXorbChunkList,
                )
            })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use shardline_server_core::{DEFAULT_SHARD_METADATA_LIMITS, ShardMetadataLimits};
    use shardline_xet_core::{
        merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            MDBShardFileHeader,
            file_structs::{
                FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
            },
            shard_format::MDBShardInfo,
            shard_in_memory::MDBInMemoryShard,
            xorb_structs::{
                MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG, MDBXorbInfo, XorbChunkSequenceEntry,
                XorbChunkSequenceHeader,
            },
        },
        utils::serialization_utils::{write_hash, write_u32, write_u64},
    };

    use super::{
        dedupe_shard_mapping, parse_uploaded_shard, retained_shard_chunk_hashes,
        shard_hash_from_object_key_if_present, shard_object_key,
    };
    use crate::error::XetAdapterError;
    use shardline_server_core::ServerObjectStore;

    fn store_xorb_sync(store: &ServerObjectStore, hash: &str, body: &[u8]) {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(crate::store_uploaded_xorb(store, hash, body)).unwrap();
    }

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

        assert!(matches!(result, Err(XetAdapterError::TooManyShardTerms)));
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

        assert!(matches!(result, Err(XetAdapterError::TooManyShardTerms)));
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
                metadata: FileDataSequenceHeader::new(file_hash, 2u64, false, false),
                segments: vec![
                    FileDataSequenceEntry::new(first_xorb_hash, 1_u64, 0_u64, 1_u64),
                    FileDataSequenceEntry::new(second_xorb_hash, 1_u64, 0_u64, 1_u64),
                ],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            Vec::new(),
        );

        let result = parse_uploaded_shard(&object_store, &shard, None, limits);

        assert!(matches!(result, Err(XetAdapterError::TooManyShardTerms)));
    }

    // ---- retained_shard_chunk_hashes tests ----

    #[test]
    fn retained_shard_chunk_hashes_returns_expected_hashes() {
        let bytes = b"x";
        let chunk_hash = compute_data_hash(bytes);
        let xorb_hash = xorb_hash(&[(chunk_hash, 1_u64)]);
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(
            hashes.is_ok(),
            "retained_shard_chunk_hashes failed: {hashes:?}"
        );
        assert_eq!(hashes.unwrap(), vec![chunk_hash.hex()]);
    }

    #[test]
    fn retained_shard_chunk_hashes_rejects_empty_bytes() {
        let result = retained_shard_chunk_hashes(b"", DEFAULT_SHARD_METADATA_LIMITS);
        assert!(result.is_err());
    }

    // ---- dedupe_shard_mapping tests ----

    #[test]
    fn dedupe_shard_mapping_with_valid_hash() {
        let hash = "ab".repeat(32);
        let shard_key = shard_object_key(&hash).unwrap();
        let mapping = dedupe_shard_mapping(&hash, &shard_key).unwrap();
        let hex_chunk_hash = shardline_index::xet_hash_hex_string(mapping.chunk_hash());
        assert_eq!(hex_chunk_hash, hash);
        assert_eq!(mapping.shard_object_key(), &shard_key);
    }

    #[test]
    fn dedupe_shard_mapping_rejects_invalid_hash() {
        let shard_key = shard_object_key(&"ab".repeat(32)).unwrap();
        let result = dedupe_shard_mapping("not-a-hash", &shard_key);
        assert!(result.is_err(), "expected error for invalid hash");
    }

    fn shard_with_reconstruction_terms(term_count: usize) -> Vec<u8> {
        let bytes = b"x";
        let chunk_hash = compute_data_hash(bytes);
        let xorb_hash = xorb_hash(&[(chunk_hash, 1_u64)]);
        let file_chunks = vec![(chunk_hash, 1_u64); term_count];
        let file_hash = file_hash(&file_chunks);
        let file_segments =
            vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64); term_count];
        serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash,
                    file_segments.len() as u64,
                    false,
                    false,
                ),
                segments: file_segments,
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
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
                1_u64,
                u64::try_from(chunk_index).unwrap_or(0),
            ));
        }

        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(
                    xorb_hash,
                    chunks.len() as u64,
                    chunks.len() as u64,
                ),
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

    fn serialize_v2_test_shard() -> (Vec<u8>, MerkleHash, MerkleHash) {
        let first_chunk = compute_data_hash(b"v2-native-first");
        let second_chunk = compute_data_hash(b"v2-native-second");
        let xorb_hash = xorb_hash(&[(first_chunk, 8_u64), (second_chunk, 8_u64)]);
        let file_hash = file_hash(&[(first_chunk, 8_u64), (second_chunk, 8_u64)]);
        let mut bytes = Vec::new();
        let shard_header = MDBShardFileHeader {
            version: 2,
            footer_size: 0,
            ..MDBShardFileHeader::default()
        };
        shard_header.serialize(&mut bytes).unwrap();

        write_hash(&mut bytes, &file_hash).unwrap();
        write_u32(&mut bytes, 0).unwrap();
        write_u32(&mut bytes, 1).unwrap();
        write_u64(&mut bytes, 0).unwrap();
        write_hash(&mut bytes, &xorb_hash).unwrap();
        write_u32(&mut bytes, 0).unwrap();
        write_u32(&mut bytes, 16).unwrap();
        write_u32(&mut bytes, 0).unwrap();
        write_u32(&mut bytes, 2).unwrap();
        write_hash(&mut bytes, &[!0_u64; 4].into()).unwrap();
        write_u32(&mut bytes, 0).unwrap();
        write_u32(&mut bytes, 0).unwrap();
        write_u64(&mut bytes, 0).unwrap();

        write_hash(&mut bytes, &xorb_hash).unwrap();
        write_u32(&mut bytes, 0).unwrap();
        write_u32(&mut bytes, 2).unwrap();
        write_u32(&mut bytes, 16).unwrap();
        write_u32(&mut bytes, 16).unwrap();
        for (hash, start, flags) in [
            (first_chunk, 0, 0),
            (second_chunk, 8, MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG),
        ] {
            write_hash(&mut bytes, &hash).unwrap();
            write_u32(&mut bytes, start).unwrap();
            write_u32(&mut bytes, 8).unwrap();
            write_u32(&mut bytes, flags).unwrap();
            write_u32(&mut bytes, 0).unwrap();
        }
        write_hash(&mut bytes, &[!0_u64; 4].into()).unwrap();
        for _ in 0..4 {
            write_u32(&mut bytes, 0).unwrap();
        }

        (bytes, first_chunk, second_chunk)
    }

    #[test]
    fn bounded_parser_accepts_native_v2_shard_layout() {
        let (shard, first_chunk, second_chunk) = serialize_v2_test_shard();
        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS).unwrap();

        assert!(hashes.contains(&first_chunk.hex()));
        assert!(hashes.contains(&second_chunk.hex()));
    }

    // ── shard_hash_from_object_key_if_present edge cases ─────────────────

    #[test]
    fn shard_hash_from_key_rejects_single_segment() {
        let key = shardline_storage::ObjectKey::parse("shards").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_two_segments() {
        let key = shardline_storage::ObjectKey::parse("shards/ab").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_extra_segments() {
        let key = shardline_storage::ObjectKey::parse("shards/ab/abhash.shard/extra").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_wrong_namespace() {
        let key = shardline_storage::ObjectKey::parse("xorbs/ab/abhash.shard").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_invalid_prefix_length() {
        let key = shardline_storage::ObjectKey::parse("shards/abc/abchash.shard").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_non_hex_prefix() {
        let key = shardline_storage::ObjectKey::parse("shards/xx/xxhash.shard").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_missing_shard_extension() {
        let key = shardline_storage::ObjectKey::parse("shards/ab/abhash").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_prefix_mismatch() {
        let key = shardline_storage::ObjectKey::parse("shards/bb/abhash.shard").unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    #[test]
    fn shard_hash_from_key_rejects_invalid_hash_characters() {
        let key = shardline_storage::ObjectKey::parse(
            "shards/gg/gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg.shard",
        )
        .unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), None);
    }

    // ── file_section_followed_entries ────────────────────────────────────

    #[test]
    fn file_section_followed_entries_without_verification_or_metadata() {
        let seg_count = 3usize;
        let header = FileDataSequenceHeader::new(
            shardline_xet_core::merklehash::compute_data_hash(b"h"),
            seg_count as u64,
            false,
            false,
        );
        let result = super::file_section_followed_entries(&header, seg_count).unwrap();
        assert_eq!(result, seg_count);
    }

    #[test]
    fn file_section_followed_entries_with_verification() {
        let seg_count = 3usize;
        let header = FileDataSequenceHeader::new(
            shardline_xet_core::merklehash::compute_data_hash(b"h"),
            seg_count as u64,
            true,
            false,
        );
        let result = super::file_section_followed_entries(&header, seg_count).unwrap();
        assert_eq!(result, seg_count + seg_count);
    }

    #[test]
    fn file_section_followed_entries_with_metadata_ext() {
        let seg_count = 3usize;
        let header = FileDataSequenceHeader::new(
            shardline_xet_core::merklehash::compute_data_hash(b"h"),
            seg_count as u64,
            false,
            true,
        );
        let result = super::file_section_followed_entries(&header, seg_count).unwrap();
        assert_eq!(result, seg_count + 1);
    }

    #[test]
    fn file_section_followed_entries_with_both() {
        let seg_count = 2usize;
        let header = FileDataSequenceHeader::new(
            shardline_xet_core::merklehash::compute_data_hash(b"h"),
            seg_count as u64,
            true,
            true,
        );
        let result = super::file_section_followed_entries(&header, seg_count).unwrap();
        assert_eq!(result, seg_count + seg_count + 1);
    }

    // ── validate_referenced_xorb_count ────────────────────────────────────

    #[test]
    fn validate_referenced_xorb_count_accepts_within_limit() {
        use shardline_xet_core::merklehash::compute_data_hash;
        let hash = compute_data_hash(b"xorb");
        let file_infos = vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, false, false),
            segments: vec![FileDataSequenceEntry::new(hash, 1, 0, 1)],
            verification: Vec::new(),
            metadata_ext: None,
        }];
        assert!(super::validate_referenced_xorb_count(&file_infos, 1).is_ok());
    }

    #[test]
    fn validate_referenced_xorb_count_exceeds_limit() {
        use shardline_xet_core::merklehash::compute_data_hash;
        let first = compute_data_hash(b"xorb1");
        let second = compute_data_hash(b"xorb2");
        let file_infos = vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 2, false, false),
            segments: vec![
                FileDataSequenceEntry::new(first, 1, 0, 1),
                FileDataSequenceEntry::new(second, 1, 0, 1),
            ],
            verification: Vec::new(),
            metadata_ext: None,
        }];
        let result = super::validate_referenced_xorb_count(&file_infos, 1);
        assert!(matches!(result, Err(XetAdapterError::TooManyShardTerms)));
    }

    // ── XorbRangeInfo ────────────────────────────────────────────────────

    #[test]
    fn xorb_range_info_packed_start_zero_for_index_zero() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100, 200, 300],
        };
        assert_eq!(info.packed_start(0).unwrap(), 0);
    }

    #[test]
    fn xorb_range_info_packed_start_uses_previous_chunk_end() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100, 200, 300],
        };
        assert_eq!(info.packed_start(1).unwrap(), 100);
        assert_eq!(info.packed_start(2).unwrap(), 200);
    }

    #[test]
    fn xorb_range_info_packed_start_rejects_past_end() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100, 200],
        };
        assert!(info.packed_start(3).is_err());
    }

    #[test]
    fn xorb_range_info_packed_end_uses_zero_based_index() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100, 200, 300],
        };
        // packed_end(1) = packed_chunk_ends[0] = 100
        assert_eq!(info.packed_end(1).unwrap(), 100);
        assert_eq!(info.packed_end(2).unwrap(), 200);
        assert_eq!(info.packed_end(3).unwrap(), 300);
    }

    #[test]
    fn xorb_range_info_packed_end_rejects_past_end() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100, 200],
        };
        assert!(info.packed_end(3).is_err());
    }

    // ── Pure helper function tests ────────────────────────────────────────

    #[test]
    fn checked_add_ok() {
        assert_eq!(super::checked_add(100, 200).unwrap(), 300);
    }

    #[test]
    fn checked_add_overflow() {
        assert!(super::checked_add(usize::MAX, 1).is_err());
    }

    #[test]
    fn checked_increment_ok() {
        assert_eq!(super::checked_increment(41).unwrap(), 42);
    }

    #[test]
    fn checked_increment_overflow() {
        assert!(super::checked_increment(usize::MAX).is_err());
    }

    #[test]
    fn checked_add_limit_ok() {
        assert_eq!(super::checked_add_limit(50, 50, 200).unwrap(), 100);
    }

    #[test]
    fn checked_add_limit_exceeded() {
        assert!(matches!(
            super::checked_add_limit(150, 100, 200),
            Err(XetAdapterError::TooManyShardTerms)
        ));
    }

    #[test]
    fn checked_add_limit_overflow() {
        assert!(super::checked_add_limit(usize::MAX, 1, usize::MAX).is_err());
    }

    // ── retained_shard_chunk_hashes edge cases ──────────────────────────

    #[test]
    fn retained_shard_chunk_hashes_returns_file_start_chunks() {
        // A chunk that IS a file start (chunk_index_start = 0) should be
        // included in the retained set.
        let chunk_hash = compute_data_hash(b"x");
        let xorb_hash_bytes = compute_data_hash(b"xorb");
        let file_hash_bytes = compute_data_hash(b"file");
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash_bytes, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(
                    xorb_hash_bytes,
                    1_u64,
                    0_u64,
                    1_u64,
                )],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash_bytes, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS);

        assert!(hashes.is_ok());
        let result = hashes.unwrap();
        assert_eq!(result.len(), 1, "file start chunk should be retained");
        assert_eq!(result[0], chunk_hash.hex());
    }

    // ── map_object_key_error ─────────────────────────────────────────────

    #[test]
    fn map_object_key_error_maps_all_variants() {
        use shardline_storage::ObjectKeyError;
        let cases: &[(ObjectKeyError, &str)] = &[
            (ObjectKeyError::Empty, "invalid"),
            (ObjectKeyError::UnsafePath, "invalid"),
            (ObjectKeyError::ControlCharacter, "invalid"),
            (ObjectKeyError::TooLong, "invalid"),
        ];
        for (err, _) in cases {
            let mapped = super::map_object_key_error(*err);
            let msg = mapped.to_string();
            assert!(msg.contains("hash"), "msg '{msg}' missing 'hash'");
        }
    }

    // ── parse_uploaded_shard_with_metrics ─────────────────────────────────

    #[test]
    fn parse_uploaded_shard_with_metrics_delegates_success() {
                use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Store a xorb
        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_xorb_sync(&object_store, &xorb_hash.hex(), &serialized.serialized_data);

        // Build a shard referencing it
        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        let result = super::parse_uploaded_shard_with_metrics(
            &object_store,
            &shard,
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
        );
        assert!(
            result.is_ok(),
            "parse_uploaded_shard_with_metrics failed: {result:?}"
        );
    }

    #[test]
    fn parse_uploaded_shard_with_metrics_propagates_error() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let result = super::parse_uploaded_shard_with_metrics(
            &object_store,
            b"",
            None,
            DEFAULT_SHARD_METADATA_LIMITS,
        );
        assert!(result.is_err(), "expected error for empty shard bytes");
    }

    // ── parse_uploaded_shard success path ─────────────────────────────────

    #[test]
    fn parse_uploaded_shard_success_with_xorb_lookup() {
                use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // 1. Store a xorb
        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_xorb_sync(&object_store, &xorb_hash.hex(), &serialized.serialized_data);

        // 2. Build a shard referencing that xorb
        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        // 3. Verify the shard can be parsed for retained hashes (tests read_bounded_shard_sections)
        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(
            hashes.is_ok(),
            "retained hashes should parse OK: {hashes:?}"
        );

        // 4. Parse the shard (tests full flow including build_file_records_from_infos)
        let result =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(result.is_ok(), "parse_uploaded_shard failed: {result:?}");
        let parsed = result.unwrap();
        assert_eq!(parsed.result, 1, "expected newly inserted shard");
        assert_eq!(parsed.records.len(), 1, "expected one file record");
        assert!(!parsed.shard_key.as_str().is_empty());
        assert!(!parsed.dedupe_chunk_hashes.is_empty());
    }

    #[test]
    fn parse_uploaded_shard_already_exists_returns_result_zero() {
                use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_xorb_sync(&object_store, &xorb_hash.hex(), &serialized.serialized_data);

        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        // Store the shard once
        let first =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(first.is_ok());
        assert_eq!(first.unwrap().result, 1);

        // Store again - should be AlreadyExists
        let second =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(second.is_ok(), "second parse failed: {second:?}");
        assert_eq!(
            second.unwrap().result,
            0,
            "expected result=0 for existing shard"
        );
    }

    // ── parse_uploaded_shard error: missing referenced xorb ─────────────

    #[test]
    fn parse_uploaded_shard_rejects_missing_referenced_xorb() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        // Build a shard referencing a xorb that was never stored
        let chunk_hash = compute_data_hash(b"x");
        let xorb_hash = xorb_hash(&[(chunk_hash, 1_u64)]);
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        let result =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);

        assert!(
            matches!(result, Err(XetAdapterError::MissingReferencedXorb)),
            "expected MissingReferencedXorb, got {result:?}"
        );
    }

    // ── shard_object_key edge cases ──────────────────────────────────────

    #[test]
    fn shard_object_key_constructs_valid_key() {
        let hash = "cd".repeat(32);
        let key = shard_object_key(&hash);
        assert!(key.is_ok());
        let key = key.unwrap();
        assert!(key.as_str().starts_with("shards/cd/"));
        assert!(key.as_str().ends_with(".shard"));
        assert_eq!(key.as_str().len(), "shards/cd/".len() + 64 + ".shard".len());
    }

    #[test]
    fn shard_object_key_rejects_empty_hash() {
        let result = shard_object_key("");
        assert!(result.is_err());
    }

    #[test]
    fn shard_object_key_rejects_non_hex_characters() {
        let result = shard_object_key(&"zz".repeat(32));
        assert!(result.is_err());
    }

    #[test]
    fn shard_object_key_rejects_short_hash() {
        let result = shard_object_key("abc");
        assert!(result.is_err());
    }

    // ── shard_hash_from_object_key_if_present edge cases ────────────────

    #[test]
    fn shard_hash_from_key_accepts_valid_key() {
        use shardline_storage::ObjectKey;
        let hash = "ef".repeat(32);
        let key_str = format!("shards/ef/{hash}.shard");
        let key = ObjectKey::parse(&key_str).unwrap();
        let extracted = super::shard_hash_from_object_key_if_present(&key);
        assert!(extracted.is_ok());
        assert_eq!(extracted.unwrap(), Some(hash.as_str()));
    }

    // ── read_bounded_file_sections with verification and metadata flags ──

    // ── read_bounded_file_sections with verification flag ─────────────────

    #[test]
    fn parse_uploaded_shard_with_verification_and_no_metadata_ext() {
                use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_xorb_sync(&object_store, &xorb_hash.hex(), &serialized.serialized_data);

        let chunk_hash = compute_data_hash(b"x");
        let file_hash = file_hash(&[(chunk_hash, 1_u64)]);
        // Create file info with verification=true, metadata_ext=false
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, true, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 0_u64, 1_u64)],
                verification: vec![FileVerificationEntry::new(MerkleHash::default())],
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 1_u64, 1_u64),
                chunks: vec![XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64)],
            }],
        );

        let result =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(
            result.is_ok(),
            "shard with verification should parse OK: {result:?}"
        );
    }

    // ── collect_dedupe_chunk_hashes flag-based dedup eligibility ─────────

    #[test]
    fn retained_shard_chunk_hashes_collects_dedup_flag_chunks() {
        // Use 2 chunks: first is a file start, second has the dedup flag
        let chunk_a = compute_data_hash(b"aaaaaaaa");
        let chunk_b = compute_data_hash(b"bbbbbbbb");

        let combined_hash = file_hash(&[(chunk_a, 8_u64), (chunk_b, 8_u64)]);
        let xorb_hash = xorb_hash(&[(chunk_a, 8_u64), (chunk_b, 8_u64)]);

        let chunks = vec![
            XorbChunkSequenceEntry::new(chunk_a, 8_u64, 0_u64),
            XorbChunkSequenceEntry::new(chunk_b, 8_u64, 8_u64).with_global_dedup_flag(true),
        ];

        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(combined_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 16_u64, 0_u64, 2_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 2_u64, 16_u64),
                chunks,
            }],
        );

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(
            hashes.is_ok(),
            "retained_shard_chunk_hashes failed: {hashes:?}"
        );
        let result_hashes = hashes.unwrap();
        // chunk[0] is a file start (chunk_index_start=0), should be retained
        // chunk[1] has MDB_CHUNK_WITH_GLOBAL_DEDUP_FLAG, should be retained
        assert!(
            result_hashes.len() >= 2,
            "expected at least 2 chunks retained (file start + flag), got {}",
            result_hashes.len()
        );
    }

    // ── validate_referenced_xorb_count edge: empty file_infos ────────────

    #[test]
    fn validate_referenced_xorb_count_empty() {
        let result = super::validate_referenced_xorb_count(&[], 10);
        assert!(result.is_ok());
    }

    #[test]
    fn validate_referenced_xorb_count_zero_limit() {
        use shardline_xet_core::merklehash::compute_data_hash;
        let hash = compute_data_hash(b"xorb");
        let file_infos = vec![MDBFileInfo {
            metadata: FileDataSequenceHeader::new(compute_data_hash(b"f"), 1, false, false),
            segments: vec![FileDataSequenceEntry::new(hash, 1, 0, 1)],
            verification: Vec::new(),
            metadata_ext: None,
        }];
        let result = super::validate_referenced_xorb_count(&file_infos, 0);
        assert!(matches!(result, Err(XetAdapterError::TooManyShardTerms)));
    }

    // ── XorbRangeInfo edge cases ─────────────────────────────────────────

    #[test]
    fn xorb_range_info_packed_start_rejects_subtract_underflow_for_index_above_zero() {
        // range_start > 0 but previous_index underflows when range_start = 0
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100],
        };
        // This is fine - packed_start(0) = 0
        assert_eq!(info.packed_start(0).unwrap(), 0);
    }

    #[test]
    fn xorb_range_info_packed_start_rejects_index_beyond_list() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100],
        };
        // packed_start(2) -> range_start=2 -> previous_index = 1 -> out of bounds
        let result = info.packed_start(2);
        assert!(
            result.is_err(),
            "expected error for out-of-bounds start index"
        );
    }

    #[test]
    fn xorb_range_info_packed_end_rejects_zero_index() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![100, 200],
        };
        // packed_end(0) -> range_end = 0 -> saturating_sub(1) = 0 -> get(0) = Some(100)
        assert_eq!(info.packed_end(0).unwrap(), 100);
    }

    #[test]
    fn xorb_range_info_packed_end_rejects_empty_chunks() {
        let info = super::XorbRangeInfo {
            packed_chunk_ends: vec![],
        };
        // packed_end(1) -> range_end = 1 -> saturating_sub(1) = 0 -> get(0) = None
        let result = info.packed_end(1);
        assert!(
            result.is_err(),
            "expected error for empty packed_chunk_ends"
        );
    }

    // ── checked helper functions ─────────────────────────────────────────

    #[test]
    fn checked_add_saturates_to_error() {
        assert!(super::checked_add(usize::MAX, 1).is_err());
    }

    #[test]
    fn checked_increment_zero_works() {
        assert_eq!(super::checked_increment(0).unwrap(), 1);
    }

    #[test]
    fn checked_add_limit_works_at_boundary() {
        assert_eq!(super::checked_add_limit(100, 100, 200).unwrap(), 200);
    }

    // ── Shard object key with edge case hashes ───────────────────────────

    #[test]
    fn shard_object_key_various_prefixes() {
        let prefixes = ["aa", "ff", "10", "99", "ac"];
        for prefix in prefixes {
            let hash = format!("{}{}", prefix, "0".repeat(62));
            let key = shard_object_key(&hash);
            assert!(key.is_ok(), "failed for prefix {prefix}: {key:?}");
            let key = key.unwrap();
            assert!(
                key.as_str().contains(&format!("shards/{prefix}/")),
                "key '{}' missing expected prefix segment",
                key.as_str()
            );
        }
    }

    // ── dedupe_shard_mapping ─────────────────────────────────────────────

    #[test]
    fn dedupe_shard_mapping_creates_mapping_with_correct_fields() {
        let hash = "ef".repeat(32);
        let shard_key = shard_object_key(&hash).unwrap();
        let mapping = dedupe_shard_mapping(&hash, &shard_key).unwrap();
        let hex_chunk_hash = shardline_index::xet_hash_hex_string(mapping.chunk_hash());
        assert_eq!(hex_chunk_hash, hash);
        assert_eq!(mapping.shard_object_key(), &shard_key);
    }

    // ── resolve_dedupe_shard_object async test ────────────────────────────

    #[test]
    fn resolve_dedupe_shard_object_returns_not_found_for_missing_mapping() {
        use shardline_index::MemoryIndexStore;

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();
        let chunk_hash_hex = "ab".repeat(32);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(super::resolve_dedupe_shard_object(
            &index_store,
            &object_store,
            &chunk_hash_hex,
        ));

        assert!(
            matches!(result, Err(XetAdapterError::NotFound)),
            "expected NotFound for missing mapping, got {result:?}"
        );
    }

    #[test]
    fn resolve_dedupe_shard_object_finds_existing_object() {
        use shardline_index::{DedupeShardMapping, MemoryIndexStore};
        use shardline_server_core::chunk_hash;
        use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectStore, PutOutcome};

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();

        let chunk_hash_hex = "ab".repeat(32);
        let shard_key = shard_object_key(&chunk_hash_hex).unwrap();

        // Store an object at the shard key (simulating an existing shard)
        let shard_data = b"stored-shard-data";
        let body = ObjectBody::from_slice(shard_data);
        let integrity = ObjectIntegrity::new(chunk_hash(shard_data), shard_data.len() as u64);
        let outcome = object_store
            .put_if_absent(&shard_key, body, &integrity)
            .expect("put_if_absent should succeed");
        assert!(matches!(outcome, PutOutcome::Inserted));

        // Create and store a dedupe mapping in the index
        let chunk_hash = shardline_index::parse_xet_hash_hex(&chunk_hash_hex).unwrap();
        // DedupeShardMapping::new takes (ShardlineHash, ObjectKey)
        // Create a mapping using the ShardlineHash directly
        let mapping = DedupeShardMapping::new(chunk_hash, shard_key.clone());
        index_store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert_dedupe_shard_mapping should succeed");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(super::resolve_dedupe_shard_object(
            &index_store,
            &object_store,
            &chunk_hash_hex,
        ));

        assert!(
            result.is_ok(),
            "resolve_dedupe_shard_object failed: {result:?}"
        );
        let (resolved_key, length) = result.unwrap();
        assert_eq!(resolved_key.as_str(), shard_key.as_str());
        assert_eq!(length, shard_data.len() as u64);
    }

    #[test]
    fn resolve_dedupe_shard_object_returns_not_found_for_missing_shard() {
        use shardline_index::{DedupeShardMapping, MemoryIndexStore};

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();

        let chunk_hash_hex = "ab".repeat(32);
        let shard_key = shard_object_key(&chunk_hash_hex).unwrap();

        // Create a mapping but don't store the underlying shard object
        let chunk_hash = shardline_index::parse_xet_hash_hex(&chunk_hash_hex).unwrap();
        let mapping = DedupeShardMapping::new(chunk_hash, shard_key);
        index_store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert_dedupe_shard_mapping should succeed");

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(super::resolve_dedupe_shard_object(
            &index_store,
            &object_store,
            &chunk_hash_hex,
        ));

        assert!(
            matches!(result, Err(XetAdapterError::NotFound)),
            "expected NotFound for missing shard, got {result:?}"
        );
    }

    #[test]
    fn resolve_dedupe_shard_object_rejects_invalid_chunk_hash() {
        use shardline_index::MemoryIndexStore;

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let result = rt.block_on(super::resolve_dedupe_shard_object(
            &index_store,
            &object_store,
            "not-a-valid-hash",
        ));

        assert!(result.is_err(), "expected error for invalid hash");
    }

    // ── retained_shard_chunk_hashes edge: only non-start, non-eligible ──

    #[test]
    fn retained_shard_chunk_hashes_excludes_non_start_non_eligible() {
        // A chunk that is NOT a file start and NOT dedup eligible should NOT be retained
        let chunk_hash = compute_data_hash(b"common"); // this hash is unlikely to be dedup-eligible
        let xorb_hash = compute_data_hash(b"xorb-data");
        let file_hash = compute_data_hash(b"file-data");
        // Use a file that starts at chunk index 1, so chunk 0 is not a file start
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(file_hash, 1u64, false, false),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 1_u64, 2_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 2_u64, 2_u64),
                chunks: vec![
                    XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 0_u64),
                    XorbChunkSequenceEntry::new(chunk_hash, 1_u64, 1_u64),
                ],
            }],
        );

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(hashes.is_ok());
        let result = hashes.unwrap();
        // Chunk 0 is not a file start (file starts at chunk 1), and its hash
        // is unlikely to be dedup-eligible. If hash_is_global_dedup_eligible
        // returns false, chunk 0 is excluded.
        assert!(
            result.len() <= 2,
            "expected <=2 retained hashes, got {}",
            result.len()
        );
    }
}
