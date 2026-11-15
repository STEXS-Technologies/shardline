use std::{
    collections::{BTreeSet, HashMap, HashSet, hash_map::Entry},
    fs::File,
    io::{BufReader, Cursor, Read, Seek},
    path::Path,
};

use shardline_index::{
    AsyncIndexStore, DedupeShardMapping, FileChunkRecord, FileRecord, XetFileIdentity,
    parse_xet_hash_hex,
};
use shardline_protocol::RepositoryScope;
use shardline_server_core::{
    InvalidSerializedShardError, ServerObjectStore, ShardMetadataLimits, chunk_hash, content_hash,
};
use shardline_storage::{ObjectBody, ObjectIntegrity, ObjectKey, ObjectStore, PutOutcome};
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

#[cfg(test)]
use super::validate_serialized_xorb;
use super::{ValidatedXorb, ValidatedXorbChunk, xorb_object_key};

use crate::checkpoint_reader;
mod dedup;
mod file_digests;
#[cfg(test)]
mod file_digests_tests;
mod streaming_source;
mod verification;
pub use dedup::{
    dedup_xorb_candidates, recover_dedup_xorb_descriptor,
    recover_dedup_xorb_descriptor_from_candidates, recover_xorb_descriptor,
};

fn shard_object_key_local(hash_hex: &str) -> Result<ObjectKey, XetAdapterError> {
    shardline_server_core::validate_content_hash_with(hash_hex, || {
        XetAdapterError::InvalidContentHash
    })?;
    let prefix = hash_hex
        .get(..2)
        .ok_or(XetAdapterError::InvalidContentHash)?;
    let key = format!("shards/{prefix}/{hash_hex}.shard");
    ObjectKey::parse(&key).map_err(XetAdapterError::from)
}

#[derive(Debug)]
pub struct ParsedShardUpload {
    pub result: u8,
    pub records: Vec<FileRecord>,
    pub file_identities: Vec<XetFileIdentity>,
    pub shard_key: ObjectKey,
    pub dedupe_chunk_hashes: Vec<String>,
}

impl ParsedShardUpload {
    pub(crate) async fn register<Commit, CommitFuture>(
        self,
        commit: Commit,
    ) -> Result<crate::ShardUploadResponse, XetAdapterError>
    where
        Commit:
            FnOnce(Vec<FileRecord>, Vec<DedupeShardMapping>, Vec<XetFileIdentity>) -> CommitFuture,
        CommitFuture: std::future::Future<Output = Result<(), XetAdapterError>>,
    {
        let mappings = self
            .dedupe_chunk_hashes
            .iter()
            .map(|hash| dedupe_shard_mapping(hash, &self.shard_key))
            .collect::<Result<Vec<_>, _>>()?;
        // Observe cancellation queued during synchronous verification before
        // invoking publication, even when the callback completes immediately.
        tokio::task::yield_now().await;
        commit(self.records, mappings, self.file_identities).await?;
        Ok(crate::ShardUploadResponse {
            result: self.result,
        })
    }
}

/// Fully verified shard metadata which has not published any object or record.
/// This owned value can cross a blocking-worker boundary; dropping it publishes
/// nothing. Publication must remain with the request's current owner.
#[derive(Debug)]
pub struct PreparedShardUpload {
    objects: ServerObjectStore,
    normalized: NormalizedShardUpload,
    references: Vec<ObjectKey>,
    descriptors: Vec<shardline_index::XetXorbDescriptor>,
}

/// Performs bounded, read-only verification of a native shard and its sources.
///
/// # Errors
/// Returns an error for malformed metadata, missing/corrupt sources or limits.
pub fn prepare_uploaded_shard<R: Read + Seek>(
    reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
) -> Result<PreparedShardUpload, XetAdapterError> {
    prepare_uploaded_shard_with_checkpoint(reader, object_store, repository_scope, limits, &|| {
        Ok(())
    })
}

/// Read-only preparation with a request-lifetime check between verification units.
/// The callback must return an interrupted I/O error after request cancellation.
/// S3 materialization checks between response frames; an already-waiting read
/// retains the storage backend's timeout. Local reads retain the source inode.
///
/// # Errors
/// Returns verification, storage, limit or checkpoint errors without publication.
pub fn prepare_uploaded_shard_with_checkpoint<R: Read + Seek>(
    reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
) -> Result<PreparedShardUpload, XetAdapterError> {
    prepare_shard(
        reader,
        object_store,
        repository_scope,
        limits,
        checkpoint,
        false,
    )
}

/// Overlap SHA-256 with source authentication using an independently admitted
/// CPU worker. The caller must supply and retain its admission token; this
/// function releases it only after all digest workers have stopped.
///
/// # Errors
/// Returns verification, storage, limit or checkpoint errors without publication.
pub fn prepare_uploaded_shard_with_digest_worker<R: Read + Seek, P: Send>(
    reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    permit: P,
) -> Result<PreparedShardUpload, XetAdapterError> {
    let _permit = permit;
    prepare_shard(
        reader,
        object_store,
        repository_scope,
        limits,
        checkpoint,
        true,
    )
}

fn prepare_shard<R: Read + Seek>(
    reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    parallel_digest: bool,
) -> Result<PreparedShardUpload, XetAdapterError> {
    let mut references = HashSet::new();
    let mut descriptors = Vec::new();
    let preparation = parse_shard_records_with_checkpoint(
        &mut checkpoint_reader::CheckpointReader { reader, checkpoint },
        object_store,
        repository_scope,
        limits,
        Some(&mut references),
        Some(&mut descriptors),
        VerificationWork {
            checkpoint,
            parallel_digest,
        },
    );
    checkpoint()?;
    let normalized = preparation?;
    let mut references: Vec<_> = references.into_iter().collect();
    references.sort_unstable_by(|left, right| left.as_str().cmp(right.as_str()));
    Ok(PreparedShardUpload {
        objects: object_store.clone(),
        normalized,
        references,
        descriptors,
    })
}

impl PreparedShardUpload {
    /// Persists the immutable shard and atomically publishes metadata through
    /// the caller's callback. Verification workers must never call this method.
    ///
    /// # Errors
    /// Returns storage, mapping or metadata-publication failure.
    pub async fn register<Commit, CommitFuture>(
        self,
        commit: Commit,
    ) -> Result<crate::ShardUploadResponse, XetAdapterError>
    where
        Commit: FnOnce(
            Vec<FileRecord>,
            Vec<DedupeShardMapping>,
            Vec<ObjectKey>,
            Vec<XetFileIdentity>,
            Vec<shardline_index::XetXorbDescriptor>,
        ) -> CommitFuture,
        CommitFuture: std::future::Future<Output = Result<(), XetAdapterError>>,
    {
        let parsed = persist_normalized_shard(&self.objects, self.normalized)?;
        parsed
            .register(|records, mappings, identities| {
                commit(
                    records,
                    mappings,
                    self.references,
                    identities,
                    self.descriptors,
                )
            })
            .await
    }
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
    let mut reader = Cursor::new(uploaded_shard);
    parse_uploaded_shard_from_reader(&mut reader, object_store, repository_scope, limits, None)
}

/// Parses a shard from a seekable file without copying the uploaded bytes into
/// the process heap. Only the bounded metadata sections and normalized shard
/// representation are retained.
///
/// # Errors
///
/// Returns an error when the file cannot be opened or the shard is invalid.
pub fn parse_uploaded_shard_file(
    object_store: &ServerObjectStore,
    path: &Path,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
) -> Result<ParsedShardUpload, XetAdapterError> {
    let mut file = File::open(path).map_err(XetAdapterError::Io)?;
    parse_uploaded_shard_from_reader(&mut file, object_store, repository_scope, limits, None)
}

pub(crate) fn parse_uploaded_shard_from_reader<R: Read + Seek>(
    reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    member_references: Option<&mut HashSet<ObjectKey>>,
) -> Result<ParsedShardUpload, XetAdapterError> {
    parse_uploaded_shard_with_descriptors(
        reader,
        object_store,
        repository_scope,
        limits,
        member_references,
        None,
    )
}

pub(crate) fn parse_uploaded_shard_with_descriptors<R: Read + Seek>(
    reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    member_references: Option<&mut HashSet<ObjectKey>>,
    descriptors: Option<&mut Vec<shardline_index::XetXorbDescriptor>>,
) -> Result<ParsedShardUpload, XetAdapterError> {
    let parsed_shard = parse_shard_records_from_reader(
        reader,
        object_store,
        repository_scope,
        limits,
        member_references,
        descriptors,
    )?;
    persist_normalized_shard(object_store, parsed_shard)
}

fn persist_normalized_shard(
    object_store: &ServerObjectStore,
    parsed_shard: NormalizedShardUpload,
) -> Result<ParsedShardUpload, XetAdapterError> {
    let shard_key = shard_object_key_local(&parsed_shard.shard_hash_hex)?;
    let shard_length = u64::try_from(parsed_shard.normalized_bytes.len())?;
    let integrity = ObjectIntegrity::new(chunk_hash(&parsed_shard.normalized_bytes), shard_length);
    let stored = object_store.put_if_absent(
        &shard_key,
        ObjectBody::from_slice(&parsed_shard.normalized_bytes),
        &integrity,
    );
    let stored = stored?;

    Ok(ParsedShardUpload {
        // The conditional write already verifies an existing object's bytes.
        // Its outcome, rather than an earlier existence observation, decides
        // whether this request inserted the shard.
        result: match stored {
            PutOutcome::AlreadyExists => 0,
            PutOutcome::Inserted => 1,
        },
        records: parsed_shard.records,
        file_identities: parsed_shard.file_identities,
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
    retained_shard_chunk_hashes_from_reader(&mut shard_reader, limits)
}

/// Reads retained chunk metadata from a seekable shard without requiring the
/// caller to first materialize the entire shard in memory.
///
/// # Errors
///
/// Returns an error when seeking, deserialization, or bounded section parsing fails.
pub fn retained_shard_chunk_hashes_from_reader<R: Read + std::io::Seek>(
    mut shard_reader: &mut R,
    limits: ShardMetadataLimits,
) -> Result<Vec<String>, XetAdapterError> {
    shard_reader
        .seek(std::io::SeekFrom::Start(0))
        .map_err(XetAdapterError::Io)?;
    let header = MDBShardFileHeader::deserialize(&mut shard_reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    read_bounded_shard_sections(&mut shard_reader, limits, header.version)
        .map(|bounded_shard| bounded_shard.dedupe_chunk_hashes)
}

#[derive(Debug)]
struct NormalizedShardUpload {
    shard_hash_hex: String,
    normalized_bytes: Vec<u8>,
    records: Vec<FileRecord>,
    file_identities: Vec<XetFileIdentity>,
    dedupe_chunk_hashes: Vec<String>,
}

fn parse_shard_records_from_reader<R: Read + Seek>(
    shard_reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    member_references: Option<&mut HashSet<ObjectKey>>,
    descriptors: Option<&mut Vec<shardline_index::XetXorbDescriptor>>,
) -> Result<NormalizedShardUpload, XetAdapterError> {
    parse_shard_records_with_checkpoint(
        shard_reader,
        object_store,
        repository_scope,
        limits,
        member_references,
        descriptors,
        VerificationWork {
            checkpoint: &|| Ok(()),
            parallel_digest: false,
        },
    )
}

#[derive(Clone, Copy)]
struct VerificationWork<'work> {
    checkpoint: &'work (dyn Fn() -> std::io::Result<()> + Sync),
    parallel_digest: bool,
}

fn parse_shard_records_with_checkpoint<R: Read + Seek>(
    mut shard_reader: &mut R,
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    limits: ShardMetadataLimits,
    member_references: Option<&mut HashSet<ObjectKey>>,
    descriptors: Option<&mut Vec<shardline_index::XetXorbDescriptor>>,
    work: VerificationWork<'_>,
) -> Result<NormalizedShardUpload, XetAdapterError> {
    let checkpoint = work.checkpoint;
    checkpoint()?;
    shard_reader
        .seek(std::io::SeekFrom::Start(0))
        .map_err(XetAdapterError::Io)?;
    let header = MDBShardFileHeader::deserialize(&mut shard_reader)
        .map_err(|error| invalid_serialized_shard(&error))?;
    let version = header.version;
    let bounded_shard = read_bounded_shard_sections(&mut shard_reader, limits, version)?;
    validate_referenced_xorb_count(&bounded_shard.file_infos, limits.max_xorbs().get())?;
    let mut xorb_cache = HashMap::new();
    let mut file_digests = file_digests::FileDigestVerifier::default();
    file_digests.parallel_digest = work.parallel_digest;
    let mut member_references = member_references;

    // Visit sources in logical file order so validation and SHA-256 derivation
    // share the same bounded materialization. Verify all client CAS metadata,
    // including unreferenced blocks, before producing a publishable result.
    let (records, file_identities) = build_file_records_with_cache(
        &bounded_shard.file_infos,
        object_store,
        repository_scope,
        member_references.as_deref_mut(),
        &mut xorb_cache,
        &mut file_digests,
        checkpoint,
    )?;
    drop(file_digests);

    let mut in_memory_shard = MDBInMemoryShard::default();
    for file_info in bounded_shard.file_infos {
        checkpoint()?;
        in_memory_shard
            .add_file_reconstruction_info(file_info)
            .map_err(|error| invalid_serialized_shard(&error))?;
    }
    for xorb_info in bounded_shard.xorb_infos {
        checkpoint()?;
        let verified = match xorb_cache.entry(xorb_info.metadata.xorb_hash) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                let (verified, _source) = load_verified_xorb_with_checkpoint(
                    object_store,
                    &entry.key().hex(),
                    member_references.as_deref_mut(),
                    checkpoint,
                )?;
                entry.insert(verified)
            }
        };
        checkpoint()?;
        verification::verify_xorb_members(&xorb_info, verified.chunks())?;
        in_memory_shard
            .add_xorb_block(xorb_info)
            .map_err(|error| invalid_serialized_shard(&error))?;
    }

    let normalized_bytes = in_memory_shard
        .to_bytes()
        .map_err(|error| invalid_serialized_shard(&error))?;
    let shard_hash_hex = compute_data_hash(&normalized_bytes).hex();
    if let Some(descriptors) = descriptors {
        for info in xorb_cache.values() {
            checkpoint()?;
            // The derived standard-protocol lookup has its own memory ceiling.
            // Do not tighten historical ingestion acceptance as a side effect
            // of installing an optional acceleration index.
            if info.chunks().len() > shardline_index::MAX_XET_DESCRIPTOR_CHUNKS {
                continue;
            }
            descriptors.push(info.descriptor());
        }
        descriptors.sort_unstable_by(|left, right| left.hash.cmp(&right.hash));
    }

    Ok(NormalizedShardUpload {
        shard_hash_hex,
        normalized_bytes,
        records,
        file_identities,
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

        checked_add_limit(file_infos.len(), 1, limits.max_files().get())?;

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
        let mut verification = Vec::new();
        if header.contains_verification() {
            verification.reserve_exact(segment_count);
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

        checked_add_limit(xorb_infos.len(), 1, limits.max_xorbs().get())?;

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

fn build_file_records_with_cache(
    file_infos: &[MDBFileInfo],
    object_store: &ServerObjectStore,
    repository_scope: Option<&RepositoryScope>,
    mut member_references: Option<&mut HashSet<ObjectKey>>,
    xorb_cache: &mut HashMap<MerkleHash, ValidatedXorb>,
    file_digests: &mut file_digests::FileDigestVerifier,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
) -> Result<(Vec<FileRecord>, Vec<XetFileIdentity>), XetAdapterError> {
    let mut records = Vec::with_capacity(file_infos.len());
    let mut file_identities = Vec::new();

    for file_info in file_infos {
        checkpoint()?;
        let file_id = file_info.metadata.file_hash.hex();
        let expected_sha256 = file_info
            .metadata_ext
            .as_ref()
            .map(|metadata| metadata.sha256.hex());
        let mut digest = file_digests::FileDigest::with_scope(expected_sha256, repository_scope);
        digest.enable_parallel(
            file_digests.parallel_digest
                && file_info.segments.iter().any(|term| {
                    term.unpacked_segment_bytes >= file_digests::MIN_PARALLEL_SOURCE_BYTES
                }),
        );
        let mut offset = 0_u64;
        let mut chunks = Vec::with_capacity(file_info.segments.len());
        let mut digested_through = 0;

        for (segment_index, segment) in file_info.segments.iter().enumerate() {
            checkpoint()?;
            let hash = segment.xorb_hash.hex();
            let range_start = segment.chunk_index_start;
            let range_end = segment.chunk_index_end;
            if range_end <= range_start {
                return Err(
                    InvalidSerializedShardError::ShardFileTermEmptyOrInvertedChunkRange.into(),
                );
            }

            let xorb_info = match xorb_cache.entry(segment.xorb_hash) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    // Drop the previous spool before materializing another source.
                    file_digests.release_source();
                    let ordered = file_digests::ordered_source_terms(
                        file_info
                            .segments
                            .get(segment_index..)
                            .ok_or(XetAdapterError::Overflow)?,
                    )?;
                    let mut remaining = ordered;
                    digest.begin_terms(ordered);
                    let (loaded, source) = load_verified_xorb_with_visitor(
                        object_store,
                        &hash,
                        member_references.as_deref_mut(),
                        checkpoint,
                        &mut |member, data| {
                            checkpoint()?;
                            digest.update_validating_terms(&mut remaining, member, data);
                            Ok(())
                        },
                    )?;
                    digest.finish_terms();
                    digested_through = segment_index
                        .checked_add(ordered.len())
                        .ok_or(XetAdapterError::Overflow)?;
                    file_digests.retain_verified_source(&hash, source);
                    entry.insert(loaded)
                }
            };
            let packed_start = xorb_info.packed_start(range_start)?;
            let packed_end = xorb_info.packed_end(range_end)?;
            verification::verify_term_length(segment, xorb_info.chunks())?;
            if segment_index >= digested_through {
                let ordered = file_digests::ordered_source_terms(
                    file_info
                        .segments
                        .get(segment_index..)
                        .ok_or(XetAdapterError::Overflow)?,
                )?;
                digest.update_cached_terms(
                    file_digests,
                    object_store,
                    ordered,
                    xorb_info,
                    checkpoint,
                )?;
                digested_through = segment_index
                    .checked_add(ordered.len())
                    .ok_or(XetAdapterError::Overflow)?;
            }
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

        // The file hash is also a proof of knowledge of the selected member
        // hashes. Never publish an arbitrary claimed identity for guessed ranges
        // of a shared Xorb. All ranges were checked above; iterate the verified
        // cache without allocating a vector proportional to logical file size.
        let members = file_info
            .segments
            .iter()
            .flat_map(|segment| {
                xorb_cache
                    .get(&segment.xorb_hash)
                    .into_iter()
                    .flat_map(move |info| {
                        info.chunks()
                            .iter()
                            .skip(usize::try_from(segment.chunk_index_start).unwrap_or(usize::MAX))
                            .take(
                                usize::try_from(
                                    segment
                                        .chunk_index_end
                                        .saturating_sub(segment.chunk_index_start),
                                )
                                .unwrap_or(0),
                            )
                    })
            })
            .map(|chunk| {
                (
                    MerkleHash::from(*chunk.hash().as_bytes()),
                    chunk.unpacked_len(),
                )
            });
        verification::verify_file_hash(file_info.metadata.file_hash, members, checkpoint)?;

        let sha256 = digest.finish()?;
        records.push(FileRecord {
            deduplication_domain: None,
            file_id: file_id.clone(),
            content_hash: content_hash(offset, 0, &chunks),
            total_bytes: offset,
            chunk_size: 0,
            storage_repr: shardline_index::StorageRepresentation::WholeFileV1,
            repository_scope: repository_scope.cloned(),
            chunks,
        });
        if let (Some(repository_scope), Some(sha256)) = (repository_scope, sha256) {
            shardline_server_core::validate_content_hash_with(&sha256, || {
                XetAdapterError::InvalidContentHash
            })?;
            file_identities.push(XetFileIdentity {
                repository_scope: repository_scope.clone(),
                sha256,
                file_id,
                total_bytes: offset,
            });
        }
    }

    Ok((records, file_identities))
}

fn validate_referenced_xorb_count(
    file_infos: &[MDBFileInfo],
    max_xorbs: usize,
) -> Result<(), XetAdapterError> {
    let mut referenced_xorbs = HashSet::new();
    for file_info in file_infos {
        for segment in &file_info.segments {
            referenced_xorbs.insert(segment.xorb_hash);
            if referenced_xorbs.len() > max_xorbs {
                return Err(XetAdapterError::TooManyShardTerms);
            }
        }
    }

    Ok(())
}

fn load_verified_xorb(
    object_store: &ServerObjectStore,
    hash_hex: &str,
) -> Result<ValidatedXorb, XetAdapterError> {
    load_verified_xorb_with_checkpoint(object_store, hash_hex, None, &|| Ok(()))
        .map(|(info, _source)| info)
}

fn load_verified_xorb_with_checkpoint(
    object_store: &ServerObjectStore,
    hash_hex: &str,
    member_references: Option<&mut HashSet<ObjectKey>>,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
) -> Result<
    (
        ValidatedXorb,
        Option<shardline_server_core::MaterializedObject>,
    ),
    XetAdapterError,
> {
    load_verified_xorb_with_visitor(
        object_store,
        hash_hex,
        member_references,
        checkpoint,
        &mut |_, _| Ok(()),
    )
}

fn load_verified_xorb_with_visitor(
    object_store: &ServerObjectStore,
    hash_hex: &str,
    member_references: Option<&mut HashSet<ObjectKey>>,
    checkpoint: &(dyn Fn() -> std::io::Result<()> + Sync),
    visitor: &mut (dyn FnMut(u64, &[u8]) -> std::io::Result<()> + Send),
) -> Result<
    (
        ValidatedXorb,
        Option<shardline_server_core::MaterializedObject>,
    ),
    XetAdapterError,
> {
    checkpoint()?;
    let key = xorb_object_key(hash_hex)?;
    let (validated, source) = if let ServerObjectStore::S3(store) = object_store {
        (
            streaming_source::validate(
                store,
                &key,
                parse_xet_hash_hex(hash_hex)?,
                checkpoint,
                visitor,
            )?,
            None,
        )
    } else {
        let source =
            object_store.materialize_bounded_object_if_present_with_checkpoint(&key, 0, checkpoint);
        checkpoint()?;
        let Some((mut reader, _length)) = source? else {
            tracing::warn!(xorb_hash = hash_hex, "shard referenced a missing Xorb");
            return Err(XetAdapterError::MissingReferencedXorb);
        };
        let expected_hash = parse_xet_hash_hex(hash_hex)?;
        let validation = {
            // Footer fields and member headers are small reads. Buffer only this
            // validation pass, within its admitted worker, and discard read-ahead
            // before retaining the source for subsequent member authentication.
            let mut buffered = BufReader::with_capacity(64 * 1024, &mut reader);
            crate::xorb::validate_serialized_xorb_with_data_and_visitor(
                &mut checkpoint_reader::CheckpointReader {
                    reader: &mut buffered,
                    checkpoint,
                },
                expected_hash,
                0,
                Vec::new(),
                visitor,
            )
        };
        checkpoint()?;
        let (validated, _decoded) = validation?;
        (validated, Some(reader))
    };
    if let Some(references) = member_references {
        for chunk in validated.chunks() {
            let hash = shardline_index::xet_hash_hex_string(chunk.hash());
            references.insert(shardline_server_core::chunk_object_key(&hash)?);
        }
    }
    Ok((validated, source))
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

impl ValidatedXorb {
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
        self.chunks()
            .get(previous_index)
            .map(ValidatedXorbChunk::packed_end)
            .ok_or_else(|| {
                XetAdapterError::from(
                    InvalidSerializedShardError::ShardTermRangeStartedPastXorbChunkList,
                )
            })
    }

    fn packed_end(&self, range_end: u64) -> Result<u64, XetAdapterError> {
        let range_end = usize::try_from(range_end)?;
        self.chunks()
            .get(range_end.saturating_sub(1))
            .map(ValidatedXorbChunk::packed_end)
            .ok_or_else(|| {
                XetAdapterError::from(
                    InvalidSerializedShardError::ShardTermRangeEndedPastXorbChunkList,
                )
            })
    }
}

#[cfg(test)]
mod empty_files;
#[cfg(test)]
pub(crate) mod test_support;

#[cfg(test)]
mod tests {
    use super::test_support::{serialize_test_shard, single_chunk_shard};
    use std::num::NonZeroUsize;

    use shardline_server_core::{DEFAULT_SHARD_METADATA_LIMITS, ShardMetadataLimits};
    use shardline_xet_core::{
        merklehash::{MerkleHash, compute_data_hash, file_hash, xorb_hash},
        metadata_shard::{
            MDBShardFileHeader,
            file_structs::{
                FileDataSequenceEntry, FileDataSequenceHeader, FileVerificationEntry, MDBFileInfo,
            },
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
        rt.block_on(crate::store_uploaded_xorb(store, hash, body))
            .unwrap();
    }

    #[test]
    fn member_reference_collection_and_cached_terms_reuse_s3_reads() {
        use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };
        use std::io::{Read, Write};
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        use std::time::{Duration, Instant};

        let serialized = SerializedXorbObject::from_xorb_with_compression(
            build_raw_xorb(2, ChunkSize::Fixed(256)),
            CompressionScheme::None,
            true,
        )
        .unwrap();
        let hash = serialized.hash;
        let mut outcomes = Vec::new();
        for (collect_members, verify_sha256) in
            [(false, false), (true, false), (false, true), (true, true)]
        {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            listener.set_nonblocking(true).unwrap();
            let stop = Arc::new(AtomicBool::new(false));
            let server_stop = Arc::clone(&stop);
            let body = serialized.serialized_data.clone();
            let path = format!(
                "/assets/xorbs/default/{}/{}.xorb",
                hash.hex().get(..2).unwrap(),
                hash.hex()
            );
            let server = std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut heads = 0;
                let mut gets = 0;
                while !server_stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                    let mut stream = match listener.accept() {
                        Ok((stream, _)) => stream,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            std::thread::sleep(Duration::from_millis(2));
                            continue;
                        }
                        Err(error) => return Err(error),
                    };
                    stream
                        .set_read_timeout(Some(Duration::from_secs(1)))
                        .unwrap();
                    let mut request = Vec::new();
                    let mut buffer = [0_u8; 1024];
                    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                        let count = stream.read(&mut buffer).unwrap();
                        assert!(count > 0);
                        request.extend_from_slice(buffer.get(..count).unwrap());
                    }
                    let request = String::from_utf8(request).unwrap();
                    let get = request.starts_with(&format!("GET {path} "));
                    assert!(
                        get || request.starts_with(&format!("HEAD {path} ")),
                        "unexpected object read"
                    );
                    if get {
                        gets += 1;
                    } else {
                        heads += 1;
                    }
                    let range = request.lines().find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("range: bytes=")
                            .map(str::to_owned)
                    });
                    let (start, end) = range
                        .as_ref()
                        .map(|range| {
                            let (start, end) = range.split_once('-').unwrap();
                            (
                                start.parse::<usize>().unwrap(),
                                end.parse::<usize>().unwrap(),
                            )
                        })
                        .unwrap_or_else(|| (0, body.len().saturating_sub(1)));
                    let selected = body.get(start..=end).unwrap();
                    if get && range.is_some() {
                        write!(stream, "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Mon, 12 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n", body.len(), selected.len()).unwrap();
                    } else {
                        write!(stream, "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Mon, 12 Oct 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n", body.len()).unwrap();
                    }
                    if get {
                        stream.write_all(selected).unwrap();
                    }
                }
                Ok((heads, gets))
            });
            let objects = ServerObjectStore::s3(
                shardline_storage::S3ObjectStoreConfig::new(
                    "assets".to_owned(),
                    "us-east-1".to_owned(),
                )
                .with_endpoint(Some(format!("http://{address}")))
                .with_allow_http(true)
                .with_credentials(
                    Some(shardline_protocol::SecretString::from_secret(
                        "fixture-access",
                    )),
                    Some(shardline_protocol::SecretString::from_secret(
                        "fixture-secret",
                    )),
                    None,
                ),
            )
            .unwrap();
            let info = MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash(&[(compute_data_hash(&[0_u8; 256]), 256_u64); 4]),
                    3_u64,
                    false,
                    verify_sha256,
                ),
                segments: vec![
                    FileDataSequenceEntry::new(hash, 512, 0, 2),
                    FileDataSequenceEntry::new(hash, 256, 0, 1),
                    FileDataSequenceEntry::new(hash, 256, 1, 2),
                ],
                verification: Vec::new(),
                metadata_ext: if verify_sha256 {
                    use sha2::{Digest, Sha256};
                    Some(super::FileMetadataExt::new(
                        MerkleHash::from_hex(&format!("{:x}", Sha256::digest([0_u8; 1024])))
                            .unwrap(),
                    ))
                } else {
                    None
                },
            };
            let mut references = std::collections::HashSet::new();
            let shard = serialize_test_shard(
                vec![info],
                vec![MDBXorbInfo {
                    metadata: XorbChunkSequenceHeader::new(hash, 2, 512),
                    chunks: vec![
                        XorbChunkSequenceEntry::new(compute_data_hash(&[0_u8; 256]), 256, 0),
                        XorbChunkSequenceEntry::new(compute_data_hash(&[0_u8; 256]), 256, 256),
                    ],
                }],
            );
            let mut descriptors = Vec::new();
            let parsed = super::parse_shard_records_from_reader(
                &mut std::io::Cursor::new(shard),
                &objects,
                None,
                DEFAULT_SHARD_METADATA_LIMITS,
                if collect_members {
                    Some(&mut references)
                } else {
                    None
                },
                Some(&mut descriptors),
            )
            .unwrap();
            assert!(parsed.file_identities.is_empty());
            assert_eq!(descriptors.len(), 1);
            assert_eq!(
                descriptors.first().unwrap().total_bytes,
                u64::try_from(serialized.serialized_data.len()).unwrap()
            );
            assert!(descriptors.first().unwrap().has_valid_structure());
            stop.store(true, Ordering::Relaxed);
            let reads = server.join().unwrap().unwrap();
            assert_eq!(
                reads.1,
                1_usize.saturating_add(usize::from(verify_sha256)),
                "one full validation plus one combined authenticated range for adjacent cached terms"
            );
            if collect_members {
                assert_eq!(references.len(), 1);
            } else {
                assert!(references.is_empty());
            }
            outcomes.push((parsed.records, reads));
        }
        for pair in outcomes.as_chunks::<2>().0 {
            assert_eq!(
                pair.first(),
                pair.last(),
                "collecting references must preserve records and S3 read counts"
            );
        }
    }

    #[test]
    fn shard_object_key_preserves_native_layout_and_round_trips() {
        let hashes = ["ab", "cd", "ef"]
            .map(|pair| pair.repeat(32))
            .into_iter()
            .chain(
                ["aa", "ff", "10", "99", "ac"].map(|prefix| format!("{prefix}{}", "0".repeat(62))),
            );
        for hash in hashes {
            let key = shard_object_key(&hash).unwrap();
            assert_eq!(key.as_str(), format!("shards/{}/{hash}.shard", &hash[..2]));
            assert_eq!(
                shard_hash_from_object_key_if_present(&key).unwrap(),
                Some(hash.as_str()),
                "round trip for {hash}",
            );
        }
    }

    #[test]
    fn shard_object_key_rejects_invalid_hashes() {
        for hash in [
            "asset.bin".to_owned(),
            "A".repeat(64),
            String::new(),
            "zz".repeat(32),
            "abc".to_owned(),
        ] {
            assert!(
                matches!(
                    shard_object_key(&hash),
                    Err(XetAdapterError::InvalidContentHash)
                ),
                "invalid hash {hash:?}",
            );
        }
    }

    #[test]
    fn shard_hash_discovery_ignores_unrelated_object_keys() {
        for path in [
            "shards",
            "shards/ab",
            "shards/ab/abhash.shard/extra",
            "xorbs/ab/abhash.shard",
            "shards/abc/abchash.shard",
            "shards/xx/xxhash.shard",
            "shards/ab/abhash",
            "shards/bb/abhash.shard",
            "shards/gg/gggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggggg.shard",
        ] {
            let key = shardline_storage::ObjectKey::parse(path).unwrap();
            assert_eq!(
                shard_hash_from_object_key_if_present(&key).unwrap(),
                None,
                "unrelated key {path}",
            );
        }
    }

    #[test]
    fn shard_upload_rejects_excessive_reconstruction_terms_before_xorb_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

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
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let shard =
            shard_with_xorb_chunks(DEFAULT_SHARD_METADATA_LIMITS.max_xorb_chunks().get() + 1);
        let result =
            parse_uploaded_shard(&object_store, &shard, None, DEFAULT_SHARD_METADATA_LIMITS);

        assert!(matches!(result, Err(XetAdapterError::TooManyShardTerms)));
    }

    #[test]
    fn shard_upload_rejects_excessive_unique_referenced_xorbs_before_lookup() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();

        let max_one = NonZeroUsize::new(1).unwrap();
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
    fn retained_shard_chunk_hashes_returns_file_start_chunks() {
        let chunk_hash = compute_data_hash(b"x");
        for (xorb_hash, file_hash) in [
            (
                xorb_hash(&[(chunk_hash, 1_u64)]),
                file_hash(&[(chunk_hash, 1_u64)]),
            ),
            (compute_data_hash(b"xorb"), compute_data_hash(b"file")),
        ] {
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
            let hashes =
                retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS).unwrap();
            assert_eq!(hashes, vec![chunk_hash.hex()]);

            let mut reader = std::io::Cursor::new(&shard);
            reader.set_position(1);
            assert_eq!(
                super::retained_shard_chunk_hashes_from_reader(
                    &mut reader,
                    DEFAULT_SHARD_METADATA_LIMITS
                )
                .unwrap(),
                hashes
            );
        }
    }

    #[test]
    fn retained_shard_chunk_hashes_rejects_empty_bytes() {
        let result = retained_shard_chunk_hashes(b"", DEFAULT_SHARD_METADATA_LIMITS);
        assert!(result.is_err());
    }

    // ---- dedupe_shard_mapping tests ----

    #[test]
    fn dedupe_shard_mapping_preserves_chunk_hash_and_shard_key() {
        for (chunk, shard) in [("ab", "ab"), ("ef", "ef"), ("ab", "ef")] {
            let hash = chunk.repeat(32);
            let shard_key = shard_object_key(&shard.repeat(32)).unwrap();
            let mapping = dedupe_shard_mapping(&hash, &shard_key).unwrap();
            assert_eq!(
                shardline_index::xet_hash_hex_string(mapping.chunk_hash()),
                hash
            );
            assert_eq!(mapping.shard_object_key(), &shard_key);
        }
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

    #[test]
    fn file_section_entry_counts_include_verification_and_metadata() {
        for (segments, verification, metadata, expected) in [
            (3, false, false, 3),
            (3, true, false, 6),
            (3, false, true, 4),
            (2, true, true, 5),
        ] {
            let header = FileDataSequenceHeader::new(
                compute_data_hash(b"h"),
                segments as u64,
                verification,
                metadata,
            );
            assert_eq!(
                super::file_section_followed_entries(&header, segments).unwrap(),
                expected,
                "segments={segments}, verification={verification}, metadata={metadata}",
            );
        }
    }

    #[test]
    fn referenced_xorb_limit_counts_distinct_containers() {
        use shardline_xet_core::merklehash::compute_data_hash;

        for (names, limit, allowed) in [
            (vec!["xorb"], 1, true),
            (vec!["xorb1", "xorb2"], 1, false),
            (vec!["xorb"], 0, false),
            (vec!["xorb", "xorb"], 1, true),
        ] {
            let file_infos = [MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    compute_data_hash(b"f"),
                    names.len() as u64,
                    false,
                    false,
                ),
                segments: names
                    .iter()
                    .map(|name| {
                        FileDataSequenceEntry::new(compute_data_hash(name.as_bytes()), 1, 0, 1)
                    })
                    .collect(),
                verification: Vec::new(),
                metadata_ext: None,
            }];
            let result = super::validate_referenced_xorb_count(&file_infos, limit);
            if allowed {
                assert!(
                    result.is_ok(),
                    "containers={names:?}, limit={limit}: {result:?}"
                );
            } else {
                assert!(
                    matches!(result, Err(XetAdapterError::TooManyShardTerms)),
                    "containers={names:?}, limit={limit}: {result:?}"
                );
            }
        }
    }

    // ── ValidatedXorb ────────────────────────────────────────────────────

    fn range_info(ends: &[u64]) -> super::ValidatedXorb {
        let mut start = 0;
        let verified_chunks = ends
            .iter()
            .map(|&end| {
                let chunk = super::ValidatedXorbChunk::new(
                    shardline_protocol::ShardlineHash::from_bytes([0; 32]),
                    start,
                    end,
                    start,
                    end,
                );
                start = end;
                chunk
            })
            .collect();
        let length = ends.last().copied().unwrap_or(0);
        super::ValidatedXorb::new(
            shardline_protocol::ShardlineHash::from_bytes([0; 32]),
            length,
            length,
            length,
            verified_chunks,
        )
    }

    #[test]
    fn xorb_range_start_maps_chunk_boundaries_and_rejects_missing_entries() {
        let cases: &[(&[u64], u64, Option<u64>)] = &[
            (&[100, 200, 300], 0, Some(0)),
            (&[100, 200, 300], 1, Some(100)),
            (&[100, 200, 300], 2, Some(200)),
            (&[100, 200], 3, None),
            (&[100], 0, Some(0)),
            (&[100], 2, None),
        ];
        for &(ends, index, expected) in cases {
            assert_eq!(
                range_info(ends).packed_start(index).ok(),
                expected,
                "ends={ends:?}, start={index}",
            );
        }
    }

    #[test]
    fn xorb_range_end_preserves_zero_index_compatibility_and_bounds() {
        let cases: &[(&[u64], u64, Option<u64>)] = &[
            (&[100, 200, 300], 1, Some(100)),
            (&[100, 200, 300], 2, Some(200)),
            (&[100, 200, 300], 3, Some(300)),
            (&[100, 200], 3, None),
            (&[100, 200], 0, Some(100)),
            (&[], 1, None),
        ];
        for &(ends, index, expected) in cases {
            assert_eq!(
                range_info(ends).packed_end(index).ok(),
                expected,
                "ends={ends:?}, end={index}",
            );
        }
    }

    #[test]
    fn checked_add_preserves_values_and_reports_overflow() {
        assert_eq!(super::checked_add(100, 200).unwrap(), 300);
        assert!(matches!(
            super::checked_add(usize::MAX, 1),
            Err(XetAdapterError::Overflow),
        ));
    }

    #[test]
    fn checked_add_limit_distinguishes_boundary_limit_and_overflow() {
        for (left, right, limit, expected) in [
            (0, 1, 1, 1),
            (41, 1, 42, 42),
            (50, 50, 200, 100),
            (100, 100, 200, 200),
        ] {
            assert_eq!(
                super::checked_add_limit(left, right, limit).unwrap(),
                expected
            );
        }
        assert!(matches!(
            super::checked_add_limit(150, 100, 200),
            Err(XetAdapterError::TooManyShardTerms),
        ));
        assert!(matches!(
            super::checked_add_limit(usize::MAX, 1, usize::MAX),
            Err(XetAdapterError::Overflow),
        ));
    }

    // ── parse_uploaded_shard_with_metrics ─────────────────────────────────

    fn stored_single_chunk_shard(
        object_store: &ServerObjectStore,
        verification: Vec<FileVerificationEntry>,
    ) -> Vec<u8> {
        use shardline_xet_core::xorb_object::{
            CompressionScheme, SerializedXorbObject,
            xorb_format_test_utils::{ChunkSize, build_raw_xorb},
        };

        let raw = build_raw_xorb(1, ChunkSize::Fixed(256));
        let serialized =
            SerializedXorbObject::from_xorb_with_compression(raw, CompressionScheme::None, true)
                .unwrap();
        let xorb_hash = serialized.hash;
        store_xorb_sync(object_store, &xorb_hash.hex(), &serialized.serialized_data);

        single_chunk_shard(xorb_hash, verification)
    }

    #[test]
    fn parse_uploaded_shard_with_metrics_delegates_success() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let shard = stored_single_chunk_shard(&object_store, Vec::new());

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
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let shard = stored_single_chunk_shard(&object_store, Vec::new());

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS);
        assert!(
            hashes.is_ok(),
            "retained hashes should parse OK: {hashes:?}"
        );

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
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let shard = stored_single_chunk_shard(&object_store, Vec::new());

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

    // ── read_bounded_file_sections with verification flag ─────────────────

    #[test]
    fn parse_uploaded_shard_with_verification_and_no_metadata_ext() {
        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let shard = stored_single_chunk_shard(
            &object_store,
            vec![FileVerificationEntry::new(MerkleHash::default())],
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
        assert!(!super::hash_is_global_dedup_eligible(&chunk_b));

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

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS).unwrap();
        let mut expected = vec![chunk_a.hex(), chunk_b.hex()];
        expected.sort();
        assert_eq!(hashes, expected);
    }

    // ── validate_referenced_xorb_count edge: empty file_infos ────────────

    #[test]
    fn validate_referenced_xorb_count_empty() {
        let result = super::validate_referenced_xorb_count(&[], 10);
        assert!(result.is_ok());
    }

    // ── resolve_dedupe_shard_object async test ────────────────────────────

    #[tokio::test]
    async fn resolve_dedupe_shard_object_returns_not_found_for_missing_mapping() {
        use shardline_index::MemoryIndexStore;

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();
        let chunk_hash_hex = "ab".repeat(32);

        let result =
            super::resolve_dedupe_shard_object(&index_store, &object_store, &chunk_hash_hex).await;

        assert!(
            matches!(result, Err(XetAdapterError::NotFound)),
            "expected NotFound for missing mapping, got {result:?}"
        );
    }

    #[tokio::test]
    async fn resolve_dedupe_shard_object_finds_existing_object() {
        use shardline_index::MemoryIndexStore;
        use shardline_server_core::chunk_hash;
        use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, PutOutcome};

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();

        let chunk_hash_hex = "ab".repeat(32);
        let shard_key = shard_object_key(&chunk_hash_hex).unwrap();

        // Store an object at the shard key (simulating an existing shard)
        let shard_data = b"stored-shard-data";
        let body = ObjectBody::from_slice(shard_data);
        let integrity = ObjectIntegrity::new(chunk_hash(shard_data), shard_data.len() as u64);
        let outcome = AsyncObjectStore::put_if_absent(&object_store, &shard_key, body, &integrity)
            .await
            .expect("put_if_absent should succeed");
        assert!(matches!(outcome, PutOutcome::Inserted));

        let mapping = dedupe_shard_mapping(&chunk_hash_hex, &shard_key).unwrap();
        index_store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert_dedupe_shard_mapping should succeed");

        let (resolved_key, length) =
            super::resolve_dedupe_shard_object(&index_store, &object_store, &chunk_hash_hex)
                .await
                .expect("stored shard mapping resolves");
        assert_eq!(resolved_key.as_str(), shard_key.as_str());
        assert_eq!(length, shard_data.len() as u64);
    }

    #[tokio::test]
    async fn resolve_dedupe_shard_object_returns_not_found_for_missing_shard() {
        use shardline_index::MemoryIndexStore;

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();

        let chunk_hash_hex = "ab".repeat(32);
        let shard_key = shard_object_key(&chunk_hash_hex).unwrap();

        // Create a mapping but don't store the underlying shard object
        let mapping = dedupe_shard_mapping(&chunk_hash_hex, &shard_key).unwrap();
        index_store
            .upsert_dedupe_shard_mapping(&mapping)
            .expect("upsert_dedupe_shard_mapping should succeed");

        let result =
            super::resolve_dedupe_shard_object(&index_store, &object_store, &chunk_hash_hex).await;

        assert!(
            matches!(result, Err(XetAdapterError::NotFound)),
            "expected NotFound for missing shard, got {result:?}"
        );
    }

    #[tokio::test]
    async fn resolve_dedupe_shard_object_rejects_invalid_chunk_hash() {
        use shardline_index::MemoryIndexStore;

        let temp = tempfile::tempdir().unwrap();
        let object_store = ServerObjectStore::local(temp.path().join("objects")).unwrap();
        let index_store = MemoryIndexStore::new();

        let result =
            super::resolve_dedupe_shard_object(&index_store, &object_store, "not-a-valid-hash")
                .await;

        assert!(result.is_err(), "expected error for invalid hash");
    }

    // ── retained_shard_chunk_hashes edge: only non-start, non-eligible ──

    #[test]
    fn retained_shard_chunk_hashes_excludes_non_start_non_eligible() {
        let excluded = compute_data_hash(b"common");
        let retained = compute_data_hash(b"file-start");
        assert!(!super::hash_is_global_dedup_eligible(&excluded));
        assert_ne!(excluded, retained);
        let xorb_hash = xorb_hash(&[(excluded, 1_u64), (retained, 1_u64)]);
        let shard = serialize_test_shard(
            vec![MDBFileInfo {
                metadata: FileDataSequenceHeader::new(
                    file_hash(&[(retained, 1_u64)]),
                    1u64,
                    false,
                    false,
                ),
                segments: vec![FileDataSequenceEntry::new(xorb_hash, 1_u64, 1_u64, 2_u64)],
                verification: Vec::new(),
                metadata_ext: None,
            }],
            vec![MDBXorbInfo {
                metadata: XorbChunkSequenceHeader::new(xorb_hash, 2_u64, 2_u64),
                chunks: vec![
                    XorbChunkSequenceEntry::new(excluded, 1_u64, 0_u64),
                    XorbChunkSequenceEntry::new(retained, 1_u64, 1_u64),
                ],
            }],
        );

        let hashes = retained_shard_chunk_hashes(&shard, DEFAULT_SHARD_METADATA_LIMITS).unwrap();
        assert_eq!(hashes, vec![retained.hex()]);
    }
}
