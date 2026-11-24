//! Hub ref outbox and immutable archive consumer.
//!
//! This slice covers Hub ref batches only. It does not certify R1 history or
//! enable protected acknowledgments; the required immutable version manifests
//! and isolated restore are separate gates.

use crate::hub::{HubRefChange, canonical_ref_name};
use async_trait::async_trait;
use ed25519_dalek::{SigningKey, VerifyingKey};
use shardline_protocol::ByteRange;
use shardline_reliability::{
    HubRefBatchArchiveAnchor, HubRefBatchArchiveCheckpointPin, HubRefBatchArchiveEvent,
    HubRefBatchArchiveRecordV1, HubRefBatchHead, HubRefBatchManifestReferenceV1,
    HubRefBatchSnapshot, ReliabilityError, SnapshotEvidenceEvent,
};
use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey};
use std::num::NonZeroUsize;
use thiserror::Error;

pub(crate) const MAX_HUB_REF_EVENT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_PENDING_REPOSITORY_PAGE: usize = 256;

pub(crate) fn stable_id_segment(id: &str) -> String {
    ObjectIntegrity::for_bytes(id.as_bytes())
        .hash()
        .hex_string()
}

pub(crate) fn hub_ref_transaction_id(repository_id: &str, sequence: u64) -> String {
    format!("hub-ref:{}:{sequence}", stable_id_segment(repository_id))
}

fn hub_ref_archive_key(
    deployment_id: &str,
    repository_id: &str,
    sequence: u64,
    checksum: &str,
) -> Result<ObjectKey, HubRecoveryError> {
    Ok(ObjectKey::parse(&format!(
        "recovery/v1/{}/hub/{}/{sequence:020}-{checksum}.json",
        stable_id_segment(deployment_id),
        stable_id_segment(repository_id),
    ))?)
}

/// The Hub-only recovery mode currently supported by the schema.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum HubRecoveryMode {
    /// No outbox events are emitted. This is the migration default.
    #[default]
    Off,
    /// Append committed ref batches to SQL and archive asynchronously.
    Shadow,
}

impl HubRecoveryMode {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
        }
    }
}

pub(crate) fn validate_mode_transition(
    current_mode: &str,
    current_deployment_id: Option<&str>,
    has_history: bool,
    requested_mode: HubRecoveryMode,
    requested_deployment_id: Option<&str>,
) -> Result<(), HubRecoveryError> {
    match (
        current_mode,
        current_deployment_id,
        requested_mode,
        requested_deployment_id,
    ) {
        ("off", _, HubRecoveryMode::Off, None) => Ok(()),
        ("off", _, HubRecoveryMode::Shadow, Some(_)) if !has_history => Ok(()),
        ("off", _, HubRecoveryMode::Shadow, Some(_)) => Err(HubRecoveryError::Outbox(
            "cannot resume Hub SHADOW after an OFF interval with prior outbox history".into(),
        )),
        ("shadow", Some(current), HubRecoveryMode::Shadow, Some(requested))
            if current == requested =>
        {
            Ok(())
        }
        ("shadow", Some(_), HubRecoveryMode::Off, None) => Ok(()),
        ("shadow", _, HubRecoveryMode::Shadow, Some(_)) => Err(HubRecoveryError::Outbox(
            "cannot change Hub recovery deployment identity after SHADOW has started".into(),
        )),
        _ => Err(HubRecoveryError::Outbox(
            "stored Hub recovery mode is invalid".into(),
        )),
    }
}

pub(crate) fn validate_requested_mode(
    mode: HubRecoveryMode,
    deployment_id: Option<&str>,
) -> Result<Option<String>, HubRecoveryError> {
    match (mode, deployment_id) {
        (HubRecoveryMode::Off, None) => Ok(None),
        (HubRecoveryMode::Shadow, Some(value))
            if !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control) =>
        {
            Ok(Some(value.to_owned()))
        }
        _ => Err(HubRecoveryError::Outbox(
            "shadow mode requires a valid stable deployment ID; OFF takes no ID".into(),
        )),
    }
}

/// One typed batch read from the committed SQL outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubRecoveryOutboxItem {
    pub repository_id: String,
    pub domain_sequence: u64,
    pub transaction_id: String,
    /// Receipt checksum of the immediately preceding committed event, if any.
    pub previous_archive_digest_blake3: Option<String>,
    pub event: HubRefBatchArchiveEvent,
}

impl HubRecoveryOutboxItem {
    pub(crate) fn verify_identity(&self) -> Result<(), HubRecoveryError> {
        if self.repository_id != self.event.after.repository_id
            || self.domain_sequence != self.event.sequence
            || self.transaction_id != self.event.operation.operation_id
        {
            return Err(HubRecoveryError::OutboxEventMismatch);
        }
        if (self.domain_sequence == 1) != self.previous_archive_digest_blake3.is_none()
            || self
                .previous_archive_digest_blake3
                .as_ref()
                .is_some_and(|digest| {
                    digest.len() != 64
                        || !digest
                            .bytes()
                            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                })
        {
            return Err(HubRecoveryError::SequenceGap);
        }
        Ok(())
    }
}

/// Errors from the outbox, archive object store, or authenticated record reader.
#[derive(Debug, Error)]
pub enum HubRecoveryError {
    #[error("Hub recovery outbox database error: {0}")]
    Outbox(String),
    #[error("Hub recovery object store error: {0}")]
    Storage(String),
    #[error("Hub recovery archive record failed validation: {0}")]
    Evidence(#[from] ReliabilityError),
    #[error("Hub recovery archive record encoding failed: {0}")]
    Encoding(#[from] serde_json::Error),
    #[error("Hub recovery archive key was invalid: {0}")]
    Key(#[from] shardline_storage::ObjectKeyError),
    #[error("Hub recovery archive object is missing")]
    MissingObject,
    #[error("Hub recovery archive object length or digest did not match")]
    IntegrityMismatch,
    #[error("Hub recovery archive event identity did not match its SQL outbox row")]
    OutboxEventMismatch,
    #[error("Hub recovery archive has a missing predecessor")]
    SequenceGap,
    #[error("Hub recovery archive record exceeds its size bound")]
    RecordTooLarge,
    #[error("Hub pending repository page limit exceeds its fixed bound")]
    PageLimitExceeded,
}

/// Adapter boundary for the existing SQL outbox. Each call owns a short SQL
/// transaction; implementations must not hold SQL locks during archive I/O.
#[async_trait]
pub trait HubRecoveryOutboxStore: Send + Sync {
    /// Reads authoritative mode and deployment identity without modifying config.
    async fn hub_recovery_mode(
        &self,
    ) -> Result<(HubRecoveryMode, Option<String>), HubRecoveryError> {
        Err(HubRecoveryError::Outbox(
            "recovery mode read is unsupported".into(),
        ))
    }

    /// Returns an ordered, bounded keyset page of repositories with pending events.
    async fn pending_hub_ref_repositories_after(
        &self,
        cursor: Option<&str>,
        limit: NonZeroUsize,
    ) -> Result<Vec<String>, HubRecoveryError> {
        let _ = (cursor, limit);
        Err(HubRecoveryError::Outbox(
            "pending repository paging is unsupported".into(),
        ))
    }
    /// Enables/disables the Hub shadow outbox. SHADOW requires a stable external
    /// deployment identity; OFF clears it and emits no events.
    ///
    /// # Errors
    /// Returns a database or configuration error.
    async fn set_hub_recovery_mode(
        &self,
        mode: HubRecoveryMode,
        deployment_id: Option<&str>,
    ) -> Result<(), HubRecoveryError>;

    /// Reads the next contiguous unarchived event for one repository.
    ///
    /// # Errors
    /// Returns a database or sequence-gap error.
    async fn next_pending_hub_ref_event(
        &self,
        repository_id: &str,
    ) -> Result<Option<HubRecoveryOutboxItem>, HubRecoveryError>;

    /// Marks one event durable after the external object has been re-read and
    /// authenticated. Must only advance the domain high water mark by one.
    ///
    /// # Errors
    /// Returns a database error or a stale/noncontiguous row error.
    async fn mark_hub_ref_event_archived(
        &self,
        item: &HubRecoveryOutboxItem,
        archive: &HubRecoveryArchiveObject,
    ) -> Result<(), HubRecoveryError>;
}

/// Builds one canonical signed-evidence input for a complete SQL batch.
/// Both database adapters use this helper; only their transaction SQL differs.
pub(crate) fn build_hub_ref_batch_event(
    deployment_id: &str,
    repository_id: &str,
    domain_sequence: u64,
    transaction_id: &str,
    changes: &[HubRefChange],
    manifest_refs: &[HubRefBatchManifestReferenceV1],
) -> Result<HubRefBatchArchiveEvent, HubRecoveryError> {
    // Hub has no independent tenant metadata. Use deployment as its tenant
    // scope and preserve the repository ID as an opaque value.
    let tenant_id = deployment_id;
    let mut ordered = changes.to_vec();
    ordered.sort_by(|left, right| {
        canonical_ref_name(&left.ref_name).cmp(canonical_ref_name(&right.ref_name))
    });
    let before = ordered
        .iter()
        .map(|change| HubRefBatchHead {
            ref_name: canonical_ref_name(&change.ref_name).to_owned(),
            head_sha: change.expected_sha.clone(),
        })
        .collect();
    let after = ordered
        .into_iter()
        .map(|change| HubRefBatchHead {
            ref_name: canonical_ref_name(&change.ref_name).to_owned(),
            head_sha: change.new_sha,
        })
        .collect();
    let before = HubRefBatchSnapshot::new(
        deployment_id,
        "shardline-hub",
        tenant_id,
        repository_id,
        domain_sequence,
        transaction_id,
        before,
    )?;
    let mut manifest_refs = manifest_refs.to_vec();
    manifest_refs.sort_by(|left, right| left.revision_sha.cmp(&right.revision_sha));
    let after = HubRefBatchSnapshot::new_with_manifest_refs(
        deployment_id,
        "shardline-hub",
        tenant_id,
        repository_id,
        domain_sequence,
        transaction_id,
        after,
        manifest_refs,
    )?;
    Ok(SnapshotEvidenceEvent::new(domain_sequence, before, after)?)
}

/// Locator and storage checksum for a verified immutable archive object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubRecoveryArchiveObject {
    pub(crate) key: ObjectKey,
    pub(crate) checksum_blake3: String,
    pub(crate) byte_length: u64,
    pub(crate) event: HubRefBatchArchiveEvent,
}

impl HubRecoveryArchiveObject {
    /// Returns the immutable object locator.
    #[must_use]
    pub const fn key(&self) -> &ObjectKey {
        &self.key
    }

    /// Returns the verified object checksum.
    #[must_use]
    pub fn checksum_blake3(&self) -> &str {
        &self.checksum_blake3
    }

    /// Returns the byte length verified during immutable readback.
    #[must_use]
    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }
}

/// Writes and reads back the genesis StateChronicle record for one committed
/// Hub batch (sequence one). Later receipts must use the predecessor-aware
/// outbox consumer. The SQL transaction has already committed before this runs.
///
/// # Errors
/// Returns an error if signing, immutable storage, readback, or record
/// verification fails.
pub async fn put_hub_ref_archive_record<S: AsyncObjectStore>(
    store: &S,
    event: HubRefBatchArchiveEvent,
    signing_key: &SigningKey,
    key_id: &str,
) -> Result<HubRecoveryArchiveObject, HubRecoveryError> {
    put_hub_ref_archive_record_inner(store, event, signing_key, key_id, None).await
}

async fn put_hub_ref_archive_record_inner<S: AsyncObjectStore>(
    store: &S,
    event: HubRefBatchArchiveEvent,
    signing_key: &SigningKey,
    key_id: &str,
    previous: Option<&HubRefBatchArchiveAnchor>,
) -> Result<HubRecoveryArchiveObject, HubRecoveryError> {
    if previous.is_none() && event.sequence != 1 {
        return Err(HubRecoveryError::SequenceGap);
    }
    let verifying_key = VerifyingKey::from(signing_key);
    let record = match previous {
        Some(previous) => HubRefBatchArchiveRecordV1::sign_with_previous_with_key_id(
            event,
            signing_key,
            key_id,
            previous,
        )?,
        None => HubRefBatchArchiveRecordV1::sign_with_key_id(event, signing_key, key_id)?,
    };
    let bytes = match previous {
        Some(previous) => record.canonical_bytes_with_previous(&verifying_key, previous)?,
        None => record.canonical_bytes(&verifying_key)?,
    };
    let integrity = ObjectIntegrity::for_bytes(&bytes);
    let checksum = integrity.hash().hex_string();
    let event_state = &record.event.after;
    let key = hub_ref_archive_key(
        &event_state.deployment_id,
        &event_state.repository_id,
        event_state.domain_sequence,
        &checksum,
    )?;
    store
        .put_if_absent(&key, ObjectBody::from_slice(&bytes), &integrity)
        .await
        .map_err(|error| HubRecoveryError::Storage(error.to_string()))?;

    let object = HubRecoveryArchiveObject {
        key,
        checksum_blake3: checksum,
        byte_length: integrity.length(),
        event: record.event.clone(),
    };
    let verified =
        read_hub_ref_archive_record_inner(store, &object, &verifying_key, previous).await?;
    if verified.event != object.event {
        return Err(HubRecoveryError::IntegrityMismatch);
    }
    Ok(object)
}

/// Reads and authenticates a genesis Hub receipt (sequence one).
///
/// # Errors
/// Returns an error if storage, length, byte integrity, format, event digest,
/// or StateChronicle signature verification fails. Later records require the
/// predecessor-aware reader.
pub async fn read_hub_ref_archive_record<S: AsyncObjectStore>(
    store: &S,
    object: &HubRecoveryArchiveObject,
    verifying_key: &VerifyingKey,
) -> Result<HubRefBatchArchiveRecordV1, HubRecoveryError> {
    read_hub_ref_archive_record_inner(store, object, verifying_key, None).await
}

/// Reads and verifies a Hub archive receipt against its signed predecessor.
///
/// # Errors
/// Returns storage, checksum, format, signature, domain, or sequence errors.
pub async fn read_hub_ref_archive_record_with_previous<S: AsyncObjectStore>(
    store: &S,
    object: &HubRecoveryArchiveObject,
    verifying_key: &VerifyingKey,
    previous: &HubRefBatchArchiveAnchor,
) -> Result<HubRefBatchArchiveRecordV1, HubRecoveryError> {
    read_hub_ref_archive_record_inner(store, object, verifying_key, Some(previous)).await
}

async fn read_hub_ref_archive_record_inner<S: AsyncObjectStore>(
    store: &S,
    object: &HubRecoveryArchiveObject,
    verifying_key: &VerifyingKey,
    previous: Option<&HubRefBatchArchiveAnchor>,
) -> Result<HubRefBatchArchiveRecordV1, HubRecoveryError> {
    let bytes = read_verified_hub_archive_bytes(
        store,
        &object.key,
        &object.checksum_blake3,
        Some(object.byte_length),
    )
    .await?;
    let record = match previous {
        Some(previous) => {
            HubRefBatchArchiveRecordV1::decode_with_previous(&bytes, verifying_key, previous)?.0
        }
        None => HubRefBatchArchiveRecordV1::decode(&bytes, verifying_key)?,
    };
    if previous.is_none() && record.event.sequence != 1 {
        return Err(HubRecoveryError::SequenceGap);
    }
    Ok(record)
}

async fn read_verified_hub_archive_bytes<S: AsyncObjectStore>(
    store: &S,
    key: &ObjectKey,
    expected_checksum: &str,
    expected_length: Option<u64>,
) -> Result<Vec<u8>, HubRecoveryError> {
    let metadata = store
        .metadata(key)
        .await
        .map_err(|error| HubRecoveryError::Storage(error.to_string()))?
        .ok_or(HubRecoveryError::MissingObject)?;
    let byte_length = metadata.length();
    if expected_length.is_some_and(|expected| expected != byte_length) || byte_length == 0 {
        return Err(HubRecoveryError::IntegrityMismatch);
    }
    if byte_length > 8 * 1024 * 1024 {
        return Err(HubRecoveryError::RecordTooLarge);
    }
    let last_byte = byte_length
        .checked_sub(1)
        .ok_or(HubRecoveryError::IntegrityMismatch)?;
    let range = ByteRange::new(0, last_byte)
        .map_err(|_conversion_error| HubRecoveryError::IntegrityMismatch)?;
    let bytes = store
        .read_range(key, range)
        .await
        .map_err(|error| HubRecoveryError::Storage(error.to_string()))?;
    let integrity = ObjectIntegrity::for_bytes(&bytes);
    if integrity.length() != byte_length || integrity.hash().hex_string() != expected_checksum {
        return Err(HubRecoveryError::IntegrityMismatch);
    }
    Ok(bytes)
}

async fn read_previous_archive_anchor<S: AsyncObjectStore>(
    store: &S,
    item: &HubRecoveryOutboxItem,
    verifying_key: &VerifyingKey,
) -> Result<Option<HubRefBatchArchiveAnchor>, HubRecoveryError> {
    item.verify_identity()?;
    let Some(checksum) = item.previous_archive_digest_blake3.as_deref() else {
        return Ok(None);
    };
    let previous_sequence = item
        .domain_sequence
        .checked_sub(1)
        .ok_or(HubRecoveryError::SequenceGap)?;
    let state = &item.event.after;
    let key = hub_ref_archive_key(
        &state.deployment_id,
        &state.repository_id,
        previous_sequence,
        checksum,
    )?;
    let bytes = read_verified_hub_archive_bytes(store, &key, checksum, None).await?;
    let pin = HubRefBatchArchiveCheckpointPin::new(
        state.deployment_id.clone(),
        state.provider_id.clone(),
        state.tenant_id.clone(),
        state.repository_id.clone(),
        previous_sequence,
        checksum.to_owned(),
    )?;
    let (_record, anchor) =
        HubRefBatchArchiveRecordV1::decode_pinned_anchor(&bytes, verifying_key, &pin, checksum)?;
    Ok(Some(anchor))
}

/// Archives the next SQL outbox item and advances its archived watermark only
/// after a verified immutable put and readback. Retries are idempotent.
///
/// # Errors
/// Returns an outbox, archive, or verification error. A failure leaves the SQL
/// outbox item pending for retry.
pub async fn archive_next_hub_ref_event<O, S>(
    outbox: &O,
    archive: &S,
    repository_id: &str,
    signing_key: &SigningKey,
    key_id: &str,
) -> Result<Option<HubRecoveryArchiveObject>, HubRecoveryError>
where
    O: HubRecoveryOutboxStore + ?Sized,
    S: AsyncObjectStore,
{
    let Some(item) = outbox.next_pending_hub_ref_event(repository_id).await? else {
        return Ok(None);
    };
    item.verify_identity()?;
    let verifying_key = VerifyingKey::from(signing_key);
    let previous = read_previous_archive_anchor(archive, &item, &verifying_key).await?;
    let object = put_hub_ref_archive_record_inner(
        archive,
        item.event.clone(),
        signing_key,
        key_id,
        previous.as_ref(),
    )
    .await?;
    if object.event != item.event {
        return Err(HubRecoveryError::OutboxEventMismatch);
    }
    outbox.mark_hub_ref_event_archived(&item, &object).await?;
    Ok(Some(object))
}

/// Parses and verifies one SQL outbox event shared by both adapter readers.
pub(crate) fn decode_hub_ref_event(
    repository_id: &str,
    sequence: i64,
    transaction_id: String,
    event_json: &str,
    previous_archive_digest_blake3: Option<String>,
) -> Result<HubRecoveryOutboxItem, HubRecoveryError> {
    if event_json.len() > MAX_HUB_REF_EVENT_BYTES {
        return Err(HubRecoveryError::RecordTooLarge);
    }
    let event: HubRefBatchArchiveEvent = serde_json::from_str(event_json)?;
    event.verify_integrity()?;
    let parsed_sequence =
        u64::try_from(sequence).map_err(|_conversion_error| HubRecoveryError::SequenceGap)?;
    if event.sequence != parsed_sequence
        || event.operation.operation_id != transaction_id
        || event.after.repository_id != repository_id
    {
        return Err(HubRecoveryError::OutboxEventMismatch);
    }
    Ok(HubRecoveryOutboxItem {
        repository_id: repository_id.to_owned(),
        domain_sequence: parsed_sequence,
        transaction_id,
        previous_archive_digest_blake3,
        event,
    })
}
