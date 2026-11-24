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
    HubRefBatchArchiveEvent, HubRefBatchArchiveRecordV1, HubRefBatchHead, HubRefBatchSnapshot,
    ReliabilityError, SnapshotEvidenceEvent,
};
use shardline_storage::{AsyncObjectStore, ObjectBody, ObjectIntegrity, ObjectKey};
use thiserror::Error;

pub(crate) const MAX_HUB_REF_EVENT_BYTES: usize = 8 * 1024 * 1024;

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

/// One typed batch read from the committed SQL outbox.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubRecoveryOutboxItem {
    pub repository_id: String,
    pub domain_sequence: u64,
    pub transaction_id: String,
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
}

/// Adapter boundary for the existing SQL outbox. Each call owns a short SQL
/// transaction; implementations must not hold SQL locks during archive I/O.
#[async_trait]
pub trait HubRecoveryOutboxStore: Send + Sync {
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
    let after = HubRefBatchSnapshot::new(
        deployment_id,
        "shardline-hub",
        tenant_id,
        repository_id,
        domain_sequence,
        transaction_id,
        after,
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

/// Writes and reads back a signed StateChronicle record for one committed Hub
/// batch. The SQL transaction has already committed before this function runs.
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
    let verifying_key = VerifyingKey::from(signing_key);
    let record = HubRefBatchArchiveRecordV1::sign_with_key_id(event, signing_key, key_id)?;
    let bytes = record.canonical_bytes(&verifying_key)?;
    let integrity = ObjectIntegrity::for_bytes(&bytes);
    let checksum = integrity.hash().hex_string();
    let event_state = &record.event.after;
    let key = ObjectKey::parse(&format!(
        "recovery/v1/{}/hub/{}/{:020}-{}.json",
        hex::encode(event_state.deployment_id.as_bytes()),
        hex::encode(event_state.repository_id.as_bytes()),
        event_state.domain_sequence,
        checksum,
    ))?;
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
    let verified = read_hub_ref_archive_record(store, &object, &verifying_key).await?;
    if verified.event != object.event {
        return Err(HubRecoveryError::IntegrityMismatch);
    }
    Ok(object)
}

/// Reads and authenticates a previously stored Hub batch archive object.
///
/// # Errors
/// Returns an error if storage, length, byte integrity, format, event digest,
/// or StateChronicle signature verification fails.
pub async fn read_hub_ref_archive_record<S: AsyncObjectStore>(
    store: &S,
    object: &HubRecoveryArchiveObject,
    verifying_key: &VerifyingKey,
) -> Result<HubRefBatchArchiveRecordV1, HubRecoveryError> {
    let metadata = store
        .metadata(&object.key)
        .await
        .map_err(|error| HubRecoveryError::Storage(error.to_string()))?
        .ok_or(HubRecoveryError::MissingObject)?;
    if metadata.length() != object.byte_length || object.byte_length == 0 {
        return Err(HubRecoveryError::IntegrityMismatch);
    }
    if object.byte_length > 8 * 1024 * 1024 {
        return Err(HubRecoveryError::RecordTooLarge);
    }
    let last_byte = object
        .byte_length
        .checked_sub(1)
        .ok_or(HubRecoveryError::IntegrityMismatch)?;
    let range = ByteRange::new(0, last_byte)
        .map_err(|_conversion_error| HubRecoveryError::IntegrityMismatch)?;
    let bytes = store
        .read_range(&object.key, range)
        .await
        .map_err(|error| HubRecoveryError::Storage(error.to_string()))?;
    let integrity = ObjectIntegrity::for_bytes(&bytes);
    if integrity.length() != object.byte_length
        || integrity.hash().hex_string() != object.checksum_blake3
    {
        return Err(HubRecoveryError::IntegrityMismatch);
    }
    Ok(HubRefBatchArchiveRecordV1::decode(&bytes, verifying_key)?)
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
    O: HubRecoveryOutboxStore,
    S: AsyncObjectStore,
{
    let Some(item) = outbox.next_pending_hub_ref_event(repository_id).await? else {
        return Ok(None);
    };
    item.verify_identity()?;
    let object =
        put_hub_ref_archive_record(archive, item.event.clone(), signing_key, key_id).await?;
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
        event,
    })
}
