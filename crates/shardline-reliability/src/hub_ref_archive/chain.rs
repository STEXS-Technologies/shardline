//! Bounded predecessor verification for signed Hub batch receipts.
//!
//! The sequence is repository-domain scoped, while the operation ID is a
//! transaction ID and therefore changes for each batch. This layer reuses the
//! existing StateChronicle commit body and signature format.

use ed25519_dalek::VerifyingKey;

use crate::{
    ReliabilityError, ReliabilityMerkleCommit, build_reliability_merkle_commit,
    build_reliability_merkle_commit_with_previous,
};

use super::{
    HUB_REF_ARCHIVE_SCHEMA, HubRefBatchArchiveEvent, HubRefBatchArchiveRecordV1,
    MAX_ARCHIVE_RECORD_BYTES, parse_key_id, sign_record,
};

/// Identity and object-digest pin retained outside the replaceable archive.
/// It is trust metadata, not a second signature or commitment format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubRefBatchArchiveCheckpointPin {
    deployment_id: String,
    provider_id: String,
    tenant_id: String,
    repository_id: String,
    sequence: u64,
    archive_digest_blake3: String,
}

impl HubRefBatchArchiveCheckpointPin {
    /// Constructs a checkpoint pin from externally retained identity and digest.
    ///
    /// # Errors
    /// Returns an error for an empty/invalid identity, sequence zero, or a
    /// malformed lowercase BLAKE3 hex digest.
    pub fn new(
        deployment_id: impl Into<String>,
        provider_id: impl Into<String>,
        tenant_id: impl Into<String>,
        repository_id: impl Into<String>,
        sequence: u64,
        archive_digest_blake3: impl Into<String>,
    ) -> Result<Self, ReliabilityError> {
        let pin = Self {
            deployment_id: deployment_id.into(),
            provider_id: provider_id.into(),
            tenant_id: tenant_id.into(),
            repository_id: repository_id.into(),
            sequence,
            archive_digest_blake3: archive_digest_blake3.into(),
        };
        if [
            pin.deployment_id.as_str(),
            pin.provider_id.as_str(),
            pin.tenant_id.as_str(),
            pin.repository_id.as_str(),
        ]
        .iter()
        .any(|identity| {
            identity.is_empty() || identity.len() > 1024 || identity.chars().any(char::is_control)
        }) || pin.sequence == 0
            || pin.archive_digest_blake3.len() != 64
            || !pin
                .archive_digest_blake3
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(pin)
    }
}

/// A trusted, verified StateChronicle predecessor for a bounded Hub archive
/// suffix. The caller must pin this checkpoint outside the replaceable archive
/// when freshness or rollback detection is required.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubRefBatchArchiveAnchor {
    deployment_id: String,
    provider_id: String,
    tenant_id: String,
    repository_id: String,
    sequence: u64,
    commitment: ReliabilityMerkleCommit,
}

impl HubRefBatchArchiveAnchor {
    /// Verifies a separately pinned receipt and promotes it to a suffix anchor.
    ///
    /// This checks the pin's domain/sequence/digest against the selected record,
    /// plus its signature, event integrity and event root. It deliberately does
    /// not walk the receipt's parent history. A signature alone does not prove
    /// freshness: the pin must be retained outside the replaceable archive, and
    /// the caller must verify the record bytes against the pinned digest before
    /// calling this method.
    ///
    /// # Errors
    /// Returns an error for an invalid record, signature, event binding, or
    /// unsupported schema.
    pub fn from_pinned_record(
        record: &HubRefBatchArchiveRecordV1,
        verifying_key: &VerifyingKey,
        pin: &HubRefBatchArchiveCheckpointPin,
        verified_archive_digest_blake3: &str,
    ) -> Result<Self, ReliabilityError> {
        validate_record_schema(record)?;
        validate_event_domain(&record.event)?;
        let event_commitment = build_reliability_merkle_commit(&record.event)?;
        let state = &record.event.after;
        if pin.archive_digest_blake3 != verified_archive_digest_blake3
            || pin.deployment_id != state.deployment_id
            || pin.provider_id != state.provider_id
            || pin.tenant_id != state.tenant_id
            || pin.repository_id != state.repository_id
            || pin.sequence != record.event.sequence
            || record.signed_commit.body.sequence != record.event.sequence
            || record.signed_commit.body.event_merkle_root
                != event_commitment.body.event_merkle_root
            || record.signed_commit.body.event_count != event_commitment.body.event_count
        {
            return Err(ReliabilityError::Merkle(
                "trusted Hub archive anchor does not bind its event".into(),
            ));
        }
        crate::compatibility::verify_commit_signature(&record.signed_commit, verifying_key)?;
        let mut event_commitment = event_commitment;
        event_commitment.body = record.signed_commit.body.clone();
        Ok(anchor_from_record(record, event_commitment))
    }

    /// Returns the deployment identity covered by the Hub event.
    #[must_use]
    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }

    /// Returns the provider identity covered by the Hub event.
    #[must_use]
    pub fn provider_id(&self) -> &str {
        &self.provider_id
    }

    /// Returns the tenant identity covered by the Hub event.
    #[must_use]
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// Returns the repository identity covered by the Hub event.
    #[must_use]
    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }

    /// Returns the last verified repository-domain sequence.
    #[must_use]
    pub const fn sequence(&self) -> u64 {
        self.sequence
    }
}

impl HubRefBatchArchiveRecordV1 {
    /// Signs one batch as the next repository-domain receipt after `previous`.
    ///
    /// Transaction IDs and touched ref sets may differ between batches. The
    /// deployment, provider, tenant, and repository must remain the same, and
    /// the sequence must increase by exactly one.
    ///
    /// # Errors
    /// Returns an error for an invalid event, a wrong domain or sequence, or a
    /// StateChronicle signing failure.
    pub fn sign_with_previous_with_key_id(
        event: HubRefBatchArchiveEvent,
        signing_key: &ed25519_dalek::SigningKey,
        key_id: &str,
        previous: &HubRefBatchArchiveAnchor,
    ) -> Result<Self, ReliabilityError> {
        let key_id = parse_key_id(key_id)?;
        validate_event_domain(&event)?;
        ensure_domain_and_sequence(&event, previous)?;
        let commitment =
            build_reliability_merkle_commit_with_previous(&event, Some(&previous.commitment))?;
        sign_record(event, &commitment, signing_key, key_id)
    }

    /// Verifies one batch as the next receipt after `previous`, returning the
    /// current verified receipt as the next bounded anchor.
    ///
    /// # Errors
    /// Returns an error for an invalid event/signature, a wrong domain or
    /// sequence, or a commit body that does not contain the expected Chronicle
    /// parent and state-root links.
    pub fn verify_with_previous(
        &self,
        verifying_key: &VerifyingKey,
        previous: &HubRefBatchArchiveAnchor,
    ) -> Result<HubRefBatchArchiveAnchor, ReliabilityError> {
        validate_record_schema(self)?;
        validate_event_domain(&self.event)?;
        ensure_domain_and_sequence(&self.event, previous)?;
        let expected =
            build_reliability_merkle_commit_with_previous(&self.event, Some(&previous.commitment))?;
        verify_record_against(
            self,
            &expected,
            verifying_key,
            "signed Hub archive commitment does not extend its trusted predecessor",
        )?;
        Ok(anchor_from_record(self, expected))
    }

    /// Encodes a receipt only after verifying its predecessor link and signature.
    ///
    /// # Errors
    /// Returns an error for invalid evidence, links, signatures, serialization,
    /// or an encoded record over the archive size bound.
    pub fn canonical_bytes_with_previous(
        &self,
        verifying_key: &VerifyingKey,
        previous: &HubRefBatchArchiveAnchor,
    ) -> Result<Vec<u8>, ReliabilityError> {
        self.verify_with_previous(verifying_key, previous)?;
        encode_record(self)
    }

    /// Decodes and verifies one receipt against its trusted predecessor.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unauthenticated, or
    /// predecessor-discontinuous records.
    pub fn decode_with_previous(
        bytes: &[u8],
        verifying_key: &VerifyingKey,
        previous: &HubRefBatchArchiveAnchor,
    ) -> Result<(Self, HubRefBatchArchiveAnchor), ReliabilityError> {
        let record = decode_record(bytes)?;
        let next = record.verify_with_previous(verifying_key, previous)?;
        Ok((record, next))
    }

    /// Decodes a record selected by an external freshness checkpoint.
    ///
    /// The caller supplies a pin whose digest and identity/sequence were retained
    /// outside the archive, and passes the actual digest already verified by the
    /// archive reader. The returned anchor authenticates the event and signed
    /// commit, but does not prove the record's parent history.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unauthenticated, or
    /// event-mismatched records.
    pub fn decode_pinned_anchor(
        bytes: &[u8],
        verifying_key: &VerifyingKey,
        pin: &HubRefBatchArchiveCheckpointPin,
        verified_archive_digest_blake3: &str,
    ) -> Result<(Self, HubRefBatchArchiveAnchor), ReliabilityError> {
        let record = decode_record(bytes)?;
        let anchor = HubRefBatchArchiveAnchor::from_pinned_record(
            &record,
            verifying_key,
            pin,
            verified_archive_digest_blake3,
        )?;
        Ok((record, anchor))
    }
}

fn ensure_domain_and_sequence(
    event: &HubRefBatchArchiveEvent,
    previous: &HubRefBatchArchiveAnchor,
) -> Result<(), ReliabilityError> {
    let state = &event.after;
    let expected = previous
        .sequence
        .checked_add(1)
        .ok_or(ReliabilityError::ChainDiscontinuity)?;
    if state.deployment_id != previous.deployment_id
        || state.provider_id != previous.provider_id
        || state.tenant_id != previous.tenant_id
        || state.repository_id != previous.repository_id
    {
        return Err(ReliabilityError::OperationMismatch);
    }
    if event.sequence != expected {
        return Err(ReliabilityError::ChainDiscontinuity);
    }
    Ok(())
}

pub(super) fn validate_event_domain(
    event: &HubRefBatchArchiveEvent,
) -> Result<(), ReliabilityError> {
    event.verify_integrity()?;
    if event.sequence == 0
        || event.before.domain_sequence != event.sequence
        || event.after.domain_sequence != event.sequence
        || event.before.deployment_id != event.after.deployment_id
        || event.before.provider_id != event.after.provider_id
        || event.before.tenant_id != event.after.tenant_id
        || event.before.repository_id != event.after.repository_id
    {
        return Err(ReliabilityError::OperationMismatch);
    }
    Ok(())
}

pub(super) fn validate_record_schema(
    record: &HubRefBatchArchiveRecordV1,
) -> Result<(), ReliabilityError> {
    if record.schema_version != HUB_REF_ARCHIVE_SCHEMA {
        return Err(ReliabilityError::Merkle(
            "unsupported Hub archive record version".into(),
        ));
    }
    Ok(())
}

fn anchor_from_record(
    record: &HubRefBatchArchiveRecordV1,
    commitment: ReliabilityMerkleCommit,
) -> HubRefBatchArchiveAnchor {
    let state = &record.event.after;
    HubRefBatchArchiveAnchor {
        deployment_id: state.deployment_id.clone(),
        provider_id: state.provider_id.clone(),
        tenant_id: state.tenant_id.clone(),
        repository_id: state.repository_id.clone(),
        sequence: record.event.sequence,
        commitment,
    }
}

pub(super) fn verify_record_against(
    record: &HubRefBatchArchiveRecordV1,
    expected: &ReliabilityMerkleCommit,
    verifying_key: &VerifyingKey,
    mismatch: &'static str,
) -> Result<(), ReliabilityError> {
    if record.signed_commit.body != expected.body {
        return Err(ReliabilityError::Merkle(mismatch.into()));
    }
    crate::compatibility::verify_commit_signature(&record.signed_commit, verifying_key)
}

pub(super) fn encode_record(
    record: &HubRefBatchArchiveRecordV1,
) -> Result<Vec<u8>, ReliabilityError> {
    let bytes = serde_json::to_vec(record)?;
    if bytes.len() > MAX_ARCHIVE_RECORD_BYTES {
        return Err(ReliabilityError::Merkle(
            "Hub archive record exceeds byte limit".into(),
        ));
    }
    Ok(bytes)
}

pub(super) fn decode_record(bytes: &[u8]) -> Result<HubRefBatchArchiveRecordV1, ReliabilityError> {
    if bytes.len() > MAX_ARCHIVE_RECORD_BYTES {
        return Err(ReliabilityError::Merkle(
            "Hub archive record exceeds byte limit".into(),
        ));
    }
    Ok(serde_json::from_slice(bytes)?)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::{HubRefBatchHead, HubRefBatchSnapshot, SnapshotEvidenceEvent};
    use ed25519_dalek::SigningKey;
    use statechronicle_domain_compat::intent::KeyId;

    fn event(
        sequence: u64,
        transaction_id: &str,
        repository_id: &str,
        ref_name: &str,
        before_sha: Option<&str>,
        after_sha: Option<&str>,
    ) -> HubRefBatchArchiveEvent {
        event_in_domain(
            sequence,
            transaction_id,
            "deploy-a",
            "shardline-hub",
            "tenant-a",
            repository_id,
            ref_name,
            before_sha,
            after_sha,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn event_in_domain(
        sequence: u64,
        transaction_id: &str,
        deployment_id: &str,
        provider_id: &str,
        tenant_id: &str,
        repository_id: &str,
        ref_name: &str,
        before_sha: Option<&str>,
        after_sha: Option<&str>,
    ) -> HubRefBatchArchiveEvent {
        let before = HubRefBatchSnapshot::new(
            deployment_id,
            provider_id,
            tenant_id,
            repository_id,
            sequence,
            transaction_id,
            vec![HubRefBatchHead {
                ref_name: ref_name.to_owned(),
                head_sha: before_sha.map(str::to_owned),
            }],
        )
        .unwrap();
        let after = HubRefBatchSnapshot::new(
            deployment_id,
            provider_id,
            tenant_id,
            repository_id,
            sequence,
            transaction_id,
            vec![HubRefBatchHead {
                ref_name: ref_name.to_owned(),
                head_sha: after_sha.map(str::to_owned),
            }],
        )
        .unwrap();
        SnapshotEvidenceEvent::new(sequence, before, after).unwrap()
    }

    fn sign_genesis(
        event: HubRefBatchArchiveEvent,
        key: &SigningKey,
    ) -> HubRefBatchArchiveRecordV1 {
        HubRefBatchArchiveRecordV1::sign(event, key, KeyId::new("hub-test-key".to_owned()).unwrap())
            .unwrap()
    }

    fn sign_next(
        event: HubRefBatchArchiveEvent,
        key: &SigningKey,
        previous: &HubRefBatchArchiveAnchor,
    ) -> HubRefBatchArchiveRecordV1 {
        HubRefBatchArchiveRecordV1::sign_with_previous_with_key_id(
            event,
            key,
            "hub-test-key",
            previous,
        )
        .unwrap()
    }

    fn pinned_anchor(
        record: &HubRefBatchArchiveRecordV1,
        verifying_key: &VerifyingKey,
        digest: &str,
    ) -> HubRefBatchArchiveAnchor {
        let state = &record.event.after;
        let pin = HubRefBatchArchiveCheckpointPin::new(
            state.deployment_id.clone(),
            state.provider_id.clone(),
            state.tenant_id.clone(),
            state.repository_id.clone(),
            record.event.sequence,
            digest,
        )
        .unwrap();
        HubRefBatchArchiveAnchor::from_pinned_record(record, verifying_key, &pin, digest).unwrap()
    }

    #[test]
    fn transaction_ids_and_touched_ref_sets_may_change_in_a_linked_suffix() {
        let key = SigningKey::from_bytes(&[31; 32]);
        let verifying_key = VerifyingKey::from(&key);
        let first = sign_genesis(
            event(1, "tx-main", "org/repo", "main", Some("m0"), Some("m1")),
            &key,
        );
        // The fixture pins this authenticated first receipt as the external
        // checkpoint; subsequent verification is strictly predecessor-aware.
        let first_anchor = pinned_anchor(&first, &verifying_key, &"a".repeat(64));
        let second = sign_next(
            event(2, "tx-tags", "org/repo", "refs/tags/v1", None, Some("t1")),
            &key,
            &first_anchor,
        );
        let second_anchor = second
            .verify_with_previous(&verifying_key, &first_anchor)
            .unwrap();
        let third = sign_next(
            event(
                3,
                "tx-main-again",
                "org/repo",
                "main",
                Some("m1"),
                Some("m2"),
            ),
            &key,
            &second_anchor,
        );

        let final_anchor = third
            .verify_with_previous(&verifying_key, &second_anchor)
            .unwrap();
        assert_eq!(final_anchor.sequence(), 3);
        assert_eq!(second.event.operation.operation_id, "tx-tags");
        assert_eq!(third.event.operation.operation_id, "tx-main-again");
        assert_eq!(
            second.event.after.refs.first().unwrap().ref_name,
            "refs/tags/v1"
        );
        assert_eq!(third.event.after.refs.first().unwrap().ref_name, "main");
    }

    #[test]
    fn predecessor_rejects_wrong_domain_sequence_and_forks() {
        let key = SigningKey::from_bytes(&[32; 32]);
        let verifying_key = VerifyingKey::from(&key);
        let first = sign_genesis(
            event(1, "tx-1", "org/repo", "main", Some("a"), Some("b")),
            &key,
        );
        let anchor = pinned_anchor(&first, &verifying_key, &"b".repeat(64));

        for wrong_domain in [
            event_in_domain(
                2,
                "tx-2",
                "deploy-b",
                "shardline-hub",
                "tenant-a",
                "org/repo",
                "tags/v1",
                None,
                Some("tag"),
            ),
            event_in_domain(
                2,
                "tx-2",
                "deploy-a",
                "other-provider",
                "tenant-a",
                "org/repo",
                "tags/v1",
                None,
                Some("tag"),
            ),
            event_in_domain(
                2,
                "tx-2",
                "deploy-a",
                "shardline-hub",
                "tenant-b",
                "org/repo",
                "tags/v1",
                None,
                Some("tag"),
            ),
            event_in_domain(
                2,
                "tx-2",
                "deploy-a",
                "shardline-hub",
                "tenant-a",
                "other/repo",
                "tags/v1",
                None,
                Some("tag"),
            ),
        ] {
            assert!(matches!(
                HubRefBatchArchiveRecordV1::sign_with_previous_with_key_id(
                    wrong_domain,
                    &key,
                    "hub-test-key",
                    &anchor
                ),
                Err(ReliabilityError::OperationMismatch)
            ));
        }
        let gap = event(3, "tx-3", "org/repo", "main", Some("b"), Some("c"));
        assert!(matches!(
            HubRefBatchArchiveRecordV1::sign_with_previous_with_key_id(
                gap,
                &key,
                "hub-test-key",
                &anchor
            ),
            Err(ReliabilityError::ChainDiscontinuity)
        ));

        let fork_a = sign_next(
            event(2, "tx-2a", "org/repo", "main", Some("b"), Some("c")),
            &key,
            &anchor,
        );
        let fork_b = sign_next(
            event(2, "tx-2b", "org/repo", "refs/tags/v1", None, Some("tag")),
            &key,
            &anchor,
        );
        let canonical_anchor = fork_a
            .verify_with_previous(&verifying_key, &anchor)
            .unwrap();
        assert!(
            fork_b
                .verify_with_previous(&verifying_key, &canonical_anchor)
                .is_err()
        );

        // A distinct producer-signed fork is valid relative to the common
        // predecessor. A pinned canonical sequence-2 anchor rejects a child
        // whose parent is the competing commit.
        let competing_child = sign_next(
            event(3, "tx-3b", "org/repo", "refs/tags/v2", None, Some("tag-2")),
            &key,
            &fork_b
                .verify_with_previous(&verifying_key, &anchor)
                .unwrap(),
        );
        assert!(
            competing_child
                .verify_with_previous(&verifying_key, &canonical_anchor)
                .is_err()
        );
    }

    #[test]
    fn linked_receipts_reject_event_or_parent_tampering_and_decode_from_anchor() {
        let key = SigningKey::from_bytes(&[33; 32]);
        let verifying_key = VerifyingKey::from(&key);
        let first = sign_genesis(
            event(1, "tx-1", "org/repo", "main", Some("a"), Some("b")),
            &key,
        );
        let first_bytes = first.canonical_bytes(&verifying_key).unwrap();
        let first_pin = HubRefBatchArchiveCheckpointPin::new(
            "deploy-a",
            "shardline-hub",
            "tenant-a",
            "org/repo",
            1,
            "c".repeat(64),
        )
        .unwrap();
        assert!(
            HubRefBatchArchiveRecordV1::decode_pinned_anchor(
                &first_bytes,
                &verifying_key,
                &first_pin,
                &"d".repeat(64),
            )
            .is_err()
        );
        let wrong_domain_pin = HubRefBatchArchiveCheckpointPin::new(
            "deploy-a",
            "shardline-hub",
            "tenant-a",
            "other/repo",
            1,
            "c".repeat(64),
        )
        .unwrap();
        assert!(
            HubRefBatchArchiveRecordV1::decode_pinned_anchor(
                &first_bytes,
                &verifying_key,
                &wrong_domain_pin,
                &"c".repeat(64),
            )
            .is_err()
        );
        let (decoded_first, first_anchor) = HubRefBatchArchiveRecordV1::decode_pinned_anchor(
            &first_bytes,
            &verifying_key,
            &first_pin,
            &"c".repeat(64),
        )
        .unwrap();
        assert_eq!(decoded_first, first);

        let second = sign_next(
            event(2, "tx-2", "org/repo", "refs/tags/v1", None, Some("tag")),
            &key,
            &first_anchor,
        );
        let bytes = second
            .canonical_bytes_with_previous(&verifying_key, &first_anchor)
            .unwrap();
        let (decoded_second, second_anchor) =
            HubRefBatchArchiveRecordV1::decode_with_previous(&bytes, &verifying_key, &first_anchor)
                .unwrap();
        assert_eq!(decoded_second, second);
        assert_eq!(second_anchor.sequence(), 2);

        let mut tampered_event = second.clone();
        tampered_event
            .event
            .after
            .refs
            .first_mut()
            .unwrap()
            .head_sha = Some("forged".into());
        assert!(
            tampered_event
                .verify_with_previous(&verifying_key, &first_anchor)
                .is_err()
        );

        let mut tampered_parent = second;
        tampered_parent.signed_commit.body.parent_commit_id = None;
        assert!(
            tampered_parent
                .verify_with_previous(&verifying_key, &first_anchor)
                .is_err()
        );
    }
}
