//! Signed, atomic Hub reference batch evidence for external recovery archives.

mod chain;

pub use chain::{HubRefBatchArchiveAnchor, HubRefBatchArchiveCheckpointPin};

use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use statechronicle_domain_compat::{commit::Commit, intent::KeyId, signed::Signed};

use crate::{
    OperationIdentity, OperationKind, ReliabilityError, SnapshotEvidence, SnapshotEvidenceEvent,
    build_reliability_merkle_commit_with_previous,
};

const HUB_REF_ARCHIVE_SCHEMA: u16 = 1;
/// Maximum serialized size accepted for a signed Hub ref archive record.
pub const MAX_HUB_REF_ARCHIVE_RECORD_BYTES: usize = 8 * 1024 * 1024;
const MAX_ARCHIVE_RECORD_BYTES: usize = MAX_HUB_REF_ARCHIVE_RECORD_BYTES;
const MAX_REFS_PER_BATCH: usize = 100_000;

/// Validates an archive signing key ID using StateChronicle's canonical parser.
///
/// # Errors
/// Returns the existing reliability error when StateChronicle rejects the ID.
pub fn validate_hub_ref_archive_key_id(key_id: &str) -> Result<(), ReliabilityError> {
    KeyId::new(key_id.to_owned())
        .map(|_key_id| ())
        .map_err(|error| ReliabilityError::Merkle(error.to_string()))
}

/// The resulting head of one ref in a Hub batch transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefBatchHead {
    pub ref_name: String,
    pub head_sha: Option<String>,
}

/// Verified immutable version manifest bound to the exact published revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefBatchManifestReferenceV1 {
    pub deployment_id: String,
    pub repository_id: String,
    pub revision_sha: String,
    pub digest_blake3: String,
    pub object_key: String,
}

impl HubRefBatchManifestReferenceV1 {
    /// Constructs a revision-bound manifest locator.
    ///
    /// # Errors
    /// Returns an error when identity, digest, revision, or object key is invalid.
    pub fn new(
        deployment_id: String,
        repository_id: String,
        revision_sha: String,
        digest_blake3: String,
        object_key: String,
    ) -> Result<Self, ReliabilityError> {
        let value = Self {
            deployment_id,
            repository_id,
            revision_sha,
            digest_blake3,
            object_key,
        };
        value.validate()?;
        Ok(value)
    }

    #[must_use]
    pub fn revision_sha(&self) -> &str {
        &self.revision_sha
    }
    #[must_use]
    pub fn deployment_id(&self) -> &str {
        &self.deployment_id
    }
    #[must_use]
    pub fn repository_id(&self) -> &str {
        &self.repository_id
    }
    #[must_use]
    pub fn digest_blake3(&self) -> &str {
        &self.digest_blake3
    }
    #[must_use]
    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    fn validate(&self) -> Result<(), ReliabilityError> {
        let key_parts = self.object_key.split('/').collect::<Vec<_>>();
        if !matches!(self.revision_sha.len(), 40 | 64)
            || !self
                .revision_sha
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
            || self.digest_blake3.len() != 64
            || !self
                .digest_blake3
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            || self.object_key.is_empty()
            || self.object_key.len() > 4096
            || self.object_key.chars().any(char::is_control)
            || key_parts.len() != 7
            || key_parts.first() != Some(&"recovery")
            || key_parts.get(1) != Some(&"v1")
            || key_parts.get(4) != Some(&"versions")
            || !key_parts.get(2).is_some_and(|part| {
                part.len() == 64
                    && part
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            || !key_parts.get(3).is_some_and(|part| {
                part.len() == 64
                    && part
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
            || key_parts.get(5).copied() != Some(&self.digest_blake3[..2])
            || key_parts.get(6).copied() != Some(self.digest_blake3.as_str())
            || self.deployment_id.is_empty()
            || self.deployment_id.len() > 256
            || self.deployment_id.chars().any(char::is_control)
            || self.repository_id.is_empty()
            || self.repository_id.len() > 1024
            || self.repository_id.chars().any(char::is_control)
        {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(())
    }
}

/// Materialized before or after state for one atomic ref publication batch.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefBatchSnapshot {
    pub deployment_id: String,
    pub provider_id: String,
    pub tenant_id: String,
    pub repository_id: String,
    pub domain_sequence: u64,
    pub transaction_id: String,
    /// Sorted by canonical ref name; the vector's order is part of the encoding.
    pub refs: Vec<HubRefBatchHead>,
    /// Sorted unique manifest locators for revisions published by this batch.
    #[serde(default)]
    pub manifest_refs: Vec<HubRefBatchManifestReferenceV1>,
}

impl HubRefBatchSnapshot {
    /// Constructs a validated snapshot for one repository-domain sequence.
    ///
    /// # Errors
    /// Returns an error for missing/invalid identity fields, an empty or
    /// oversized batch, duplicate/unsorted refs, or sequence zero.
    pub fn new(
        deployment_id: impl Into<String>,
        provider_id: impl Into<String>,
        tenant_id: impl Into<String>,
        repository_id: impl Into<String>,
        domain_sequence: u64,
        transaction_id: impl Into<String>,
        refs: Vec<HubRefBatchHead>,
    ) -> Result<Self, ReliabilityError> {
        Self::new_with_manifest_refs(
            deployment_id,
            provider_id,
            tenant_id,
            repository_id,
            domain_sequence,
            transaction_id,
            refs,
            Vec::new(),
        )
    }

    /// Constructs a validated snapshot including revision-bound manifests.
    ///
    /// # Errors
    /// Returns an error for invalid domain identity, ref ordering, or manifest binding.
    #[allow(
        clippy::too_many_arguments,
        reason = "extends the existing fixed snapshot constructor with its manifest list"
    )]
    pub fn new_with_manifest_refs(
        deployment_id: impl Into<String>,
        provider_id: impl Into<String>,
        tenant_id: impl Into<String>,
        repository_id: impl Into<String>,
        domain_sequence: u64,
        transaction_id: impl Into<String>,
        refs: Vec<HubRefBatchHead>,
        manifest_refs: Vec<HubRefBatchManifestReferenceV1>,
    ) -> Result<Self, ReliabilityError> {
        let snapshot = Self {
            deployment_id: deployment_id.into(),
            provider_id: provider_id.into(),
            tenant_id: tenant_id.into(),
            repository_id: repository_id.into(),
            domain_sequence,
            transaction_id: transaction_id.into(),
            refs,
            manifest_refs,
        };
        snapshot.validate()?;
        Ok(snapshot)
    }

    fn validate(&self) -> Result<(), ReliabilityError> {
        for (field, value) in [
            ("deployment_id", self.deployment_id.as_str()),
            ("provider_id", self.provider_id.as_str()),
            ("tenant_id", self.tenant_id.as_str()),
            ("repository_id", self.repository_id.as_str()),
            ("transaction_id", self.transaction_id.as_str()),
        ] {
            if value.is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        if self.domain_sequence == 0 {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        if self.refs.is_empty() || self.refs.len() > MAX_REFS_PER_BATCH {
            return Err(ReliabilityError::EmptyField("refs"));
        }
        if self.manifest_refs.len() > MAX_REFS_PER_BATCH {
            return Err(ReliabilityError::OperationMismatch);
        }
        let mut previous: Option<&str> = None;
        for entry in &self.refs {
            if entry.ref_name.is_empty()
                || entry.ref_name.len() > 1024
                || entry.ref_name.chars().any(char::is_control)
                || entry.head_sha.as_ref().is_some_and(|head| {
                    head.is_empty() || head.len() > 1024 || head.chars().any(char::is_control)
                })
                || previous.is_some_and(|last| last >= entry.ref_name.as_str())
            {
                return Err(ReliabilityError::OperationMismatch);
            }
            previous = Some(&entry.ref_name);
        }
        let mut previous_revision: Option<&str> = None;
        for manifest in &self.manifest_refs {
            manifest.validate()?;
            if previous_revision.is_some_and(|last| last >= manifest.revision_sha.as_str())
                || !self
                    .refs
                    .iter()
                    .any(|head| head.head_sha.as_deref() == Some(&manifest.revision_sha))
                || manifest.deployment_id != self.deployment_id
                || manifest.repository_id != self.repository_id
                || !manifest.object_key.ends_with(&format!(
                    "/versions/{}/{}",
                    &manifest.digest_blake3[..2],
                    manifest.digest_blake3
                ))
                || !manifest.object_key.starts_with("recovery/v1/")
            {
                return Err(ReliabilityError::OperationMismatch);
            }
            previous_revision = Some(&manifest.revision_sha);
        }
        Ok(())
    }

    fn same_transaction(&self, after: &Self) -> bool {
        self.deployment_id == after.deployment_id
            && self.provider_id == after.provider_id
            && self.tenant_id == after.tenant_id
            && self.repository_id == after.repository_id
            && self.domain_sequence == after.domain_sequence
            && self.transaction_id == after.transaction_id
            && self.manifest_refs.is_empty()
            && self.refs.len() == after.refs.len()
            && self
                .refs
                .iter()
                .zip(&after.refs)
                .all(|(before, next)| before.ref_name == next.ref_name)
    }
}

impl SnapshotEvidence for HubRefBatchSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(OperationIdentity::new(
            self.provider_id.clone(),
            self.repository_id.clone(),
            self.transaction_id.clone(),
            OperationKind::MetadataCommit,
        )?
        .with_object_key(format!(
            "hub-ref-batch:{}:{}",
            self.tenant_id, self.domain_sequence
        )))
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        self.validate()?;
        after.validate()?;
        if !self.same_transaction(after)
            || self
                .refs
                .iter()
                .zip(&after.refs)
                .all(|(before, next)| before.head_sha == next.head_sha)
        {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(())
    }
}

/// One signed StateChronicle commitment for the complete multi-ref SQL batch.
pub type HubRefBatchArchiveEvent = SnapshotEvidenceEvent<HubRefBatchSnapshot>;

/// Externally stored Hub batch record. Its signature covers the entire ordered
/// before/after ref batch and the deployment, tenant, sequence, and transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefBatchArchiveRecordV1 {
    pub schema_version: u16,
    pub event: HubRefBatchArchiveEvent,
    pub signed_commit: Signed<Commit>,
}

impl HubRefBatchArchiveRecordV1 {
    /// Parses a StateChronicle key ID and signs a validated committed-batch
    /// evidence event. Callers do not need to depend on Chronicle's domain
    /// crate merely to configure the archive signer.
    ///
    /// # Errors
    /// Returns an error when the key ID or event is invalid, or signing fails.
    pub fn sign_with_key_id(
        event: HubRefBatchArchiveEvent,
        signing_key: &SigningKey,
        key_id: &str,
    ) -> Result<Self, ReliabilityError> {
        let key_id = parse_key_id(key_id)?;
        Self::sign(event, signing_key, key_id)
    }

    /// Signs a validated committed-batch evidence event with the deployment key.
    ///
    /// # Errors
    /// Returns an error when the event is invalid or StateChronicle signing fails.
    pub fn sign(
        event: HubRefBatchArchiveEvent,
        signing_key: &SigningKey,
        key_id: KeyId,
    ) -> Result<Self, ReliabilityError> {
        chain::validate_event_domain(&event)?;
        let commitment = build_reliability_merkle_commit_with_previous(&event, None)?;
        sign_record(event, &commitment, signing_key, key_id)
    }

    /// Verifies the event, signed StateChronicle commitment, and exact binding.
    ///
    /// # Errors
    /// Returns an error for unsupported format, invalid event data, a bad
    /// signature, or a signed body that does not match the batch event.
    pub fn verify(&self, verifying_key: &VerifyingKey) -> Result<(), ReliabilityError> {
        chain::validate_record_schema(self)?;
        chain::validate_event_domain(&self.event)?;
        let expected = build_reliability_merkle_commit_with_previous(&self.event, None)?;
        chain::verify_record_against(
            self,
            &expected,
            verifying_key,
            "signed Hub archive commitment does not match its event",
        )
    }

    /// Serializes the versioned record in deterministic struct/vector order.
    ///
    /// # Errors
    /// Returns an error when the signature is invalid or the encoded record is
    /// larger than the archive record bound.
    pub fn canonical_bytes(
        &self,
        verifying_key: &VerifyingKey,
    ) -> Result<Vec<u8>, ReliabilityError> {
        self.verify(verifying_key)?;
        chain::encode_record(self)
    }

    /// Decodes bounded bytes and authenticates the complete record.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unknown-field, unsupported,
    /// invalid, or unauthenticated records.
    pub fn decode(bytes: &[u8], verifying_key: &VerifyingKey) -> Result<Self, ReliabilityError> {
        let record = chain::decode_record(bytes)?;
        record.verify(verifying_key)?;
        Ok(record)
    }
}

fn parse_key_id(key_id: &str) -> Result<KeyId, ReliabilityError> {
    KeyId::new(key_id.to_owned()).map_err(|error| ReliabilityError::Merkle(error.to_string()))
}

fn sign_record(
    event: HubRefBatchArchiveEvent,
    commitment: &crate::ReliabilityMerkleCommit,
    signing_key: &SigningKey,
    key_id: KeyId,
) -> Result<HubRefBatchArchiveRecordV1, ReliabilityError> {
    Ok(HubRefBatchArchiveRecordV1 {
        schema_version: HUB_REF_ARCHIVE_SCHEMA,
        event,
        signed_commit: commitment.sign(signing_key, key_id)?,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use ed25519_dalek::VerifyingKey;

    fn snapshots() -> (HubRefBatchSnapshot, HubRefBatchSnapshot) {
        let before = HubRefBatchSnapshot::new(
            "deploy-a",
            "shardline-hub",
            "org",
            "org/repo",
            1,
            "tx-1",
            vec![
                HubRefBatchHead {
                    ref_name: "main".into(),
                    head_sha: Some("old-main".into()),
                },
                HubRefBatchHead {
                    ref_name: "refs/tags/v1".into(),
                    head_sha: None,
                },
            ],
        )
        .unwrap();
        let after = HubRefBatchSnapshot::new(
            "deploy-a",
            "shardline-hub",
            "org",
            "org/repo",
            1,
            "tx-1",
            vec![
                HubRefBatchHead {
                    ref_name: "main".into(),
                    head_sha: Some("new-main".into()),
                },
                HubRefBatchHead {
                    ref_name: "refs/tags/v1".into(),
                    head_sha: Some("tag-1".into()),
                },
            ],
        )
        .unwrap();
        (before, after)
    }

    #[test]
    fn signed_record_binds_all_refs_and_domain_order() {
        let (before, after) = snapshots();
        let event = SnapshotEvidenceEvent::new(1, before, after).unwrap();
        let signing_key = SigningKey::from_bytes(&[23; 32]);
        let key_id = KeyId::new("test-recovery-key".into()).unwrap();
        let record = HubRefBatchArchiveRecordV1::sign(event, &signing_key, key_id).unwrap();
        let verifying_key = VerifyingKey::from(&signing_key);
        let bytes = record.canonical_bytes(&verifying_key).unwrap();
        let restored = HubRefBatchArchiveRecordV1::decode(&bytes, &verifying_key).unwrap();
        assert_eq!(record, restored);

        let mut corrupted = record;
        corrupted
            .event
            .after
            .refs
            .first_mut()
            .expect("the test fixture has a ref")
            .head_sha = Some("forged-head".into());
        assert!(corrupted.verify(&verifying_key).is_err());
    }

    #[test]
    fn batch_requires_sorted_unique_refs_and_non_noop_transition() {
        let unsorted = HubRefBatchSnapshot::new(
            "deploy-a",
            "hub",
            "org",
            "org/repo",
            1,
            "tx",
            vec![
                HubRefBatchHead {
                    ref_name: "z".into(),
                    head_sha: None,
                },
                HubRefBatchHead {
                    ref_name: "a".into(),
                    head_sha: None,
                },
            ],
        );
        assert!(unsorted.is_err());
        let (before, after) = snapshots();
        assert!(before.validate_evidence_transition(&after).is_ok());
        assert!(after.validate_evidence_transition(&after).is_err());
    }

    #[test]
    fn manifest_reference_is_revision_domain_and_locator_bound() {
        let digest = "a".repeat(64);
        let valid_locator = || {
            HubRefBatchManifestReferenceV1::new(
                "deploy-a".into(),
                "org/repo".into(),
                "b".repeat(40),
                digest.clone(),
                format!(
                    "recovery/v1/{}/{}/versions/aa/{digest}",
                    "c".repeat(64),
                    "d".repeat(64)
                ),
            )
            .unwrap()
        };
        let valid = valid_locator();
        assert!(
            HubRefBatchManifestReferenceV1::new(
                "deploy-a".into(),
                "org/repo".into(),
                "not-a-revision".into(),
                digest.clone(),
                valid.object_key.clone(),
            )
            .is_err()
        );
        assert!(
            HubRefBatchManifestReferenceV1::new(
                "deploy-a".into(),
                "org/repo".into(),
                "b".repeat(40),
                digest.clone(),
                format!("recovery/v1/../../versions/aa/{digest}"),
            )
            .is_err()
        );

        let refs = vec![HubRefBatchHead {
            ref_name: "main".into(),
            head_sha: Some("b".repeat(40)),
        }];
        assert!(
            HubRefBatchSnapshot::new_with_manifest_refs(
                "deploy-a",
                "shardline-hub",
                "deploy-a",
                "org/repo",
                1,
                "tx",
                refs.clone(),
                vec![valid.clone()],
            )
            .is_ok()
        );
        let wrong_domain = HubRefBatchManifestReferenceV1::new(
            "deploy-b".into(),
            "org/repo".into(),
            "b".repeat(40),
            digest.clone(),
            valid.object_key.clone(),
        )
        .unwrap();
        assert!(
            HubRefBatchSnapshot::new_with_manifest_refs(
                "deploy-a",
                "shardline-hub",
                "deploy-a",
                "org/repo",
                1,
                "tx",
                refs.clone(),
                vec![wrong_domain],
            )
            .is_err()
        );
        let wrong_revision = HubRefBatchManifestReferenceV1::new(
            "deploy-a".into(),
            "org/repo".into(),
            "e".repeat(40),
            digest,
            valid.object_key,
        )
        .unwrap();
        assert!(
            HubRefBatchSnapshot::new_with_manifest_refs(
                "deploy-a",
                "shardline-hub",
                "deploy-a",
                "org/repo",
                1,
                "tx",
                refs,
                vec![wrong_revision],
            )
            .is_err()
        );
    }
}
