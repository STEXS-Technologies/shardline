//! Signed, atomic Hub reference batch evidence for external recovery archives.

use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use statechronicle_domain_compat::{commit::Commit, intent::KeyId, signed::Signed};

use crate::{
    OperationIdentity, OperationKind, ReliabilityError, SnapshotEvidence, SnapshotEvidenceEvent,
    build_reliability_merkle_commit_with_previous,
};

const HUB_REF_ARCHIVE_SCHEMA: u16 = 1;
const MAX_ARCHIVE_RECORD_BYTES: usize = 8 * 1024 * 1024;
const MAX_REFS_PER_BATCH: usize = 100_000;

/// The resulting head of one ref in a Hub batch transition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefBatchHead {
    pub ref_name: String,
    pub head_sha: Option<String>,
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
        let snapshot = Self {
            deployment_id: deployment_id.into(),
            provider_id: provider_id.into(),
            tenant_id: tenant_id.into(),
            repository_id: repository_id.into(),
            domain_sequence,
            transaction_id: transaction_id.into(),
            refs,
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
        Ok(())
    }

    fn same_transaction(&self, after: &Self) -> bool {
        self.deployment_id == after.deployment_id
            && self.provider_id == after.provider_id
            && self.tenant_id == after.tenant_id
            && self.repository_id == after.repository_id
            && self.domain_sequence == after.domain_sequence
            && self.transaction_id == after.transaction_id
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
        let key_id = KeyId::new(key_id.to_owned())
            .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
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
        event.verify_integrity()?;
        let commitment = build_reliability_merkle_commit_with_previous(&event, None)?;
        Ok(Self {
            schema_version: HUB_REF_ARCHIVE_SCHEMA,
            event,
            signed_commit: commitment.sign(signing_key, key_id)?,
        })
    }

    /// Verifies the event, signed StateChronicle commitment, and exact binding.
    ///
    /// # Errors
    /// Returns an error for unsupported format, invalid event data, a bad
    /// signature, or a signed body that does not match the batch event.
    pub fn verify(&self, verifying_key: &VerifyingKey) -> Result<(), ReliabilityError> {
        if self.schema_version != HUB_REF_ARCHIVE_SCHEMA {
            return Err(ReliabilityError::Merkle(
                "unsupported Hub archive record version".into(),
            ));
        }
        self.event.verify_integrity()?;
        let expected = build_reliability_merkle_commit_with_previous(&self.event, None)?;
        if self.signed_commit.body != expected.body {
            return Err(ReliabilityError::Merkle(
                "signed Hub archive commitment does not match its event".into(),
            ));
        }
        let current_signed: statechronicle::domain::signed::Signed<
            statechronicle::domain::commit::Commit,
        > = crate::compatibility::protocol_value(&self.signed_commit)?;
        let protocol_key =
            statechronicle_ed25519::VerifyingKey::from_bytes(&verifying_key.to_bytes())
                .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
        statechronicle::commit::sign::verify_commit(&current_signed, &protocol_key)
            .map_err(|error| ReliabilityError::Merkle(error.to_string()))
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
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_ARCHIVE_RECORD_BYTES {
            return Err(ReliabilityError::Merkle(
                "Hub archive record exceeds byte limit".into(),
            ));
        }
        Ok(bytes)
    }

    /// Decodes bounded bytes and authenticates the complete record.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unknown-field, unsupported,
    /// invalid, or unauthenticated records.
    pub fn decode(bytes: &[u8], verifying_key: &VerifyingKey) -> Result<Self, ReliabilityError> {
        if bytes.len() > MAX_ARCHIVE_RECORD_BYTES {
            return Err(ReliabilityError::Merkle(
                "Hub archive record exceeds byte limit".into(),
            ));
        }
        let record: Self = serde_json::from_slice(bytes)?;
        record.verify(verifying_key)?;
        Ok(record)
    }
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
}
