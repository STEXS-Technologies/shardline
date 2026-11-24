//! Signed, state-only snapshots of a Hub repository's current ref state.
//!
//! This format records one materialized state and its archive watermark. It
//! does not fabricate a before/after ref transition and does not prove that a
//! signed checkpoint is the freshest available checkpoint.

use ed25519_dalek::{SigningKey, VerifyingKey};
use serde::{Deserialize, Serialize};
use statechronicle_domain_compat::{commit::Commit, intent::KeyId, signed::Signed};

use crate::{
    EvidenceEventMetadata, HubRefBatchHead, OperationIdentity, OperationKind, ReliabilityError,
    build_reliability_merkle_commit,
};

const HUB_REF_STATE_CHECKPOINT_SCHEMA: u16 = 1;
/// Maximum sorted current refs accepted in a Hub state checkpoint source.
pub const MAX_HUB_REF_STATE_CHECKPOINT_REFS: usize = 100_000;
const MAX_CHECKPOINT_RECORD_BYTES: usize = crate::MAX_HUB_REF_ARCHIVE_RECORD_BYTES;
const CHECKPOINT_OPERATION_ID: &str = "hub-state-checkpoint-v1";
const CHECKPOINT_OBJECT_KEY: &str = "repository-state";

/// Complete ref state and the SQL/archive watermarks observed in one snapshot.
///
/// A signed instance requires `committed_sequence == archived_sequence`; the
/// distinct values remain in the encoded state so verification binds the exact
/// database read rather than a derived value. `last_archived_receipt_digest`
/// pins the receipt at that watermark, but a caller must retain the checkpoint
/// digest or watermark outside the archive to detect replacement by older data.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefStateCheckpointV1 {
    pub deployment_id: String,
    pub repository_id: String,
    pub refs: Vec<HubRefBatchHead>,
    pub committed_sequence: u64,
    pub archived_sequence: u64,
    pub last_archived_receipt_digest: Option<String>,
}

impl HubRefStateCheckpointV1 {
    /// Validates SQL source fields before deciding whether their archive
    /// watermark is complete enough to sign. Unequal watermarks are permitted
    /// here so adapters can report pending work without duplicating the record
    /// format's identity, order, size, and digest rules.
    ///
    /// # Errors
    /// Returns an error when identities, refs, sequence ordering, or digest
    /// pairing violate the checkpoint format.
    pub fn validate_source_shape(
        deployment_id: Option<&str>,
        repository_id: &str,
        refs: &[HubRefBatchHead],
        committed_sequence: u64,
        archived_sequence: u64,
        last_archived_receipt_digest: Option<&str>,
    ) -> Result<(), ReliabilityError> {
        if repository_id.is_empty()
            || repository_id.len() > 1024
            || repository_id.chars().any(char::is_control)
            || deployment_id.is_some_and(|deployment| {
                deployment.is_empty()
                    || deployment.len() > 256
                    || deployment.chars().any(char::is_control)
            })
        {
            return Err(ReliabilityError::EmptyField("checkpoint identity"));
        }
        if archived_sequence > committed_sequence {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        match (archived_sequence, last_archived_receipt_digest) {
            (0, None) => {}
            (0, Some(_)) | (_, None) => return Err(ReliabilityError::OperationMismatch),
            (_, Some(digest))
                if digest.len() == 64
                    && digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)) => {}
            (_, Some(_)) => return Err(ReliabilityError::OperationMismatch),
        }
        if refs.len() > MAX_HUB_REF_STATE_CHECKPOINT_REFS {
            return Err(ReliabilityError::OperationMismatch);
        }
        let mut previous: Option<&str> = None;
        for head in refs {
            if head.ref_name.is_empty()
                || head.ref_name.len() > 1024
                || head.ref_name.chars().any(char::is_control)
                || head.head_sha.as_ref().is_some_and(|sha| {
                    sha.is_empty() || sha.len() > 1024 || sha.chars().any(char::is_control)
                })
                || previous.is_some_and(|last| last >= head.ref_name.as_str())
            {
                return Err(ReliabilityError::OperationMismatch);
            }
            previous = Some(&head.ref_name);
        }
        Ok(())
    }

    /// Constructs a validated materialized-state checkpoint.
    ///
    /// # Errors
    /// Rejects unequal committed/archive watermarks, invalid digest/watermark
    /// combinations, invalid identities, and malformed or unsorted refs.
    pub fn new(
        deployment_id: impl Into<String>,
        repository_id: impl Into<String>,
        refs: Vec<HubRefBatchHead>,
        committed_sequence: u64,
        archived_sequence: u64,
        last_archived_receipt_digest: Option<String>,
    ) -> Result<Self, ReliabilityError> {
        let state = Self {
            deployment_id: deployment_id.into(),
            repository_id: repository_id.into(),
            refs,
            committed_sequence,
            archived_sequence,
            last_archived_receipt_digest,
        };
        state.validate()?;
        Ok(state)
    }

    fn validate(&self) -> Result<(), ReliabilityError> {
        Self::validate_source_shape(
            Some(&self.deployment_id),
            &self.repository_id,
            &self.refs,
            self.committed_sequence,
            self.archived_sequence,
            self.last_archived_receipt_digest.as_deref(),
        )?;
        if self.committed_sequence != self.archived_sequence {
            return Err(ReliabilityError::ChainDiscontinuity);
        }
        Ok(())
    }

    fn evidence(&self) -> Result<HubRefStateCheckpointEvidenceV1, ReliabilityError> {
        Ok(HubRefStateCheckpointEvidenceV1 {
            operation: OperationIdentity::new(
                self.deployment_id.clone(),
                self.repository_id.clone(),
                CHECKPOINT_OPERATION_ID,
                OperationKind::MetadataCommit,
            )?
            .with_object_key(CHECKPOINT_OBJECT_KEY),
            state: self.clone(),
        })
    }
}

/// Authenticated wrapper for a state-only Hub repository checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HubRefStateCheckpointRecordV1 {
    pub schema_version: u16,
    pub state: HubRefStateCheckpointV1,
    pub signed_commit: Signed<Commit>,
}

impl HubRefStateCheckpointRecordV1 {
    /// Signs a state-only checkpoint with the configured deployment key.
    ///
    /// # Errors
    /// Returns an error for invalid state or StateChronicle signing failures.
    pub fn sign(
        state: HubRefStateCheckpointV1,
        signing_key: &SigningKey,
        key_id: KeyId,
    ) -> Result<Self, ReliabilityError> {
        state.validate()?;
        let evidence = state.evidence()?;
        let commitment = build_reliability_merkle_commit(&evidence)?;
        Ok(Self {
            schema_version: HUB_REF_STATE_CHECKPOINT_SCHEMA,
            state,
            signed_commit: commitment.sign(signing_key, key_id)?,
        })
    }

    /// Signs a checkpoint after parsing StateChronicle's key ID format.
    ///
    /// # Errors
    /// Returns an error for an invalid key ID, state, or signing failure.
    pub fn sign_with_key_id(
        state: HubRefStateCheckpointV1,
        signing_key: &SigningKey,
        key_id: &str,
    ) -> Result<Self, ReliabilityError> {
        let key_id = KeyId::new(key_id.to_owned())
            .map_err(|error| ReliabilityError::Merkle(error.to_string()))?;
        Self::sign(state, signing_key, key_id)
    }

    /// Verifies state invariants, exact StateChronicle body binding, and signature.
    ///
    /// This authenticates the provided checkpoint only. It does not establish
    /// that the record is the latest checkpoint or that the receipt chain below
    /// its watermark is complete.
    ///
    /// # Errors
    /// Returns an error for unsupported schema, invalid state, body mismatch,
    /// or signature failure.
    pub fn verify(&self, verifying_key: &VerifyingKey) -> Result<(), ReliabilityError> {
        if self.schema_version != HUB_REF_STATE_CHECKPOINT_SCHEMA {
            return Err(ReliabilityError::Merkle(
                "unsupported Hub state checkpoint version".into(),
            ));
        }
        self.state.validate()?;
        let evidence = self.state.evidence()?;
        let expected = build_reliability_merkle_commit(&evidence)?;
        if self.signed_commit.body != expected.body {
            return Err(ReliabilityError::Merkle(
                "signed Hub state checkpoint does not match its state".into(),
            ));
        }
        crate::compatibility::verify_commit_signature(&self.signed_commit, verifying_key)
    }

    /// Verifies the record and binds it to the caller's exact materialized state.
    ///
    /// # Errors
    /// Returns an error when signature verification fails or the expected state
    /// differs from the signed checkpoint.
    pub fn verify_for(
        &self,
        expected: &HubRefStateCheckpointV1,
        verifying_key: &VerifyingKey,
    ) -> Result<(), ReliabilityError> {
        self.verify(verifying_key)?;
        expected.validate()?;
        if &self.state == expected {
            Ok(())
        } else {
            Err(ReliabilityError::StateMismatch)
        }
    }

    /// Serializes a verified checkpoint within the archive record bound.
    ///
    /// # Errors
    /// Returns an error for invalid signature/evidence, serialization failure,
    /// or an oversized record.
    pub fn canonical_bytes(
        &self,
        verifying_key: &VerifyingKey,
    ) -> Result<Vec<u8>, ReliabilityError> {
        self.verify(verifying_key)?;
        let bytes = serde_json::to_vec(self)?;
        if bytes.len() > MAX_CHECKPOINT_RECORD_BYTES {
            return Err(ReliabilityError::Merkle(
                "Hub state checkpoint exceeds byte limit".into(),
            ));
        }
        Ok(bytes)
    }

    /// Decodes bounded bytes and verifies the signed checkpoint.
    ///
    /// # Errors
    /// Returns an error for oversized, malformed, unsupported, invalid, or
    /// unauthenticated records.
    pub fn decode(bytes: &[u8], verifying_key: &VerifyingKey) -> Result<Self, ReliabilityError> {
        if bytes.len() > MAX_CHECKPOINT_RECORD_BYTES {
            return Err(ReliabilityError::Merkle(
                "Hub state checkpoint exceeds byte limit".into(),
            ));
        }
        let record: Self = serde_json::from_slice(bytes)?;
        record.verify(verifying_key)?;
        Ok(record)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct HubRefStateCheckpointEvidenceV1 {
    operation: OperationIdentity,
    state: HubRefStateCheckpointV1,
}

impl EvidenceEventMetadata for HubRefStateCheckpointEvidenceV1 {
    fn operation_identity(&self) -> &OperationIdentity {
        &self.operation
    }

    fn sequence_number(&self) -> u64 {
        self.state.archived_sequence
    }

    fn verify_integrity(&self) -> Result<(), ReliabilityError> {
        self.state.validate()?;
        if self.state.evidence()?.operation == self.operation {
            Ok(())
        } else {
            Err(ReliabilityError::OperationMismatch)
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn checkpoint() -> HubRefStateCheckpointV1 {
        HubRefStateCheckpointV1::new(
            "deployment-a",
            "org/repo",
            vec![
                HubRefBatchHead {
                    ref_name: "main".into(),
                    head_sha: Some("revision-main".into()),
                },
                HubRefBatchHead {
                    ref_name: "refs/tags/v1".into(),
                    head_sha: Some("revision-tag".into()),
                },
            ],
            2,
            2,
            Some("a".repeat(64)),
        )
        .unwrap()
    }

    #[test]
    fn source_shape_allows_pending_archive_without_relaxing_signed_checkpoint() {
        let state = checkpoint();
        HubRefStateCheckpointV1::validate_source_shape(
            Some(&state.deployment_id),
            &state.repository_id,
            &state.refs,
            3,
            2,
            state.last_archived_receipt_digest.as_deref(),
        )
        .unwrap();
        assert!(matches!(
            HubRefStateCheckpointV1::new(
                state.deployment_id.clone(),
                state.repository_id.clone(),
                state.refs.clone(),
                3,
                2,
                state.last_archived_receipt_digest,
            ),
            Err(ReliabilityError::ChainDiscontinuity)
        ));
    }

    #[test]
    fn signed_state_checkpoint_round_trips_and_binds_the_exact_source() {
        let state = checkpoint();
        let signing_key = SigningKey::from_bytes(&[61; 32]);
        let verifying_key = VerifyingKey::from(&signing_key);
        let record = HubRefStateCheckpointRecordV1::sign_with_key_id(
            state.clone(),
            &signing_key,
            "hub-state-test-key",
        )
        .unwrap();
        let bytes = record.canonical_bytes(&verifying_key).unwrap();
        let decoded = HubRefStateCheckpointRecordV1::decode(&bytes, &verifying_key).unwrap();
        assert_eq!(decoded, record);
        decoded.verify_for(&state, &verifying_key).unwrap();

        let mut stale = state;
        stale.archived_sequence = 1;
        stale.committed_sequence = 1;
        stale.last_archived_receipt_digest = Some("b".repeat(64));
        assert!(matches!(
            decoded.verify_for(&stale, &verifying_key),
            Err(ReliabilityError::StateMismatch)
        ));
    }

    #[test]
    fn empty_repository_checkpoint_at_genesis_is_valid_without_a_fake_transition() {
        let state =
            HubRefStateCheckpointV1::new("deployment-a", "org/empty", vec![], 0, 0, None).unwrap();
        let signing_key = SigningKey::from_bytes(&[62; 32]);
        let record = HubRefStateCheckpointRecordV1::sign_with_key_id(
            state.clone(),
            &signing_key,
            "hub-state-test-key",
        )
        .unwrap();
        record
            .verify_for(&state, &VerifyingKey::from(&signing_key))
            .unwrap();
    }

    #[test]
    fn checkpoint_rejects_watermark_digest_and_order_mismatches() {
        let good = checkpoint();
        assert!(
            HubRefStateCheckpointV1::new(
                "deployment-a",
                "org/repo",
                good.refs.clone(),
                3,
                2,
                Some("a".repeat(64)),
            )
            .is_err()
        );
        assert!(
            HubRefStateCheckpointV1::new(
                "deployment-a",
                "org/repo",
                good.refs.clone(),
                0,
                0,
                Some("a".repeat(64)),
            )
            .is_err()
        );
        assert!(
            HubRefStateCheckpointV1::new(
                "deployment-a",
                "org/repo",
                good.refs.clone(),
                1,
                1,
                Some("A".repeat(64)),
            )
            .is_err()
        );
        let mut unsorted = good.refs;
        unsorted.reverse();
        assert!(
            HubRefStateCheckpointV1::new(
                "deployment-a",
                "org/repo",
                unsorted,
                1,
                1,
                Some("a".repeat(64)),
            )
            .is_err()
        );
    }

    #[test]
    fn verification_rejects_tampered_domain_or_state() {
        let signing_key = SigningKey::from_bytes(&[63; 32]);
        let verifying_key = VerifyingKey::from(&signing_key);
        let mut domain_tampered = HubRefStateCheckpointRecordV1::sign_with_key_id(
            checkpoint(),
            &signing_key,
            "hub-state-test-key",
        )
        .unwrap();
        domain_tampered.state.deployment_id = "deployment-b".into();
        assert!(domain_tampered.verify(&verifying_key).is_err());

        let mut state_tampered = HubRefStateCheckpointRecordV1::sign_with_key_id(
            checkpoint(),
            &signing_key,
            "hub-state-test-key",
        )
        .unwrap();
        state_tampered
            .state
            .refs
            .first_mut()
            .expect("the test fixture has a ref")
            .head_sha = Some("forged-revision".into());
        assert!(state_tampered.verify(&verifying_key).is_err());
        assert!(
            state_tampered
                .verify(&VerifyingKey::from(&SigningKey::from_bytes(&[64; 32])))
                .is_err()
        );
    }
}
