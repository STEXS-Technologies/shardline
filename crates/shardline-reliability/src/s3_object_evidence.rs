use serde::{Deserialize, Serialize};

use crate::snapshot_event::{SnapshotEvidence, SnapshotEvidenceEvent, verify_snapshot_chain};
use crate::snapshot_log::SnapshotEvidenceLog;
use crate::{OperationIdentity, OperationKind, ReliabilityError};

/// Stable typed identity for one S3 listing-index object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3ObjectOperationId(String);

impl S3ObjectOperationId {
    fn new(scope_namespace: &str, object_key: &str) -> Self {
        [scope_namespace, object_key]
            .into_iter()
            .map(|value| format!("{}:{value}", value.len()))
            .collect::<Vec<_>>()
            .join("")
            .into()
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for S3ObjectOperationId {
    fn from(value: String) -> Self {
        Self(value)
    }
}

/// Integrity-checkable materialized state for one S3 object index row.
///
/// `None` in `entry` is the durable absent state. Keeping the full row in the
/// snapshot makes CAS and read verification cover every user-visible field,
/// not only the content pointer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3ObjectSnapshot {
    pub scope_namespace: String,
    pub object_key: String,
    pub entry: Option<S3ObjectState>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3ObjectState {
    pub file_id: String,
    pub size_bytes: u64,
    pub content_hash: String,
    pub etag: String,
    pub user_metadata: Vec<(String, String)>,
    pub updated_at_unix_seconds: i64,
}

impl S3ObjectSnapshot {
    pub fn new(
        scope_namespace: impl Into<String>,
        object_key: impl Into<String>,
        entry: Option<S3ObjectState>,
    ) -> Result<Self, ReliabilityError> {
        let snapshot = Self {
            scope_namespace: scope_namespace.into(),
            object_key: object_key.into(),
            entry,
        };
        for (field, value) in [
            ("scope_namespace", snapshot.scope_namespace.as_str()),
            ("object_key", snapshot.object_key.as_str()),
        ] {
            if value.is_empty() {
                return Err(ReliabilityError::EmptyField(field));
            }
        }
        Ok(snapshot)
    }

    fn operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        Ok(OperationIdentity::new(
            self.scope_namespace.clone(),
            "s3",
            S3ObjectOperationId::new(&self.scope_namespace, &self.object_key).0,
            OperationKind::S3Object,
        )?
        .with_object_key(self.object_key.clone()))
    }
}

impl SnapshotEvidence for S3ObjectSnapshot {
    fn evidence_operation(&self) -> Result<OperationIdentity, ReliabilityError> {
        self.operation()
    }

    fn validate_evidence_transition(&self, after: &Self) -> Result<(), ReliabilityError> {
        if self.scope_namespace != after.scope_namespace || self.object_key != after.object_key {
            return Err(ReliabilityError::OperationMismatch);
        }
        Ok(())
    }
}

pub type S3ObjectLifecycleEvent = SnapshotEvidenceEvent<S3ObjectSnapshot>;
pub type S3ObjectEvidenceLog = SnapshotEvidenceLog<S3ObjectSnapshot>;

pub fn verify_s3_object_events(
    events: &[S3ObjectLifecycleEvent],
    expected: &S3ObjectSnapshot,
) -> Result<(), ReliabilityError> {
    verify_snapshot_chain(events)?;
    if events.last().is_some_and(|event| event.after == *expected) {
        Ok(())
    } else {
        Err(ReliabilityError::StateMismatch)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn object_replacement_and_absence_are_authenticated() {
        let absent = S3ObjectSnapshot::new("tenant", "model.bin", None).unwrap();
        let present = S3ObjectSnapshot::new(
            "tenant",
            "model.bin",
            Some(S3ObjectState {
                file_id: "file-a".into(),
                size_bytes: 7,
                content_hash: "hash-a".into(),
                etag: "etag-a".into(),
                user_metadata: vec![("kind".into(), "model".into())],
                updated_at_unix_seconds: 1,
            }),
        )
        .unwrap();
        let mut log = S3ObjectEvidenceLog::baseline(absent).unwrap();
        log.record(present).unwrap();
        log.record(S3ObjectSnapshot::new("tenant", "model.bin", None).unwrap())
            .unwrap();
        verify_s3_object_events(
            log.events(),
            &S3ObjectSnapshot::new("tenant", "model.bin", None).unwrap(),
        )
        .unwrap();
    }
}
