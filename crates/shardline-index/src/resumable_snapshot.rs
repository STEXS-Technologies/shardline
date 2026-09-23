use serde::Serialize;
use shardline_reliability::{
    ReliabilityError, ResumableLifecycleState, StateChronicleDigest, canonical_state_digest,
};

use crate::{ResumableSession, ResumableSessionPart};

/// Canonical materialized state used to integrity-check a resumable session row
/// together with its authoritative part map.
#[derive(Debug, Serialize)]
struct ResumableSessionStateSnapshotV1 {
    session_id: String,
    protocol: &'static str,
    scope_namespace: String,
    target_key: String,
    attributes_json: String,
    state: ResumableLifecycleState,
    generation: u64,
    fence_epoch: u64,
    expires_at_unix_seconds: u64,
    parts: Vec<ResumablePartStateSnapshotV1>,
}

#[derive(Debug, Serialize)]
struct ResumablePartStateSnapshotV1 {
    part_number: u64,
    generation: u64,
    staging_key: String,
    size_bytes: u64,
    etag: Option<String>,
    range_start: Option<u64>,
    range_end_exclusive: Option<u64>,
}

/// Computes the canonical StateChronicle digest for the complete materialized
/// resumable-session state, including all persisted part metadata.
///
/// # Errors
///
/// Returns a reliability error if canonical serialization of the state fails.
pub fn resumable_state_digest(
    session: &ResumableSession,
    parts: &[ResumableSessionPart],
) -> Result<StateChronicleDigest, ReliabilityError> {
    let mut canonical_parts: Vec<_> = parts
        .iter()
        .map(|part| {
            let range = part.range();
            ResumablePartStateSnapshotV1 {
                part_number: part.part_number().get(),
                generation: part.generation().get(),
                staging_key: part.staging_key().to_owned(),
                size_bytes: part.size_bytes(),
                etag: part.etag().map(str::to_owned),
                range_start: range.map(|value| value.start()),
                range_end_exclusive: range.map(|value| value.end_exclusive()),
            }
        })
        .collect();
    canonical_parts.sort_by(|left, right| {
        left.part_number
            .cmp(&right.part_number)
            .then_with(|| left.generation.cmp(&right.generation))
            .then_with(|| left.staging_key.cmp(&right.staging_key))
            .then_with(|| left.size_bytes.cmp(&right.size_bytes))
            .then_with(|| left.etag.cmp(&right.etag))
            .then_with(|| left.range_start.cmp(&right.range_start))
            .then_with(|| left.range_end_exclusive.cmp(&right.range_end_exclusive))
    });

    let snapshot = ResumableSessionStateSnapshotV1 {
        session_id: session.session_id().to_owned(),
        protocol: session.protocol().as_str(),
        scope_namespace: session.scope_namespace().to_owned(),
        target_key: session.target_key().to_owned(),
        attributes_json: session.attributes_json().to_owned(),
        state: session.state(),
        generation: session.generation().get(),
        fence_epoch: session.fence_epoch().get(),
        expires_at_unix_seconds: session.expires_at().as_secs(),
        parts: canonical_parts,
    };
    canonical_state_digest(&snapshot)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::num::NonZeroU64;

    use super::*;

    fn session() -> ResumableSession {
        ResumableSession::new(
            "session-1".to_owned(),
            crate::ResumableSessionProtocol::LfsPatch,
            "scope".to_owned(),
            "target".to_owned(),
            std::time::Duration::from_secs(100),
        )
    }

    fn part(staging_key: &str) -> ResumableSessionPart {
        ResumableSessionPart::new(
            NonZeroU64::MIN,
            NonZeroU64::MIN,
            staging_key.to_owned(),
            10,
            Some("etag".to_owned()),
        )
    }

    #[test]
    fn complete_materialized_state_digest_changes_with_part_metadata() {
        let session = session();
        let first = resumable_state_digest(&session, &[part("staging/one")])
            .expect("valid snapshot serializes");
        let second = resumable_state_digest(&session, &[part("staging/two")])
            .expect("valid snapshot serializes");
        assert_ne!(first, second);
    }

    #[test]
    fn complete_materialized_state_digest_is_deterministic() {
        let session = session();
        let parts = vec![part("staging/one")];
        assert_eq!(
            resumable_state_digest(&session, &parts).expect("valid snapshot serializes"),
            resumable_state_digest(&session, &parts).expect("valid snapshot serializes")
        );
    }

    #[test]
    fn complete_materialized_state_digest_is_independent_of_part_order() {
        let session = session();
        let first = ResumableSessionPart::new(
            NonZeroU64::new(1).expect("non-zero part number"),
            NonZeroU64::MIN,
            "staging/one".to_owned(),
            10,
            Some("etag-one".to_owned()),
        );
        let second = ResumableSessionPart::new(
            NonZeroU64::new(2).expect("non-zero part number"),
            NonZeroU64::MIN,
            "staging/two".to_owned(),
            20,
            Some("etag-two".to_owned()),
        );
        let ordered = resumable_state_digest(&session, &[first.clone(), second.clone()])
            .expect("valid snapshot serializes");
        let reversed =
            resumable_state_digest(&session, &[second, first]).expect("valid snapshot serializes");
        assert_eq!(ordered, reversed);
    }

    #[test]
    fn malformed_duplicate_parts_are_still_canonically_ordered() {
        let session = session();
        let first = part("staging/one");
        let second = part("staging/two");
        let ordered = resumable_state_digest(&session, &[first.clone(), second.clone()])
            .expect("valid snapshot serializes");
        let reversed =
            resumable_state_digest(&session, &[second, first]).expect("valid snapshot serializes");
        assert_eq!(ordered, reversed);
    }
}
