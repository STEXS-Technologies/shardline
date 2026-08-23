use std::{num::NonZeroU64, time::Duration};

use thiserror::Error;

/// Protocol owning a durable resumable upload session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResumableSessionProtocol {
    /// Git LFS chunked PATCH upload.
    LfsPatch,
    /// OCI blob upload.
    OciBlob,
    /// S3 multipart upload.
    S3Multipart,
}

impl ResumableSessionProtocol {
    /// Stable database representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LfsPatch => "lfs_patch",
            Self::OciBlob => "oci_blob",
            Self::S3Multipart => "s3_multipart",
        }
    }

    /// Parses the stable database representation.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "lfs_patch" => Some(Self::LfsPatch),
            "oci_blob" => Some(Self::OciBlob),
            "s3_multipart" => Some(Self::S3Multipart),
            _ => None,
        }
    }
}

/// Durable lifecycle of a resumable upload session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ResumableSessionState {
    /// Parts or ranges may be published.
    Active,
    /// The part/range map is pinned and publication is in progress.
    Completing,
    /// Final object publication completed.
    Completed,
    /// A client or operator aborted the session.
    Aborted,
    /// The database-clock TTL expired.
    Expired,
}

impl ResumableSessionState {
    /// Stable database representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completing => "completing",
            Self::Completed => "completed",
            Self::Aborted => "aborted",
            Self::Expired => "expired",
        }
    }

    /// Parses the stable database representation.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "completing" => Some(Self::Completing),
            "completed" => Some(Self::Completed),
            "aborted" => Some(Self::Aborted),
            "expired" => Some(Self::Expired),
            _ => None,
        }
    }

    /// Returns whether a compare-and-set lifecycle transition is legal.
    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Active,
                Self::Active | Self::Completing | Self::Aborted | Self::Expired
            ) | (
                Self::Completing,
                Self::Completing | Self::Active | Self::Completed
            ) | (Self::Completed, Self::Completed)
                | (Self::Aborted, Self::Aborted)
                | (Self::Expired, Self::Expired)
        )
    }

    /// True after no new part or range may become authoritative.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted | Self::Expired)
    }
}

/// Immutable staged object selected by a session's authoritative part map.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumableSessionPart {
    part_number: NonZeroU64,
    generation: NonZeroU64,
    staging_key: String,
    size_bytes: u64,
    etag: Option<String>,
}

impl ResumableSessionPart {
    /// Creates a validated staged part descriptor.
    #[must_use]
    pub const fn new(
        part_number: NonZeroU64,
        generation: NonZeroU64,
        staging_key: String,
        size_bytes: u64,
        etag: Option<String>,
    ) -> Self {
        Self {
            part_number,
            generation,
            staging_key,
            size_bytes,
            etag,
        }
    }

    #[must_use]
    pub const fn part_number(&self) -> NonZeroU64 {
        self.part_number
    }
    #[must_use]
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    #[must_use]
    pub fn staging_key(&self) -> &str {
        &self.staging_key
    }
    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.size_bytes
    }
    #[must_use]
    pub fn etag(&self) -> Option<&str> {
        self.etag.as_deref()
    }
}

/// Durable metadata for one resumable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumableSession {
    session_id: String,
    protocol: ResumableSessionProtocol,
    scope_namespace: String,
    target_key: String,
    state: ResumableSessionState,
    generation: NonZeroU64,
    fence_epoch: NonZeroU64,
    expires_at: Duration,
}

impl ResumableSession {
    /// Creates an active generation-one session.
    #[must_use]
    pub const fn new(
        session_id: String,
        protocol: ResumableSessionProtocol,
        scope_namespace: String,
        target_key: String,
        expires_at: Duration,
    ) -> Self {
        Self {
            session_id,
            protocol,
            scope_namespace,
            target_key,
            state: ResumableSessionState::Active,
            generation: NonZeroU64::MIN,
            fence_epoch: NonZeroU64::MIN,
            expires_at,
        }
    }

    #[must_use]
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    #[must_use]
    pub const fn protocol(&self) -> ResumableSessionProtocol {
        self.protocol
    }
    #[must_use]
    pub fn scope_namespace(&self) -> &str {
        &self.scope_namespace
    }
    #[must_use]
    pub fn target_key(&self) -> &str {
        &self.target_key
    }
    #[must_use]
    pub const fn state(&self) -> ResumableSessionState {
        self.state
    }
    #[must_use]
    pub const fn generation(&self) -> NonZeroU64 {
        self.generation
    }
    #[must_use]
    pub const fn fence_epoch(&self) -> NonZeroU64 {
        self.fence_epoch
    }
    #[must_use]
    pub const fn expires_at(&self) -> Duration {
        self.expires_at
    }

    /// Reconstructs a validated value from a persistence adapter.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_parts(
        session_id: String,
        protocol: ResumableSessionProtocol,
        scope_namespace: String,
        target_key: String,
        state: ResumableSessionState,
        generation: u64,
        fence_epoch: u64,
        expires_at: Duration,
    ) -> Result<Self, ResumableSessionError> {
        Ok(Self {
            session_id,
            protocol,
            scope_namespace,
            target_key,
            state,
            generation: NonZeroU64::new(generation).ok_or(ResumableSessionError::ZeroGeneration)?,
            fence_epoch: NonZeroU64::new(fence_epoch)
                .ok_or(ResumableSessionError::ZeroFenceEpoch)?,
            expires_at,
        })
    }
}

/// Invalid durable session data.
#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ResumableSessionError {
    #[error("unknown resumable-session protocol: {0}")]
    UnknownProtocol(String),
    #[error("unknown resumable-session state: {0}")]
    UnknownState(String),
    #[error("resumable-session generation must be positive")]
    ZeroGeneration,
    #[error("resumable-session fence epoch must be positive")]
    ZeroFenceEpoch,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const STATES: [ResumableSessionState; 5] = [
        ResumableSessionState::Active,
        ResumableSessionState::Completing,
        ResumableSessionState::Completed,
        ResumableSessionState::Aborted,
        ResumableSessionState::Expired,
    ];

    #[test]
    fn protocol_database_round_trip() {
        for protocol in [
            ResumableSessionProtocol::LfsPatch,
            ResumableSessionProtocol::OciBlob,
            ResumableSessionProtocol::S3Multipart,
        ] {
            assert_eq!(
                ResumableSessionProtocol::parse(protocol.as_str()),
                Some(protocol)
            );
        }
    }

    proptest! {
        #[test]
        fn terminal_states_never_transition(index in 0_usize..STATES.len()) {
            let state = *STATES.get(index).unwrap_or(&ResumableSessionState::Active);
            if state.is_terminal() {
                for candidate in STATES {
                    prop_assert_eq!(state.can_transition_to(candidate), state == candidate);
                }
            }
        }
    }
}
