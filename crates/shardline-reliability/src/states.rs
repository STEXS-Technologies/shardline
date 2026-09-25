use serde::{Deserialize, Serialize};

/// State values accepted by the unified StateChronicle/Penelope transition
/// evidence protocol.
pub trait EvidenceState: Copy + Eq + Serialize {
    fn as_str(self) -> &'static str;

    fn can_transition_to(self, next: Self) -> bool;
}

/// Existing Shardline lifecycle states, represented without changing their
/// public or persisted spelling.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UploadLifecycleState {
    Created,
    Storing,
    Stored,
    MetadataCommitted,
    Visible,
    Failed,
}

impl UploadLifecycleState {
    /// Returns the committed forward-order rank used by idempotent callers.
    ///
    /// `Failed` is terminal and intentionally remains outside the successful
    /// committed chain at rank zero. This is a policy-neutral primitive for
    /// adapters and coordinators; transition validity remains governed by
    /// [`Self::can_transition_to`].
    #[must_use]
    pub const fn committed_rank(self) -> u8 {
        match self {
            Self::Created => 0,
            Self::Storing => 1,
            Self::Stored => 2,
            Self::MetadataCommitted => 3,
            Self::Visible => 4,
            Self::Failed => 0,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Storing => "storing",
            Self::Stored => "stored",
            Self::MetadataCommitted => "metadata_committed",
            Self::Visible => "visible",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "created" => Some(Self::Created),
            "storing" => Some(Self::Storing),
            "stored" => Some(Self::Stored),
            "metadata_committed" => Some(Self::MetadataCommitted),
            "visible" => Some(Self::Visible),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Created, Self::Created | Self::Storing | Self::Failed)
                | (Self::Storing, Self::Storing | Self::Stored | Self::Failed)
                | (
                    Self::Stored,
                    Self::Stored | Self::MetadataCommitted | Self::Failed
                )
                | (
                    Self::MetadataCommitted,
                    Self::MetadataCommitted | Self::Visible | Self::Failed
                )
                | (Self::Visible, Self::Visible)
                | (Self::Failed, Self::Failed)
        )
    }
}

impl EvidenceState for UploadLifecycleState {
    fn as_str(self) -> &'static str {
        Self::as_str(self)
    }

    fn can_transition_to(self, next: Self) -> bool {
        Self::can_transition_to(self, next)
    }
}

/// Durable lifecycle states for LFS, OCI, and S3 resumable sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResumableLifecycleState {
    Active,
    Completing,
    Completed,
    Aborted,
    Expired,
}

impl ResumableLifecycleState {
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

    #[must_use]
    pub const fn can_transition_to(self, next: Self) -> bool {
        matches!(
            (self, next),
            (
                Self::Active,
                Self::Active | Self::Completing | Self::Aborted | Self::Expired
            ) | (
                Self::Completing,
                Self::Completing | Self::Completed | Self::Aborted | Self::Expired | Self::Active
            ) | (Self::Completed, Self::Completed | Self::Active)
                | (Self::Aborted, Self::Aborted | Self::Active)
                | (Self::Expired, Self::Expired | Self::Active)
        )
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Aborted | Self::Expired)
    }
}

impl EvidenceState for ResumableLifecycleState {
    fn as_str(self) -> &'static str {
        Self::as_str(self)
    }

    fn can_transition_to(self, next: Self) -> bool {
        Self::can_transition_to(self, next)
    }
}
